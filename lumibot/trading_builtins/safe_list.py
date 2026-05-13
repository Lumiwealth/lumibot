from _thread import RLock as rlock_type


_MISSING = object()


class SafeList:
    def __init__(self, lock, initial=None):
        # PERF: backtesting is single-threaded; allow `lock=None` to skip lock overhead.
        if lock is not None and not isinstance(lock, rlock_type):
            raise ValueError("lock must be a threading.RLock")

        if initial is None:
            initial = []
        self.__lock = lock
        self.__items = list(initial)
        self.revision = 0

    def __repr__(self):
        return repr(self.__items)

    def __bool__(self):
        lock = self.__lock
        if lock is None:
            return bool(self.__items)
        with lock:
            return bool(self.__items)

    def __len__(self):
        lock = self.__lock
        if lock is None:
            return len(self.__items)
        with lock:
            return len(self.__items)

    def __iter__(self):
        lock = self.__lock
        if lock is None:
            return iter(self.__items)
        with lock:
            return iter(self.__items)

    def __contains__(self, val):
        lock = self.__lock
        if lock is None:
            return val in self.__items
        with lock:
            return val in self.__items

    def __getitem__(self, n):
        lock = self.__lock
        if lock is None:
            return self.__items[n]
        with lock:
            return self.__items[n]

    def __setitem__(self, n, val):
        lock = self.__lock
        if lock is None:
            self.__items[n] = val
            self.revision += 1
            return
        with lock:
            self.__items[n] = val
            self.revision += 1

    def __add__(self, val):
        lock = self.__lock
        if lock is None:
            result = SafeList(None)
            result.__items = list(set(self.__items + val.__items))
            return result
        with lock:
            result = SafeList(lock)
            result.__items = list(set(self.__items + val.__items))
            return result

    def append(self, value):
        lock = self.__lock
        if lock is None:
            self.__items.append(value)
            self.revision += 1
            return
        with lock:
            self.__items.append(value)
            self.revision += 1

    def remove(self, value, key=None):
        lock = self.__lock
        if lock is None:
            if key is None:
                self.__items.remove(value)
                self.revision += 1
                return
            if not isinstance(key, str):
                raise ValueError(f"key must be a string, received {key} of type {type(key)}")
            # PERF: key-based removals are heavily used in backtesting order tracking lists
            # (e.g., `key="identifier"`). Avoid rebuilding the full list each time.
            if key == "identifier":
                for idx, item in enumerate(self.__items):
                    item_identifier = getattr(item, "_identifier", _MISSING)
                    if item_identifier is _MISSING:
                        item_identifier = getattr(item, "identifier", _MISSING)
                    if item_identifier == value:
                        del self.__items[idx]
                        self.revision += 1
                        break
                return

            for idx, item in enumerate(self.__items):
                if getattr(item, key) == value:
                    del self.__items[idx]
                    self.revision += 1
                    break
            return

        with lock:
            if key is None:
                self.__items.remove(value)
                self.revision += 1
            else:
                if not isinstance(key, str):
                    raise ValueError(f"key must be a string, received {key} of type {type(key)}")
                # PERF: key-based removals are heavily used in backtesting order tracking lists
                # (e.g., `key=\"identifier\"`). Avoid rebuilding the full list each time.
                if key == "identifier":
                    for idx, item in enumerate(self.__items):
                        item_identifier = getattr(item, "_identifier", _MISSING)
                        if item_identifier is _MISSING:
                            item_identifier = getattr(item, "identifier", _MISSING)
                        if item_identifier == value:
                            del self.__items[idx]
                            self.revision += 1
                            break
                else:
                    for idx, item in enumerate(self.__items):
                        if getattr(item, key) == value:
                            del self.__items[idx]
                            self.revision += 1
                            break

    def extend(self, value):
        lock = self.__lock
        if lock is None:
            value_list = list(value)
            if value_list:
                self.__items.extend(value_list)
                self.revision += 1
            return
        with lock:
            value_list = list(value)
            if value_list:
                self.__items.extend(value_list)
                self.revision += 1

    def get_list(self):
        lock = self.__lock
        if lock is None:
            return self.__items
        with lock:
            return self.__items

    def remove_all(self):
        lock = self.__lock
        if lock is None:
            for item in self.__items:
                self.remove(item)
            return
        with lock:
            for item in self.__items:
                self.remove(item)

    def trim_to_last(self, keep_last: int) -> int:
        """Keep only the last `keep_last` items (drop the oldest).

        This is a performance primitive used by backtesting to avoid O(n log n) sorts and repeated
        `list.remove()` scans when enforcing simple retention policies on append-only event lists.
        """
        keep_last = int(keep_last or 0)
        lock = self.__lock
        if lock is None:
            if keep_last <= 0:
                removed = len(self.__items)
                self.__items = []
                if removed:
                    self.revision += 1
                return removed
            if len(self.__items) <= keep_last:
                return 0
            removed = len(self.__items) - keep_last
            self.__items = self.__items[-keep_last:]
            self.revision += 1
            return removed

        with lock:
            if keep_last <= 0:
                removed = len(self.__items)
                self.__items = []
                if removed:
                    self.revision += 1
                return removed
            if len(self.__items) <= keep_last:
                return 0
            removed = len(self.__items) - keep_last
            self.__items = self.__items[-keep_last:]
            self.revision += 1
            return removed


class SafeOrderDict:
    """Dict-backed SafeList variant for order tracking buckets.

    WHY: Backtests remove orders by `identifier` extremely frequently. List-backed removals are
    O(n) scans and show up as a dominant CPU cost in high-churn minute backtests.

    This container stores orders in an insertion-ordered dict keyed by `order.identifier`, giving:
    - O(1) remove-by-identifier
    - O(1) contains-by-identifier
    - stable iteration order (dict insertion order)

    NOTE: This is intended for backtesting-only order buckets. It does not provide list indexing.
    """

    def __init__(self, lock=None, initial=None):
        if lock is not None and not isinstance(lock, rlock_type):
            raise ValueError("lock must be a threading.RLock")
        self.__lock = lock
        self.__items: dict[str, object] = {}
        self.revision = 0
        if initial:
            for item in initial:
                self.append(item)

    @staticmethod
    def _identifier_for(item):
        identifier = getattr(item, "_identifier", _MISSING)
        if identifier is not _MISSING:
            return identifier
        identifier = getattr(item, "identifier", _MISSING)
        if identifier is not _MISSING:
            return identifier
        return None

    def __repr__(self):
        return repr(list(self.__items.values()))

    def __bool__(self):
        lock = self.__lock
        if lock is None:
            return bool(self.__items)
        with lock:
            return bool(self.__items)

    def __len__(self):
        lock = self.__lock
        if lock is None:
            return len(self.__items)
        with lock:
            return len(self.__items)

    def __iter__(self):
        lock = self.__lock
        if lock is None:
            return iter(self.__items.values())
        with lock:
            return iter(list(self.__items.values()))

    def __contains__(self, val):
        lock = self.__lock
        if lock is None:
            return self._contains_unlocked(val)
        with lock:
            return self._contains_unlocked(val)

    def _contains_unlocked(self, val):
        if isinstance(val, str):
            return val in self.__items
        identifier = self._identifier_for(val)
        if identifier is None:
            return False
        return identifier in self.__items

    def append(self, value):
        identifier = self._identifier_for(value)
        if identifier is None:
            raise ValueError("SafeOrderDict items must have an identifier")
        lock = self.__lock
        if lock is None:
            self.__items[str(identifier)] = value
            self.revision += 1
            return
        with lock:
            self.__items[str(identifier)] = value
            self.revision += 1

    def remove(self, value, key=None):
        lock = self.__lock
        if lock is None:
            return self._remove_unlocked(value, key=key)
        with lock:
            return self._remove_unlocked(value, key=key)

    def get(self, identifier, default=None):
        lock = self.__lock
        if lock is None:
            return self.__items.get(str(identifier), default)
        with lock:
            return self.__items.get(str(identifier), default)

    def _remove_unlocked(self, value, key=None):
        if key is None:
            if isinstance(value, str):
                removed = self.__items.pop(value, None)
                if removed is not None:
                    self.revision += 1
                return
            identifier = self._identifier_for(value)
            if identifier is None:
                return
            removed = self.__items.pop(str(identifier), None)
            if removed is not None:
                self.revision += 1
            return

        if key == "identifier":
            removed = self.__items.pop(str(value), None)
            if removed is not None:
                self.revision += 1
            return

        # Fallback: scan by arbitrary key (should be rare for order buckets).
        if not isinstance(key, str):
            raise ValueError(f"key must be a string, received {key} of type {type(key)}")
        for identifier, item in list(self.__items.items()):
            if getattr(item, key, _MISSING) == value:
                removed = self.__items.pop(identifier, None)
                if removed is not None:
                    self.revision += 1
                break

    def extend(self, value):
        lock = self.__lock
        if lock is None:
            for item in value:
                self.append(item)
            return
        with lock:
            for item in value:
                self.append(item)

    def get_list(self):
        lock = self.__lock
        if lock is None:
            return self.__items.values()
        with lock:
            return list(self.__items.values())
