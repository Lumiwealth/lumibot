from _thread import RLock as rlock_type


class SafeList:
    def __init__(self, lock, initial=None):
        if not isinstance(lock, rlock_type):
            raise ValueError("lock must be a threading.RLock")

        if initial is None:
            initial = []
        self.__lock = lock
        self.__items = initial

    def __repr__(self):
        return repr(self.__items)

    def __iter__(self):
        with self.__lock:
            return iter(self.__items)

    def __contains__(self, val):
        with self.__lock:
            return val in self.__items

    def __getitem__(self, n):
        with self.__lock:
            return self.__items[n]

    def __setitem__(self, n, val):
        with self.__lock:
            self.__items[n] = val

    def __add__(self, val):
        with self.__lock:
            result = SafeList(self.__lock)
            result.__items = list(set(self.__items + val.__items))
            return result

    def append(self, value):
        with self.__lock:
            self.__items.append(value)

    def remove(self, value, key=None):
        with self.__lock:
            if key is None:
                self.__items.remove(value)
            else:
                if not isinstance(key, str):
                    raise ValueError(
                        "key must be a string, received %r of type %s"
                        % (key, type(key))
                    )
                self.__items = [
                    item for item in self.__items if getattr(item, key) != value
                ]

    def extend(self, value):
        with self.__lock:
            self.__items.extend(value)

    def get_list(self):
        with self.__lock:
            return self.__items
