from threading import RLock

from lumibot.trading_builtins import SafeList


def test_safe_list_copies_initial_values():
    initial = [1, 2]
    safe_list = SafeList(RLock(), initial=initial)

    initial.append(3)

    assert list(safe_list.get_list()) == [1, 2]
