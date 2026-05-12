from lumibot.entities import Asset, Order


def test_submitted_is_equivalent_to_open_and_new():
    assert Order.is_equivalent_status("submitted", "open")
    assert Order.is_equivalent_status("open", "submitted")
    assert Order.is_equivalent_status("submitted", "new")
    assert Order.is_equivalent_status("new", "submitted")


def test_submitted_is_not_equivalent_to_filled():
    assert not Order.is_equivalent_status("submitted", "filled")
    assert not Order.is_equivalent_status("submitted", "fill")


def test_instance_equivalent_status_respects_submitted():
    asset = Asset("SPY")
    o1 = Order(strategy="s", asset=asset, side="buy", quantity=1)
    o2 = Order(strategy="s", asset=asset, side="buy", quantity=1)

    o1.status = "open"
    o2.status = "submitted"
    assert o1.equivalent_status(o2)
    assert o2.equivalent_status(o1)
