import types
from types import SimpleNamespace
import pytest

from lumibot.tools.projectx_helpers import (
    create_bracket_meta,
    normalize_bracket_entry_tag,
    derive_base_tag,
    bracket_child_tag,
    build_unique_order_tag,
    select_effective_prices,
    early_store_bracket_meta,
    restore_bracket_meta_if_needed,
    should_spawn_bracket_children,
    build_bracket_child_spec,
)


class DummyLogger:
    def __init__(self):
        self.messages = []
    def warning(self, msg):
        self.messages.append(("warning", msg))
    def debug(self, msg):
        self.messages.append(("debug", msg))


class DummyClient:
    def __init__(self, tick=0.25):
        self.tick = tick
    def round_to_tick_size(self, price, tick_size):
        if price is None:
            return None
        # use simple round to nearest tick (matches helpers behavior)
        return round(price / (tick_size or self.tick)) * (tick_size or self.tick)


def test_create_bracket_meta_shape():
    meta = create_bracket_meta(4100.0, 4050.0)
    assert meta == {
        'tp_price': 4100.0,
        'sl_price': 4050.0,
        'children': {},
        'active': True,
        'base_tag': None,
    }


@pytest.mark.parametrize(
    "tag,expected_norm,expected_base",
    [
        ("MYTAG", "BRK_ENTRY_MyTaG".replace("MyTaG", "MYTAG"), "MYTAG"),
        ("BRK_ENTRY_BASE", "BRK_ENTRY_BASE", "BASE"),
        ("BRK_TP_BASE", "BRK_ENTRY_BASE", "BASE"),
        ("BRK_STOP_BASE", "BRK_ENTRY_BASE", "BASE"),
        (None, None, None),
    ],
)
def test_normalize_bracket_entry_tag(tag, expected_norm, expected_base):
    norm, base = normalize_bracket_entry_tag(tag)
    assert norm == expected_norm
    assert base == expected_base


@pytest.mark.parametrize(
    "tag,expected",
    [
        ("BRK_ENTRY_BASE", "BASE"),
        ("BRK_TP_BASE", "BASE"),
        ("BRK_STOP_BASE", "BASE"),
        ("PLAIN", "PLAIN"),
        (None, None),
    ],
)
def test_derive_base_tag(tag, expected):
    assert derive_base_tag(tag) == expected


def test_bracket_child_tag_variants():
    assert bracket_child_tag('tp', 'BASE') == 'BRK_TP_BASE'
    assert bracket_child_tag('sl', 'BASE') == 'BRK_STOP_BASE'
    with pytest.raises(ValueError):
        bracket_child_tag('xyz', 'BASE')


def test_build_unique_order_tag_generates_and_preserves():
    o = SimpleNamespace(tag=None, strategy="MyStrat")
    t1 = build_unique_order_tag(o)
    assert t1 and t1.upper().startswith("MYSTRAT-"[:8])
    # preserve non-empty
    o.tag = "KEEP"
    t2 = build_unique_order_tag(o)
    assert t2 == "KEEP"
    # blank string replaced
    o.tag = "  "
    t3 = build_unique_order_tag(o)
    assert t3 and t3 != "  "


def test_select_effective_prices_precedence_and_rounding():
    # order with primary prices
    strategy = SimpleNamespace(logger=DummyLogger())
    order = SimpleNamespace(
        strategy=strategy,
        limit_price=4100.12,
        stop_price=4050.12,
        secondary_limit_price=None,
        secondary_stop_price=None,
        take_profit_price=None,
        stop_loss_price=None,
    )
    client = DummyClient(tick=0.25)
    lp, sp = select_effective_prices(order, client, tick_size=0.25)
    assert lp == 4100.0  # rounded to nearest tick
    assert sp == 4050.0

    # secondary prices override
    order.secondary_limit_price = 4111.11
    order.secondary_stop_price = 4044.44
    lp2, sp2 = select_effective_prices(order, client, tick_size=0.25)
    assert lp2 == 4111.0  # 4111.11 rounds to 4111.0 with 0.25 tick
    assert sp2 == 4044.5

    # deprecated fields produce warnings via strategy.logger
    order.take_profit_price = 1
    order.stop_loss_price = 1
    _ = select_effective_prices(order, client, tick_size=0.25)
    warns = [m for lvl, m in strategy.logger.messages if lvl == "warning"]
    assert any("deprecated" in m for m in warns)


def test_early_store_and_restore_bracket_meta():
    store = {}
    meta = create_bracket_meta(10, 9)
    early_store_bracket_meta(store, "temp_key", meta)
    assert "temp_key" in store

    # restore attaches to order when cache has matching id
    order = SimpleNamespace(id="A")
    cached = SimpleNamespace(_synthetic_bracket=meta)
    restored = restore_bracket_meta_if_needed(order, {"A": cached}, {}, None)
    assert restored is True
    assert hasattr(order, "_synthetic_bracket")


@pytest.mark.parametrize(
    "meta,already_submitted,expected",
    [
        (create_bracket_meta(1, 1), False, (True, 'ok')),
        (create_bracket_meta(None, None), False, (False, 'no_tp_sl')),
    ],
)
def test_should_spawn_bracket_children(meta, already_submitted, expected):
    parent = SimpleNamespace(_bracket_children_submitted=already_submitted, side='buy')
    eligible, reason = should_spawn_bracket_children(meta, parent)
    assert (eligible, reason) == expected


def test_build_bracket_child_spec_shapes():
    parent = SimpleNamespace(side='buy')
    tp_spec = build_bracket_child_spec(parent, 'tp', 100.0, 'BASE')
    assert tp_spec['side'] == 'sell' and tp_spec['order_type'] == 'limit'
    assert tp_spec['price_key'] == 'limit_price' and tp_spec['price_value'] == 100.0
    assert tp_spec['tag'] == 'BRK_TP_BASE'

    sl_spec = build_bracket_child_spec(parent, 'sl', 90.0, 'BASE')
    assert sl_spec['side'] == 'sell' and sl_spec['order_type'] == 'stop'
    assert sl_spec['price_key'] == 'stop_price' and sl_spec['price_value'] == 90.0
    assert sl_spec['tag'] == 'BRK_STOP_BASE'

    with pytest.raises(ValueError):
        build_bracket_child_spec(parent, 'bad', 1.0, 'BASE')
