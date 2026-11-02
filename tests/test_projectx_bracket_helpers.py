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
    from lumibot.brokers.projectx import ProjectX
    assert t1 and len(t1) == 54
    assert ProjectX.strategy_tag_matches(t1, 'MyStrat')
    assert not ProjectX.strategy_tag_matches(t1, 'OtherStrat')
    # preserve non-empty
    o.tag = "KEEP"
    t2 = build_unique_order_tag(o)
    assert t2 == "KEEP"
    # blank string replaced
    o.tag = "  "
    t3 = build_unique_order_tag(o)
    assert t3 and t3 != "  "


@pytest.mark.parametrize('name', ['SyntheticLifecycleStrategy', 'Research-' * 30, 'Strategy Ω ' * 20])
def test_generated_bracket_tags_fit_provider_limit_without_losing_owner(name):
    from lumibot.brokers.projectx import ProjectX
    order = SimpleNamespace(tag=None, strategy=name)
    tag = build_unique_order_tag(order)
    for prefix in ('', 'BRK_ENTRY_', 'BRK_STOP_', 'BRK_TP_'):
        assert len((prefix + tag).encode('utf-8')) <= 64
        assert ProjectX.strategy_tag_matches(prefix + tag, name)
        assert not ProjectX.strategy_tag_matches(prefix + tag, name + 'Other')


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
    parent = SimpleNamespace(_bracket_children_submitted=already_submitted, side='buy', status='filled', quantity=1)
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


@pytest.mark.parametrize('status', ['unprocessed', 'new', 'submitted', 'canceled', 'error'])
def test_unfilled_parent_cannot_spawn_exit_orders(status):
    parent = SimpleNamespace(status=status, _bracket_children_submitted=False)
    eligible, _ = should_spawn_bracket_children(create_bracket_meta(110, 90), parent)
    assert eligible is False


@pytest.mark.parametrize('generation', [1, 35, 36, 2**31-1])
def test_generation_tags_preserve_full_parent_and_strategy_identity(generation):
    from lumibot.brokers.projectx import ProjectX
    from lumibot.tools.projectx_helpers import parse_bracket_tag
    order = SimpleNamespace(tag=None, strategy='Full strategy Ω identity ' * 5)
    base = build_unique_order_tag(order)
    for kind in ('tp', 'sl'):
        tag = bracket_child_tag(kind, base, generation=generation)
        assert len(tag.encode('utf-8')) <= 64
        assert parse_bracket_tag(tag) == (kind, generation, base)
        assert derive_base_tag(tag) == base
        assert ProjectX.strategy_tag_matches(tag, order.strategy)
        assert not ProjectX.strategy_tag_matches(tag, order.strategy + 'foreign')


@pytest.mark.parametrize('tag', ['BG0T:BASE', 'BG01T:BASE', 'BG-T:BASE', 'BG1X:BASE', 'BG1T:', 'BGZZZZZZZT:' + 'x'*54])
def test_ambiguous_generation_tag_is_not_accepted(tag):
    from lumibot.tools.projectx_helpers import parse_bracket_tag
    assert parse_bracket_tag(tag) is None


def test_generation_tag_limit_rejects_before_submission():
    with pytest.raises(ValueError, match='64-character'):
        bracket_child_tag('tp', 'x'*54, generation=36**6)


@pytest.mark.parametrize('alias', ['order_place', 'place_order'])
@pytest.mark.parametrize('status,payload,outcome', [
    (200, {'success': True, 'errorCode': 0, 'orderId': 123}, 'accepted'),
    (200, {'success': 'false', 'errorCode': 0, 'orderId': 123}, 'unknown'),
    (200, {'success': False, 'errorCode': 6, 'orderId': 123}, 'unknown'),
    (500, {'success': True, 'errorCode': 0, 'orderId': 123}, 'unknown'),
    (200, {'success': False, 'errorCode': 2, 'orderId': 123}, 'rejected'),
])
def test_real_client_placement_aliases_share_the_structured_receipt_contract(alias, status, payload, outcome):
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import ProjectX, ProjectXClient, ProjectXOrderSubmissionError
    api = ProjectX({'firm': 'TEST', 'base_url': 'https://example.test/'}, 'synthetic')
    response = Mock(status_code=status)
    response.json.return_value = payload
    api._post = Mock(return_value=response)
    client = object.__new__(ProjectXClient)
    client.api = api
    def submit():
        if alias == 'place_order':
            return client.place_order(account_id=1, contract_id='CON.F.US.MES.Z25', side='buy', quantity=1, order_type=1, price=5000)
        return client.order_place(account_id=1, contract_id='CON.F.US.MES.Z25', side=0, size=1, type=1, limit_price=5000)
    if outcome == 'accepted':
        result = submit()
        assert result['success'] is True and result['orderId'] == 123
    else:
        with pytest.raises(ProjectXOrderSubmissionError) as error:
            submit()
        assert error.value.outcome == outcome
        assert error.value.response['success'] == payload['success']
    api._post.assert_called_once()
    assert api._post.call_args.kwargs['json']['type'] == 1
