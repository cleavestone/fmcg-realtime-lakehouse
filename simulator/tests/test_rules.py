"""Business rules: order state machine, stock allocation, pricing and master-data changes."""

import random
from collections import Counter
from decimal import Decimal

import pytest
from simulator.config import load_config, section
from simulator.events import EVENTS

from simulator import rules

# --- Order state machine --------------------------------------------------------


def test_orders_only_move_forward():
    assert rules.next_status("placed") == "confirmed"
    assert rules.next_status("confirmed") == "shipped"
    assert rules.next_status("shipped") == "delivered"


@pytest.mark.parametrize("status", ["delivered", "cancelled"])
def test_terminal_statuses_have_no_next(status):
    assert rules.next_status(status) is None
    assert status in rules.TERMINAL_STATUSES
    assert not rules.can_cancel(status)


def test_full_walk_reaches_delivered_in_three_steps():
    status, steps = "placed", 0
    while (nxt := rules.next_status(status)) is not None:
        status, steps = nxt, steps + 1
    assert (status, steps) == ("delivered", 3)


def test_only_placed_or_confirmed_can_be_cancelled():
    assert rules.can_cancel("placed")
    assert rules.can_cancel("confirmed")
    assert not rules.can_cancel("shipped")


def test_state_machine_matches_schema_statuses():
    schema = {"placed", "confirmed", "shipped", "delivered", "cancelled"}
    assert schema == rules.OPEN_STATUSES | rules.TERMINAL_STATUSES


# --- Inventory ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("requested", "on_hand", "expected"),
    [(10, 50, 10), (10, 10, 10), (10, 4, 4), (10, 0, 0), (10, -3, 0)],
)
def test_allocation_never_drives_stock_negative(requested, on_hand, expected):
    qty = rules.allocate(requested, on_hand)
    assert qty == expected
    assert on_hand - qty >= min(on_hand, 0)


def test_case_skus_order_smaller_quantities():
    rng = random.Random(1)
    singles = [rules.line_quantity(rng, [10, 80], False, 6) for _ in range(500)]
    cases = [rules.line_quantity(rng, [10, 80], True, 6) for _ in range(500)]
    assert min(cases) >= 1
    assert sum(cases) * 4 < sum(singles)


# --- Pricing and master data -------------------------------------------------------


def test_price_change_rounds_to_cents_and_has_floor():
    assert rules.changed_price(Decimal("70.00"), 5) == Decimal("73.50")
    assert rules.changed_price(Decimal("70.00"), -3.33) == Decimal("67.67")
    assert rules.changed_price(Decimal("1.00"), -10) == Decimal("1.00")


def test_price_change_pct_within_bounds():
    rng = random.Random(3)
    pcts = [rules.price_change_pct(rng, 3, 10) for _ in range(1000)]
    assert all(3 <= abs(p) <= 10 for p in pcts)
    assert any(p > 0 for p in pcts) and any(p < 0 for p in pcts)


@pytest.mark.parametrize("tier", rules.TIERS)
def test_tier_moves_exactly_one_step(tier):
    rng = random.Random(5)
    for _ in range(50):
        new = rules.step_tier(tier, rng)
        assert new != tier
        assert abs(rules.TIERS.index(new) - rules.TIERS.index(tier)) == 1


def test_credit_limit_change_rounds_to_thousands():
    new = rules.changed_credit_limit(Decimal("403000.00"), 12.5)
    assert new == Decimal("453000.00")
    assert rules.changed_credit_limit(Decimal("1000.00"), -50) == Decimal("1000.00")


# --- Weighted choices ----------------------------------------------------------------


def test_pick_distinct_returns_unique_ids_and_caps_k():
    rng = random.Random(7)
    picked = rules.pick_distinct(rng, [1, 2, 3], [1, 1, 1], 10)
    assert sorted(picked) == [1, 2, 3]


def test_event_weights_cover_known_events_and_sample_proportionally():
    cfg = section(load_config(), "simulator")
    weights = cfg["event_weights"]
    assert set(weights) == set(EVENTS)

    rng = random.Random(42)
    n = 50_000
    counts = Counter(rules.weighted_choice(rng, weights) for _ in range(n))
    total = sum(weights.values())
    for name, w in weights.items():
        assert abs(counts[name] / n - w / total) < 0.01, name
