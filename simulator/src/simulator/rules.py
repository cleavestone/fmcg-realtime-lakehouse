"""Business rules shared by the seed and the live simulator. Pure functions, no database."""

import random
from decimal import ROUND_HALF_UP, Decimal

from faker import Faker

# --- Order state machine ------------------------------------------------------

NEXT_STATUS: dict[str, str] = {
    "placed": "confirmed",
    "confirmed": "shipped",
    "shipped": "delivered",
}
TERMINAL_STATUSES = frozenset({"delivered", "cancelled"})
OPEN_STATUSES = frozenset(NEXT_STATUS)
CANCELLABLE_STATUSES = frozenset({"placed", "confirmed"})  # once shipped, it can't be cancelled


def next_status(status: str) -> str | None:
    """The only forward move from `status`, or None if the order is terminal."""
    return NEXT_STATUS.get(status)


def can_cancel(status: str) -> bool:
    return status in CANCELLABLE_STATUSES


# --- Inventory ----------------------------------------------------------------


def allocate(requested: int, on_hand: int) -> int:
    """Quantity that can be fulfilled without stock going negative (0 = skip the line)."""
    return max(0, min(requested, on_hand))


# --- Pricing and master data --------------------------------------------------

CENT = Decimal("0.01")
TIERS = ("bronze", "silver", "gold")


def changed_price(price: Decimal, pct: float) -> Decimal:
    """Apply a percentage change, rounded to cents, never below 1.00."""
    new = (price * (1 + Decimal(str(pct)) / 100)).quantize(CENT, rounding=ROUND_HALF_UP)
    return max(new, Decimal("1.00"))


def price_change_pct(rng: random.Random, min_pct: float, max_pct: float) -> float:
    """A move of min..max percent, up or down."""
    return round(rng.uniform(min_pct, max_pct), 2) * rng.choice((1, -1))


def step_tier(tier: str, rng: random.Random) -> str:
    """Move one tier up or down (bronze can only go up, gold only down)."""
    i = TIERS.index(tier)
    if i == 0:
        return TIERS[1]
    if i == len(TIERS) - 1:
        return TIERS[-2]
    return TIERS[i + rng.choice((1, -1))]


def credit_limit(rng: random.Random, lo: int, hi: int) -> Decimal:
    return Decimal(rng.randrange(lo, hi + 1, 1000)).quantize(CENT)


def changed_credit_limit(limit: Decimal, pct: float) -> Decimal:
    """Scale a credit limit by pct, rounded to the nearest 1,000."""
    new = limit * (1 + Decimal(str(pct)) / 100)
    return max(Decimal(1000), (new / 1000).quantize(Decimal(1)) * 1000).quantize(CENT)


# --- Weighted choices and order shape -------------------------------------------


def weighted_choice(rng: random.Random, weights: dict[str, float]) -> str:
    keys = list(weights)
    return rng.choices(keys, weights=[weights[k] for k in keys])[0]


def pick_distinct(rng: random.Random, ids: list[int], weights: list[float], k: int) -> list[int]:
    """Weighted sample of up to k distinct ids (popular items appear far more often)."""
    k = min(k, len(ids))
    chosen: list[int] = []
    while len(chosen) < k:
        pid = rng.choices(ids, weights=weights)[0]
        if pid not in chosen:
            chosen.append(pid)
    return chosen


def line_quantity(rng: random.Random, qty_range: list[int], is_case: bool, divisor: int) -> int:
    """Units for one order line; case SKUs are ordered in far smaller quantities."""
    qty = rng.randint(*qty_range)
    return max(1, qty // divisor) if is_case else qty


def discount(rng: random.Random, options: list[float]) -> Decimal:
    return Decimal(str(rng.choice(options))).quantize(CENT)


def store_name(fake: Faker, rng: random.Random, channel: str) -> str:
    if channel == "kiosk":
        return rng.choice([f"{fake.first_name()}'s Kiosk", f"Duka la {fake.first_name()}"])
    if channel == "supermarket":
        return f"{fake.last_name()} Supermarket"
    return f"{fake.last_name()} Wholesalers"
