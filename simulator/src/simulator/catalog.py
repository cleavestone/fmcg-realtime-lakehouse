"""Hand-written FMCG catalog: fictional Kenyan brands, product lines and pack sizes.

Every pack size is sold both as a single unit and as a distributor case, which is
how an FMCG distributor actually lists SKUs. Prices are in KES.
"""

from dataclasses import dataclass
from decimal import Decimal

# category -> brand -> code, product lines (line name, [(pack size, single-unit price)]), case qty
CATALOG: dict[str, list[dict]] = {
    "beverages": [
        {
            "brand": "Tamu",
            "code": "TAM",
            "case": 24,
            "lines": [
                ("Cola", [("330ml", 50), ("500ml", 70), ("1L", 110), ("2L", 190)]),
                ("Orange Soda", [("500ml", 70), ("2L", 190)]),
                ("Lemon Soda", [("500ml", 70)]),
            ],
        },
        {
            "brand": "Kilima Springs",
            "code": "KIL",
            "case": 12,
            "lines": [
                ("Still Water", [("500ml", 40), ("1L", 70), ("5L", 220)]),
                ("Sparkling Water", [("500ml", 60)]),
            ],
        },
        {
            "brand": "Mavuno",
            "code": "MAV",
            "case": 12,
            "lines": [
                ("Mango Nectar", [("250ml", 45), ("1L", 180)]),
                ("Tropical Juice", [("250ml", 45), ("1L", 180)]),
            ],
        },
        {
            "brand": "Simba Energy",
            "code": "SIM",
            "case": 24,
            "lines": [("Energy Drink", [("250ml", 100), ("500ml", 170)])],
        },
        {
            "brand": "Majani",
            "code": "MAJ",
            "case": 20,
            "lines": [
                ("Black Tea Leaves", [("100g", 90), ("250g", 200), ("500g", 380)]),
                ("Tea Bags", [("50s", 160), ("100s", 300)]),
            ],
        },
        {
            "brand": "Kahawa Hills",
            "code": "KAH",
            "case": 12,
            "lines": [("Instant Coffee", [("50g", 250), ("100g", 450)])],
        },
    ],
    "dairy": [
        {
            "brand": "Savanna Fresh",
            "code": "SAV",
            "case": 12,
            "lines": [
                ("Fresh Milk", [("500ml", 65), ("1L", 120)]),
                ("Long Life Milk", [("500ml", 75), ("1L", 140)]),
            ],
        },
        {
            "brand": "Maziwa Bora",
            "code": "MAZ",
            "case": 12,
            "lines": [
                ("Strawberry Yoghurt", [("150g", 55), ("500g", 170)]),
                ("Vanilla Yoghurt", [("150g", 55), ("500g", 170)]),
                ("Mala", [("500ml", 80)]),
            ],
        },
        {
            "brand": "Nyota",
            "code": "NYO",
            "case": 12,
            "lines": [("Salted Butter", [("250g", 290), ("500g", 560)])],
        },
        {
            "brand": "Ranchi",
            "code": "RAN",
            "case": 10,
            "lines": [("Cheddar Cheese", [("200g", 420), ("500g", 980)])],
        },
        {
            "brand": "Kitamu",
            "code": "KIT",
            "case": 24,
            "lines": [
                ("Chocolate Milk", [("250ml", 60), ("500ml", 100)]),
                ("Strawberry Milk", [("250ml", 60)]),
            ],
        },
        {
            "brand": "Laini",
            "code": "LAI",
            "case": 12,
            "lines": [("Pure Ghee", [("250g", 380), ("500g", 720)])],
        },
    ],
    "snacks": [
        {
            "brand": "Crunchy Ridge",
            "code": "CRU",
            "case": 24,
            "lines": [
                ("Salted Crisps", [("50g", 50), ("150g", 130)]),
                ("Chilli Crisps", [("50g", 50), ("150g", 130)]),
                ("Tomato Crisps", [("50g", 50)]),
                ("Cheese Crisps", [("50g", 55)]),
            ],
        },
        {
            "brand": "Biskuti",
            "code": "BIS",
            "case": 24,
            "lines": [
                ("Glucose Biscuits", [("100g", 40), ("250g", 95)]),
                ("Cream Biscuits", [("100g", 60), ("300g", 160)]),
                ("Digestive Biscuits", [("200g", 140)]),
                ("Ginger Nuts", [("150g", 80)]),
            ],
        },
        {
            "brand": "Karanga Gold",
            "code": "KAR",
            "case": 24,
            "lines": [("Roasted Peanuts", [("50g", 40), ("200g", 140)])],
        },
        {
            "brand": "Chapa",
            "code": "CHA",
            "case": 24,
            "lines": [("Butter Popcorn", [("80g", 70)]), ("Caramel Popcorn", [("80g", 80)])],
        },
        {
            "brand": "Nduma",
            "code": "NDU",
            "case": 24,
            "lines": [("Cassava Crisps", [("100g", 90)])],
        },
        {
            "brand": "Utamu",
            "code": "UTA",
            "case": 12,
            "lines": [("Lollipops", [("50s", 250)]), ("Toffee", [("100s", 300)])],
        },
    ],
    "personal_care": [
        {
            "brand": "Usafi",
            "code": "USA",
            "case": 24,
            "lines": [
                ("Bar Soap", [("100g", 60), ("225g", 130)]),
                ("Antiseptic Soap", [("175g", 150)]),
                ("Body Wash", [("250ml", 280)]),
            ],
        },
        {
            "brand": "Tabasamu",
            "code": "TAB",
            "case": 24,
            "lines": [
                ("Toothpaste", [("50ml", 90), ("100ml", 170), ("150ml", 230)]),
                ("Toothbrush", [("1pc", 80)]),
                ("Mouthwash", [("250ml", 320)]),
            ],
        },
        {
            "brand": "Nywele Care",
            "code": "NYW",
            "case": 12,
            "lines": [
                ("Shampoo", [("200ml", 320), ("400ml", 580)]),
                ("Hair Food", [("250ml", 220)]),
            ],
        },
        {
            "brand": "Ngozi",
            "code": "NGO",
            "case": 12,
            "lines": [
                ("Petroleum Jelly", [("100ml", 110), ("250ml", 230)]),
                ("Body Lotion", [("200ml", 260), ("400ml", 480)]),
            ],
        },
        {
            "brand": "Freshi",
            "code": "FRE",
            "case": 12,
            "lines": [("Roll-on Deodorant", [("50ml", 250)]), ("Body Spray", [("150ml", 350)])],
        },
        {
            "brand": "Dada",
            "code": "DAD",
            "case": 24,
            "lines": [("Sanitary Pads", [("8s", 120), ("16s", 220)])],
        },
    ],
    "household": [
        {
            "brand": "Jua",
            "code": "JUA",
            "case": 12,
            "lines": [
                ("Detergent Powder", [("500g", 160), ("1kg", 300), ("3kg", 850)]),
                ("Liquid Detergent", [("1L", 350), ("2L", 650)]),
                ("Laundry Bar", [("800g", 180)]),
            ],
        },
        {
            "brand": "Safi Home",
            "code": "SAF",
            "case": 12,
            "lines": [
                ("Dishwashing Liquid", [("400ml", 150), ("750ml", 260)]),
                ("Multipurpose Cleaner", [("500ml", 220)]),
                ("Glass Cleaner", [("500ml", 240)]),
            ],
        },
        {
            "brand": "Ngao",
            "code": "NGA",
            "case": 12,
            "lines": [
                ("Bleach", [("250ml", 70), ("750ml", 180)]),
                ("Toilet Cleaner", [("500ml", 230)]),
            ],
        },
        {
            "brand": "Mwanga",
            "code": "MWA",
            "case": 24,
            "lines": [("Candles", [("6pc", 120)]), ("Safety Matches", [("10pk", 50)])],
        },
        {
            "brand": "Pamba",
            "code": "PAM",
            "case": 10,
            "lines": [
                ("Toilet Tissue", [("1 roll", 35), ("4 rolls", 140), ("10 rolls", 330)]),
                ("Serviettes", [("100s", 110)]),
            ],
        },
    ],
}

# Distributors give a small per-unit discount when buying by the case.
CASE_PRICE_FACTOR = Decimal("0.95")


@dataclass(frozen=True)
class Product:
    sku: str
    name: str
    brand: str
    category: str
    pack_size: str
    unit_price: Decimal


def build_products() -> list[Product]:
    """Expand CATALOG into SKUs: each pack size as a single unit and as a case."""
    products: list[Product] = []
    for category, brands in CATALOG.items():
        for brand in brands:
            n = 0
            for line, packs in brand["lines"]:
                for pack, price in packs:
                    single = Decimal(price).quantize(Decimal("0.01"))
                    case_qty = brand["case"]
                    case_price = (single * case_qty * CASE_PRICE_FACTOR).quantize(Decimal("1"))
                    for pack_size, unit_price, suffix in (
                        (pack, single, "EA"),
                        (
                            f"{pack} x{case_qty}",
                            Decimal(case_price).quantize(Decimal("0.01")),
                            f"C{case_qty}",
                        ),
                    ):
                        n += 1
                        products.append(
                            Product(
                                sku=f"{category[:3].upper()}-{brand['code']}-{n:03d}-{suffix}",
                                name=f"{brand['brand']} {line} {pack_size}",
                                brand=brand["brand"],
                                category=category,
                                pack_size=pack_size,
                                unit_price=unit_price,
                            )
                        )
    return products
