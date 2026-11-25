"""Static reference data: countries, currencies, channels, departments,
categories, calendar effects, FX, promotions and marketing campaigns.

Everything here is deterministic (pure functions of a seed / date) so that
the operational database, file exports, REST API and event stream always
agree with each other.
"""

from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class Country:
    code: str
    name: str
    region: str
    currency: str
    vat_rate: float
    utc_offset: float  # hours, fixed (no DST) - good enough for traffic shaping
    locale: str
    traffic_weight: float
    timezone: str


COUNTRIES: tuple[Country, ...] = (
    Country("US", "United States", "North America", "USD", 0.07, -5, "en_US", 22.0, "America/New_York"),
    Country("CA", "Canada", "North America", "CAD", 0.13, -5, "en_CA", 4.0, "America/Toronto"),
    Country("MX", "Mexico", "Latin America", "MXN", 0.16, -6, "es_MX", 3.0, "America/Mexico_City"),
    Country("BR", "Brazil", "Latin America", "BRL", 0.17, -3, "pt_BR", 4.0, "America/Sao_Paulo"),
    Country("GB", "United Kingdom", "Europe", "GBP", 0.20, 0, "en_GB", 9.0, "Europe/London"),
    Country("IE", "Ireland", "Europe", "EUR", 0.23, 0, "en_IE", 1.0, "Europe/Dublin"),
    Country("FR", "France", "Europe", "EUR", 0.20, 1, "fr_FR", 6.0, "Europe/Paris"),
    Country("DE", "Germany", "Europe", "EUR", 0.19, 1, "de_DE", 8.0, "Europe/Berlin"),
    Country("ES", "Spain", "Europe", "EUR", 0.21, 1, "es_ES", 3.5, "Europe/Madrid"),
    Country("IT", "Italy", "Europe", "EUR", 0.22, 1, "it_IT", 3.5, "Europe/Rome"),
    Country("NL", "Netherlands", "Europe", "EUR", 0.21, 1, "nl_NL", 2.5, "Europe/Amsterdam"),
    Country("BE", "Belgium", "Europe", "EUR", 0.21, 1, "nl_BE", 1.2, "Europe/Brussels"),
    Country("AT", "Austria", "Europe", "EUR", 0.20, 1, "de_AT", 1.0, "Europe/Vienna"),
    Country("PT", "Portugal", "Europe", "EUR", 0.23, 0, "pt_PT", 0.8, "Europe/Lisbon"),
    Country("SE", "Sweden", "Europe", "SEK", 0.25, 1, "sv_SE", 1.3, "Europe/Stockholm"),
    Country("DK", "Denmark", "Europe", "DKK", 0.25, 1, "da_DK", 0.8, "Europe/Copenhagen"),
    Country("NO", "Norway", "Europe", "NOK", 0.25, 1, "no_NO", 0.8, "Europe/Oslo"),
    Country("FI", "Finland", "Europe", "EUR", 0.24, 2, "fi_FI", 0.6, "Europe/Helsinki"),
    Country("PL", "Poland", "Europe", "PLN", 0.23, 1, "pl_PL", 2.0, "Europe/Warsaw"),
    Country("CZ", "Czechia", "Europe", "CZK", 0.21, 1, "cs_CZ", 0.8, "Europe/Prague"),
    Country("CH", "Switzerland", "Europe", "CHF", 0.081, 1, "de_CH", 1.0, "Europe/Zurich"),
    Country("JP", "Japan", "Asia Pacific", "JPY", 0.10, 9, "ja_JP", 5.0, "Asia/Tokyo"),
    Country("KR", "South Korea", "Asia Pacific", "KRW", 0.10, 9, "ko_KR", 2.0, "Asia/Seoul"),
    Country("IN", "India", "Asia Pacific", "INR", 0.18, 5.5, "en_IN", 4.0, "Asia/Kolkata"),
    Country("SG", "Singapore", "Asia Pacific", "SGD", 0.09, 8, "en_US", 1.0, "Asia/Singapore"),
    Country("AU", "Australia", "Asia Pacific", "AUD", 0.10, 10, "en_AU", 3.0, "Australia/Sydney"),
    Country("NZ", "New Zealand", "Asia Pacific", "NZD", 0.15, 12, "en_NZ", 0.7, "Pacific/Auckland"),
    Country(
        "ZA", "South Africa", "Middle East & Africa", "ZAR", 0.15, 2, "en_US", 1.0, "Africa/Johannesburg"
    ),
    Country("AE", "United Arab Emirates", "Middle East & Africa", "AED", 0.05, 4, "ar_AE", 1.2, "Asia/Dubai"),
    Country("TR", "Turkey", "Middle East & Africa", "TRY", 0.20, 3, "tr_TR", 1.5, "Europe/Istanbul"),
)
COUNTRY_BY_CODE = {c.code: c for c in COUNTRIES}

# currency -> (name, minor units, base units per USD, annual drift)
CURRENCIES: dict[str, tuple[str, int, float, float]] = {
    "USD": ("US Dollar", 2, 1.0, 0.0),
    "CAD": ("Canadian Dollar", 2, 1.36, 0.0),
    "MXN": ("Mexican Peso", 2, 17.5, 0.02),
    "BRL": ("Brazilian Real", 2, 5.0, 0.04),
    "GBP": ("Pound Sterling", 2, 0.79, 0.0),
    "EUR": ("Euro", 2, 0.92, 0.0),
    "SEK": ("Swedish Krona", 2, 10.5, 0.0),
    "DKK": ("Danish Krone", 2, 6.88, 0.0),
    "NOK": ("Norwegian Krone", 2, 10.7, 0.0),
    "PLN": ("Polish Zloty", 2, 4.0, 0.0),
    "CZK": ("Czech Koruna", 2, 23.0, 0.0),
    "CHF": ("Swiss Franc", 2, 0.88, -0.01),
    "JPY": ("Japanese Yen", 0, 150.0, 0.01),
    "KRW": ("South Korean Won", 0, 1330.0, 0.01),
    "INR": ("Indian Rupee", 2, 83.0, 0.02),
    "SGD": ("Singapore Dollar", 2, 1.34, 0.0),
    "AUD": ("Australian Dollar", 2, 1.52, 0.0),
    "NZD": ("New Zealand Dollar", 2, 1.65, 0.0),
    "ZAR": ("South African Rand", 2, 18.5, 0.03),
    "AED": ("UAE Dirham", 2, 3.6725, 0.0),  # pegged
    "TRY": ("Turkish Lira", 2, 32.0, 0.25),
}

SALES_CHANNELS: dict[str, str] = {
    "web": "Desktop and mobile web storefront",
    "mobile_app": "Native iOS / Android application",
    "marketplace": "Third-party marketplace partners",
    "social_commerce": "In-app checkout on social platforms",
}

TRAFFIC_SOURCES: dict[str, float] = {
    "organic_search": 0.30,
    "direct": 0.22,
    "paid_search": 0.16,
    "social": 0.12,
    "email": 0.09,
    "affiliate": 0.06,
    "referral": 0.05,
}

DEVICES: dict[str, float] = {"mobile": 0.56, "desktop": 0.37, "tablet": 0.07}
BROWSERS: dict[str, dict[str, float]] = {
    "mobile": {"Safari": 0.48, "Chrome": 0.44, "Samsung Internet": 0.06, "Firefox": 0.02},
    "desktop": {"Chrome": 0.62, "Safari": 0.14, "Edge": 0.14, "Firefox": 0.10},
    "tablet": {"Safari": 0.60, "Chrome": 0.36, "Firefox": 0.04},
}

# department -> (categories, price range USD, return rate, margin)
DEPARTMENTS: dict[str, tuple[tuple[str, ...], tuple[float, float], float, float]] = {
    "Electronics": (
        ("Smartphones", "Laptops", "Headphones", "Smart Home", "Cameras"),
        (25, 1800),
        0.06,
        0.18,
    ),
    "Fashion": (
        ("Women's Clothing", "Men's Clothing", "Shoes", "Bags", "Accessories"),
        (12, 400),
        0.22,
        0.55,
    ),
    "Home & Kitchen": (
        ("Cookware", "Furniture", "Bedding", "Home Decor", "Small Appliances"),
        (8, 900),
        0.08,
        0.40,
    ),
    "Beauty": (("Skincare", "Makeup", "Fragrance", "Hair Care", "Grooming"), (5, 180), 0.05, 0.60),
    "Sports & Outdoors": (
        ("Fitness", "Cycling", "Camping", "Team Sports", "Running"),
        (10, 1200),
        0.09,
        0.38,
    ),
    "Toys & Games": (
        ("Board Games", "Building Sets", "Dolls", "Outdoor Play", "Puzzles"),
        (6, 250),
        0.05,
        0.42,
    ),
    "Books & Media": (("Fiction", "Non-Fiction", "Children's Books", "Music", "Movies"), (4, 80), 0.03, 0.30),
    "Grocery": (("Coffee & Tea", "Snacks", "Pantry", "Beverages", "Organic"), (2, 60), 0.01, 0.25),
    "Health": (
        ("Vitamins", "Personal Care", "Medical Supplies", "Wellness", "Baby Care"),
        (4, 150),
        0.03,
        0.45,
    ),
    "Automotive": (
        ("Car Electronics", "Car Care", "Tools", "Tires & Wheels", "Interior"),
        (8, 600),
        0.07,
        0.35,
    ),
}

PRODUCT_NOUNS: dict[str, tuple[str, ...]] = {
    "Smartphones": ("Phone", "Smartphone", "Phone Case", "Charger"),
    "Laptops": ("Laptop", "Ultrabook", "Notebook", "Laptop Stand"),
    "Headphones": ("Headphones", "Earbuds", "Headset", "Speaker"),
    "Smart Home": ("Smart Plug", "Smart Bulb", "Thermostat", "Doorbell Camera"),
    "Cameras": ("Camera", "Lens", "Tripod", "Action Camera"),
    "Women's Clothing": ("Dress", "Blouse", "Jacket", "Jeans"),
    "Men's Clothing": ("Shirt", "Chinos", "Hoodie", "Blazer"),
    "Shoes": ("Sneakers", "Boots", "Loafers", "Sandals"),
    "Bags": ("Backpack", "Tote Bag", "Duffle Bag", "Wallet"),
    "Accessories": ("Watch", "Sunglasses", "Belt", "Scarf"),
    "Cookware": ("Frying Pan", "Saucepan", "Knife Set", "Dutch Oven"),
    "Furniture": ("Desk", "Office Chair", "Bookshelf", "Sofa"),
    "Bedding": ("Duvet", "Pillow", "Sheet Set", "Blanket"),
    "Home Decor": ("Lamp", "Vase", "Wall Art", "Mirror"),
    "Small Appliances": ("Blender", "Coffee Maker", "Air Fryer", "Toaster"),
    "Skincare": ("Moisturizer", "Serum", "Cleanser", "Sunscreen"),
    "Makeup": ("Lipstick", "Foundation", "Mascara", "Palette"),
    "Fragrance": ("Eau de Parfum", "Cologne", "Body Mist", "Gift Set"),
    "Hair Care": ("Shampoo", "Conditioner", "Hair Dryer", "Styling Gel"),
    "Grooming": ("Trimmer", "Razor", "Beard Oil", "Shaving Kit"),
    "Fitness": ("Dumbbell Set", "Yoga Mat", "Kettlebell", "Resistance Bands"),
    "Cycling": ("Bike Helmet", "Bike Light", "Road Bike", "Cycling Gloves"),
    "Camping": ("Tent", "Sleeping Bag", "Camping Stove", "Lantern"),
    "Team Sports": ("Football", "Basketball", "Shin Guards", "Jersey"),
    "Running": ("Running Shoes", "Running Vest", "GPS Watch", "Water Bottle"),
    "Board Games": ("Board Game", "Card Game", "Strategy Game", "Party Game"),
    "Building Sets": ("Building Set", "Construction Kit", "Robot Kit", "Marble Run"),
    "Dolls": ("Doll", "Doll House", "Action Figure", "Plush Toy"),
    "Outdoor Play": ("Trampoline", "Scooter", "Swing Set", "Water Blaster"),
    "Puzzles": ("Jigsaw Puzzle", "3D Puzzle", "Brain Teaser", "Puzzle Mat"),
    "Fiction": ("Novel", "Thriller", "Fantasy Saga", "Short Stories"),
    "Non-Fiction": ("Biography", "Cookbook", "History Book", "Self-Help Book"),
    "Children's Books": ("Picture Book", "Activity Book", "Early Reader", "Story Collection"),
    "Music": ("Vinyl Record", "CD Box Set", "Songbook", "Concert Film"),
    "Movies": ("Blu-ray", "DVD Box Set", "4K Movie", "Documentary"),
    "Coffee & Tea": ("Coffee Beans", "Green Tea", "Espresso Pods", "Herbal Tea"),
    "Snacks": ("Granola Bars", "Trail Mix", "Chocolate Box", "Crackers"),
    "Pantry": ("Olive Oil", "Pasta", "Spice Set", "Honey"),
    "Beverages": ("Sparkling Water", "Juice Pack", "Energy Drink", "Kombucha"),
    "Organic": ("Organic Oats", "Organic Quinoa", "Organic Nut Butter", "Organic Dried Fruit"),
    "Vitamins": ("Multivitamin", "Vitamin D", "Omega-3", "Probiotic"),
    "Personal Care": ("Toothbrush", "Body Wash", "Deodorant", "Hand Cream"),
    "Medical Supplies": ("Thermometer", "First Aid Kit", "Blood Pressure Monitor", "Pulse Oximeter"),
    "Wellness": ("Massage Gun", "Essential Oils", "Sleep Mask", "Foam Roller"),
    "Baby Care": ("Diapers", "Baby Wipes", "Baby Bottle", "Stroller"),
    "Car Electronics": ("Dash Cam", "Car Charger", "GPS Navigator", "Car Speaker"),
    "Car Care": ("Car Wax", "Microfiber Towels", "Pressure Washer", "Car Vacuum"),
    "Tools": ("Socket Set", "Cordless Drill", "Tool Box", "Jump Starter"),
    "Tires & Wheels": ("Tire Inflator", "Wheel Cleaner", "Tire Gauge", "Snow Chains"),
    "Interior": ("Seat Cover", "Floor Mats", "Phone Mount", "Organizer"),
}

ADJECTIVES = (
    "Classic",
    "Pro",
    "Ultra",
    "Eco",
    "Smart",
    "Premium",
    "Compact",
    "Essential",
    "Deluxe",
    "Lite",
    "Max",
    "Air",
    "Signature",
    "Urban",
    "Vintage",
    "Sport",
    "Travel",
    "Studio",
    "Prime",
    "Nova",
)

PAYMENT_METHODS: dict[str, tuple[float, float]] = {
    # method -> (share, base failure probability)
    "credit_card": (0.46, 0.055),
    "debit_card": (0.14, 0.065),
    "paypal": (0.18, 0.025),
    "apple_pay": (0.08, 0.015),
    "google_pay": (0.05, 0.018),
    "klarna": (0.05, 0.07),
    "bank_transfer": (0.04, 0.08),
}

FAILURE_REASONS = (
    "insufficient_funds",
    "card_declined",
    "fraud_suspected",
    "expired_card",
    "3ds_failed",
    "timeout",
)

CARRIERS: dict[str, tuple[int, int, float]] = {
    # carrier -> (domestic SLA days, international SLA days, delay probability)
    "DHL": (2, 5, 0.06),
    "UPS": (2, 5, 0.07),
    "FedEx": (2, 4, 0.06),
    "DPD": (2, 6, 0.09),
    "Royal Mail": (2, 7, 0.11),
    "Japan Post": (2, 7, 0.05),
    "Australia Post": (3, 8, 0.10),
}

RETURN_REASONS = ("wrong_size", "damaged", "not_as_described", "changed_mind", "late_delivery", "defective")
CANCEL_REASONS = ("customer_request", "found_cheaper", "shipping_too_slow", "duplicate_order")

SEARCH_TERMS = (
    "wireless headphones",
    "running shoes",
    "coffee maker",
    "yoga mat",
    "laptop",
    "winter jacket",
    "board game",
    "skincare set",
    "smart watch",
    "air fryer",
    "backpack",
    "gift ideas",
    "sale",
    "kids toys",
    "office chair",
    "vitamins",
    "camera lens",
    "dress",
    "sneakers",
    "tent",
)


# --------------------------------------------------------------- calendar ---
def black_friday(year: int) -> date:
    """Fourth Friday of November."""
    nov1 = date(year, 11, 1)
    first_friday = nov1 + timedelta(days=(4 - nov1.weekday()) % 7)
    return first_friday + timedelta(weeks=3)


def cyber_monday(year: int) -> date:
    return black_friday(year) + timedelta(days=3)


def is_black_friday(d: date) -> bool:
    return d == black_friday(d.year)


def demand_multiplier(d: date) -> float:
    """Daily traffic multiplier: weekly seasonality, retail seasons and peak days."""
    m = 1.0
    if d.weekday() >= 5:  # weekend shopping
        m *= 1.25
    elif d.weekday() == 0:
        m *= 1.05
    bf, cm = black_friday(d.year), cyber_monday(d.year)
    if d == bf:
        m *= 4.5
    elif d == cm:
        m *= 3.2
    elif bf - timedelta(days=6) <= d < bf or bf < d < cm:
        m *= 1.6  # Black Friday week build-up and weekend
    elif cm < d <= cm + timedelta(days=3):
        m *= 1.3
    elif date(d.year, 12, 1) <= d <= date(d.year, 12, 20):
        m *= 1.45  # Christmas shopping
    elif date(d.year, 12, 26) <= d <= date(d.year, 12, 31) or d <= date(d.year, 1, 10):
        m *= 1.25  # boxing day / January sales
    elif date(d.year, 7, 15) <= d <= date(d.year, 8, 15):
        m *= 0.88  # summer lull
    elif date(d.year, 2, 1) <= d <= date(d.year, 2, 28):
        m *= 0.92
    return m


# Share of daily traffic by LOCAL hour of day (sums to 1).
_HOURLY = (
    0.012,
    0.007,
    0.005,
    0.004,
    0.004,
    0.006,
    0.014,
    0.026,
    0.038,
    0.046,
    0.051,
    0.054,
    0.058,
    0.057,
    0.054,
    0.053,
    0.055,
    0.060,
    0.066,
    0.072,
    0.076,
    0.072,
    0.055,
    0.034,
)
HOURLY_PROFILE = tuple(h / sum(_HOURLY) for h in _HOURLY)


# --------------------------------------------------------------------- FX ---
def _unit_noise(*parts: object) -> float:
    """Deterministic pseudo-random number in [-1, 1] from arbitrary parts."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**63 - 1.0


def fx_rate(currency: str, d: date) -> float:
    """Units of `currency` per 1 USD on date `d` (deterministic, smooth random walk)."""
    _, minor, base, drift = CURRENCIES[currency]
    if currency in {"USD"}:
        return 1.0
    if currency == "AED":
        return base
    t = (d - date(2024, 1, 1)).days / 365.0
    wave = 0.025 * math.sin(2 * math.pi * t * 1.3 + (hash_int(currency) % 7)) + 0.012 * math.sin(
        2 * math.pi * t * 5.1 + (hash_int(currency) % 3)
    )
    daily = 0.003 * _unit_noise("fx", currency, d.isoformat())
    rate = base * (1 + drift) ** t * (1 + wave + daily)
    return round(rate, 6)


def to_usd(amount: float, currency: str, d: date) -> float:
    return amount / fx_rate(currency, d)


def from_usd(amount_usd: float, currency: str, d: date) -> float:
    return amount_usd * fx_rate(currency, d)


def round_money(amount: float, currency: str) -> float:
    return round(amount, CURRENCIES[currency][1])


def hash_int(value: str) -> int:
    return int.from_bytes(hashlib.md5(value.encode(), usedforsecurity=False).digest()[:4], "big")


# ------------------------------------------------------------- promotions ---
@dataclass(frozen=True)
class Promotion:
    promotion_id: str
    promotion_name: str
    promotion_type: str
    discount_pct: float
    start_date: date
    end_date: date
    channel: str | None = None
    department: str | None = None

    def is_active(self, d: date) -> bool:
        return self.start_date <= d <= self.end_date


def promotions_for_year(year: int) -> list[Promotion]:
    bf, cm = black_friday(year), cyber_monday(year)
    y = str(year)
    return [
        Promotion(
            f"PRM-{y}-NEWYEAR",
            f"New Year Sale {y}",
            "clearance",
            0.20,
            date(year, 1, 1),
            date(year, 1, 10),
            department="Fashion",
        ),
        Promotion(
            f"PRM-{y}-SPRING",
            f"Spring Refresh {y}",
            "seasonal",
            0.10,
            date(year, 3, 20),
            date(year, 4, 5),
            department="Home & Kitchen",
        ),
        Promotion(
            f"PRM-{y}-FITNESS",
            f"Fitness Month {y}",
            "percentage",
            0.12,
            date(year, 5, 1),
            date(year, 5, 14),
            department="Sports & Outdoors",
        ),
        Promotion(
            f"PRM-{y}-SUMMER", f"Summer Sale {y}", "seasonal", 0.15, date(year, 7, 1), date(year, 7, 14)
        ),
        Promotion(
            f"PRM-{y}-SCHOOL",
            f"Back to School {y}",
            "percentage",
            0.10,
            date(year, 8, 20),
            date(year, 9, 5),
            department="Books & Media",
        ),
        Promotion(
            f"PRM-{y}-APP",
            f"App Exclusive {y}",
            "percentage",
            0.08,
            date(year, 10, 1),
            date(year, 10, 21),
            channel="mobile_app",
        ),
        Promotion(
            f"PRM-{y}-SINGLES",
            f"Singles Day {y}",
            "percentage",
            0.11,
            date(year, 11, 11),
            date(year, 11, 11),
            department="Electronics",
        ),
        Promotion(
            f"PRM-{y}-BFRIDAY",
            f"Black Friday {y}",
            "seasonal",
            0.25,
            bf - timedelta(days=3),
            bf + timedelta(days=2),
        ),
        Promotion(
            f"PRM-{y}-CYBER", f"Cyber Monday {y}", "percentage", 0.20, cm, cm, department="Electronics"
        ),
        Promotion(
            f"PRM-{y}-XMAS", f"Holiday Gifting {y}", "seasonal", 0.12, date(year, 12, 5), date(year, 12, 20)
        ),
        Promotion(
            f"PRM-{y}-FREESHIP",
            f"Free Shipping Week {y}",
            "free_shipping",
            0.0,
            date(year, 6, 1),
            date(year, 6, 7),
        ),
    ]


def promotions_between(start: date, end: date) -> list[Promotion]:
    promos: list[Promotion] = []
    for year in range(start.year, end.year + 1):
        promos.extend(p for p in promotions_for_year(year) if p.end_date >= start and p.start_date <= end)
    return promos


# -------------------------------------------------------- marketing / ads ---
PAID_SOURCES = ("paid_search", "social", "email", "affiliate")


def campaign_id(source: str, d: date) -> str:
    return f"CMP-{d:%Y%m}-{source.upper().replace('_', '')}"


def campaigns_for_date(d: date) -> list[dict[str, object]]:
    """Marketing campaigns active on `d` with their daily spend (USD)."""
    out: list[dict[str, object]] = []
    for source in PAID_SOURCES:
        base = {"paid_search": 2400.0, "social": 1600.0, "email": 250.0, "affiliate": 700.0}[source]
        spend = base * demand_multiplier(d) * (1 + 0.15 * _unit_noise("spend", source, d.isoformat()))
        out.append(
            {
                "campaign_id": campaign_id(source, d),
                "campaign_name": f"{source.replace('_', ' ').title()} {d:%B %Y}",
                "traffic_source": source,
                "spend_date": d.isoformat(),
                "spend_usd": round(spend, 2),
                "impressions": int(spend * (120 if source != "email" else 900)),
                "clicks": int(spend * (1.6 if source != "email" else 12)),
            }
        )
    return out


def weighted_choice(rng: random.Random, weights: dict[str, float]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()), k=1)[0]


def utc_to_local_hour(ts: datetime, country: Country) -> int:
    return int((ts.hour + ts.minute / 60 + country.utc_offset) % 24)
