"""The word lists ``fake`` draws from. Frozen, on purpose.

A fake value is chosen by indexing one of these lists with a keyed hash of the
original, so the list *is* part of the key: reordering it, inserting a name in
the middle, or upgrading a dependency that ships its own names would give every
patient in every already-written clean topic a different fake name. Stability
across restarts and topics is the whole promise of ``fake``, so the lists live
here, in this repo, under review, rather than coming out of Faker -- the load
generator makes the same call for the same reason (``loadgen/vocab.py``).

Changing a list is therefore a breaking change to the clean topics, not a
cosmetic edit. Appending to the end is nearly as bad: ``index = digest % len``
moves every value when the length changes.

Nothing here is real. The phone numbers are in the 555-01xx range reserved for
fiction and the email domains are the RFC 2606 reserved ones, so a fake value
that escapes into an email client cannot reach a person.
"""

from __future__ import annotations

FIRST_NAMES = (
    "Alexis", "Amara", "Anders", "Bianca", "Calvin", "Camila", "Dashiell",
    "Delia", "Elena", "Emmett", "Farida", "Felix", "Gemma", "Grayson",
    "Hollis", "Imani", "Isadora", "Jonas", "Josefina", "Kaimana", "Kiran",
    "Lena", "Linus", "Mariam", "Mateo", "Nadia", "Noor", "Oscar", "Priya",
    "Quinn", "Rafael", "Rosalind", "Soren", "Sunniva", "Tamsin", "Teodoro",
    "Ulises", "Vesna", "Wendell", "Yusuf", "Zoya", "Étienne",
)

LAST_NAMES = (
    "Abara", "Ashworth", "Beaumont", "Calloway", "Dagher", "Ellsworth",
    "Fairbanks", "Garrido", "Halvorsen", "Ibarra", "Jessup", "Kovalenko",
    "Lindqvist", "Marchetti", "Nakamura", "Oyelaran", "Pemberton", "Quintero",
    "Radcliffe", "Sandoval", "Thackeray", "Ubeda", "Vasquez", "Whitfield",
    "Xiong", "Yarborough", "Zabala", "Ó Conaill",
)

STREET_NAMES = (
    "Alder", "Bellweather", "Cormorant", "Dovetail", "Elmridge", "Fenwick",
    "Gravenstein", "Harrowgate", "Ivyhurst", "Juniper", "Kestrel", "Larkspur",
    "Meridian", "Northgate", "Orchard", "Pinehollow", "Quarry", "Rosewood",
    "Sycamore", "Thistledown", "Underhill", "Voyager", "Windmere", "Yarrow",
)

STREET_TYPES = ("Street", "Avenue", "Lane", "Court", "Terrace", "Way", "Circle")

# Deliberately invented place names: a real city name paired with a real zip3
# would put a row back on the map, which is the thing city-level de-id exists
# to prevent.
CITIES = (
    "Ashford Hollow", "Belmont Junction", "Cedar Falls Heights", "Dunmore",
    "Eastbrook", "Fairhaven Mills", "Glenmara", "Havenport", "Inverness Park",
    "Kingsholm", "Lakewood Crossing", "Marbury", "Northvale", "Oakhurst",
    "Pinecrest", "Riverton", "Stonebridge", "Thornbury", "Westmarch",
)

# RFC 2606 reserved domains: none of these can receive mail.
EMAIL_DOMAINS = ("example.com", "example.org", "example.net", "mail.example.com")

# The same fictional exchange the load generator uses.
AREA_CODES = ("203", "206", "212", "312", "413", "415", "508", "617")

COMPANY_HEADS = (
    "Alderpoint", "Brightwater", "Cobblestone", "Dunhollow", "Everstone",
    "Foxglove", "Greenmantle", "Harborview", "Ironwood", "Junegrass",
    "Kestrelmark", "Longmeadow", "Northlight", "Overbrook", "Pinebrook",
    "Quillfeather", "Redstone", "Silverbirch", "Tallgrass", "Wolfsbane",
)

COMPANY_TAILS = (
    "Group", "Partners", "Holdings", "Industries", "Collective", "Works",
    "Associates", "Cooperative", "Systems", "Trust",
)
