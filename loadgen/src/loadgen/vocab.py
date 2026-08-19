"""Fixed word lists and templates the generator draws from.

Kept out of ``seed.py`` so the generation logic stays readable, and kept out of
Faker so the parts of a row that a human will actually read while debugging --
the note prose, the diagnosis codes, the clinic names -- do not change when
Faker ships a new release. Faker is used for names, streets and companies; the
clinical vocabulary is ours.

Nothing here is real. The phone numbers are all in the 555-01xx range reserved
for fiction, and the addresses are Faker's.
"""

from __future__ import annotations

from dataclasses import dataclass

SPECIALTIES = [
    "Family Medicine",
    "Internal Medicine",
    "Pediatrics",
    "Psychiatry",
    "Endocrinology",
    "Cardiology",
    "Orthopedics",
    "Dermatology",
    "Behavioral Health",
    "Obstetrics & Gynecology",
]

CREDENTIALS = ["MD", "DO", "NP", "PA-C", "PhD", "LICSW"]

CLINIC_LOCATIONS = [
    "Riverside Clinic - Suite 200",
    "Riverside Clinic - Annex",
    "Downtown Health Center",
    "Northgate Family Practice",
    "Lakeview Behavioral Health",
    "Telehealth",
    "Telehealth (phone)",
    "Home visit",
]

# Area codes for the fictional 555-01xx exchange.
AREA_CODES = ["617", "413", "508", "203", "212", "415", "312", "206"]

EMAIL_DOMAINS = [
    "example.com",
    "example.org",
    "mail.example.net",
    "inbox.example.com",
    "example.co",
]


@dataclass(frozen=True)
class Condition:
    """An ICD-10 code plus enough English to write a coherent note about it."""

    code: str
    label: str
    complaint: str
    plan: str


CONDITIONS = [
    Condition("E11.9", "Type 2 diabetes mellitus without complications",
              "fatigue and increased thirst over the past month",
              "continue metformin 500mg BID, repeat A1c in three months"),
    Condition("I10", "Essential (primary) hypertension",
              "occasional morning headaches, home readings around 148/92",
              "increase lisinopril to 20mg daily, home BP log for four weeks"),
    Condition("J45.909", "Unspecified asthma, uncomplicated",
              "night-time cough and rescue inhaler use twice weekly",
              "add low-dose ICS, review inhaler technique at next visit"),
    Condition("M54.5", "Low back pain",
              "lower back pain after lifting at work, no radiation",
              "NSAIDs as needed, physical therapy referral placed"),
    Condition("F41.1", "Generalized anxiety disorder",
              "persistent worry and poor sleep since a job change",
              "continue CBT weekly, reassess sertraline dose in six weeks"),
    Condition("F32.1", "Major depressive disorder, single episode, moderate",
              "low mood and anhedonia for roughly two months",
              "start sertraline 50mg daily, safety plan reviewed and documented"),
    Condition("E78.5", "Hyperlipidemia, unspecified",
              "no symptoms; abnormal lipid panel at annual screening",
              "dietary counselling, recheck lipids in six months"),
    Condition("K21.9", "Gastro-esophageal reflux disease without esophagitis",
              "burning chest discomfort after evening meals",
              "trial of PPI for eight weeks, avoid late meals"),
    Condition("N39.0", "Urinary tract infection, site not specified",
              "dysuria and urinary frequency for three days",
              "nitrofurantoin 100mg BID for five days, return if febrile"),
    Condition("R51.9", "Headache, unspecified",
              "intermittent unilateral headache with photophobia",
              "headache diary, consider triptan trial if frequency increases"),
    Condition("Z00.00", "Encounter for general adult medical examination",
              "here for a routine annual physical, no complaints",
              "age-appropriate screening ordered, immunizations updated"),
    Condition("J06.9", "Acute upper respiratory infection, unspecified",
              "three days of sore throat, congestion and low-grade fever",
              "supportive care, return precautions discussed"),
    Condition("M25.561", "Pain in right knee",
              "right knee pain on stairs, worse in the evening",
              "x-ray ordered, quadriceps strengthening exercises given"),
    Condition("G47.00", "Insomnia, unspecified",
              "difficulty falling asleep most nights for two months",
              "sleep hygiene counselling, avoid screens after 22:00"),
    Condition("Z79.899", "Other long term (current) drug therapy",
              "medication review, no new symptoms",
              "refills sent to pharmacy, annual monitoring labs ordered"),
]

# CPT-shaped procedure codes. Office visits, labs and a couple of imaging
# studies -- enough spread that billed amounts are not all the same magnitude.
PROCEDURES = [
    ("99213", "Office visit, established patient, low complexity", 118.00, 210.00),
    ("99214", "Office visit, established patient, moderate complexity", 175.00, 320.00),
    ("99204", "Office visit, new patient, moderate complexity", 220.00, 410.00),
    ("99395", "Preventive visit, established patient, 18-39 years", 190.00, 300.00),
    ("90834", "Psychotherapy, 45 minutes", 130.00, 240.00),
    ("80053", "Comprehensive metabolic panel", 28.00, 96.00),
    ("83036", "Hemoglobin A1c", 22.00, 74.00),
    ("93000", "Electrocardiogram, routine with interpretation", 45.00, 165.00),
    ("71046", "Chest x-ray, 2 views", 95.00, 340.00),
    ("20610", "Arthrocentesis, major joint", 150.00, 420.00),
]

CLAIM_STATUSES = ["submitted", "pending", "paid", "denied", "appealed"]

RELATIONS = [
    "spouse",
    "sister",
    "brother",
    "daughter",
    "son",
    "mother",
    "father",
    "partner",
    "neighbour",
]

# Names that are not ASCII. Faker's non-English locales would do this too, but
# their output moves between Faker releases and these are load-bearing -- they
# are the guaranteed unicode cases, so they live here where they cannot drift.
#
# Given name and surname are paired rather than drawn independently, so the
# result is a plausible name rather than two scripts stapled together. The list
# is ordered so consecutive entries change script: the generator hands them out
# in order, which is what makes "at least six unicode patients" also mean "at
# least six different scripts" instead of six accented Latin names.
UNICODE_NAMES = [
    ("Zoë", "Nyström"),          # Latin, Nordic diacritics
    ("Мария", "Иванова"),        # Cyrillic
    ("美玲", "王"),               # Han
    ("Δημήτρης", "Οικονόμου"),   # Greek
    ("유진", "박"),               # Hangul
    ("محمد", "الحسيني"),          # Arabic, right-to-left
    ("Þóra", "Hafþórsdóttir"),   # Latin, Icelandic thorn
    ("Bảo", "Đặng"),             # Latin, Vietnamese tone marks
    ("Ayşe", "Şahin"),           # Latin, Turkish dotted and dotless i
    ("Дмитрий", "Петров"),       # Cyrillic
    ("Renée", "Křížová"),        # Latin, Czech carons
    ("José", "Álvarez"),         # Latin, Spanish
]

# City / state / zip triples whose zip starts with a zero. Real zip3s, so a
# geographic generalization behaves the way it would in production.
LEADING_ZERO_PLACES = [
    ("Boston", "MA", "02134"),
    ("Peabody", "MA", "01960"),
    ("New Haven", "CT", "06510"),
    ("Hoboken", "NJ", "07030"),
    ("Nashua", "NH", "03062"),
]

PHARMACIES = [
    "Riverside Pharmacy, 4th St",
    "Corner Drug (Main & Elm)",
    "MailOrder Rx",
    "Northgate Apothecary",
]

# Intake questions. The keys vary between rows on purpose: intake_answers is a
# jsonb document, not a table, and a policy that hardcoded a key list would be
# wrong the first time the intake form changed.
INTAKE_KEYS = [
    "reason_for_visit",
    "current_medications",
    "allergies",
    "emergency_contact",
    "preferred_pharmacy",
    "interpreter_needed",
    "transport_needed",
    "notes_for_provider",
    "employer",
    "referred_by",
]

# Free text a patient typed into an intake form. Misspellings and lowercase are
# deliberate: this is the part of the schema a column-level policy never sees.
INTAKE_FREE_TEXT = [
    "back pain again, same as last time",
    "my sister {relative} said i should get this checked",
    "need a note for work, my manager at {employer} is asking",
    "pls call my cell {phone} not the home number",
    "ran out of my pills last tuesday",
    "same as before but worse in the mornings",
    "i can only come after 3pm, school pickup",
    "reaction to penicillin when i was a kid",
    "no insurance card yet, husband's plan starts next month",
    "prefer to be seen by a female provider if possible",
]

# Note bodies. Names, dates, phone numbers, employers, relatives, addresses and
# the MRN itself are woven into clinical prose that has to survive
# de-identification for the row to be worth keeping -- this is the case that
# decides whether the policy works at all.
NOTE_TEMPLATES: dict[str, list[str]] = {
    "progress": [
        "{patient} ({age}yo, DOB {dob}) seen today for follow-up of {label}. "
        "Reports {complaint}. {relative_title} {relative} accompanied and adds that symptoms "
        "began around {onset}. Patient works at {employer} and asks about work restrictions. "
        "Exam unremarkable. Plan: {plan}. Follow-up with {provider} in {weeks} weeks. "
        "Best contact number is {phone}.",

        "Follow-up visit, {patient}. MRN {mrn}. Interval history since {onset}: {complaint}. "
        "Medications reconciled against {pharmacy}. Discussed {label} at length; patient's "
        "{relation} {relative} joined by phone from {phone}. Plan: {plan}. "
        "Return in {weeks} weeks or sooner with return precautions.",
    ],
    "intake": [
        "New patient intake for {patient}, DOB {dob}, of {street}, {city} {state} {postal}. "
        "Referred by {referrer}. Chief complaint: {complaint}. Working diagnosis {label} ({code}). "
        "Emergency contact {relative} ({relation}) at {phone}. Employed at {employer}. "
        "Insurance card scanned; MRN assigned {mrn}. Plan: {plan}.",

        "Intake completed with {patient} ({age}yo). Patient relocated from {city} in {onset} and "
        "has no prior records here. Presenting concern: {complaint}. Family history obtained from "
        "{relative}, {relation}, reachable at {phone}. Assessment: {label}. Plan: {plan}.",
    ],
    "discharge": [
        "Discharge summary for {patient}, MRN {mrn}. Treated for {label} ({code}). Course "
        "uncomplicated. Discharged home to {street}, {city} with {relative} ({relation}) providing "
        "supervision overnight; {relative} can be reached at {phone}. Plan: {plan}. "
        "Follow-up appointment with {provider} scheduled.",

        "{patient} discharged today. Presented with {complaint}; final diagnosis {label}. "
        "Prescriptions sent to {pharmacy}. Patient declined transport and will be driven by their "
        "{relation}. Written instructions given, including {plan}. Employer note provided for "
        "{employer}.",
    ],
    "telephone": [
        "Telephone encounter with {patient} at {phone}. Calling about {complaint}. Reviewed chart; "
        "known {label}. Advised {plan}. Patient will call back if symptoms worsen. "
        "Call lasted approximately {minutes} minutes.",

        "Inbound call from {relative}, the patient's {relation}, on behalf of {patient} (DOB {dob}). "
        "Reports {complaint}. Consent to discuss on file. Advised {plan} and offered an appointment "
        "with {provider}. Callback number {phone}.",
    ],
    "addendum": [
        "Addendum to the note of {onset} for {patient}, MRN {mrn}. Correcting the medication list: "
        "the dose recorded earlier was wrong. Current regimen confirmed with {pharmacy}. "
        "Diagnosis of {label} ({code}) unchanged. Plan: {plan}.",

        "Addendum: after the visit on {onset}, {patient}'s {relation} {relative} called from {phone} "
        "with additional history relevant to {label}. Adjusting plan accordingly: {plan}. "
        "Original note left in place and marked amended.",
    ],
}
