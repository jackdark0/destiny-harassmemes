"""
classify_v3.py — Classify tweets targeting Destiny into harassment tiers.

Architecture: flat keyword match (v1 style) with clearer category taxonomy.
All tweets in this dataset are directed at Destiny as part of a documented
harassment campaign. Categories describe the attack vector used.

Changes from v1:
  - Plural-safe matching (word-boundary prefix only, no trailing \b)
  - Split monolithic categories into specific attack vectors
  - Added: homophobia, emasculation, sexism, zionist_smear, predator_labels
  - Israel/Zionist bare mentions → borderline (not auto-harassment)
  - Recovered missing text from results.json (498 rows had empty text in v1 output)

Input:  results.json, classified.csv (for tweet_id/handle/url roster)
Output: classified_v3.csv — all tweets reclassified
"""

import csv
import json
import os
import re
from collections import Counter, defaultdict

# ---------------------------------------------------------------------------
# Keyword dictionaries
# ---------------------------------------------------------------------------

HARASSMENT_KEYWORDS: dict[str, list[str]] = {
    # --- CSAM / pedophilia accusations ---
    "court_memes": [
        "pedo", "pedophile", "pedophilia", "pedostiny", "sexpestiny",
        "csam", "cp on your computer", "child porn",
        "groomed minor", "groom minor", "groom underage",
        "sex tape to a minor", "predator", "groomer",
        "15 year old", "16 year old", "rose was 17",
        "ask for nudes", "minors on your platform",
        "15-year-olds were hot", "child molester",
        "disseminate", "leaked", "revenge porn",
        "underage nudes", "underage sexual", "underage",
        "touch kids", "touching kids", "talking to minors",
        "sex offender", "child rapist", "child abuse", "child abuser",
        "grooming", "groomed", "grooming a minor",
        "spreading pictures", "spreading nudes", "spreading underage", 
        "underage girl", "fuck kids", "CSA material",
    ],

    # --- homophobia ---
    "homophobia": [
        "fag", "faggot", "homo", "sodamite", "sodomite",
        "gay cuck", "gay retard",
        "queer", "blowing some guy",
        "closet", "bj clip", "suck", "sucking",
        "wrapped around a cock", "cock sucker", "gay",
        "fucked in the ass", "filmed gay",
        "@NickJFuentes",
    ],
        # --- antifan ---
    "antifan": [
        "jstlk",  "Pestiny", 
    ],
    # --- Emasculation / gender-policing ---
    "emasculation": [
        "effeminate", "beta male", "beta", "soy boy", "soyboy", "soy",
        "soft palmed", "sissy", "pussy", "meek",
        
    ],
    # --- sexism / misogynistic framing ---
    "sexism": [
        "whore", "slut", "bitch", "thot",
        "bpd whore", "white knight",
        "just like a woman", "like a girl",
         "dumb cunt",
    ],
    # --- weird sex stuff ---
    "cuck": [
        "sex tape", 
        "coomer", "hiv positive", 
        "cuck", "cuckold", "cucked",
    ],
    # --- relationship mockery ---
    "relationship_mockery": [
        "open marriage", "open relationship", "wife left",
        "banging a new chick", "plow my wife",
        "other men banging", "banging his wife",
        "ex wife", "ex-wife", "ex husband", "ex-husband",
        "fuck your wife", "fuck your girlfriend", "his wife",
        "your wife", "she left",
    ],
    # --- family attacks ---
    "family_attacks": [
        "your son", "your kids", "nazi son", "your offspring",
        "raise your son", "starving your kids",
        "deadbeat dad", "absent father",
        "someone's father",
    ],
    # --- Ableist slurs ---
    "ableist_slurs": [
        "retard", "retarded", "retart", "spastic", "sperg", "sperging",
    ],
    # --- mmm ---
    "vague_insults": [
        "freak", "creep", "sex pest", "sexpest",
        "screeching", "goblin", "disgusting",
        "fucking loser", "deviant", "piece of shit",
        "scumbag", "subhuman", "gross", "pathetic", "disgrace",
        "dumbass", "dumb fuck", "dumb fucker"
        "maggot", "dog shit", "brain damage",
        "grifter", "shill", "bad faith", "bad actor", "bas actor",
        "dishonest", "liar", "fraud", "sellout",
        "hack", "clout chasing", "corrupt",
        "hypocrite", "hypocritical",
        "net negative", "waste of time", "moran",
        "deluded", "unhinged", "deranged", "mentally ill",
        "insane", "psycho", "narcissist", "sociopath",
        "lost your mind", "brain rot",
        "manlet", "midget", "midgeet", "ugly", "fat", "broccoli",
        "stained t shirt", "looking like mankind", "fugly",
        "fragile little face", "attention whore",
        "nazi", "white supremacist", "white supremacy", "racist",
        "sexist", "misogynist", "misogyny", "homophobe", "homophobia",
        "transphobe", "transphobia", "islamophobe", "islamophobia", 
        "strange", "embarrassing", "cringe", "cringey", "cringy", "weird", "awkward",
        "nutjob", "nut job", "nut-job", "weirdo", "creepy", "creep",
        "stupid", "idiot", "imbecile", "moron", "dunce", "dumb", "stupidest", "dumbest",
    ],
    # --- Threats / violence ---
    "death_violence": [
        "beat your ass", "end yourself", "kill yourself", "kys",
        "line us up", "should be shot", "deserves to die",
        "hope you die", "neck yourself",
        "death threat",
    ],
    # --- end the stigma (balls) ---
    "stimulant_abuse": [
        "addict", "meth", "drug addict", "junkie",
        "crackhead", "coke addict", "snorting",
        "meth mouth", "meth abusing", 
        "adderall", "adderal", "vyvanse", "ritalin",
        "jaw", "tweaking", "tweaker",
    ],

    # --- (((them))) ---
    "zionism": [
        "zionist", "hasbara",
        "paid shill for israel", "shill for israel",
        "aipac",
        "genocide", "supporting genocide", "genocide loving",
        "genocide supporter", "genocide apologist",
        "ethnic cleansing", "pro genocide", "team genocide",
        "genocide tourism", "war crime",
        "israel", "pro israel", "pro-israel",
        "idf",
        "Norman Finkelstein",
    ],

    # --- Antisemautism ---
    "antisemitism": [
        "the jews", "goyim", "goi", "globalist",
        "ubermensch",
        "jew", "kike", "yid", "heeb",  
    ],

    # --- gusano ---
    "gusano": [
        "gusano",
    ],
}

BORDERLINE_KEYWORDS: dict[str, list[str]] = {

    "sex-stuff": [
        "sex tape", "cock", "dick", "cum", "getting railed",
    ],
}


# ---------------------------------------------------------------------------
# Matching — plural-safe (prefix word boundary only)
# ---------------------------------------------------------------------------

def _match_keywords(text: str, kw_dict: dict[str, list[str]]) -> list[str]:
    """Match keywords with leading word boundary only (handles plurals/suffixes)."""
    text_lower = text.lower()
    matched = []
    for category, keywords in kw_dict.items():
        for kw in keywords:
            # \b at start only — so "sex pest" matches "sex pests", 
            # "retard" matches "retards"/"retarded", etc.
            pattern = r"\b" + re.escape(kw.lower())
            if re.search(pattern, text_lower):
                matched.append(category)
                break
    return matched


def classify_tweet(text: str | None) -> tuple[str, list[str]]:
    if text is None:
        return "image_only", []

    harassment_cats = _match_keywords(text, HARASSMENT_KEYWORDS)
    if harassment_cats:
        return "harassment", harassment_cats

    borderline_cats = _match_keywords(text, BORDERLINE_KEYWORDS)
    if borderline_cats:
        return "borderline", borderline_cats

    text_lower = text.lower()
    for sig in NOT_HARASSMENT_SIGNALS:
        if sig in text_lower:
            return "not_harassment", []

    if len(text.split()) <= 30:
        return "not_harassment", []

    return "unclassified", []


# ---------------------------------------------------------------------------
# I/O — same as v1
# ---------------------------------------------------------------------------

def load_results(path):
    records = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                records[str(obj["tweet_id"])] = obj
            except (json.JSONDecodeError, KeyError):
                pass
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_path = os.path.join(script_dir, "results.json")
    classified_out = os.path.join(script_dir, "classified_v3.csv")

    fieldnames = [
        "tweet_id", "handle", "url", "tier", "categories",
        "text", "manual_tier", "manual_category",
    ]

    rows_out = []
    tier_counts = Counter()
    cat_counts = Counter()

    with open(results_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if obj.get("is_deleted"):
                tier = "deleted"
                row_out = {k: "" for k in fieldnames}
                row_out["tweet_id"] = str(obj.get("tweet_id", ""))
                row_out["handle"] = obj.get("handle", "")
                row_out["url"] = obj.get("url", "")
                row_out["tier"] = tier
                rows_out.append(row_out)
                tier_counts[tier] += 1
                continue

            if obj.get("is_suspended"):
                tier = "suspended"
                row_out = {k: "" for k in fieldnames}
                row_out["tweet_id"] = str(obj.get("tweet_id", ""))
                row_out["handle"] = obj.get("handle", "")
                row_out["url"] = obj.get("url", "")
                row_out["tier"] = tier
                rows_out.append(row_out)
                tier_counts[tier] += 1
                continue

            text = (obj.get("text") or "").strip()
            tier, cats = classify_tweet(text if text else None)
            row_out = {k: "" for k in fieldnames}
            row_out["tweet_id"] = str(obj.get("tweet_id", ""))
            row_out["handle"] = obj.get("handle", "")
            row_out["url"] = obj.get("url", "")
            row_out["text"] = text
            row_out["tier"] = tier
            row_out["categories"] = "|".join(cats)
            rows_out.append(row_out)
            tier_counts[tier] += 1
            for c in cats:
                cat_counts[c] += 1

    with open(classified_out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows_out:
            writer.writerow(row)

    print("\n=== Classification summary (v3) ===")
    print(f"Total rows: {len(rows_out)}")
    for t in sorted(tier_counts, key=tier_counts.get, reverse=True):
        print(f"  {t:<30} {tier_counts[t]}")
    print(f"\nCategory hits:")
    for c in sorted(cat_counts, key=cat_counts.get, reverse=True):
        print(f"  {c:<30} {cat_counts[c]}")


if __name__ == "__main__":
    main()
