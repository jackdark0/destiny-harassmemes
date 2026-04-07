import csv
from collections import Counter
from cluster_analysis import build_cluster_lines

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

XLSX_FILE = 'classified_v3_csv.xlsx'
CSV_FILE = 'classified_v3.csv'
OUTPUT_FILE = r'C:\Users\User\Documents\research-memes\misc-cringe\harassment_sumary-jarvis.md'

ORIGINAL_SOURCE = (
    'https://docs.google.com/document/d/e/'
    '2PACX-1vT1h223eGlE0zskYe8IwUxtgDcrHetkVz9-JmJ2oSnNmGVUg54c2bcBrkFItnEMFCzZ0F9grWQEtvzH/pub'
)

ALIASES = {
    'sexual_degredation': 'sexual_degradation',
    'relationshop_mockery': 'relationship_mockery',
    'death_threats': 'death_violence',
}

DISPLAY_NAMES = {
    'vague_insults':        'vague insults',
    'court_memes':          'court memes / pedo',
    'zionism':              'zionism / genocide',
    'homophobia':           'homophobia',
    'sexual_degradation':   'weird sex stuff',
    'ableist_slurs':        'ableism',
    'antisemitism':         'antisemautism',
    'stimulant_abuse':      'stimulant abuse',
    'relationship_mockery': 'relationship mockery',
    'family_attacks':       'family attacks',
    'sexism':               'sexism',
    'emasculation':         'emasculation',
    'death_violence':       'rope / threats',
    'antifan':              'antifan',
    'racism':               'racism',
    'gusano':               'gusano',
    'transphobia':          'transphobia',
}

CATEGORY_ORDER = list(DISPLAY_NAMES.keys())


def clean(v):
    if v is None:
        return ''
    s = str(v).strip()
    return '' if s.lower() == 'nan' else s


def normalize_category(cat):
    cat = cat.strip().replace(' ', '_')
    return ALIASES.get(cat, cat)


def parse_categories(raw):
    raw = clean(raw)
    if not raw:
        return []
    return [normalize_category(p) for p in raw.split('|') if p.strip()]


def effective_tier_cats(tier, categories, manual_category):
    """
    Rules (in order):
      1. manual_category == 'not_harassment'  → (not_harassment, [])
      2. manual_category non-empty            → (harassment, cats from manual_category)
      3. manual_category empty                → (tier col value, cats from categories col)
    """
    tier = clean(tier)
    mc = clean(manual_category)

    if mc.lower() == 'not_harassment':
        return 'not_harassment', []
    elif mc:
        return 'harassment', parse_categories(mc)
    else:
        return tier, parse_categories(categories)


def read_rows():
    if HAS_PANDAS:
        try:
            df = pd.read_excel(XLSX_FILE, dtype=str)
            return df.to_dict('records')
        except FileNotFoundError:
            pass
        try:
            df = pd.read_csv(CSV_FILE, encoding='utf-8', encoding_errors='replace', dtype=str)
            return df.to_dict('records')
        except FileNotFoundError:
            pass
    with open(CSV_FILE, encoding='utf-8', errors='replace', newline='') as f:
        return list(csv.DictReader(f))


def fp(n, d, decimals=1):
    if d == 0:
        return '0%'
    v = n / d * 100
    return f'{round(v)}%' if decimals == 0 else f'{v:.{decimals}f}%'


def main():
    rows = read_rows()
    total = len(rows)

    handles = set()
    tier_counts = Counter()
    category_counts = Counter()
    multi_cat_rows = 0

    for row in rows:
        handle = clean(row.get('handle', ''))
        if handle:
            handles.add(handle)

        eff_tier, cats = effective_tier_cats(
            row.get('tier', ''),
            row.get('categories', ''),
            row.get('manual_category', ''),
        )

        if clean(row.get('manual_category', '')).lower() == 'not_harassment':
            tier_counts['explicit_not_harassment'] += 1

        if eff_tier == 'harassment':
            tier_counts['harassment'] += 1
            known = [c for c in cats if c in DISPLAY_NAMES]
            if len(known) > 1:
                multi_cat_rows += 1
            for c in known:
                category_counts[c] += 1
        elif eff_tier in ('suspended', 'deleted', 'unclassified'):
            tier_counts[eff_tier] += 1
        elif eff_tier == 'not_harassment':
            tier_counts['not_harassment'] += 1
        elif eff_tier == 'borderline':
            tier_counts['borderline'] += 1
        else:
            # image_only, empty tier, unknown → not harassment
            tier_counts['not_harassment'] += 1

    unique_accounts = len(handles)
    harassment_count = tier_counts['harassment']
    explicit_not_harassment_count = tier_counts['explicit_not_harassment']
    not_harassment_count = tier_counts['not_harassment'] - explicit_not_harassment_count
    borderline_count = tier_counts['borderline']
    unclassified_count = tier_counts['unclassified']
    suspended_count = tier_counts['suspended']
    deleted_count = tier_counts['deleted']
    sorted_cats = sorted(
        [(cat, category_counts[cat]) for cat in CATEGORY_ORDER if category_counts[cat] > 0],
        key=lambda x: -x[1],
    )

    lines = [
        '## results',
        '### overview',
        '|thing|number|%|',
        '|---|---|',
        f'|scraped posts|{total}|',
        f'|unique accounts|{unique_accounts} ({fp(unique_accounts, total, 0)})|',
        f'|definitely harassment|{harassment_count}|{fp(harassment_count, total)}|',
        f'|maybe harassment|{borderline_count}|{fp(borderline_count, total)}|',
        f'|definitely not harassment|{explicit_not_harassment_count}|{fp(explicit_not_harassment_count, total)}|',
        f'|probably not harassment / unclassified|{not_harassment_count + unclassified_count}|{fp(not_harassment_count + unclassified_count, total)}|',
        f'|suspended / deleted|{suspended_count + deleted_count}|{fp(suspended_count + deleted_count, total)}|',
        '### categories',
        '|tag|#|%|',
        '|---|---|---|',
    ]

    for cat, count in sorted_cats:
        lines.append(f'|{DISPLAY_NAMES[cat]}|{count}|{fp(count, harassment_count, 0)}|')

    lines += build_cluster_lines(rows)

    output = '\n'.join(lines)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(output)

    print(output)


if __name__ == '__main__':
    main()
