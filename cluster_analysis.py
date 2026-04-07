import os
import csv
from collections import Counter
from itertools import combinations

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

CSV_FILE = 'classified_v3.csv'
XLSX_FILE = 'classified_v3_csv.xlsx'
OUTPUT_FILE = 'cluster_analysis.md'

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
    'antisemitism':         'antisemautism*',
    'stimulant_abuse':      'stimulant abuse',
    'relationship_mockery': 'relationship mockery',
    'family_attacks':       'family attacks',
    'sexism':               'sexism',
    'emasculation':         'emasculation',
    'death_violence':       'minecraft / threats',
    'antifan':              'antifan',
    'racism':               'racism',
    'gusano':               'gusano',
    'transphobia':          'transphobia',
}

CATEGORY_ORDER = list(DISPLAY_NAMES.keys())

EXCLUDE_FROM_CLUSTERS = {'vague_insults'}


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
        if os.path.exists(XLSX_FILE):
            df = pd.read_excel(XLSX_FILE, dtype=str)
            return df.to_dict('records')
        if os.path.exists(CSV_FILE):
            df = pd.read_csv(CSV_FILE, encoding='utf-8', encoding_errors='replace', dtype=str)
            return df.to_dict('records')
    with open(CSV_FILE, encoding='utf-8', errors='replace', newline='') as f:
        return list(csv.DictReader(f))


def dn(cat):
    return DISPLAY_NAMES.get(cat, cat)


def fp(n, d, decimals=1):
    if d == 0:
        return '0%'
    v = n / d * 100
    return f'{round(v)}%' if decimals == 0 else f'{v:.{decimals}f}%'


def build_cluster_lines(rows):
    post_cat_sets = []
    for row in rows:
        eff_tier, cats = effective_tier_cats(
            row.get('tier', ''),
            row.get('categories', ''),
            row.get('manual_category', ''),
        )
        if eff_tier == 'harassment':
            known = [c for c in cats if c in DISPLAY_NAMES and c not in EXCLUDE_FROM_CLUSTERS]
            seen = set()
            deduped = []
            for c in known:
                if c not in seen:
                    seen.add(c)
                    deduped.append(c)
            post_cat_sets.append(frozenset(deduped))

    total_harassment = len(post_cat_sets)

    cat_counts = Counter()
    for s in post_cat_sets:
        for c in s:
            cat_counts[c] += 1

    overlap_counts = {2: 0, 3: 0, '4+': 0}
    for s in post_cat_sets:
        n = len(s)
        if n == 2:
            overlap_counts[2] += 1
        elif n == 3:
            overlap_counts[3] += 1
        elif n >= 4:
            overlap_counts['4+'] += 1

    pair_counts = Counter()
    for s in post_cat_sets:
        cats = sorted(s)
        for a, b in combinations(cats, 2):
            pair_counts[(a, b)] += 1

    filtered_pairs = [(pair, cnt) for pair, cnt in pair_counts.items() if cnt >= 3]
    filtered_pairs.sort(key=lambda x: -x[1])

    combo_counts = Counter()
    for s in post_cat_sets:
        if len(s) >= 2:
            ordered = sorted(s, key=lambda c: CATEGORY_ORDER.index(c) if c in CATEGORY_ORDER else 999)
            label = ' + '.join(dn(c) for c in ordered)
            combo_counts[label] += 1

    top_combos = combo_counts.most_common(15)

    lines = [
        '### cluster analysis',
        '#### multiple tags',
        f'of {total_harassment} posts deemed harassment, {sum(overlap_counts.values())} hit multiple categories',
        '',
        '| # of tags | number | % of harassment posts |',
        '|---|---|---|',
        f'| exactly 2 | {overlap_counts[2]} | {fp(overlap_counts[2], total_harassment)} |',
        f'| exactly 3 | {overlap_counts[3]} | {fp(overlap_counts[3], total_harassment)} |',
        f'| 4 or more | {overlap_counts["4+"]} | {fp(overlap_counts["4+"], total_harassment)} |',
        '#### co-occurrence matrix',
        '_pairs that appear >3 times._',
        '',
        '| tag A | tag B | co-occurrences | % of A | % of B |',
        '|---|---|---|---|---|',
    ]

    for (a, b), cnt in filtered_pairs:
        pct_a = fp(cnt, cat_counts[a])
        pct_b = fp(cnt, cat_counts[b])
        lines.append(f'| {dn(a)} | {dn(b)} | {cnt} | {pct_a} | {pct_b} |')

    lines += [
        '',
        '#### most common tag combinations',
        '| Combination | Count | % of harassment |',
        '|---|---|---|',
    ]

    for combo, cnt in top_combos:
        lines.append(f'| {combo} | {cnt} | {fp(cnt, total_harassment)} |')

    return lines


def main():
    rows = read_rows()

    # Collect per-post known category sets for harassment posts only
    post_cat_sets = []
    for row in rows:
        eff_tier, cats = effective_tier_cats(
            row.get('tier', ''),
            row.get('categories', ''),
            row.get('manual_category', ''),
        )
        if eff_tier == 'harassment':
            known = [c for c in cats if c in DISPLAY_NAMES and c not in EXCLUDE_FROM_CLUSTERS]
            # deduplicate while preserving order
            seen = set()
            deduped = []
            for c in known:
                if c not in seen:
                    seen.add(c)
                    deduped.append(c)
            post_cat_sets.append(frozenset(deduped))

    total_harassment = len(post_cat_sets)

    # Per-category counts
    cat_counts = Counter()
    for s in post_cat_sets:
        for c in s:
            cat_counts[c] += 1

    # ── Section 1: High-overlap posts ────────────────────────────────────────
    overlap_counts = {2: 0, 3: 0, '4+': 0}
    for s in post_cat_sets:
        n = len(s)
        if n == 2:
            overlap_counts[2] += 1
        elif n == 3:
            overlap_counts[3] += 1
        elif n >= 4:
            overlap_counts['4+'] += 1

    # ── Section 2: Co-occurrence matrix ──────────────────────────────────────
    pair_counts = Counter()
    for s in post_cat_sets:
        cats = sorted(s)
        for a, b in combinations(cats, 2):
            pair_counts[(a, b)] += 1

    # Filter pairs with co-occurrence >= 3
    filtered_pairs = [(pair, cnt) for pair, cnt in pair_counts.items() if cnt >= 3]
    filtered_pairs.sort(key=lambda x: -x[1])

    # ── Section 3: Most common tag combinations ───────────────────────────────
    combo_counts = Counter()
    for s in post_cat_sets:
        if len(s) >= 2:
            # Sort by CATEGORY_ORDER for consistent display
            ordered = sorted(s, key=lambda c: CATEGORY_ORDER.index(c) if c in CATEGORY_ORDER else 999)
            label = ' + '.join(dn(c) for c in ordered)
            combo_counts[label] += 1

    top_combos = combo_counts.most_common(15)

    # ── Build output ──────────────────────────────────────────────────────────
    lines = [
        '# Cluster Analysis',
        '',
        f'_Based on {total_harassment} harassment posts._',
        '',
        '---',
        '',
        '## 1. High-overlap posts',
        '',
        'Posts tagged with multiple categories simultaneously.',
        '',
        '| Tag count | Posts | % of harassment |',
        '|---|---|---|',
        f'| exactly 2 | {overlap_counts[2]} | {fp(overlap_counts[2], total_harassment)} |',
        f'| exactly 3 | {overlap_counts[3]} | {fp(overlap_counts[3], total_harassment)} |',
        f'| 4 or more | {overlap_counts["4+"]} | {fp(overlap_counts["4+"], total_harassment)} |',
        '',
        '---',
        '',
        '## 2. Co-occurrence matrix',
        '',
        '_Pairs that appear together at least 3 times. "% of A" = what share of category A\'s posts also carry category B (and vice versa)._',
        '',
        '| Category A | Category B | Co-occurrences | % of A | % of B |',
        '|---|---|---|---|---|',
    ]

    for (a, b), cnt in filtered_pairs:
        pct_a = fp(cnt, cat_counts[a])
        pct_b = fp(cnt, cat_counts[b])
        lines.append(f'| {dn(a)} | {dn(b)} | {cnt} | {pct_a} | {pct_b} |')

    lines += [
        '',
        '---',
        '',
        '## 3. Most common tag combinations',
        '',
        '_Top 15 exact multi-category combinations (single-category posts excluded)._',
        '',
        '| Combination | Count | % of harassment |',
        '|---|---|---|',
    ]

    for combo, cnt in top_combos:
        lines.append(f'| {combo} | {cnt} | {fp(cnt, total_harassment)} |')

    output = '\n'.join(lines)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(output)

    print(output)


if __name__ == '__main__':
    main()
