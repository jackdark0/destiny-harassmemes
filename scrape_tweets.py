"""
scrape_tweets.py
----------------
Scrapes tweet content from a CSV of URLs using Playwright (headed or headless).
Designed for overnight runs: rate-limited, resumable, and fault-tolerant.

SETUP:
    pip install playwright pandas
    playwright install chromium

USAGE:
    python scrape_tweets.py --input destiny_harassment_tweets.csv --output results.json

    # To resume an interrupted run (skips already-scraped tweet IDs):
    python scrape_tweets.py --input destiny_harassment_tweets.csv --output results.json --resume

    # To run headless (no browser window):
    python scrape_tweets.py --input destiny_harassment_tweets.csv --output results.json --headless

    # Override rate-limit detection thresholds:
    python scrape_tweets.py --input ... --output ... --no-content-threshold 8 --cooldown 900

NOTES:
    - You need to be logged into X in the Chromium profile this script uses.
      On first run, it will open a browser window. Log in manually, then
      press Enter in the terminal to begin scraping.
    - Results are written incrementally (after every tweet), so a crash
      won't lose your progress. Use --resume to pick up where you left off.
    - no_content results are NOT written to output so they will be retried
      automatically on resume.
    - If N consecutive no_content results are detected, the script pauses
      for a cooldown period then resumes automatically.
"""

import argparse
import json
import os
import random
import time
import csv
from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# ── Config ────────────────────────────────────────────────────────────────────
DELAY_MIN  = 8.0     # seconds between requests (min)
DELAY_MAX  = 15.0    # seconds between requests (max)
BREATHE_EVERY = 20   # take a longer pause every N tweets
BREATHE_MIN   = 45   # seconds for the longer pause (min)
BREATHE_MAX   = 90   # seconds for the longer pause (max)

NO_CONTENT_THRESHOLD = 5      # consecutive no_content hits before pausing
COOLDOWN_SECONDS     = 1000    # 30 minutes default cooldown

TIMEOUT   = 20_000    # ms to wait for page elements
USER_DATA_DIR = "./x_browser_profile"  # persists login session between runs
# ─────────────────────────────────────────────────────────────────────────────


def load_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_existing(output_path: str) -> set[str]:
    """Return set of tweet_ids already scraped."""
    if not Path(output_path).exists():
        return set()
    seen = set()
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                seen.add(str(obj.get("tweet_id", "")))
            except json.JSONDecodeError:
                pass
    return seen


def append_result(output_path: str, result: dict):
    """Append a single result as a JSON line (NDJSON format)."""
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(result, ensure_ascii=False) + "\n")


def scrape_tweet(page, url: str, tweet_id: str, handle: str) -> dict:
    """
    Navigate to a tweet and extract available data.
    Returns a result dict regardless of success/failure.
    """
    result = {
        "tweet_id":    tweet_id,
        "handle":      handle,
        "url":         url,
        "scraped_at":  datetime.utcnow().isoformat(),
        "status":      "error",
        "error":       None,
        "text":        None,
        "timestamp":   None,
        "display_name": None,
        "reply_count":  None,
        "repost_count": None,
        "like_count":   None,
        "is_deleted":   False,
        "is_suspended": False,
    }

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
        time.sleep(1.5)  # let JS render

        # ── Find the article for this specific tweet ───────────────────────
        # Do this first — replies may show "This post is unavailable" for the
        # parent context above, which would falsely trigger deleted detection.
        article = page.query_selector(f'article:has(a[href*="/status/{tweet_id}"])')

        if not article:
            # Target tweet didn't load — check page text to find out why
            page_text = page.inner_text("body").lower()

            if any(x in page_text for x in [
                "this post is unavailable",
                "this post was deleted",
                "tweet has been deleted",
            ]):
                result["status"] = "deleted"
                result["is_deleted"] = True
                return result

            if any(x in page_text for x in [
                "account suspended",
                "this account has been suspended",
                "this post is from a suspended account",
            ]):
                result["status"] = "suspended"
                result["is_suspended"] = True
                return result

            if "this page doesn't exist" in page_text:
                result["status"] = "not_found"
                result["is_deleted"] = True
                return result

            if "something went wrong" in page_text or "try again" in page_text:
                result["status"] = "error"
                result["error"] = "X returned an error page"
                return result

        scope = article if article else page

        # ── Tweet text ─────────────────────────────────────────────────────
        text_el = scope.query_selector('[data-testid="tweetText"]')
        if text_el:
            result["text"] = text_el.inner_text()
        elif scope.query_selector("time"):
            # Photo/video-only tweet — no text but the tweet loaded fine
            result["text"] = None
        else:
            result["status"] = "no_content"
            result["error"] = "tweetText element not found"
            return result

        # ── Timestamp ──────────────────────────────────────────────────────
        time_el = scope.query_selector("time")
        if time_el:
            result["timestamp"] = time_el.get_attribute("datetime")

        # ── Display name ───────────────────────────────────────────────────
        name_el = scope.query_selector('[data-testid="User-Name"]')
        if name_el:
            result["display_name"] = name_el.inner_text().split("\n")[0]

        # ── Engagement counts ──────────────────────────────────────────────
        # These are inside aria-label attributes on the action buttons
        for testid, field in [
            ("reply",    "reply_count"),
            ("retweet",  "repost_count"),
            ("like",     "like_count"),
        ]:
            el = scope.query_selector(f'[data-testid="{testid}"]')
            if el:
                label = el.get_attribute("aria-label") or ""
                # aria-label is like "42 replies" or "1,204 likes"
                parts = label.strip().split()
                if parts and parts[0].replace(",", "").isdigit():
                    result[field] = int(parts[0].replace(",", ""))

        result["status"] = "ok"

    except PlaywrightTimeout:
        result["status"] = "timeout"
        result["error"] = f"Timed out loading {url}"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result


def cooldown_pause(seconds: int):
    """Sleep for `seconds`, printing a countdown every minute."""
    print(f"\n⏸  Rate-limit cooldown: pausing for {seconds // 60}m {seconds % 60}s ...")
    end = time.time() + seconds
    while True:
        remaining = end - time.time()
        if remaining <= 0:
            break
        mins, secs = divmod(int(remaining), 60)
        print(f"   Resuming in {mins}m {secs:02d}s ...", end="\r", flush=True)
        time.sleep(min(30, remaining))
    print("\n▶  Cooldown complete, resuming.\n")


def main():
    parser = argparse.ArgumentParser(description="Scrape tweets from a CSV of URLs.")
    parser.add_argument("--input",    required=True,  help="Input CSV (handle, tweet_id, url)")
    parser.add_argument("--output",   required=True,  help="Output NDJSON file")
    parser.add_argument("--resume",   action="store_true", help="Skip already-scraped tweet IDs")
    parser.add_argument("--headless", action="store_true", help="Run browser headlessly")
    parser.add_argument(
        "--no-content-threshold", type=int, default=NO_CONTENT_THRESHOLD,
        help=f"Consecutive no_content hits before pausing (default: {NO_CONTENT_THRESHOLD})"
    )
    parser.add_argument(
        "--cooldown", type=int, default=COOLDOWN_SECONDS,
        help=f"Cooldown pause in seconds when threshold hit (default: {COOLDOWN_SECONDS})"
    )
    args = parser.parse_args()

    rows = load_csv(args.input)
    print(f"Loaded {len(rows)} tweets from {args.input}")

    already_done = set()
    if args.resume:
        already_done = load_existing(args.output)
        print(f"Resuming: {len(already_done)} already scraped, "
              f"{len(rows) - len(already_done)} remaining")

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=USER_DATA_DIR,
            headless=args.headless,
            executable_path=r"C:\Users\User\AppData\Local\BraveSoftware\Brave-Browser\Application\brave.exe",
            args=["--disable-blink-features=AutomationControlled"],
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = browser.new_page()

        # ── First-run login prompt ─────────────────────────────────────────
        if not args.headless:
            page.goto("https://x.com/login")
            print("\n" + "="*60)
            print("If you're not logged in, do so now in the browser window.")
            print("When you're logged in and see your X feed, press Enter here.")
            print("="*60)
            input()

        # ── Main scrape loop ───────────────────────────────────────────────
        ok = deleted = suspended = errors = skipped = 0
        consecutive_no_content = 0
        tweets_since_breathe   = 0

        for i, row in enumerate(rows):
            tweet_id = str(row["tweet_id"])
            handle   = row["handle"]
            url      = row["url"]

            if tweet_id in already_done:
                skipped += 1
                continue

            result = scrape_tweet(page, url, tweet_id, handle)
            s = result["status"]

            # ── no_content: don't save, track streak ──────────────────────
            if s == "no_content":
                consecutive_no_content += 1
                print(
                    f"[{i+1}/{len(rows)}] @{handle} — no_content "
                    f"(streak: {consecutive_no_content}/{args.no_content_threshold})"
                )
                if consecutive_no_content >= args.no_content_threshold:
                    print(
                        f"\n🚨 {consecutive_no_content} consecutive no_content results — "
                        f"likely rate-limited."
                    )
                    cooldown_pause(args.cooldown)
                    consecutive_no_content = 0  # reset after cooldown
                # Do NOT write to output — will be retried on resume
                # Still apply a short delay before next attempt
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                continue

            # ── Successful or terminal result — write it ───────────────────
            consecutive_no_content = 0  # reset streak on any real result
            append_result(args.output, result)

            if s == "ok":          ok += 1
            elif s == "deleted":   deleted += 1
            elif s == "suspended": suspended += 1
            else:                  errors += 1

            total_done = ok + deleted + suspended + errors + len(already_done)
            print(
                f"[{total_done}/{len(rows)}] @{handle} — {s}"
                + (f": {result['text'][:70]!r}..." if s == "ok" and result["text"] else "")
            )

            # ── Breathing pause every N tweets ────────────────────────────
            tweets_since_breathe += 1
            if tweets_since_breathe >= BREATHE_EVERY:
                breathe = random.uniform(BREATHE_MIN, BREATHE_MAX)
                print(f"\n😮‍💨 Breathing pause: {breathe:.0f}s ...\n")
                time.sleep(breathe)
                tweets_since_breathe = 0
            else:
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        browser.close()

    print("\n" + "="*60)
    print(f"Done!")
    print(f"  OK:        {ok}")
    print(f"  Deleted:   {deleted}")
    print(f"  Suspended: {suspended}")
    print(f"  Errors:    {errors}")
    print(f"  Skipped:   {skipped} (already done)")
    print(f"Results written to: {args.output}")
    print("="*60)


if __name__ == "__main__":
    main()