"""
Analyze cached Untappd checkin data to detect weekly tap takeovers at
Hotel Sweeneys (Thursdays).

Looks at checkins on Thursdays and Fridays each week, groups by brewery,
and identifies the dominant brewery — likely the tap takeover guest.

Usage:
    python analyze_takeovers.py
    python analyze_takeovers.py --min-checkins 3
    python analyze_takeovers.py --output csv
    python analyze_takeovers.py --output json
"""

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

CACHE_FILE = Path(__file__).parent / "checkins_cache.json"
OUTPUT_DIR = Path(__file__).parent / "output"


def load_checkins():
    if not CACHE_FILE.exists():
        print("ERROR: No cached checkins found. Run fetch_checkins.py first.")
        sys.exit(1)
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("checkins", [])


def parse_date(date_str):
    """Parse Untappd date format: 'Sat, 01 Jan 2022 12:00:00 +0000'"""
    try:
        return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
    except ValueError:
        return None


def get_thursday_week_key(dt):
    """
    Get a week key based on the Thursday of that week.
    Thursday = weekday 3. If it's Friday (4), go back 1 day to the Thursday.
    """
    weekday = dt.weekday()
    if weekday == 3:  # Thursday
        thursday = dt.date()
    elif weekday == 4:  # Friday
        thursday = (dt - timedelta(days=1)).date()
    else:
        # Shouldn't happen if we filter correctly, but handle gracefully
        days_since_thursday = (weekday - 3) % 7
        thursday = (dt - timedelta(days=days_since_thursday)).date()
    return thursday


def detect_takeovers(checkins, min_checkins=3, min_ratio=0.4):
    """
    Detect tap takeovers by analyzing Thursday/Friday checkin patterns.

    A tap takeover is detected when:
    - A single brewery has >= min_checkins unique checkins on a Thu/Fri
    - That brewery represents >= min_ratio of all checkins that day/week
    - The brewery is NOT the venue's usual house taps

    Returns a list of detected takeovers sorted by date.
    """
    # Filter to Thursday (3) and Friday (4) checkins only
    thu_fri_checkins = []
    for c in checkins:
        dt = parse_date(c.get("created_at", ""))
        if dt and dt.weekday() in (3, 4):
            c["_parsed_date"] = dt
            thu_fri_checkins.append(c)

    if not thu_fri_checkins:
        print("No Thursday/Friday checkins found in the data.")
        return []

    print(f"Found {len(thu_fri_checkins)} Thursday/Friday checkins to analyze.\n")

    # Group checkins by week (keyed by Thursday date)
    weeks = defaultdict(list)
    for c in thu_fri_checkins:
        week_key = get_thursday_week_key(c["_parsed_date"])
        weeks[week_key] = weeks.get(week_key, [])
        weeks[week_key].append(c)

    # First pass: count overall brewery frequency to identify "house" breweries
    all_brewery_counts = Counter()
    for c in checkins:
        brewery = c.get("brewery_name", "Unknown")
        all_brewery_counts[brewery] += 1

    total_checkins = len(checkins)
    # A brewery appearing in > 15% of ALL checkins is likely a house tap
    house_breweries = set()
    for brewery, count in all_brewery_counts.items():
        if total_checkins > 0 and count / total_checkins > 0.15:
            house_breweries.add(brewery)

    if house_breweries:
        print(f"Detected likely house breweries (>15% of all checkins):")
        for hb in sorted(house_breweries):
            pct = all_brewery_counts[hb] / total_checkins * 100
            print(f"  - {hb} ({pct:.1f}%)")
        print()

    # Analyze each week
    takeovers = []
    for thursday_date in sorted(weeks.keys()):
        week_checkins = weeks[thursday_date]
        brewery_counter = Counter()
        brewery_beers = defaultdict(set)
        brewery_checkin_details = defaultdict(list)
        week_events = {}  # event_name -> {brewery, beers, count, url}

        for c in week_checkins:
            brewery = c.get("brewery_name", "Unknown")
            beer = c.get("beer_name", "Unknown")
            brewery_counter[brewery] += 1
            brewery_beers[brewery].add(beer)
            brewery_checkin_details[brewery].append({
                "beer": beer,
                "style": c.get("beer_style", ""),
                "user": c.get("user", ""),
                "rating": c.get("rating", 0),
                "date": c.get("created_at", ""),
            })

            # Track event data — strongest signal for a takeover
            event_name = c.get("event_name", "")
            if event_name:
                if event_name not in week_events:
                    week_events[event_name] = {
                        "breweries": Counter(),
                        "beers": set(),
                        "count": 0,
                        "url": c.get("event_url", ""),
                        "event_id": c.get("event_id"),
                    }
                week_events[event_name]["breweries"][brewery] += 1
                week_events[event_name]["beers"].add(beer)
                week_events[event_name]["count"] += 1

        total_week = sum(brewery_counter.values())
        if total_week < 2:
            continue

        # Priority 1: If there's an event, use that as the takeover signal
        if week_events:
            # Pick the event with the most checkins
            best_event = max(week_events.items(), key=lambda x: x[1]["count"])
            event_name = best_event[0]
            event_info = best_event[1]
            # The dominant brewery in the event
            top_brewery = event_info["breweries"].most_common(1)[0][0]
            takeovers.append({
                "date": thursday_date.isoformat(),
                "brewery": top_brewery,
                "checkins": event_info["count"],
                "unique_beers": len(event_info["beers"]),
                "total_checkins_that_week": total_week,
                "share_pct": round(event_info["count"] / total_week * 100, 1),
                "beers": sorted(event_info["beers"]),
                "event_name": event_name,
                "event_url": event_info["url"],
                "event_id": event_info["event_id"],
                "source": "event",
                "details": brewery_checkin_details.get(top_brewery, []),
            })
            continue

        # Priority 2: Fallback to brewery dominance heuristic
        for brewery, count in brewery_counter.most_common():
            if brewery in house_breweries:
                continue

            ratio = count / total_week
            unique_beers = len(brewery_beers[brewery])

            if count >= min_checkins and ratio >= min_ratio:
                takeovers.append({
                    "date": thursday_date.isoformat(),
                    "brewery": brewery,
                    "checkins": count,
                    "unique_beers": unique_beers,
                    "total_checkins_that_week": total_week,
                    "share_pct": round(ratio * 100, 1),
                    "beers": sorted(brewery_beers[brewery]),
                    "source": "heuristic",
                    "details": brewery_checkin_details[brewery],
                })
                break  # Only take the top non-house brewery per week
            elif count >= min_checkins and unique_beers >= 3:
                # Multiple unique beers from same brewery is a strong signal
                takeovers.append({
                    "date": thursday_date.isoformat(),
                    "brewery": brewery,
                    "checkins": count,
                    "unique_beers": unique_beers,
                    "total_checkins_that_week": total_week,
                    "share_pct": round(ratio * 100, 1),
                    "beers": sorted(brewery_beers[brewery]),
                    "source": "heuristic",
                    "details": brewery_checkin_details[brewery],
                })
                break

    return takeovers


def print_takeovers(takeovers):
    """Print a formatted table of detected takeovers."""
    if not takeovers:
        print("No tap takeovers detected.")
        return

    print(f"{'Date':<14} {'Brewery':<35} {'Checkins':>8} {'Beers':>6} {'Share':>7}  {'Source'}")
    print("-" * 90)

    current_year = None
    for t in takeovers:
        year = t["date"][:4]
        if year != current_year:
            if current_year is not None:
                print()
            print(f"--- {year} ---")
            current_year = year

        source = t.get("source", "")
        event_name = t.get("event_name", "")
        label = event_name if event_name else source
        print(
            f"{t['date']:<14} {t['brewery']:<35} {t['checkins']:>8} "
            f"{t['unique_beers']:>6} {t['share_pct']:>6.1f}%  {label}"
        )

    event_count = sum(1 for t in takeovers if t.get("source") == "event")
    heuristic_count = sum(1 for t in takeovers if t.get("source") == "heuristic")
    print(f"\nTotal tap takeovers detected: {len(takeovers)}")
    print(f"  From events: {event_count}  |  From checkin heuristic: {heuristic_count}")


def export_csv(takeovers, filepath):
    """Export takeovers to CSV."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Date", "Brewery", "Checkins", "Unique Beers",
            "Total Week Checkins", "Share %", "Beer List",
            "Event Name", "Event URL", "Source"
        ])
        for t in takeovers:
            writer.writerow([
                t["date"],
                t["brewery"],
                t["checkins"],
                t["unique_beers"],
                t["total_checkins_that_week"],
                t["share_pct"],
                " | ".join(t["beers"]),
                t.get("event_name", ""),
                t.get("event_url", ""),
                t.get("source", ""),
            ])
    print(f"CSV exported to {filepath}")


def export_json(takeovers, filepath):
    """Export takeovers to JSON."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    # Remove internal detail data for clean export
    clean = []
    for t in takeovers:
        entry = {k: v for k, v in t.items() if k not in ("details",)}
        clean.append(entry)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)
    print(f"JSON exported to {filepath}")


def show_weekly_breakdown(checkins):
    """
    Show ALL weeks with their brewery breakdown — useful for debugging
    or finding weeks where detection thresholds didn't trigger.
    """
    thu_fri = []
    for c in checkins:
        dt = parse_date(c.get("created_at", ""))
        if dt and dt.weekday() in (3, 4):
            c["_parsed_date"] = dt
            thu_fri.append(c)

    weeks = defaultdict(list)
    for c in thu_fri:
        week_key = get_thursday_week_key(c["_parsed_date"])
        weeks[week_key].append(c)

    print(f"\n{'Thursday':<14} {'Total':>6}  Breweries")
    print("-" * 80)
    for thursday in sorted(weeks.keys()):
        wc = weeks[thursday]
        brewery_counts = Counter(c.get("brewery_name", "?") for c in wc)
        top3 = brewery_counts.most_common(3)
        desc = ", ".join(f"{b} ({n})" for b, n in top3)
        print(f"{thursday.isoformat():<14} {len(wc):>6}  {desc}")


def main():
    parser = argparse.ArgumentParser(
        description="Detect tap takeovers from cached Untappd checkin data"
    )
    parser.add_argument(
        "--min-checkins",
        type=int,
        default=3,
        help="Minimum checkins from a brewery to count as takeover (default: 3)",
    )
    parser.add_argument(
        "--min-ratio",
        type=float,
        default=0.3,
        help="Minimum share of week's checkins for a brewery (default: 0.3)",
    )
    parser.add_argument(
        "--output",
        choices=["csv", "json", "both"],
        help="Export results to file",
    )
    parser.add_argument(
        "--breakdown",
        action="store_true",
        help="Show weekly brewery breakdown for all weeks",
    )
    args = parser.parse_args()

    checkins = load_checkins()
    print(f"Loaded {len(checkins)} checkins from cache.\n")

    if args.breakdown:
        show_weekly_breakdown(checkins)
        print()

    takeovers = detect_takeovers(
        checkins,
        min_checkins=args.min_checkins,
        min_ratio=args.min_ratio,
    )

    print_takeovers(takeovers)

    if args.output in ("csv", "both"):
        export_csv(takeovers, OUTPUT_DIR / "takeovers.csv")
    if args.output in ("json", "both"):
        export_json(takeovers, OUTPUT_DIR / "takeovers.json")


if __name__ == "__main__":
    main()
