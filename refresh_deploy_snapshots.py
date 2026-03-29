"""Refresh committed deploy snapshots for the Vercel site.

This script is designed for CI use (for example, GitHub Actions) so the
public Vercel deployment stays current without relying on a local machine.

What it does:
1. Fetches recent Untappd venue checkins (optionally reusing an existing cache)
2. Detects takeover weeks, including heuristic "secret" takeovers
3. Enriches takeover beers with labels, details, and ratings
4. Rebuilds committed deploy snapshot JSON files under ``data/``
5. Updates the inline fallback constants and build label in ``index.html``
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

import fetch_checkins
from analyze_takeovers import detect_takeovers, export_json
from server import (
    DEPLOY_ALLOWED_USERS_FILE,
    DEPLOY_BEER_INFO_FILE,
    DEPLOY_CACHE_SUMMARY_FILE,
    DEPLOY_CURRENT_EVENTS_FILE,
    DEPLOY_PAST_EVENTS_FILE,
    DEPLOY_TAKEOVERS_FILE,
    PROJECT_DIR,
    build_takeover_access_payload,
    build_past_events_from_checkins,
    build_past_events_from_takeovers,
    compute_member_results_for_takeovers,
    enrich_takeovers_with_beer_data,
    get_beer_info,
    get_cache_summary_data,
    get_combined_beer_info_lookup,
    has_usable_beer_info,
    load_json_file,
    load_members_data,
    scrape_current_events,
)

load_dotenv()

INDEX_HTML_FILE = PROJECT_DIR / "index.html"
OUTPUT_TAKEOVERS_FILE = PROJECT_DIR / "output" / "takeovers.json"


def _has_real_secret(value: str | None) -> bool:
    candidate = (value or "").strip()
    if not candidate:
        return False
    lowered = candidate.lower()
    return lowered not in {
        "changeme",
        "your_access_token_here",
        "your-client-id",
        "your-client-secret",
        "replace-me",
    }


def ensure_refresh_auth_configured():
    has_access_token = _has_real_secret(os.getenv("UNTAPPD_ACCESS_TOKEN"))
    has_client_id = _has_real_secret(os.getenv("UNTAPPD_CLIENT_ID"))
    has_client_secret = _has_real_secret(os.getenv("UNTAPPD_CLIENT_SECRET"))

    if has_access_token:
        return "UNTAPPD_ACCESS_TOKEN"
    if has_client_id and has_client_secret:
        return "UNTAPPD_CLIENT_ID/UNTAPPD_CLIENT_SECRET"

    raise RuntimeError(
        "Untappd refresh auth is not configured. Set UNTAPPD_ACCESS_TOKEN, or both "
        "UNTAPPD_CLIENT_ID and UNTAPPD_CLIENT_SECRET. In GitHub Actions, add them as repository secrets."
    )


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def collect_takeover_beer_ids(takeovers):
    beer_ids = []
    seen = set()
    for takeover in takeovers:
        for beer in takeover.get("beer_details") or []:
            beer_id = beer.get("beer_id")
            if beer_id is None:
                continue
            beer_key = str(beer_id)
            if beer_key in seen:
                continue
            seen.add(beer_key)
            beer_ids.append(beer_key)
    return beer_ids


def refresh_beer_details_for_takeovers(takeovers, skip_refresh=False):
    existing_lookup = get_combined_beer_info_lookup()
    beer_ids = collect_takeover_beer_ids(takeovers)

    if not skip_refresh:
        for beer_id in beer_ids:
            cached = existing_lookup.get(beer_id, {})
            if has_usable_beer_info(cached) and cached.get("rating_score") not in (None, ""):
                continue
            print(f"Refreshing beer info for {beer_id}...")
            get_beer_info(int(beer_id))

    combined_lookup = get_combined_beer_info_lookup()
    return {
        beer_id: combined_lookup[beer_id]
        for beer_id in beer_ids
        if beer_id in combined_lookup
    }


def fallback_json(path: Path, default):
    payload = load_json_file(path)
    return payload if payload is not None else default


def build_public_cache_summary(build_unix: int):
    summary = dict(get_cache_summary_data())
    refreshed_at = datetime.fromtimestamp(build_unix, tz=timezone.utc)
    summary["has_token"] = False
    summary["error_history"] = []
    summary["refreshed_at"] = refreshed_at.isoformat()
    summary["refreshed_at_unix"] = build_unix
    summary["build_label"] = str(build_unix)
    summary["latest_checkin_at"] = summary.get("newest_date")
    summary["refresh_source"] = "github-actions" if os.getenv("GITHUB_ACTIONS") == "true" else "manual-refresh-script"
    summary["warning_after_minutes"] = 90
    summary["stale_after_minutes"] = 180
    return summary


def strip_internal_takeover_fields(takeovers):
    cleaned = []
    for takeover in takeovers:
        cleaned.append({k: v for k, v in takeover.items() if k not in {"details"}})
    return cleaned


def replace_inline_constant(source_text: str, constant_name: str, js_literal: str) -> str:
    pattern = re.compile(
        rf"const {re.escape(constant_name)} = .*?;\n(?=(?:const|let|var|function)\b)",
        re.S,
    )
    replacement = f"const {constant_name} = {js_literal};\n"
    updated_text, replacements = pattern.subn(lambda _match: replacement, source_text, count=1)
    if replacements != 1:
        raise RuntimeError(f"Could not update inline constant {constant_name}")
    return updated_text


def update_index_inline_snapshots(takeovers, current_events, past_events, beer_info, cache_summary, build_label):
    html = INDEX_HTML_FILE.read_text(encoding="utf-8")

    html = replace_inline_constant(html, "INLINE_DEPLOY_BUILD_LABEL", json.dumps(build_label))
    html = replace_inline_constant(html, "INLINE_DEPLOY_TAKEOVERS", json.dumps(takeovers, indent=2, ensure_ascii=False))
    html = replace_inline_constant(html, "INLINE_DEPLOY_CURRENT_EVENTS", json.dumps(current_events, indent=2, ensure_ascii=False))
    html = replace_inline_constant(html, "INLINE_DEPLOY_PAST_EVENTS", json.dumps(past_events, indent=2, ensure_ascii=False))
    html = replace_inline_constant(html, "INLINE_DEPLOY_BEER_INFO", json.dumps(beer_info, indent=2, ensure_ascii=False))
    html = replace_inline_constant(html, "INLINE_DEPLOY_CACHE_SUMMARY", json.dumps(cache_summary, indent=2, ensure_ascii=False))

    INDEX_HTML_FILE.write_text(html, encoding="utf-8")
    return build_label


def refresh_snapshots(skip_fetch=False, skip_beer_refresh=False, since_date=None):
    venue_id = int(os.getenv("VENUE_ID", "107565"))
    build_unix = int(datetime.now(timezone.utc).timestamp())

    if not skip_fetch:
        auth_source = ensure_refresh_auth_configured()
        print(f"Using Untappd auth via {auth_source}.")
        print(f"Fetching latest checkins for venue {venue_id}...")
        fetch_checkins.fetch_checkins(venue_id, since_date=since_date)
    else:
        print("Skipping checkin fetch; using existing local cache.")

    cache = fetch_checkins.load_cache()
    checkins = cache.get("checkins", [])
    if not checkins:
        raise RuntimeError("No checkins available after refresh")

    print(f"Analyzing {len(checkins)} cached checkins...")
    takeovers = detect_takeovers(checkins)
    allowed_users = build_takeover_access_payload(takeovers)
    beer_info_lookup = refresh_beer_details_for_takeovers(takeovers, skip_refresh=skip_beer_refresh)
    takeovers = enrich_takeovers_with_beer_data(takeovers)
    takeovers = compute_member_results_for_takeovers(takeovers, checkins, load_members_data())
    public_takeovers = strip_internal_takeover_fields(takeovers)

    current_events = fallback_json(DEPLOY_CURRENT_EVENTS_FILE, [])
    try:
        scraped_current_events = scrape_current_events(venue_id)
        if scraped_current_events:
            current_events = scraped_current_events
    except Exception as exc:
        print(f"Current events scrape failed, keeping previous snapshot: {exc}")

    past_events = build_past_events_from_checkins(checkins)
    if not past_events:
        past_events = build_past_events_from_takeovers(public_takeovers)
    if not past_events:
        past_events = fallback_json(DEPLOY_PAST_EVENTS_FILE, [])

    cache_summary = build_public_cache_summary(build_unix)

    write_json(DEPLOY_TAKEOVERS_FILE, public_takeovers)
    write_json(DEPLOY_BEER_INFO_FILE, beer_info_lookup)
    write_json(DEPLOY_CURRENT_EVENTS_FILE, current_events)
    write_json(DEPLOY_PAST_EVENTS_FILE, past_events)
    write_json(DEPLOY_CACHE_SUMMARY_FILE, cache_summary)
    write_json(DEPLOY_ALLOWED_USERS_FILE, allowed_users)

    OUTPUT_TAKEOVERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    export_json(public_takeovers, OUTPUT_TAKEOVERS_FILE)

    build_label = update_index_inline_snapshots(
        takeovers=public_takeovers,
        current_events=current_events,
        past_events=past_events,
        beer_info=beer_info_lookup,
        cache_summary=cache_summary,
        build_label=str(build_unix),
    )

    print("Refresh complete:")
    print(f"  Build label: {build_label}")
    print(f"  Checkins: {len(checkins)}")
    print(f"  Takeovers: {len(public_takeovers)}")
    print(f"  Current events: {len(current_events)}")
    print(f"  Past events: {len(past_events)}")
    print(f"  Beer info records: {len(beer_info_lookup)}")
    print(f"  Eligible login users: {allowed_users.get('eligible_count', 0)}")


def main():
    parser = argparse.ArgumentParser(description="Refresh committed deploy snapshots for Vercel")
    parser.add_argument("--skip-fetch", action="store_true", help="Reuse the existing local checkin cache")
    parser.add_argument("--skip-beer-refresh", action="store_true", help="Do not call Untappd beer enrichment for missing beer details")
    parser.add_argument("--since", type=str, help="Optional cutoff date for fetch_checkins (YYYY-MM-DD)")
    args = parser.parse_args()

    refresh_snapshots(
        skip_fetch=args.skip_fetch,
        skip_beer_refresh=args.skip_beer_refresh,
        since_date=args.since,
    )


if __name__ == "__main__":
    main()
