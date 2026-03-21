"""
Web server for Hotel Sweeneys Tap Takeover Log.

Serves the frontend, provides API endpoints for controlling the fetcher,
and exposes real-time status for the dashboard.

Usage:
    python server.py
    python server.py --port 8908
"""

import argparse
import html
import json
import os
import re
import sys
import threading
import time
import unicodedata
import webbrowser
from datetime import datetime, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen

from dotenv import load_dotenv

load_dotenv()

# Import fetcher functions
from fetch_checkins import (
    api_get, load_cache, save_cache, get_access_token,
    merge_checkin_record,
    CACHE_FILE, RATE_LIMIT_DELAY, login_oauth,
)
import fetch_checkins

PROJECT_DIR = Path(__file__).parent
DEFAULT_PORT = 8908
MONITOR_INTERVAL_SECONDS = 7 * 24 * 60 * 60
BEER_INFO_CACHE_FILE = PROJECT_DIR / "beer_info_cache.json"
DEPLOY_DATA_DIR = PROJECT_DIR / "data"
DEPLOY_TAKEOVERS_FILE = DEPLOY_DATA_DIR / "deploy_takeovers.json"
DEPLOY_BEER_INFO_FILE = DEPLOY_DATA_DIR / "deploy_beer_info.json"
DEPLOY_CACHE_SUMMARY_FILE = DEPLOY_DATA_DIR / "deploy_cache_summary.json"
DEPLOY_CURRENT_EVENTS_FILE = DEPLOY_DATA_DIR / "deploy_current_events.json"
APP_VERSION = os.getenv("APP_VERSION", "v1.0")
VENUE_SLUG = os.getenv("VENUE_SLUG", "hotel-sweeneys")
IS_VERCEL = bool(os.getenv("VERCEL"))

def mask_token(msg):
    """Remove access tokens from error messages."""
    return re.sub(r'access_token=[A-Za-z0-9]+', 'access_token=***', str(msg))


def get_build_info():
    """Return app version metadata with a build number based on latest file mtime."""
    tracked_files = [
        PROJECT_DIR / "index.html",
        PROJECT_DIR / "server.py",
        PROJECT_DIR / "fetch_checkins.py",
        PROJECT_DIR / "analyze_takeovers.py",
    ]
    latest_mtime = max(
        (path.stat().st_mtime for path in tracked_files if path.exists()),
        default=time.time(),
    )
    build_unix = int(latest_mtime)
    commit_sha = (
        os.getenv("VERCEL_GIT_COMMIT_SHA")
        or os.getenv("GITHUB_SHA")
        or ""
    ).strip()
    build_label = commit_sha[:7] if commit_sha else str(build_unix)
    return {
        "version": APP_VERSION,
        "build_unix": build_unix,
        "build_label": build_label,
        "build_iso": datetime.fromtimestamp(build_unix, tz=timezone.utc).isoformat(),
        "deployment_target": "vercel" if IS_VERCEL else "local",
        "read_only": IS_VERCEL,
        "supports_collector": not IS_VERCEL,
    }


def load_json_file(path):
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def get_cache_summary_data():
    cache = load_cache()
    checkins = cache.get("checkins", [])
    if not checkins:
        snapshot = load_json_file(DEPLOY_CACHE_SUMMARY_FILE)
        if isinstance(snapshot, dict):
            snapshot.setdefault("has_token", get_access_token() is not None)
            return snapshot
    return {
        "venue_id": cache.get("venue_id"),
        "total_checkins": len(checkins),
        "oldest_checkin_id": cache.get("oldest_checkin_id"),
        "oldest_date": checkins[-1]["created_at"] if checkins else None,
        "newest_date": checkins[0]["created_at"] if checkins else None,
        "has_token": get_access_token() is not None,
    }


def load_takeover_data():
    """Load cached takeovers or derive them dynamically from the checkin cache."""
    takeover_file = PROJECT_DIR / "output" / "takeovers.json"
    takeover_data = load_json_file(takeover_file)
    if isinstance(takeover_data, list):
        return enrich_takeovers_with_beer_data(takeover_data)

    snapshot_data = load_json_file(DEPLOY_TAKEOVERS_FILE)
    if isinstance(snapshot_data, list):
        return enrich_takeovers_with_beer_data(snapshot_data)

    cache = load_cache()
    checkins = cache.get("checkins", [])
    if not checkins:
        return []

    from analyze_takeovers import detect_takeovers

    takeovers = detect_takeovers(checkins)
    return enrich_takeovers_with_beer_data([
        {k: v for k, v in t.items() if k not in ("details",)} for t in takeovers
    ])


def merge_beer_info_record(base_info, extra_info):
    merged = dict(base_info or {})
    for key, value in (extra_info or {}).items():
        if value not in (None, ""):
            merged[key] = value
    return merged


def get_deploy_beer_info_lookup():
    deploy_lookup = load_json_file(DEPLOY_BEER_INFO_FILE)
    return deploy_lookup if isinstance(deploy_lookup, dict) else {}


def get_combined_beer_info_lookup():
    combined = {}
    deploy_lookup = get_deploy_beer_info_lookup()
    runtime_lookup = load_beer_info_cache()

    for source_lookup in (deploy_lookup, runtime_lookup):
        for beer_id, info in source_lookup.items():
            combined[str(beer_id)] = merge_beer_info_record(combined.get(str(beer_id), {}), info)

    return combined


def enrich_takeovers_with_beer_data(takeovers):
    beer_lookup = get_combined_beer_info_lookup()
    if not beer_lookup:
        return takeovers

    enriched = []
    for takeover in takeovers:
        takeover_copy = dict(takeover)
        beer_details = []
        for beer_detail in takeover.get("beer_details", []) or []:
            beer_id = beer_detail.get("beer_id")
            lookup_entry = beer_lookup.get(str(beer_id)) if beer_id is not None else None
            beer_details.append(merge_beer_info_record(beer_detail, lookup_entry))
        if beer_details:
            takeover_copy["beer_details"] = beer_details
        enriched.append(takeover_copy)
    return enriched


def strip_html(value):
    text = re.sub(r"<[^>]+>", " ", value or "")
    return " ".join(html.unescape(text).split())


def fetch_public_html(url):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_public_page(url):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=30) as response:
        final_url = response.geturl()
        html_text = response.read().decode("utf-8", errors="replace")
    return final_url, html_text


def slugify_untappd_segment(value):
    normalized = unicodedata.normalize("NFKD", (value or "").replace("ø", "o").replace("Ø", "O"))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()).strip("-")
    return re.sub(r"-+", "-", slug)


def build_untappd_beer_page_url(beer_id, beer_name="", brewery_name=""):
    beer_slug = slugify_untappd_segment(beer_name)
    brewery_slug = slugify_untappd_segment(brewery_name)
    slug_parts = [part for part in (brewery_slug, beer_slug) if part]
    if not slug_parts:
        return ""
    return f"https://untappd.com/b/{'-'.join(slug_parts)}/{beer_id}"


def is_usable_untappd_beer_page(final_url, html_text, beer_id):
    parsed = urlparse(final_url or "")
    if parsed.netloc.lower() not in {"untappd.com", "www.untappd.com"}:
        return False

    hints = [
        f"/{beer_id}",
        "ratings",
        "abv",
        '<meta property="og:description"',
        'application/ld+json',
        'assets.untappd.com/site/beer_logos',
    ]
    html_lower = (html_text or "").lower()
    return any(hint.lower() in html_lower for hint in hints)


def first_regex_group(pattern, text, flags=0):
    match = re.search(pattern, text, flags)
    if not match:
        return ""
    group_value = match.group(1) if match.lastindex else match.group(0)
    return html.unescape(group_value).strip()


def scrape_beer_info_from_page(beer_id, seed_info=None):
    seed_info = seed_info or {}
    candidate_urls = [f"https://untappd.com/beer/{beer_id}"]
    canonical_url = build_untappd_beer_page_url(
        beer_id,
        seed_info.get("beer_name", ""),
        seed_info.get("brewery_name", ""),
    )
    if canonical_url and canonical_url not in candidate_urls:
        candidate_urls.append(canonical_url)

    page_url = candidate_urls[0]
    html_text = ""
    for candidate_url in candidate_urls:
        candidate_page_url, candidate_html_text = fetch_public_page(candidate_url)
        page_url = candidate_page_url
        html_text = candidate_html_text
        if is_usable_untappd_beer_page(candidate_page_url, candidate_html_text, beer_id):
            break

    info = {
        "beer_id": beer_id,
        "beer_page_url": page_url,
        "beer_name": "",
        "beer_label": "",
        "beer_label_hd": "",
        "beer_style": "",
        "beer_abv": None,
        "beer_ibu": None,
        "beer_description": "",
        "rating_score": None,
        "rating_count": None,
        "brewery_name": "",
        "brewery_label": "",
        "brewery_label_hd": "",
        "brewery_country": "",
    }

    info["beer_label_hd"] = first_regex_group(
        r'(https://assets\.untappd\.com/site/beer_logos_hd/[^"\']+)',
        html_text,
        re.I,
    )
    info["beer_label"] = first_regex_group(
        r'(https://assets\.untappd\.com/site/beer_logos/[^"\']+)',
        html_text,
        re.I,
    )

    title_text = first_regex_group(r'<title>(.*?)</title>', html_text, re.S | re.I)
    if title_text and ' - ' in title_text:
        title_parts = [part.strip() for part in title_text.split(' - ') if part.strip()]
        if title_parts:
            info["beer_name"] = title_parts[0]
        if len(title_parts) > 1:
            info["brewery_name"] = title_parts[1]

    json_ld_match = re.search(r'<script type="application/ld\+json">(.*?)</script>', html_text, re.S | re.I)
    if json_ld_match:
        try:
            json_ld = json.loads(html.unescape(json_ld_match.group(1)))
            product_name = json_ld.get("name", "")
            if product_name and not info["beer_name"]:
                info["beer_name"] = product_name
            info["beer_description"] = json_ld.get("description", "") or info["beer_description"]
            brand = json_ld.get("brand", {}) or {}
            info["brewery_name"] = brand.get("name", "") or info["brewery_name"]
            aggregate = json_ld.get("aggregateRating", {}) or {}
            rating_value = aggregate.get("ratingValue")
            review_count = aggregate.get("reviewCount")
            if rating_value not in (None, ""):
                info["rating_score"] = float(rating_value)
            if review_count not in (None, ""):
                info["rating_count"] = int(float(review_count))
        except Exception:
            pass

    if not info["beer_name"]:
        info["beer_name"] = strip_html(first_regex_group(r'<h1[^>]*>(.*?)</h1>', html_text, re.S | re.I))

    og_description = first_regex_group(r'<meta\s+property="og:description"\s+content="([^"]+)"', html_text, re.I)
    if og_description:
        style_match = re.search(r' is a (.*?) which has a rating of ', og_description)
        rating_match = re.search(r'rating of ([0-9]+\.?[0-9]*) out of 5, with ([0-9,]+) ratings', og_description)
        if style_match and not info["beer_style"]:
            info["beer_style"] = style_match.group(1).strip()
        if rating_match:
            if info["rating_score"] is None:
                info["rating_score"] = float(rating_match.group(1))
            if info["rating_count"] is None:
                info["rating_count"] = int(rating_match.group(2).replace(",", ""))

    keywords = first_regex_group(r'<meta\s+name="keywords"\s+content="([^"]+)"', html_text, re.I)
    if keywords:
        parts = [part.strip() for part in keywords.split(",") if part.strip()]
        if len(parts) >= 3 and not info["beer_style"]:
            info["beer_style"] = parts[2]
        if len(parts) >= 4 and not info["brewery_country"]:
            info["brewery_country"] = parts[3]

    if info["beer_abv"] is None:
        abv_match = re.search(r'([0-9]+\.?[0-9]*)%\s*ABV', html_text, re.I)
        if abv_match:
            info["beer_abv"] = float(abv_match.group(1))

    if info["beer_ibu"] is None:
        ibu_match = re.search(r'([0-9]+\.?[0-9]*)\s*IBU', html_text, re.I)
        if ibu_match:
            info["beer_ibu"] = float(ibu_match.group(1))

    return info


def scrape_event_detail(event_url):
    try:
        html_text = fetch_public_html(event_url)
    except Exception:
        return {}

    description = ""
    desc_match = re.search(
        r'<div class="event-details-desc">\s*<p>(.*?)</p>\s*</div>',
        html_text,
        re.S,
    )
    if desc_match:
        description = strip_html(desc_match.group(1))

    where_match = re.search(
        r'<div class="event-where event-mobile">.*?<p>(.*?)</p>\s*</div>',
        html_text,
        re.S,
    )
    where_text = strip_html(where_match.group(1)) if where_match else ""

    image_match = re.search(r'<img\s+src="([^"]+utfb-images[^"]+)" alt="event image">', html_text)
    image_url = html.unescape(image_match.group(1)) if image_match else ""

    return {
        "description": description,
        "where": where_text,
        "image_url": image_url,
    }


def scrape_current_events(venue_id):
    events_url = f"https://untappd.com/v/{VENUE_SLUG}/{venue_id}/events"
    html_text = fetch_public_html(events_url)

    pattern = re.compile(
        r'<div class="event-item"[^>]*data-track-venue-impression="[^"]*event_id-(\d+)[^"]*"[^>]*>(.*?)</div>\s*</div>\s*<script',
        re.S,
    )
    events = []
    for match in pattern.finditer(html_text):
        event_id = int(match.group(1))
        block = match.group(2)

        title_match = re.search(r'<h4 class="name"><a href="([^"]+)">(.*?)</a></h4>', block, re.S)
        if not title_match:
            continue

        relative_url = title_match.group(1)
        event_url = relative_url if relative_url.startswith("http") else f"https://untappd.com{relative_url}"
        title = strip_html(title_match.group(2))

        date_match = re.search(r'<p class="date"[^>]*>(.*?)</p>', block, re.S)
        date_text = strip_html(date_match.group(1)) if date_match else ""

        meta_match = re.search(r'<span class="meta">(.*?)</span>', block, re.S)
        meta_text = strip_html(meta_match.group(1)) if meta_match else ""

        interest_match = re.search(r'<span class="words">(.*?)</span>', block, re.S)
        interest_text = strip_html(interest_match.group(1)) if interest_match else ""

        image_match = re.search(r'<div class="event-image">\s*<img\s+src="([^"]+)"', block, re.S)
        image_url = html.unescape(image_match.group(1)) if image_match else ""

        detail = scrape_event_detail(event_url)
        if detail.get("image_url"):
            image_url = detail["image_url"]

        events.append({
            "event_id": event_id,
            "event_name": title,
            "event_url": event_url,
            "date_text": date_text,
            "meta": meta_text,
            "interest_text": interest_text,
            "description": detail.get("description", ""),
            "where": detail.get("where", ""),
            "image_url": image_url,
            "source": "current-events-page",
        })

    return events


def load_current_events_data(venue_id):
    try:
        events = scrape_current_events(venue_id)
        if events:
            return events
    except Exception:
        pass

    snapshot_events = load_json_file(DEPLOY_CURRENT_EVENTS_FILE)
    if isinstance(snapshot_events, list):
        return snapshot_events

    return []

def run_takeover_analysis():
    """Run takeover analysis and export JSON results."""
    from analyze_takeovers import load_checkins, detect_takeovers, export_json

    checkins = load_checkins()
    takeovers = detect_takeovers(checkins)
    output_dir = PROJECT_DIR / "output"
    output_dir.mkdir(exist_ok=True)
    export_json(takeovers, output_dir / "takeovers.json")
    return {"takeovers": len(takeovers)}


def load_beer_info_cache():
    if BEER_INFO_CACHE_FILE.exists():
        try:
            with open(BEER_INFO_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}


def save_beer_info_cache(cache):
    with open(BEER_INFO_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def has_usable_beer_info(info):
    if not info:
        return False
    has_image = bool(
        info.get("beer_label")
        or info.get("beer_label_hd")
        or info.get("brewery_label")
        or info.get("brewery_label_hd")
    )
    has_rich_details = any(
        info.get(field) not in (None, "")
        for field in ("beer_abv", "beer_ibu", "beer_description", "rating_score", "rating_count")
    )
    return has_image or has_rich_details


def find_cached_beer_info(beer_id):
    cache = load_cache()
    for checkin in cache.get("checkins", []):
        if checkin.get("beer_id") != beer_id:
            continue
        return {
            "beer_id": beer_id,
            "beer_name": checkin.get("beer_name", ""),
            "beer_label": checkin.get("beer_label", ""),
            "beer_label_hd": checkin.get("beer_label", ""),
            "beer_style": checkin.get("beer_style", ""),
            "beer_abv": checkin.get("beer_abv"),
            "beer_ibu": None,
            "beer_description": "",
            "rating_score": checkin.get("beer_auth_rating"),
            "rating_count": None,
            "brewery_name": checkin.get("brewery_name", ""),
            "brewery_label": "",
            "brewery_label_hd": "",
            "brewery_country": "",
        }
    return None


def get_beer_info(beer_id):
    beer_id_str = str(beer_id)
    cache = load_beer_info_cache()
    cached_entry = cache.get(beer_id_str)
    if has_usable_beer_info(cached_entry):
        return cached_entry

    cached_info = find_cached_beer_info(beer_id)
    if has_usable_beer_info(cached_info):
        cache[beer_id_str] = cached_info
        save_beer_info_cache(cache)
        return cached_info

    info = cached_info or {"beer_id": beer_id}

    try:
        data = api_get(f"beer/info/{beer_id_str}")
        response = data.get("response", {})
        beer = response.get("beer", {})
        brewery = response.get("brewery", {})

        info.update({
            "beer_id": beer.get("bid") or beer_id,
            "beer_name": beer.get("beer_name", "") or info.get("beer_name", ""),
            "beer_label": beer.get("beer_label", "") or info.get("beer_label", ""),
            "beer_label_hd": beer.get("beer_label_hd", "") or info.get("beer_label_hd", ""),
            "beer_style": beer.get("beer_style", "") or info.get("beer_style", ""),
            "beer_abv": beer.get("beer_abv") if beer.get("beer_abv") not in (None, "") else info.get("beer_abv"),
            "beer_ibu": beer.get("beer_ibu") if beer.get("beer_ibu") not in (None, "") else info.get("beer_ibu"),
            "beer_description": beer.get("beer_description", "") or info.get("beer_description", ""),
            "rating_score": beer.get("rating_score") if beer.get("rating_score") not in (None, "") else info.get("rating_score"),
            "rating_count": beer.get("rating_count") if beer.get("rating_count") not in (None, "") else info.get("rating_count"),
            "brewery_name": brewery.get("brewery_name", "") or info.get("brewery_name", ""),
            "brewery_label": brewery.get("brewery_label", "") or info.get("brewery_label", ""),
            "brewery_label_hd": brewery.get("brewery_label_hd", "") or info.get("brewery_label_hd", ""),
            "brewery_country": brewery.get("country_name", "") or info.get("brewery_country", ""),
        })
    except Exception:
        pass

    if not has_usable_beer_info(info) or not info.get("beer_description") or not info.get("beer_label"):
        try:
            scraped_info = scrape_beer_info_from_page(beer_id, info)
            info.update({
                key: value
                for key, value in scraped_info.items()
                if value not in (None, "") or key not in info
            })
        except Exception:
            pass

    cache[beer_id_str] = info
    save_beer_info_cache(cache)
    return info

# ── Shared fetcher state (read by the status endpoint) ──────────────────────
class FetcherState:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.status = "idle"           # idle | running | paused | done | error
        self.total_checkins = 0
        self.batches_fetched = 0
        self.rate_limit_remaining = None
        self.rate_limit_total = 100
        self.errors_400 = 0
        self.errors_other = 0
        self.oldest_date = ""
        self.newest_date = ""
        self.target_date = ""
        self.last_batch_time = None
        self.next_request_at = None    # timestamp of next API call
        self.message = ""              # human-readable status line
        self.stop_requested = False
        self.throttle_until = None     # datetime when throttle ends
        self.last_request_url = ""   # last GET URL sent to API
        self.monitoring_enabled = True
        self.next_monitor_at = None
        self.last_analysis_at = ""
        self.last_analysis_takeovers = None
        self.last_analysis_error = ""
        self.last_run_mode = ""

    def to_dict(self):
        with self.lock:
            now = time.time()
            next_req = 0
            if self.next_request_at and self.running:
                next_req = max(0, self.next_request_at - now)

            throttle_remaining = 0
            if self.throttle_until:
                throttle_remaining = max(0, self.throttle_until - now)

            next_monitor = 0
            if self.monitoring_enabled and self.next_monitor_at:
                next_monitor = max(0, self.next_monitor_at - now)

            return {
                "running": self.running,
                "status": self.status,
                "total_checkins": self.total_checkins,
                "batches_fetched": self.batches_fetched,
                "rate_limit_remaining": self.rate_limit_remaining,
                "rate_limit_total": self.rate_limit_total,
                "errors_400": self.errors_400,
                "errors_other": self.errors_other,
                "oldest_date": self.oldest_date,
                "newest_date": self.newest_date,
                "target_date": self.target_date,
                "next_request_in": round(next_req, 1),
                "throttle_remaining": round(throttle_remaining, 1),
                "message": self.message,
                "last_request_url": self.last_request_url,
                "monitoring_enabled": self.monitoring_enabled,
                "next_monitor_in": round(next_monitor, 1),
                "next_monitor_at": self.next_monitor_at,
                "last_analysis_at": self.last_analysis_at,
                "last_analysis_takeovers": self.last_analysis_takeovers,
                "last_analysis_error": self.last_analysis_error,
                "last_run_mode": self.last_run_mode,
            }

    def reset(self):
        with self.lock:
            self.running = False
            self.status = "idle"
            self.batches_fetched = 0
            self.errors_400 = 0
            self.errors_other = 0
            self.message = ""
            self.stop_requested = False
            self.throttle_until = None
            self.next_request_at = None
            self.last_request_url = ""

fetcher_state = FetcherState()
fetcher_state.next_request_at = None


# ── Background fetcher thread ───────────────────────────────────────────────
def run_fetcher(venue_id, since_date=None, mode="backfill"):
    """Background thread: fetch checkins and update shared state."""
    import requests as req_lib

    state = fetcher_state
    state.reset()
    mode = mode if mode in ("backfill", "monitor") else "backfill"

    with state.lock:
        state.running = True
        state.status = "running"
        state.message = "Starting..."
        state.last_run_mode = mode

    cache = load_cache()
    if cache["venue_id"] != venue_id:
        cache = {"venue_id": venue_id, "checkins": [], "oldest_checkin_id": None}

    existing_ids = {c["checkin_id"] for c in cache["checkins"]}
    existing_by_id = {c["checkin_id"]: c for c in cache["checkins"]}
    if mode == "monitor":
        max_id = None
        overlap_streak = 0
        overlap_stop = 2
    else:
        # Resume from the oldest cached cursor so the collector keeps pushing
        # farther back in history across runs.
        max_id = cache.get("oldest_checkin_id")
        overlap_streak = None
        overlap_stop = None

    if since_date:
        cutoff = datetime.strptime(since_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        now = datetime.now(timezone.utc)
        cutoff = now.replace(year=now.year - 3)

    with state.lock:
        state.target_date = cutoff.strftime("%Y-%m-%d")
        state.total_checkins = len(cache["checkins"])
        if cache["checkins"]:
            state.newest_date = cache["checkins"][0].get("created_at", "")
            state.oldest_date = cache["checkins"][-1].get("created_at", "")
        if mode == "monitor":
            state.message = f"Starting monitoring sync ({len(cache['checkins'])} cached)"
        elif max_id:
            state.message = f"Resuming historical backfill ({len(cache['checkins'])} cached)"
        else:
            state.message = f"Starting fresh scan ({len(cache['checkins'])} checkins cached)"

    def finish_collection(base_message, run_analysis=True):
        save_cache(cache)

        analysis_suffix = ""
        if run_analysis:
            analysis_at = datetime.now(timezone.utc).isoformat()
            try:
                result = run_takeover_analysis()
                analysis_suffix = f" Auto-analysis complete: {result['takeovers']} takeovers."
                with state.lock:
                    state.last_analysis_at = analysis_at
                    state.last_analysis_takeovers = result["takeovers"]
                    state.last_analysis_error = ""
            except Exception as e:
                analysis_suffix = f" Auto-analysis failed: {e}"
                with state.lock:
                    state.last_analysis_at = analysis_at
                    state.last_analysis_takeovers = None
                    state.last_analysis_error = str(e)

        with state.lock:
            state.total_checkins = len(cache["checkins"])
            state.message = f"{base_message}{analysis_suffix}"
            state.status = "done"
            state.running = False
            if state.monitoring_enabled:
                state.next_monitor_at = time.time() + MONITOR_INTERVAL_SECONDS

        return

    done = False

    while not done:
        # Check for stop request
        with state.lock:
            if state.stop_requested:
                state.message = "Stopped by user"
                state.status = "idle"
                state.running = False
                save_cache(cache)
                return

        params = {"limit": 25}
        if max_id:
            params["max_id"] = max_id

        with state.lock:
            state.batches_fetched += 1
            state.message = f"Fetching batch {state.batches_fetched}..."
            state.status = "running"

        try:
            data = api_get(f"venue/checkins/{venue_id}", params)

            # Read rate limit and last URL from module-level variables
            if fetch_checkins.last_request_url:
                with state.lock:
                    state.last_request_url = fetch_checkins.last_request_url
            if fetch_checkins.last_rate_limit_remaining is not None:
                with state.lock:
                    state.rate_limit_remaining = fetch_checkins.last_rate_limit_remaining

        except req_lib.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            # 400 or 500 while paging older history; save what we have and stop.
            if status_code in (400, 500) and max_id:
                with state.lock:
                    state.errors_400 += 1
                label = "monitoring" if mode == "monitor" else "paging older history"
                finish_collection(f"Stopped while {label} at max_id={max_id}. {len(cache['checkins'])} total cached.")
                return
            else:
                with state.lock:
                    state.errors_other += 1
                    state.status = "error"
                    state.message = mask_token(f"API error: {e}")
                save_cache(cache)
                with state.lock:
                    state.running = False
                return

        except Exception as e:
            with state.lock:
                state.errors_other += 1
                state.status = "error"
                state.message = mask_token(f"Error: {e}")
                state.running = False
            save_cache(cache)
            return

        checkins_data = data.get("response", {}).get("checkins", {})
        items = checkins_data.get("items", [])

        if not items:
            finish_collection("No more checkins — collection complete!")
            return

        new_in_batch = 0
        for item in items:
            checkin_id = item["checkin_id"]
            created_at = item.get("created_at", "")

            try:
                checkin_dt = datetime.strptime(created_at, "%a, %d %b %Y %H:%M:%S %z")
            except ValueError:
                checkin_dt = None

            if checkin_dt and checkin_dt < cutoff:
                done = True
                break

            if checkin_id not in existing_ids:
                new_in_batch += 1
                beer = item.get("beer", {})
                brewery = item.get("brewery", {})
                event = item.get("event", None)
                record = {
                    "checkin_id": checkin_id,
                    "created_at": created_at,
                    "user": item.get("user", {}).get("user_name", ""),
                    "beer_name": beer.get("beer_name", ""),
                    "beer_id": beer.get("bid"),
                    "beer_label": beer.get("beer_label", ""),
                    "beer_style": beer.get("beer_style", ""),
                    "beer_abv": beer.get("beer_abv"),
                    "beer_auth_rating": beer.get("auth_rating"),
                    "beer_active": beer.get("beer_active"),
                    "brewery_name": brewery.get("brewery_name", ""),
                    "brewery_id": brewery.get("brewery_id"),
                    "rating": item.get("rating_score", 0),
                }
                if event and isinstance(event, dict):
                    record["event_name"] = event.get("event_name", "")
                    record["event_id"] = event.get("event_id")
                    record["event_url"] = event.get("event_url", "")
                cache["checkins"].append(record)
                existing_ids.add(checkin_id)
                existing_by_id[checkin_id] = record
            else:
                existing_by_id[checkin_id].update(merge_checkin_record(existing_by_id[checkin_id], item))

        # Update pagination
        pagination = checkins_data.get("pagination", {})
        next_url = pagination.get("next_url", "")
        if next_url and "max_id=" in next_url:
            new_max_id = next_url.split("max_id=")[-1].split("&")[0]
            if new_max_id == str(max_id):
                done = True
            else:
                max_id = new_max_id
        elif items:
            max_id = min(item["checkin_id"] for item in items) - 1
        else:
            done = True

        cache["oldest_checkin_id"] = max_id

        # Sort cache by date (newest first) after merging
        cache["checkins"].sort(key=lambda c: c.get("checkin_id", 0), reverse=True)
        save_cache(cache)

        if mode == "monitor" and new_in_batch == 0 and items:
            overlap_streak += 1
            if overlap_streak >= overlap_stop:
                finish_collection(f"Monitoring sync complete. {len(cache['checkins'])} checkins cached.")
                return
        elif mode == "monitor":
            overlap_streak = 0

        with state.lock:
            state.total_checkins = len(cache["checkins"])
            if cache["checkins"]:
                state.oldest_date = cache["checkins"][-1].get("created_at", "")
                if not state.newest_date:
                    state.newest_date = cache["checkins"][0].get("created_at", "")
            state.message = f"Cached {state.total_checkins} checkins ({new_in_batch} new this batch) — oldest: {state.oldest_date}"

        if not done:
            with state.lock:
                state.next_request_at = time.time() + RATE_LIMIT_DELAY
                state.status = "running"
            # Interruptible sleep
            for _ in range(RATE_LIMIT_DELAY):
                with state.lock:
                    if state.stop_requested:
                        state.message = "Stopped by user"
                        state.status = "idle"
                        state.running = False
                        save_cache(cache)
                        return
                time.sleep(1)

    finish_collection(f"Done! {len(cache['checkins'])} checkins collected.")


def monitor_loop():
    """Run a weekly monitoring sync while the server stays alive."""
    while True:
        time.sleep(5)

        with fetcher_state.lock:
            should_run = (
                fetcher_state.monitoring_enabled
                and not fetcher_state.running
                and fetcher_state.next_monitor_at is not None
                and time.time() >= fetcher_state.next_monitor_at
            )

        if not should_run:
            continue

        venue_id = int(os.getenv("VENUE_ID", "107565"))
        with fetcher_state.lock:
            if fetcher_state.running:
                continue
            fetcher_state.next_monitor_at = time.time() + MONITOR_INTERVAL_SECONDS
            fetcher_state.message = "Weekly monitoring run starting..."

        t = threading.Thread(target=run_fetcher, args=(venue_id, None, "monitor"), daemon=True)
        t.start()


# ── HTTP Request Handler ────────────────────────────────────────────────────
class AppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PROJECT_DIR), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/status":
            self._json_response(fetcher_state.to_dict())

        elif path == "/api/meta":
            self._json_response(get_build_info())

        elif path == "/api/current-events":
            venue_id = int(os.getenv("VENUE_ID", "107565"))
            try:
                self._json_response(load_current_events_data(venue_id))
            except Exception as e:
                self._json_response({"error": mask_token(str(e))}, 500)

        elif path == "/api/cache-summary":
            self._json_response(get_cache_summary_data())

        elif path == "/api/takeovers":
            self._json_response(load_takeover_data())

        elif path.startswith("/api/beer-info/"):
            beer_id = path.rsplit("/", 1)[-1]
            if not beer_id.isdigit():
                self._json_response({"error": "Invalid beer id"}, 400)
                return
            try:
                self._json_response(get_beer_info(int(beer_id)))
            except Exception as e:
                self._json_response({"error": mask_token(str(e))}, 500)

        elif path == "/":
            self.path = "/index.html"
            super().do_GET()
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/start":
            if fetcher_state.running:
                self._json_response({"error": "Fetcher already running"}, 409)
                return
            venue_id = int(os.getenv("VENUE_ID", "107565"))
            # Read optional since_date from body
            content_len = int(self.headers.get("Content-Length", 0))
            body = {}
            if content_len > 0:
                raw = self.rfile.read(content_len)
                try:
                    body = json.loads(raw)
                except json.JSONDecodeError:
                    pass
            since_date = body.get("since_date")
            mode = body.get("mode", "backfill")
            t = threading.Thread(target=run_fetcher, args=(venue_id, since_date, mode), daemon=True)
            t.start()
            self._json_response({"started": True})

        elif path == "/api/stop":
            with fetcher_state.lock:
                fetcher_state.stop_requested = True
            self._json_response({"stopping": True})

        elif path == "/api/reset-cache":
            if fetcher_state.running:
                self._json_response({"error": "Stop the fetcher first"}, 409)
                return
            cache = load_cache()
            cache["oldest_checkin_id"] = None
            cache["checkins"] = []
            save_cache(cache)
            takeover_file = PROJECT_DIR / "output" / "takeovers.json"
            if takeover_file.exists():
                takeover_file.unlink()
            if BEER_INFO_CACHE_FILE.exists():
                BEER_INFO_CACHE_FILE.unlink()
            fetcher_state.reset()
            self._json_response({"reset": True, "message": "Cache, takeover output, and beer info cache cleared. Ready to refetch."})

        elif path == "/api/analyze":
            try:
                result = run_takeover_analysis()
                with fetcher_state.lock:
                    fetcher_state.last_analysis_at = datetime.now(timezone.utc).isoformat()
                    fetcher_state.last_analysis_takeovers = result["takeovers"]
                    fetcher_state.last_analysis_error = ""
                self._json_response(result)
            except Exception as e:
                with fetcher_state.lock:
                    fetcher_state.last_analysis_at = datetime.now(timezone.utc).isoformat()
                    fetcher_state.last_analysis_takeovers = None
                    fetcher_state.last_analysis_error = str(e)
                self._json_response({"error": str(e)}, 500)

        else:
            self._json_response({"error": "Not found"}, 404)

    def _json_response(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # Quieter logs — only show API/POST requests, not static file serves
        if "/api/" in (args[0] if args else ""):
            super().log_message(format, *args)


def main():
    parser = argparse.ArgumentParser(description="Sweeneys Tap Takeover Log — Web Server")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Port (default: {DEFAULT_PORT})")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser on start")
    args = parser.parse_args()

    cache = load_cache()
    with fetcher_state.lock:
        if fetcher_state.next_monitor_at is None:
            if cache.get("checkins"):
                fetcher_state.next_monitor_at = time.time() + MONITOR_INTERVAL_SECONDS
            else:
                fetcher_state.next_monitor_at = time.time() + 15

    threading.Thread(target=monitor_loop, daemon=True).start()

    server = HTTPServer(("0.0.0.0", args.port), AppHandler)
    url = f"http://localhost:{args.port}"
    print(f"Server running at {url}")

    if not args.no_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        with fetcher_state.lock:
            fetcher_state.stop_requested = True
        server.server_close()


if __name__ == "__main__":
    main()
