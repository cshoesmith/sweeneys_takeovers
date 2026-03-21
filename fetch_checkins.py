"""
Fetch checkin data from Untappd API for Hotel Sweeneys (or any venue).

Caches results to a local JSON file so the process can be resumed if
interrupted (important given the 100-calls-per-hour rate limit).

Authentication uses the same OAuth proxy flow as the Friendmap app
(utpd-oauth.craftbeers.app). Run with --login to authenticate.

Usage:
    # Authenticate via OAuth (opens browser)
    python fetch_checkins.py --login

    # Search for a venue to find its ID
    python fetch_checkins.py --search "Hotel Sweeneys"

    # Fetch all checkins (resumable, handles rate limits)
    python fetch_checkins.py

    # Fetch checkins going back to a specific date
    python fetch_checkins.py --since 2023-03-01
"""

import argparse
import http.server
import json
import os
import shutil
import sys
import time
import tempfile
import threading
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://api.untappd.com/v4"
CACHE_FILE = Path(__file__).parent / "checkins_cache.json"
CACHE_BACKUP = Path(__file__).parent / "checkins_cache.backup.json"
ENV_FILE = Path(__file__).parent / ".env"
USER_AGENT = "SweeneysTakeoverLog (Untappd Venue Checkin Fetcher)"

# Same Client ID as the Friendmap app
DEFAULT_CLIENT_ID = "1F3B736E6C0A2FEF7D8C8C75879E93424F2B73FA"

# OAuth proxy services (same as Friendmap)
OAUTH_PROXIES = [
    {"id": "shoeys", "label": "Shoeys Proxy", "base_url": "https://utpd-oauth.craftbeers.app"},
    {"id": "wardy", "label": "Wardy Proxy", "base_url": "https://utpd-oauth.wardy.au"},
]

# Rate-limit: 100 calls/hour = 1 call every 36s to be safe
RATE_LIMIT_DELAY = 37  # seconds between API calls

# Last known rate limit remaining (updated by api_get)
last_rate_limit_remaining = None
last_request_url = None

LOCAL_CALLBACK_PORT = 8907
LOCAL_CALLBACK_URL = f"http://localhost:{LOCAL_CALLBACK_PORT}/callback"


def get_access_token():
    """Get the stored access token from .env."""
    token = os.getenv("UNTAPPD_ACCESS_TOKEN")
    if not token or token.startswith("your_"):
        return None
    return token


def save_token_to_env(token):
    """Save or update the access token in .env file."""
    if ENV_FILE.exists():
        content = ENV_FILE.read_text(encoding="utf-8")
        lines = content.splitlines()
        found = False
        for i, line in enumerate(lines):
            if line.startswith("UNTAPPD_ACCESS_TOKEN="):
                lines[i] = f"UNTAPPD_ACCESS_TOKEN={token}"
                found = True
                break
        if not found:
            lines.append(f"UNTAPPD_ACCESS_TOKEN={token}")
        ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    else:
        ENV_FILE.write_text(f"UNTAPPD_ACCESS_TOKEN={token}\nVENUE_ID=26498\n", encoding="utf-8")
    os.environ["UNTAPPD_ACCESS_TOKEN"] = token


def check_proxy_health(base_url):
    """Check if an OAuth proxy service is reachable."""
    try:
        resp = requests.get(f"{base_url}/health", timeout=5, headers={"User-Agent": USER_AGENT})
        return resp.ok
    except Exception:
        return False


def login_oauth():
    """
    Run the OAuth login flow using the same proxy services as Friendmap.
    Opens a browser for the user to authenticate, then captures the token
    via a local callback server.
    """
    captured_token = {"value": None}

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            # Handle token_code from Wardy proxy
            if "token_code" in params:
                token_code = params["token_code"][0]
                print(f"  Received token_code, exchanging...")
                try:
                    resp = requests.post(
                        f"{selected_proxy['base_url']}/get-token",
                        json={"token_code": token_code},
                        timeout=10,
                        headers={"User-Agent": USER_AGENT},
                    )
                    if resp.ok:
                        data = resp.json()
                        captured_token["value"] = data.get("access_token")
                    else:
                        print(f"  Token exchange failed: {resp.status_code} {resp.text[:200]}")
                except Exception as e:
                    print(f"  Token exchange failed: {e}")

            # Handle direct access_token in fragment (via hash redirect)
            if "access_token" in params:
                captured_token["value"] = params["access_token"][0]

            # Serve a page that extracts token from URL hash fragment
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            html = """
            <html><body>
            <h2>Untappd Authentication</h2>
            <p id="status">Processing...</p>
            <script>
            const hash = window.location.hash;
            if (hash && hash.includes('access_token')) {
                const token = new URLSearchParams(hash.substring(1)).get('access_token');
                if (token) {
                    fetch('/save_token?access_token=' + token).then(() => {
                        document.getElementById('status').textContent = 'Token saved! You can close this tab.';
                    });
                }
            } else {
                const params = new URLSearchParams(window.location.search);
                if (params.get('token_code') || params.get('access_token')) {
                    document.getElementById('status').textContent = 'Token received! You can close this tab.';
                } else {
                    document.getElementById('status').textContent = 'No token found. Try again.';
                }
            }
            </script>
            </body></html>
            """
            self.wfile.write(html.encode())

            if captured_token["value"]:
                threading.Thread(target=self.server.shutdown, daemon=True).start()

        def log_message(self, format, *args):
            pass  # Suppress HTTP logs

    # Find a working OAuth proxy
    selected_proxy = None
    for proxy in OAUTH_PROXIES:
        print(f"  Checking {proxy['label']}...")
        if check_proxy_health(proxy["base_url"]):
            selected_proxy = proxy
            print(f"  Using {proxy['label']}")
            break

    if selected_proxy:
        auth_url = (
            f"{selected_proxy['base_url']}/login"
            f"?next_url={LOCAL_CALLBACK_URL}"
        )
    else:
        # Fall back to standard Untappd OAuth
        client_id = os.getenv("UNTAPPD_CLIENT_ID", DEFAULT_CLIENT_ID)
        auth_url = (
            f"https://untappd.com/oauth/authenticate/"
            f"?client_id={client_id}"
            f"&response_type=token"
            f"&redirect_url={LOCAL_CALLBACK_URL}"
        )
        print("  No proxy available, using standard Untappd OAuth")

    # Start local server to receive the callback
    server = http.server.HTTPServer(("localhost", LOCAL_CALLBACK_PORT), CallbackHandler)

    print(f"\nOpening browser for Untappd login...")
    print(f"  (If browser doesn't open, visit: {auth_url})")
    webbrowser.open(auth_url)

    print("Waiting for authentication callback...")
    server.handle_request()  # Handle the callback
    # Give time for hash-fragment redirect
    server.timeout = 10
    server.handle_request()  # Handle potential /save_token follow-up
    server.server_close()

    if captured_token["value"]:
        save_token_to_env(captured_token["value"])
        print(f"\nAccess token saved to .env")
        # Verify the token works
        try:
            resp = requests.get(
                f"{API_BASE}/user/info",
                params={"access_token": captured_token["value"]},
                headers={"User-Agent": USER_AGENT},
                timeout=10,
            )
            if resp.ok:
                user = resp.json().get("response", {}).get("user", {})
                print(f"Logged in as: {user.get('user_name', '?')} ({user.get('first_name', '')})")
            else:
                print("Warning: token saved but verification failed.")
        except Exception:
            print("Warning: token saved but couldn't verify.")
    else:
        print("\nERROR: No access token received.")
        print("You can also manually set UNTAPPD_ACCESS_TOKEN in .env")
        sys.exit(1)


def api_get(endpoint, params=None):
    """Make an authenticated GET request to the Untappd API."""
    if params is None:
        params = {}

    # Prefer access_token (per-user rate limit, same as Friendmap)
    access_token = get_access_token()
    if access_token:
        params["access_token"] = access_token
    else:
        # Fall back to client_id/client_secret
        client_id = os.getenv("UNTAPPD_CLIENT_ID")
        client_secret = os.getenv("UNTAPPD_CLIENT_SECRET")
        if client_id and client_secret:
            params["client_id"] = client_id
            params["client_secret"] = client_secret
        else:
            print("ERROR: No authentication configured.")
            print("       Run: python fetch_checkins.py --login")
            sys.exit(1)

    url = f"{API_BASE}/{endpoint}"
    headers = {"User-Agent": USER_AGENT}

    global last_request_url
    req = requests.Request("GET", url, params=params, headers=headers)
    prepared = req.prepare()
    last_request_url = prepared.url

    resp = requests.get(url, params=params, headers=headers, timeout=30)

    # Check rate limiting
    remaining = resp.headers.get("X-Ratelimit-Remaining")
    if remaining is None:
        # Some responses embed rate limit in meta
        try:
            data = resp.json()
            remaining = data.get("meta", {}).get("x-ratelimit-remaining")
        except Exception:
            pass
    if remaining is not None:
        global last_rate_limit_remaining
        last_rate_limit_remaining = int(remaining)
        print(f"  Rate limit remaining: {remaining}")
        if int(remaining) <= 1:
            print("  Rate limit nearly exhausted. Waiting 60 minutes...")
            time.sleep(3600)

    if resp.status_code == 429:
        print("  Rate limited! Waiting 60 minutes before retrying...")
        time.sleep(3600)
        return api_get(endpoint, params)

    if resp.status_code == 401:
        print("ERROR: Authentication failed. Token may be expired.")
        print("       Run: python fetch_checkins.py --login")
        sys.exit(1)

    resp.raise_for_status()
    return resp.json()


def search_venue(query):
    """Search for a venue by name."""
    print(f"Searching for venue: {query}")
    data = api_get("search/venue", {"q": query, "limit": 10})

    venues = data.get("response", {}).get("venues", {}).get("items", [])
    if not venues:
        print("No venues found.")
        return

    print(f"\nFound {len(venues)} venue(s):\n")
    for v in venues:
        venue = v.get("venue", v)
        vid = venue.get("venue_id")
        name = venue.get("venue_name")
        location = venue.get("location", {})
        if isinstance(location, str):
            city, country = location, ""
        else:
            city = location.get("venue_city", "")
            country = location.get("venue_country", "")
        stats = venue.get("venue_stats", {})
        checkins = stats.get("total_count", "?") if isinstance(stats, dict) else "?"
        print(f"  ID: {vid}  |  {name}  |  {city}, {country}  |  {checkins} checkins")

    print(f"\nSet the VENUE_ID in your .env file to the correct venue ID.")


def load_cache():
    """Load cached checkins from disk, falling back to backup if corrupt."""
    for path in (CACHE_FILE, CACHE_BACKUP):
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if path == CACHE_BACKUP:
                    print(f"  Loaded from backup ({len(data.get('checkins', []))} checkins)")
                return data
            except (json.JSONDecodeError, KeyError):
                print(f"  Warning: {path.name} is corrupt, trying next...")
                continue
    return {"venue_id": None, "checkins": [], "oldest_checkin_id": None}


def save_cache(cache):
    """Persist cache to disk atomically (write temp file, then rename)."""
    # Write to a temp file in the same directory, then rename over the target.
    # This prevents a partial write from corrupting the cache.
    dir_path = CACHE_FILE.parent
    try:
        # Keep a backup of the previous good cache
        if CACHE_FILE.exists():
            shutil.copy2(CACHE_FILE, CACHE_BACKUP)

        fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp", prefix="cache_")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        # Atomic rename (on Windows, need to remove target first)
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
        Path(tmp_path).rename(CACHE_FILE)
        print(f"Cache saved to {CACHE_FILE}")
    except Exception as e:
        print(f"  Warning: cache save failed ({e}), data is in backup")

def merge_checkin_record(existing, item):
    """Merge newer API fields into an existing cached checkin record."""
    beer = item.get("beer", {})
    brewery = item.get("brewery", {})
    event = item.get("event", None)

    merged = dict(existing)
    merged.update({
        "created_at": existing.get("created_at") or item.get("created_at", ""),
        "user": existing.get("user") or item.get("user", {}).get("user_name", ""),
        "beer_name": existing.get("beer_name") or beer.get("beer_name", ""),
        "beer_id": existing.get("beer_id") or beer.get("bid"),
        "beer_label": existing.get("beer_label") or beer.get("beer_label", ""),
        "beer_style": existing.get("beer_style") or beer.get("beer_style", ""),
        "beer_abv": existing.get("beer_abv") if existing.get("beer_abv") not in (None, "") else beer.get("beer_abv"),
        "beer_auth_rating": existing.get("beer_auth_rating") if existing.get("beer_auth_rating") not in (None, "") else beer.get("auth_rating"),
        "beer_active": existing.get("beer_active") if existing.get("beer_active") not in (None, "") else beer.get("beer_active"),
        "brewery_name": existing.get("brewery_name") or brewery.get("brewery_name", ""),
        "brewery_id": existing.get("brewery_id") or brewery.get("brewery_id"),
        "rating": existing.get("rating") if existing.get("rating") not in (None, "") else item.get("rating_score", 0),
    })

    if event and isinstance(event, dict):
        merged["event_name"] = existing.get("event_name") or event.get("event_name", "")
        merged["event_id"] = existing.get("event_id") or event.get("event_id")
        merged["event_url"] = existing.get("event_url") or event.get("event_url", "")

    return merged


def fetch_checkins(venue_id, since_date=None):
    """
    Fetch all checkins for a venue, paging backward from the most recent.
    Resumes from cache if available.
    """
    cache = load_cache()

    # If cache is for a different venue, start fresh
    if cache["venue_id"] != venue_id:
        cache = {"venue_id": venue_id, "checkins": [], "oldest_checkin_id": None}

    existing_ids = {c["checkin_id"] for c in cache["checkins"]}
    existing_by_id = {c["checkin_id"]: c for c in cache["checkins"]}
    # Resume from the oldest cached page so each run keeps pushing farther back
    # in history instead of stopping after overlapping recent pages.
    max_id = cache.get("oldest_checkin_id")

    if since_date:
        cutoff = datetime.strptime(since_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        # Default: 3 years back
        now = datetime.now(timezone.utc)
        cutoff = now.replace(year=now.year - 3)

    print(f"Fetching checkins for venue {venue_id} back to {cutoff.date()}")
    if max_id:
        print(f"  Resuming historical backfill from max_id={max_id} ({len(cache['checkins'])} cached)")
    else:
        print(f"  Starting from newest ({len(cache['checkins'])} cached)")

    batch_count = 0
    done = False

    while not done:
        params = {"limit": 25}
        if max_id:
            params["max_id"] = max_id

        print(f"\nFetching batch {batch_count + 1}...")
        try:
            data = api_get(f"venue/checkins/{venue_id}", params)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            if status_code in (400, 500) and max_id:
                print(f"  {status_code} error while paging older history at max_id={max_id}.")
                print(f"  Saved {len(cache['checkins'])} total cached checkins so far.")
                save_cache(cache)
                return cache
            print(f"  API error: {e}")
            print("  Saving progress and stopping.")
            save_cache(cache)
            return cache

        checkins_data = data.get("response", {}).get("checkins", {})
        items = checkins_data.get("items", [])

        if not items:
            print("  No more checkins found.")
            done = True
            break

        new_in_batch = 0
        for item in items:
            checkin_id = item["checkin_id"]
            created_at = item.get("created_at", "")

            # Parse date
            try:
                checkin_dt = datetime.strptime(
                    created_at, "%a, %d %b %Y %H:%M:%S %z"
                )
            except ValueError:
                checkin_dt = None

            if checkin_dt and checkin_dt < cutoff:
                print(f"  Reached cutoff date ({cutoff.date()}). Stopping.")
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

        # Update pagination cursor
        pagination = checkins_data.get("pagination", {})
        next_url = pagination.get("next_url", "")
        if next_url and "max_id=" in next_url:
            new_max_id = next_url.split("max_id=")[-1].split("&")[0]
            if new_max_id == str(max_id):
                print("  Pagination stalled. Stopping.")
                done = True
            else:
                max_id = new_max_id
        elif items:
            # Fallback: use the smallest checkin_id from this batch
            max_id = min(item["checkin_id"] for item in items) - 1
        else:
            done = True

        cache["oldest_checkin_id"] = max_id
        batch_count += 1

        # Sort cache by checkin_id (newest first) after merging
        cache["checkins"].sort(key=lambda c: c.get("checkin_id", 0), reverse=True)

        # Save progress every batch
        save_cache(cache)
        total = len(cache["checkins"])
        oldest_date = cache["checkins"][-1]["created_at"] if cache["checkins"] else "?"
        print(f"  Total cached: {total} checkins ({new_in_batch} new). Oldest: {oldest_date}")

        if not done:
            print(f"  Waiting {RATE_LIMIT_DELAY}s (rate limit)...")
            time.sleep(RATE_LIMIT_DELAY)

    save_cache(cache)
    print(f"\nDone! {len(cache['checkins'])} checkins saved to {CACHE_FILE}")
    return cache


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Untappd checkins for Hotel Sweeneys"
    )
    parser.add_argument(
        "--login", action="store_true",
        help="Authenticate with Untappd via OAuth (opens browser)",
    )
    parser.add_argument(
        "--search", type=str, help="Search for a venue by name instead of fetching"
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Fetch checkins back to this date (YYYY-MM-DD). Default: 3 years ago",
    )
    args = parser.parse_args()

    if args.login:
        login_oauth()
        return

    if args.search:
        search_venue(args.search)
        return

    venue_id = os.getenv("VENUE_ID")
    if not venue_id:
        print("ERROR: Set VENUE_ID in .env (run with --search to find it)")
        sys.exit(1)

    fetch_checkins(int(venue_id), since_date=args.since)


if __name__ == "__main__":
    main()
