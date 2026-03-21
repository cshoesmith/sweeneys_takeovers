"""
Web server for Hotel Sweeneys Tap Takeover Log.

Serves the frontend, provides API endpoints for controlling the fetcher,
and exposes real-time status for the dashboard.

Usage:
    python server.py
    python server.py --port 8908
"""

import argparse
import json
import os
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv

load_dotenv()

# Import fetcher functions
from fetch_checkins import (
    api_get, load_cache, save_cache, get_access_token,
    CACHE_FILE, RATE_LIMIT_DELAY, login_oauth,
)
import fetch_checkins

PROJECT_DIR = Path(__file__).parent
DEFAULT_PORT = 8908


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

    def to_dict(self):
        with self.lock:
            now = time.time()
            next_req = 0
            if self.next_request_at and self.running:
                next_req = max(0, self.next_request_at - now)

            throttle_remaining = 0
            if self.throttle_until:
                throttle_remaining = max(0, self.throttle_until - now)

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

fetcher_state = FetcherState()
fetcher_state.next_request_at = None


# ── Background fetcher thread ───────────────────────────────────────────────
def run_fetcher(venue_id, since_date=None):
    """Background thread: fetch checkins and update shared state."""
    import requests as req_lib

    state = fetcher_state
    state.reset()

    with state.lock:
        state.running = True
        state.status = "running"
        state.message = "Starting..."

    cache = load_cache()
    if cache["venue_id"] != venue_id:
        cache = {"venue_id": venue_id, "checkins": [], "oldest_checkin_id": None}

    existing_ids = {c["checkin_id"] for c in cache["checkins"]}
    max_id = cache.get("oldest_checkin_id")

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
        if max_id:
            state.message = f"Resuming from {len(cache['checkins'])} cached checkins"

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

            # Read rate limit from the module-level variable set by api_get
            if fetch_checkins.last_rate_limit_remaining is not None:
                with state.lock:
                    state.rate_limit_remaining = fetch_checkins.last_rate_limit_remaining

        except req_lib.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                with state.lock:
                    state.errors_400 += 1
                    state.status = "paused"
                    state.message = f"400 error — throttling 20 minutes (error #{state.errors_400})"
                    state.throttle_until = time.time() + 1200
                save_cache(cache)
                # Throttle with interruptible sleep
                for _ in range(1200):
                    with state.lock:
                        if state.stop_requested:
                            state.message = "Stopped by user"
                            state.status = "idle"
                            state.running = False
                            return
                        state.throttle_until = state.throttle_until  # keep updated
                    time.sleep(1)
                with state.lock:
                    state.throttle_until = None
                    state.message = "Resuming after throttle..."
                continue
            else:
                with state.lock:
                    state.errors_other += 1
                    state.status = "error"
                    state.message = f"API error: {e}"
                save_cache(cache)
                with state.lock:
                    state.running = False
                return

        except Exception as e:
            with state.lock:
                state.errors_other += 1
                state.status = "error"
                state.message = f"Error: {e}"
                state.running = False
            save_cache(cache)
            return

        checkins_data = data.get("response", {}).get("checkins", {})
        items = checkins_data.get("items", [])

        if not items:
            with state.lock:
                state.message = "No more checkins — collection complete!"
                state.status = "done"
                state.running = False
            save_cache(cache)
            return

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
                beer = item.get("beer", {})
                brewery = item.get("brewery", {})
                event = item.get("event", None)
                record = {
                    "checkin_id": checkin_id,
                    "created_at": created_at,
                    "user": item.get("user", {}).get("user_name", ""),
                    "beer_name": beer.get("beer_name", ""),
                    "beer_id": beer.get("bid"),
                    "beer_style": beer.get("beer_style", ""),
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
        save_cache(cache)

        with state.lock:
            state.total_checkins = len(cache["checkins"])
            if cache["checkins"]:
                state.oldest_date = cache["checkins"][-1].get("created_at", "")
                if not state.newest_date:
                    state.newest_date = cache["checkins"][0].get("created_at", "")
            state.message = f"Cached {state.total_checkins} checkins — oldest: {state.oldest_date}"

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

    save_cache(cache)
    with state.lock:
        state.total_checkins = len(cache["checkins"])
        state.message = f"Done! {state.total_checkins} checkins collected."
        state.status = "done"
        state.running = False


# ── HTTP Request Handler ────────────────────────────────────────────────────
class AppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PROJECT_DIR), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/status":
            self._json_response(fetcher_state.to_dict())

        elif path == "/api/cache-summary":
            cache = load_cache()
            checkins = cache.get("checkins", [])
            summary = {
                "venue_id": cache.get("venue_id"),
                "total_checkins": len(checkins),
                "oldest_checkin_id": cache.get("oldest_checkin_id"),
                "oldest_date": checkins[-1]["created_at"] if checkins else None,
                "newest_date": checkins[0]["created_at"] if checkins else None,
                "has_token": get_access_token() is not None,
            }
            self._json_response(summary)

        elif path == "/api/takeovers":
            takeover_file = PROJECT_DIR / "output" / "takeovers.json"
            if takeover_file.exists():
                with open(takeover_file, "r", encoding="utf-8") as f:
                    self._json_response(json.load(f))
            else:
                self._json_response([])

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
            t = threading.Thread(target=run_fetcher, args=(venue_id, since_date), daemon=True)
            t.start()
            self._json_response({"started": True})

        elif path == "/api/stop":
            with fetcher_state.lock:
                fetcher_state.stop_requested = True
            self._json_response({"stopping": True})

        elif path == "/api/analyze":
            try:
                from analyze_takeovers import load_checkins, detect_takeovers, export_json
                checkins = load_checkins()
                takeovers = detect_takeovers(checkins)
                output_dir = PROJECT_DIR / "output"
                output_dir.mkdir(exist_ok=True)
                export_json(takeovers, output_dir / "takeovers.json")
                self._json_response({"takeovers": len(takeovers)})
            except Exception as e:
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
