import os
import requests
from pathlib import Path
from urllib.parse import urlencode

from flask import Flask, abort, jsonify, send_file, send_from_directory, session, redirect, request, render_template_string

from server import (
    PROJECT_DIR,
    get_beer_info,
    get_build_info,
    get_cache_summary_data,
    load_current_events_data,
    load_allowed_login_usernames,
    load_members_data,
    load_past_events_data,
    load_takeover_data,
    mask_token,
)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.getenv("UNTAPPD_CLIENT_ID", "local_development_key"))
PROXY_URL = "https://utpd-oauth.craftbeers.app/login"
PRIVILEGED_TAB_USERNAME = os.getenv("PRIVILEGED_TAB_USERNAME", "lightbeerking").strip().lower()
DEPLOY_DATA_DIR = Path(PROJECT_DIR) / "data"
DEPLOY_DATA_FILES = {
    "deploy_takeovers.json",
    "deploy_beer_info.json",
    "deploy_cache_summary.json",
    "deploy_current_events.json",
    "deploy_past_events.json",
    "deploy_allowed_users.json",
}

HTML_LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Login - Hotel Sweeneys Tap Takeovers</title>
<style>
  :root { --bg: #1a1a2e; --surface: #16213e; --card: #0f3460; --accent: #e94560; --text: #f0f0f0; --text-muted: #a0a0a0; }
  body { background-color: var(--bg); color: var(--text); font-family: -apple-system, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; }
  .login-card { background-color: var(--surface); padding: 40px; border-radius: 12px; text-align: center; box-shadow: 0 8px 16px rgba(0,0,0,0.3); border: 1px solid var(--card); max-width: 400px; width: 90%; }
  h1 { font-size: 1.5rem; margin-bottom: 20px; }
  p { margin-bottom: 30px; color: var(--text-muted); line-height: 1.5; }
  .error { color: #f48fb1; background: rgba(244, 143, 177, 0.1); padding: 10px; border-radius: 6px; margin-bottom: 20px; font-size: 0.95rem; }
  .btn.btn-untappd { background-color: #ffc000; color: #111; font-weight: 600; padding: 12px 24px; border-radius: 6px; text-decoration: none; display: inline-block; transition: background 0.2s; border: none; cursor: pointer; }
  .btn.btn-untappd:hover { background-color: #e6ac00; }
</style>
</head>
<body>
  <div class="login-card">
    <h1>Members Area</h1>
        <p>Please log in with Untappd to verify customer access and view tap takeover history.</p>
    {% if error %}
    <div class="error">{{ error }}</div>
    {% endif %}
    <a href="/auth/login" class="btn btn-untappd">Log in with Untappd</a>
  </div>
</body>
</html>
"""

@app.before_request
def require_auth():
    # Allow some endpoints to bypass auth
    allowed_endpoints = ['login', 'oauth_callback']
    if request.endpoint in allowed_endpoints:
        return
        
    # Also bypass for static assets in vercel if they reach here
    if request.path.startswith('/auth/'):
        return

    # Check session
    if not session.get('untappd_user'):
        if request.path.startswith('/api/'):
            return jsonify({"error": "Unauthorized"}), 401
        
        # If accessing root UI, show login screen
        return render_template_string(HTML_LOGIN_TEMPLATE)

@app.route('/api/index.py')
def vercel_root():
    # If the user goes explicitly to the vercel bypass
    error = request.args.get("error")
    if not session.get('untappd_user'):
        return render_template_string(HTML_LOGIN_TEMPLATE, error=error)
    return redirect('/')

@app.route("/auth/login")
def login():
    host_url = request.host_url.rstrip("/")
    callback_url = f"{host_url}/auth/callback"
    redirect_url = f"{PROXY_URL}?next_url={callback_url}"
    return redirect(redirect_url)

@app.route("/auth/callback")
def oauth_callback():
    token = request.args.get("access_token")
    token_code = request.args.get("token_code")
    
    # Handle token_code to access_token swap if proxy provides token_code
    if token_code and not token:
        try:
            resp = requests.post(
                "https://utpd-oauth.craftbeers.app/get-token",
                json={"token_code": token_code},
                timeout=10,
                headers={"User-Agent": "Untappd-Takeovers Vercel"}
            )
            if resp.ok:
                token = resp.json().get("access_token")
        except Exception:
            pass

    # If no token is provided in the query string, it might be hiding in the URL hash fragment.
    # The browser does not send hash fragments to the server, so we send a tiny JS snippet to 
    # extract it and reload the page with the token passed as a query parameter.
    if not token:
        return """
        <!DOCTYPE html>
        <html>
        <head><title>Processing Login...</title></head>
        <body style="background-color:#1a1a2e; color:white; font-family:sans-serif; text-align:center; padding-top:100px;">
        <h3>Processing Authentication...</h3>
        <script>
            const hash = window.location.hash;
            if (hash && hash.includes('access_token')) {
                const params = new URLSearchParams(hash.substring(1));
                const token = params.get('access_token');
                if (token) {
                    window.location.href = "/auth/callback?access_token=" + token;
                } else {
                    window.location.href = "/api/index.py?error=Failed+to+parse+token";
                }
            } else {
                window.location.href = "/api/index.py?error=Failed+to+authenticate.+Missing+access+token+from+Untappd.";
            }
        </script>
        </body>
        </html>
        """
        
    # Verify the token by calling the untappd API
    try:
        resp = requests.get(f"https://api.untappd.com/v4/user/info?access_token={token}", timeout=10)
        if resp.ok:
            data = resp.json()
            username = data["response"]["user"]["user_name"].lower()

            allowed_usernames = load_allowed_login_usernames()

            if username in allowed_usernames:
                session["untappd_user"] = username
                return redirect("/")
            else:
                session.pop("untappd_user", None)
                return render_template_string(HTML_LOGIN_TEMPLATE, error="Access Denied - Website access limited to customers")
        else:
            return render_template_string(HTML_LOGIN_TEMPLATE, error="Failed to verify token with Untappd API.")
    except Exception as e:
        return render_template_string(HTML_LOGIN_TEMPLATE, error=f"Authentication error: {str(e)}")


def read_only_status():
    meta = get_meta_payload()
    return {
        "running": False,
        "status": "idle",
        "total_checkins": get_cache_summary_data().get("total_checkins", 0),
        "batches_fetched": 0,
        "rate_limit_remaining": None,
        "rate_limit_total": 100,
        "errors_400": 0,
        "errors_other": 0,
        "oldest_date": get_cache_summary_data().get("oldest_date"),
        "newest_date": get_cache_summary_data().get("newest_date"),
        "target_date": "",
        "next_request_in": 0,
        "throttle_remaining": 0,
        "message": "Read-only Vercel deployment. Collector/admin features are local-only.",
        "last_request_url": "",
        "monitoring_enabled": False,
        "next_monitor_in": 0,
        "next_monitor_at": None,
        "last_analysis_at": "",
        "last_analysis_takeovers": None,
        "last_analysis_error": "",
        "last_run_mode": "",
        "deployment_target": meta.get("deployment_target"),
        "read_only": True,
        "current_user": meta.get("current_user"),
        "show_admin_tabs": meta.get("show_admin_tabs", False),
    }


def get_current_username() -> str:
    return str(session.get("untappd_user") or "").strip().lower()


def get_meta_payload():
    meta = dict(get_build_info())
    current_user = get_current_username()
    meta["current_user"] = current_user or None
    meta["show_admin_tabs"] = current_user == PRIVILEGED_TAB_USERNAME
    return meta


@app.get("/")
def root():
    return send_file(Path(PROJECT_DIR) / "index.html")


@app.get("/data/<path:filename>")
def deploy_data_file(filename: str):
    safe_name = Path(filename).name
    if safe_name not in DEPLOY_DATA_FILES:
        abort(404)
    return send_from_directory(DEPLOY_DATA_DIR, safe_name)


@app.get("/api/status")
def api_status():
    return jsonify(read_only_status())


@app.get("/api/meta")
def api_meta():
    return jsonify(get_meta_payload())


@app.get("/api/cache-summary")
def api_cache_summary():
    return jsonify(get_cache_summary_data())


@app.get("/api/members")
def api_members():
    return jsonify(load_members_data())


@app.get("/api/takeovers")
def api_takeovers():
    return jsonify(load_takeover_data())


@app.get("/api/current-events")
def api_current_events():
    try:
        venue_id = int(os.getenv("VENUE_ID", "107565"))
        return jsonify(load_current_events_data(venue_id))
    except Exception as exc:
        return jsonify({"error": mask_token(str(exc))}), 500


@app.get("/api/past-events")
def api_past_events():
    try:
        venue_id = int(os.getenv("VENUE_ID", "107565"))
        return jsonify(load_past_events_data(venue_id))
    except Exception as exc:
        return jsonify({"error": mask_token(str(exc))}), 500


@app.get("/api/beer-info/<int:beer_id>")
def api_beer_info(beer_id: int):
    try:
        return jsonify(get_beer_info(beer_id))
    except Exception as exc:
        return jsonify({"error": mask_token(str(exc))}), 500


@app.post("/api/start")
@app.post("/api/stop")
@app.post("/api/reset-cache")
@app.post("/api/analyze")
@app.post("/api/members")
@app.delete("/api/members/<path:username>")
def api_read_only_mutation():
    return jsonify({"error": "This Vercel deployment is read-only. Run the collector locally for admin features."}), 501