import os
from pathlib import Path

from flask import Flask, jsonify, send_file

from server import (
    PROJECT_DIR,
    get_beer_info,
    get_build_info,
    get_cache_summary_data,
    load_members_data,
    load_current_events_data,
    load_takeover_data,
    mask_token,
)


app = Flask(__name__)


def read_only_status():
    meta = get_build_info()
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
    }


@app.get("/")
def root():
    return send_file(Path(PROJECT_DIR) / "index.html")


@app.get("/api/status")
def api_status():
    return jsonify(read_only_status())


@app.get("/api/meta")
def api_meta():
    return jsonify(get_build_info())


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