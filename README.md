# Hotel Sweeneys Tap Takeover Log

Detects weekly **craft beer tap takeovers** at Hotel Sweeneys, Sydney by analyzing
Untappd checkin data. Each Thursday the venue features beers from a single guest
brewery — this tool identifies which brewery was featured each week by looking at
Thursday and Friday checkin patterns.

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Authenticate with Untappd

Uses the same OAuth proxy as the Friendmap app (`utpd-oauth.craftbeers.app`):

```bash
python fetch_checkins.py --login
```

This opens your browser to log in via Untappd. The access token is saved to `.env` automatically.

### 3. Configure venue (optional)

The default venue ID is **26498** (Hotel Sweeneys). To verify or change it:

```bash
copy .env.example .env   # only needed if .env doesn't exist yet
python fetch_checkins.py --search "Hotel Sweeneys"
```

Update `VENUE_ID` in `.env` if needed.

## Usage

### Find the venue ID (if needed)

```bash
python fetch_checkins.py --search "Hotel Sweeneys"
```

### Fetch checkin data

```bash
python fetch_checkins.py
```

This fetches checkins going back 3 years by default. Due to API rate limits
(100 calls/hour, 25 checkins per call), this takes a while for venues with
lots of activity. **The script is fully resumable** — progress is saved to
`checkins_cache.json` after every batch. Just re-run the command to continue.

To fetch only back to a specific date:

```bash
python fetch_checkins.py --since 2024-01-01
```

### Analyze tap takeovers

```bash
python analyze_takeovers.py
```

This reads the cached checkins, filters to Thursdays and Fridays, and identifies
weeks where a single brewery dominated — indicating a tap takeover.

**Options:**

| Flag | Description |
|------|-------------|
| `--min-checkins N` | Minimum checkins from a brewery to count as a takeover (default: 3) |
| `--min-ratio N` | Minimum share of the week's checkins (0.0–1.0, default: 0.3) |
| `--output csv` | Export results to `output/takeovers.csv` |
| `--output json` | Export results to `output/takeovers.json` |
| `--output both` | Export both CSV and JSON |
| `--breakdown` | Show brewery breakdown for every Thursday/Friday week |

**Examples:**

```bash
# Lower the threshold for quieter weeks
python analyze_takeovers.py --min-checkins 2 --min-ratio 0.25

# Export to CSV for a spreadsheet
python analyze_takeovers.py --output csv

# See the full weekly breakdown to manually verify
python analyze_takeovers.py --breakdown
```

## How detection works

1. All checkins are filtered to **Thursdays and Fridays** only
2. Checkins are grouped by week (keyed to the Thursday date)
3. Breweries appearing in >15% of *all* checkins are flagged as "house breweries"
   and excluded from takeover detection (they're always on tap)
4. For each week, the dominant non-house brewery is identified
5. A takeover is flagged when that brewery has:
   - At least N checkins (default 3), **and**
   - At least a certain share of the week's checkins (default 30%), **or**
   - At least 3 unique beers from the same brewery

## Output

The analysis prints a table like:

```
Date           Brewery                             Checkins  Beers   Share
---------------------------------------------------------------------------
--- 2024 ---
2024-01-04     Wildflower Brewing & Blending              7      5   43.8%
2024-01-11     Balter Brewing Company                     5      4   35.7%
2024-01-18     Range Brewing                              8      6   50.0%
...
```

## Files

| File | Purpose |
|------|---------|
| `fetch_checkins.py` | Fetches & caches Untappd checkins via API |
| `analyze_takeovers.py` | Detects tap takeovers from cached data |
| `checkins_cache.json` | Cached API data (auto-generated, resumable) |
| `output/takeovers.csv` | Exported results (when using `--output`) |
| `.env` | Your API credentials (not committed to git) |

## Deploying to Vercel

This project can be deployed to **Vercel in read-only mode**.

### What works on Vercel

- Viewing the Takeovers page
- Viewing current scraped Untappd events
- Beer detail popups
- Build/version metadata
- Bundled historical takeover data from committed deploy snapshots

### What does not work on Vercel

These features rely on local background jobs and persistent writable storage, which
do not fit Vercel's serverless model:

- Start/stop collector
- Reset cache
- Background monitoring thread
- Persistent local JSON cache updates in the deployed app

For full collector/admin functionality, run the app locally.

### Vercel setup

1. Push this repo to GitHub
2. Import it into Vercel
3. Set environment variables in Vercel as needed:

   - `UNTAPPD_ACCESS_TOKEN`
   - `VENUE_ID=107565`
   - `VENUE_SLUG=hotel-sweeneys`
   - `PRIVILEGED_TAB_USERNAME=your_untappd_username` (optional but recommended; controls which logged-in user sees the extra `Takeovers` and `Admin` buttons)
   - `APP_VERSION=v1.0` (optional)

4. Deploy

### Notes

- On Vercel, the app automatically switches to **read-only mode**.
- No fallback admin username is assumed for the deployed site.
- Until `PRIVILEGED_TAB_USERNAME` is explicitly configured, no login sees the extra `Takeovers` and `Admin` tab buttons.
- While that setting is missing, every logged-in user gets a reminder popup explaining that the real admin still needs to set `PRIVILEGED_TAB_USERNAME` in `.env` or in Vercel.
- Once a user can see the `Admin` tab, there is no separate frontend password prompt; `PRIVILEGED_TAB_USERNAME` is the gate for the deployed site.
- Everyone else still sees the simpler takeover page, but without the tab bar.
- A scheduled GitHub Actions workflow can now refresh the deploy snapshots automatically, so the Vercel site no longer depends on a local machine staying online.
- `data/deploy_takeovers.json` is the read-only snapshot consumed by the Vercel UI.
- `data/deploy_cache_summary.json` provides summary stats for the read-only deployment without committing the mutable runtime cache.
- The read-only viewer now shows snapshot freshness using the last successful refresh time written into `data/deploy_cache_summary.json`.
- `data/deploy_current_events.json` provides a fallback when the live event scrape is unavailable in the deployed environment.
- The visible build label is now an automatic Unix timestamp, so normal releases no longer need a manual build-stamp edit.
- The mutable runtime files (`checkins_cache.json`, `beer_info_cache.json`, `output/`) stay out of git.

## Automated refresh pipeline

To keep the Vercel site current without relying on a local machine, this repo supports a scheduled GitHub Actions refresh.

### How it works

1. GitHub Actions runs `refresh_deploy_snapshots.py` every hour.
2. The script fetches the latest Untappd venue checkins.
3. It reruns takeover detection, including heuristic/"secret" takeovers.
4. It enriches takeover beers with labels, descriptions, and ratings.
5. It rewrites the committed deploy snapshot files in `data/` and the inline fallback data in `index.html`.
6. The workflow commits those updates back to the repo, which triggers a fresh Vercel deploy.

### Required GitHub secrets

Add these repository secrets before enabling the workflow:

- `UNTAPPD_ACCESS_TOKEN` (recommended)
- `UNTAPPD_CLIENT_ID` (optional fallback)
- `UNTAPPD_CLIENT_SECRET` (optional fallback)

`VENUE_ID` is currently pinned to `107565` in `.github/workflows/refresh-deploy-snapshots.yml`.

### Manual refresh

```bash
python refresh_deploy_snapshots.py
```

For a fast local test using the existing cache without refetching Untappd data:

```bash
python refresh_deploy_snapshots.py --skip-fetch --skip-beer-refresh
```

## Rate limits

The Untappd API allows **100 calls per hour**. Each call returns up to 25
checkins. The fetch script automatically:

- Waits 37 seconds between calls to stay within limits
- Detects when the limit is nearly exhausted and pauses for an hour
- Saves progress after every batch so you can stop and resume any time
