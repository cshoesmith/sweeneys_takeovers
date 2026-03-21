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
- Dynamic takeover analysis **if** cached checkin data is bundled with the deployment

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
   - `APP_VERSION=v1.0` (optional)

4. Deploy

### Notes

- On Vercel, the app automatically switches to **read-only mode** and hides the Admin tab.
- If `output/takeovers.json` is present in the deployment, it will be served directly.
- If not, the app will try to derive takeovers dynamically from any bundled `checkins_cache.json`.

## Rate limits

The Untappd API allows **100 calls per hour**. Each call returns up to 25
checkins. The fetch script automatically:

- Waits 37 seconds between calls to stay within limits
- Detects when the limit is nearly exhausted and pauses for an hour
- Saves progress after every batch so you can stop and resume any time
