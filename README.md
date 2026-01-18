# Kindle (Goodreads) to Hardcover Sync

A "set it and forget it" tool to sync Kindle reads to Hardcover.app automatically.

## How it works
1. **Kindle** automatically updates your **Goodreads** "Read" shelf.
2. This script watches your **Goodreads RSS Feed**.
3. When a new book appears, it searches **Hardcover.app** via API.
4. It logs the book to your "Read" status on Hardcover, including the date read.

---

## Local Deployment

### 1. Setup
Clone the repository and set up your environment:

```bash
git clone https://github.com/yourusername/kindle-sync.git
cd kindle-sync
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration
Create a `.env` file in the root directory:
```bash
GOODREADS_RSS_URL="https://www.goodreads.com/review/list_rss/..."
HARDCOVER_API_TOKEN="your_hardcover_bearer_token"
```

### 3. Usage
Run the script manually to sync your books.

**Sync the last 10 books (Recommended for daily/hourly checks):**
```bash
python sync.py --limit 10
```

**Sync ALL books (First run or full backfill):**
```bash
python sync.py --limit 0
# OR
python sync.py
```

**Dry Run (See what would happen without adding books):**
```bash
python sync.py --dry-run
```

**Verbose Mode (Debug logging):**
```bash
python sync.py --verbose
```

---

## GitHub Actions Setup (Free)

This script is optimized to run **hourly** on GitHub Actions for free (approx. 720 minutes/year usage vs 2000 free minutes).

### 1. Configure Secrets
Go to your repository on GitHub:
1. Navigate to **Settings > Secrets and variables > Actions**.
2. Click **New repository secret**.
3. Add the following secrets:
    - `GOODREADS_RSS_URL`
    - `HARDCOVER_API_TOKEN`

### 2. The Automation
- The workflow runs **automatically fast** (checking only the last 10 books) every hour.
- This prevents "retry storms" and keeps your billable minutes extremely low.

### 3. Manual Full Sync via GitHub
If you need to force a full re-sync from the web:
1. Go to the **Actions** tab in your repo.
2. Select **Kindle to Hardcover Sync** on the left.
3. Click **Run workflow**.
4. In the "Number of books" input, type **0** (or leave empty) to sync EVERYTHING.
5. Click **Run workflow**.