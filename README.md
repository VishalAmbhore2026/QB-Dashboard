# QuickBooks Management Dashboard

A live, interactive dashboard for QuickBooks Desktop Enterprise 2024.
Shows cash position, AR/AP, sales trends, and top customers — with
drill-down on every number.

**Stack:** Python Flask backend + HTML/JS dashboard. Uses the free
QODBC driver bundled inside QB Enterprise 2024. Caches data in a
local SQLite file so QuickBooks doesn't get hammered on every page load.

---

## Quick start (5 minutes)

### 1. Install Python (one-time, on the Windows machine)

1. Go to https://www.python.org/downloads/
2. Download Python 3.10 or later (the big yellow button)
3. Run the installer. **Tick "Add python.exe to PATH"** on the first screen.
4. Click Install.

### 2. Set up the dashboard

1. Extract this folder somewhere simple like `C:\qb-dashboard\`
2. Double-click `setup.bat`
3. Wait for it to finish (about 2 minutes — it's installing Python libraries)

### 3. Run it

1. Double-click `run.bat`
2. Your browser will open to http://localhost:5000
3. You'll see the dashboard with demo data

**That's it.** You now have a working dashboard. It's showing fake data
because Demo Mode is ON by default. Use it to explore and decide if you
want to proceed with the real QuickBooks connection.

To stop the dashboard, close the black terminal window or press Ctrl+C in it.

---

## Connecting to real QuickBooks

When you're ready to plug in actual QB data:

### Step A — Enable ODBC inside QuickBooks

1. Open QuickBooks Enterprise 2024 as the **Admin** user
2. Switch to **Single-User Mode** (File menu → Switch to Single-User Mode)
3. Go to **File → Utilities → Set Up ODBC**
4. A window appears. Give the DSN a name like `QuickBooks Data`. Tick the read-only option. Click OK.
5. QuickBooks will pop up an "Integrated Application" permission dialog. Choose:
   - **Yes, allow this application to read data**
   - **Yes, even if QuickBooks is not running**
   - Log in as: **Admin**
6. Click Continue / Done.

### Step B — Configure the dashboard

1. Open the dashboard in your browser (http://localhost:5000)
2. Click **Settings** in the top-right
3. **Turn OFF Demo Mode** (the toggle at the top)
4. In **QuickBooks ODBC DSN Name**, type the same name you created in step A (e.g. `QuickBooks Data`)
5. Click **Test Connection**. If it succeeds, you'll see your company name and record counts.
6. Set **Company Label** to your client's business name
7. Click **Save Settings**

### Step C — First sync

1. Go back to the Dashboard
2. Click **Sync Now** (top-right)
3. First sync takes 30 seconds to a few minutes depending on the size of the company file
4. Done — you're looking at live data.

The dashboard will now auto-sync every 15 minutes (or whatever interval
you set in Settings). Users see the dashboard refresh every 60 seconds
in their browser.

---

## What's on the dashboard

**Top row — KPI cards** (all clickable for drill-down):
- Cash Position — sum of all bank + cash accounts
- Receivable (AR) — total outstanding invoice balances
- Payable (AP) — total outstanding bill balances
- Sales MTD — current month revenue
- Expenses MTD — current month outflow
- Net Income MTD

**Charts** (all clickable):
- Revenue vs Expenses — last 12 months
- AR Aging — click a bucket to see which invoices are in it
- Top Customers by Outstanding — click a bar to see their invoices
- Expense Categories — click a slice to see the bills

**Recent Transactions** — last 25 invoices & bills, sorted by date.
Click any invoice row to see line items.

**Drill levels available:**
- Cash → bank account breakdown
- AR → top customers → customer detail → invoice detail → line items
- AP → expense categories → bills in category
- Aging bucket → list of invoices in that bucket
- Any breadcrumb in the path is clickable to go back

---

## Security notes

- The dashboard runs on `localhost:5000` only by default. To access from
  another computer on the network, change the `host` parameter in `app.py`
  to `'0.0.0.0'` (already set) and open port 5000 on Windows Firewall.
  Consider adding authentication before doing this.
- The SQLite cache (`qb_cache.db`) holds a **copy** of your QB data.
  Treat it like any accounting backup — don't leave it on an unsecured
  machine, and delete it when you're done with the project.
- The connection settings (`config.json`) are stored in plain text.
  No passwords are stored — ODBC uses the QuickBooks permission you
  granted in Step A.

---

## Troubleshooting

**Test Connection fails with "Data source name not found"**
- Make sure you entered the DSN name EXACTLY as you created it in QuickBooks (case-sensitive)
- Open Windows' ODBC Data Source Administrator (search for "ODBC" in Start menu) and verify the DSN is listed

**"Could not connect to company file"**
- Open QuickBooks as Admin in Single-User Mode
- Re-check the Integrated Application permission: Edit → Preferences → Integrated Applications → Company Preferences

**Python or pip errors during setup.bat**
- Uninstall Python, reinstall with "Add to PATH" checked, and re-run setup.bat

**Dashboard loads but shows "—" everywhere**
- Click Sync Now. First load needs a sync. Check the bottom-right footer for status.

**Port 5000 already in use**
- Edit the last line of `app.py`, change `port=5000` to `port=5050`, and open `http://localhost:5050`

---

## File structure

```
qb-dashboard/
├── app.py                 ← main Flask app, API endpoints, scheduler
├── qb_connector.py        ← QODBC connection logic
├── etl.py                 ← sync QuickBooks → SQLite
├── mock_data.py           ← demo data generator
├── requirements.txt       ← Python dependencies
├── setup.bat              ← one-time install (Windows)
├── run.bat                ← launcher (Windows)
├── config.json            ← auto-created: settings
├── qb_cache.db            ← auto-created: local data cache
├── templates/             ← HTML (dashboard + settings)
└── static/
    ├── css/style.css
    └── js/                ← dashboard.js, charts.js
```

## Stopping and restarting

- **Stop:** close the black terminal window, or press Ctrl+C inside it
- **Start again:** double-click `run.bat`
- **Reset the cache:** delete `qb_cache.db` — next sync will rebuild it
- **Reset all settings:** delete `config.json` — next run restarts with defaults (Demo Mode)
