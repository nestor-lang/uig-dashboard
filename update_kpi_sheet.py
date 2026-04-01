#!/usr/bin/env python3
"""
Daily KPI Sheet Updater — Ultra Homebuyers
Updates Facebook, PPC, and SMS tabs with:
- Meta spend by month (Facebook tab col C)
- Lead pipeline metrics from Podio (all tabs)
- Pending Income + Closed Income from Whiteboard Gross Profit (all tabs)
- Length to Close average per agent (all tabs)
Runs daily at 8:10AM AST
"""

import os
import urllib.request, urllib.parse, json, gzip as _gzip, sys
from datetime import datetime

# Mode: default = all tabs; --fb-ppc-only = skip SMS (used by hourly dashboard runs)
FB_PPC_ONLY = "--fb-ppc-only" in sys.argv

META_TOKEN = os.environ["META_TOKEN"]
META_ACCOUNT = "act_882507283448364"
PODIO_CLIENT_ID = "atlas"
PODIO_SECRET = os.environ["PODIO_SECRET"]
PODIO_APP_ID = "25179555"
PODIO_APP_TOKEN = os.environ["PODIO_APP_TOKEN"]
WHITEBOARD_APP_ID = "25179507"
WHITEBOARD_APP_TOKEN = os.environ["WHITEBOARD_APP_TOKEN"]
CLOSINGS_APP_ID = "25179510"
CLOSINGS_APP_TOKEN = os.environ["CLOSINGS_APP_TOKEN"]
CLOSINGS_VIEW_ID = "61742458"
FB_VIEW_ID  = "61715439"
PPC_VIEW_ID = "61716398"
SMS_VIEW_ID = "61742132"
SHEETS_REFRESH = os.environ["GOOGLE_REFRESH_TOKEN_SHEETS"]
SH_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
SH_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
SHEET_ID = "18bW_nutcjWVWl9abxcl3d0OYoFgCgQDFPEAGaABjBDg"

# ROA rules — per Nestor's definitions
ACTIVE_OPP_EXCLUDE  = ["Not an Opportunity", "No Contact", "Unreported", ""]
BOOKED_EXCLUDE      = ["Potential Opportunity", "Not an Opportunity", "No Contact", "Unreported", ""]
OFFERED_EXCLUDE     = ["Potential Opportunity", "Opportunity | Need to Offer", "Not an Opportunity", "No Contact", "Unreported", ""]
ACCEPTED_EXCLUDE    = ["Potential Opportunity", "Opportunity | Need to Offer", "Opportunity | Offered",
                       "Not an Opportunity", "No Contact", "Unreported", ""]
DEAD_INCLUDE        = ["Opportunity | Contract Died"]
AVAILABLE_ROA       = ["Opportunity | Contract Available"]
PENDING_ROA         = ["Opportunity | Contract Assigned"]
CLOSED_ROA          = ["Opportunity | Contract Closed"]

# Column offset between Facebook and SMS tabs
# FB: Leads=D, Active=E, Booked=G, Offered=I, Accepted=K, Available=N, Pending=O, Dead=P, Closed=Q, LTC=U
# SMS: same columns shifted +7: Leads=K, Active=L, Booked=N, Offered=P, Accepted=R, Available=U, Pending=V, Dead=W, Closed=X, LTC=AB

def col(letter, offset):
    """Shift a column letter by offset positions."""
    cols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    idx = cols.index(letter) + offset
    if idx < 26:
        return cols[idx]
    return "A" + cols[idx - 26]

FB_OFFSET  = 0
SMS_OFFSET = 7

# Row maps — 3 rows per month (TEAM, Charles, Julian), starting January row 2
# Facebook and PPC share the same layout
def build_row_map():
    months = ["January","February","March","April","May","June",
              "July","Aug","Sept","Oct","Nov","Dec"]
    nums   = ["2026-01","2026-02","2026-03","2026-04","2026-05","2026-06",
              "2026-07","2026-08","2026-09","2026-10","2026-11","2026-12"]
    row_map, num_map = {}, {}
    for i, (m, n) in enumerate(zip(months, nums)):
        base = 2 + i * 3
        row_map[m] = {"team": base, "Charles": base+1, "Julian": base+2}
        num_map[m] = n
    return row_map, num_map

MONTH_ROW_MAP, MONTH_NUM_MAP = build_row_map()

def podio_post(url, data, token=None, retries=3, timeout=30):
    import io
    if token is None:
        body = urllib.parse.urlencode(data).encode()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
    else:
        body = json.dumps(data).encode()
        headers = {"Content-Type": "application/json", "Authorization": f"OAuth2 {token}",
                   "Accept-Encoding": "gzip"}
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                enc = r.headers.get("Content-Encoding", "")
                if enc == "gzip":
                    raw = _gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 420 and attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f"  Podio rate limit (420), waiting {wait}s...")
                import time; time.sleep(wait)
            else:
                raise

def gc(fields, eid):
    for f in fields:
        if f.get("external_id") == eid:
            vals = f.get("values", [])
            if not vals: return ""
            v = vals[0].get("value", "")
            if isinstance(v, dict): return v.get("text", "")
            return str(v) if v else ""
    return ""

now = datetime.now()
current_month = now.strftime("%B")  # e.g. "March"
current_month_num = now.strftime("%Y-%m")  # e.g. "2026-03"
month_start = now.strftime("%Y-%m-01")
today = now.strftime("%Y-%m-%d")

# ── Step 1: Meta spend by month (Jan–current) ──
MONTH_RANGES = [
    ("January",  "2026-01-01", "2026-01-31"),
    ("February", "2026-02-01", "2026-02-28"),
    ("March",    "2026-03-01", "2026-03-31"),
    ("April",    "2026-04-01", "2026-04-30"),
    ("May",      "2026-05-01", "2026-05-31"),
    ("June",     "2026-06-01", "2026-06-30"),
    ("July",     "2026-07-01", "2026-07-31"),
    ("Aug",      "2026-08-01", "2026-08-31"),
    ("Sept",     "2026-09-01", "2026-09-30"),
    ("Oct",      "2026-10-01", "2026-10-31"),
    ("Nov",      "2026-11-01", "2026-11-30"),
    ("Dec",      "2026-12-01", "2026-12-31"),
]
meta_spend_by_month = {}
for month_name, since, until in MONTH_RANGES:
    # Don't query future months
    if since > today:
        break
    actual_until = min(until, today)
    params = urllib.parse.urlencode({
        "fields": "spend",
        "time_range": json.dumps({"since": since, "until": actual_until}),
        "level": "account",
        "access_token": META_TOKEN
    })
    url = f"https://graph.facebook.com/v18.0/{META_ACCOUNT}/insights?{params}"
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read())
    meta_spend_by_month[month_name] = sum(float(d.get("spend", 0)) for d in data.get("data", []))

month_spend = meta_spend_by_month.get(current_month, 0)

# ── Step 1b: Google Ads spend by month (Jan–current) ──
print("Pulling Google Ads spend by month...")
google_spend_by_month = {}
try:
    import yaml
    from google.ads.googleads.client import GoogleAdsClient
    from collections import defaultdict as _defaultdict

    GOOGLE_ADS_CUSTOMER_ID = "2314793503"
    GOOGLE_ADS_CONFIG = "/tmp/google_ads_kpi.yaml"
    google_config = {
        "developer_token": os.environ["GOOGLE_DEV_TOKEN"],
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN_ADS"],
        "login_customer_id": "3944732021",
        "use_proto_plus": True
    }
    with open(GOOGLE_ADS_CONFIG, "w") as f:
        yaml.dump(google_config, f)

    ga_client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG)
    ga_service = ga_client.get_service("GoogleAdsService")

    google_monthly_query = """
        SELECT segments.month, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '2026-01-01' AND '""" + today + """'
        AND campaign.status != 'REMOVED'
    """
    raw_by_month = _defaultdict(float)
    for row in ga_service.search(customer_id=GOOGLE_ADS_CUSTOMER_ID, query=google_monthly_query):
        month_key = str(row.segments.month)[:7]  # "2026-01"
        raw_by_month[month_key] += row.metrics.cost_micros / 1_000_000

    # Convert "2026-01" keys to month names to match MONTH_RANGES
    month_num_to_name = {n: m for m, n in [(m, num) for m, num in zip(
        ["January","February","March","April","May","June",
         "July","Aug","Sept","Oct","Nov","Dec"],
        ["2026-01","2026-02","2026-03","2026-04","2026-05","2026-06",
         "2026-07","2026-08","2026-09","2026-10","2026-11","2026-12"]
    )]}
    for month_key, spend in raw_by_month.items():
        month_name = month_num_to_name.get(month_key)
        if month_name:
            google_spend_by_month[month_name] = round(spend, 2)

    print(f"  Google Ads spend: {', '.join(f'{k}=${v:,.2f}' for k,v in google_spend_by_month.items() if v > 0)}")
except Exception as e:
    print(f"  ⚠️ Google Ads spend fetch failed ({e}), PPC spend will not be updated")

# ── Step 2: Podio leads from saved view ──
auth = podio_post("https://podio.com/oauth/token", {
    "grant_type": "app", "app_id": PODIO_APP_ID, "app_token": PODIO_APP_TOKEN,
    "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
})
ptoken = auth["access_token"]

def fetch_leads_from_view(view_id, cutoff_date=None):
    """Fetch all leads from a Podio view, optionally stopping at cutoff_date (YYYY-MM-DD)."""
    leads = []
    offset = 0
    while True:
        result = podio_post(
            f"https://api.podio.com/item/app/{PODIO_APP_ID}/filter/{view_id}/",
            {"limit": 100, "offset": offset},
            token=ptoken
        )
        items = result.get("items", [])
        if not items: break
        for item in items:
            fields = item.get("fields", [])
            created = item.get("created_on", "")[:10]
            if cutoff_date and created < cutoff_date:
                return leads  # items come newest-first, stop here
            disp_item_id = None
            for f in fields:
                if f.get("external_id") == "dispositions":
                    vals = f.get("values", [])
                    if vals:
                        v = vals[0].get("value", {})
                        if isinstance(v, dict):
                            disp_item_id = v.get("item_id")
            leads.append({
                "month": item.get("created_on", "")[:7],
                "created_on": created,
                "roa": gc(fields, "admin-use-internal-dispositions"),
                "agent": gc(fields, "agent-first-name") or "Unassigned",
                "disp_item_id": disp_item_id,
                "closed_on": "",
            })
        if len(items) < 100: break
        offset += 100
    return leads

SMS_CACHE_FILE = "/Users/nestorsoto/.openclaw/workspace/data/sms_leads_cache.json"

def load_sms_cache():
    """Load cached SMS leads from disk. Returns (leads, last_created_on)."""
    import os
    if not os.path.exists(SMS_CACHE_FILE):
        return [], None
    try:
        with open(SMS_CACHE_FILE, "r") as f:
            data = json.load(f)
        leads = data.get("leads", [])
        last = data.get("last_created_on", None)
        print(f"  SMS cache loaded: {len(leads)} leads, last={last}")
        return leads, last
    except Exception as e:
        print(f"  SMS cache read failed ({e}), starting fresh")
        return [], None

def save_sms_cache(leads):
    """Save SMS leads to disk cache."""
    import os
    os.makedirs(os.path.dirname(SMS_CACHE_FILE), exist_ok=True)
    last = max((l["created_on"] for l in leads), default=None)
    with open(SMS_CACHE_FILE, "w") as f:
        json.dump({"leads": leads, "last_created_on": last}, f)
    print(f"  SMS cache saved: {len(leads)} leads, last={last}")

def podio_post_with_retry(url, data, token, retries=3, timeout=60):
    """POST to Podio with retry on timeout/network errors (not just 420)."""
    import time as _time
    import gzip as _gzip
    body = json.dumps(data).encode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"OAuth2 {token}",
        "Accept-Encoding": "gzip, deflate",
    }
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                encoding = r.headers.get("Content-Encoding", "")
                if encoding == "gzip":
                    raw = _gzip.decompress(raw)
                elif encoding == "deflate":
                    import zlib
                    raw = zlib.decompress(raw)
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 420 and attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f"  Podio rate limit (420), waiting {wait}s...")
                _time.sleep(wait)
            else:
                raise
        except Exception as e:
            if attempt < retries - 1:
                wait = 10 * (attempt + 1)
                print(f"  SMS page attempt {attempt+1} failed ({type(e).__name__}: {e}), retrying in {wait}s...")
                _time.sleep(wait)
            else:
                raise

def parse_lead(item):
    fields = item.get("fields", [])
    disp_item_id = None
    for f in fields:
        if f.get("external_id") == "dispositions":
            vals = f.get("values", [])
            if vals:
                v = vals[0].get("value", {})
                if isinstance(v, dict):
                    disp_item_id = v.get("item_id")
    created = item.get("created_on", "")[:10]
    return {
        "item_id": item.get("item_id"),
        "month": item.get("created_on", "")[:7],
        "created_on": created,
        "roa": gc(fields, "admin-use-internal-dispositions"),
        "agent": gc(fields, "agent-first-name") or "Unassigned",
        "disp_item_id": disp_item_id,
        "closed_on": "",
    }

def fetch_sms_leads_direct(cutoff_date="2026-01-01"):
    """Fetch SMS leads incrementally using disk cache.
    On first run: full fetch from cutoff_date.
    On subsequent runs: only fetch new leads since last cached item."""
    import time as _time

    cached_leads, last_cached = load_sms_cache()

    # Determine the effective cutoff — use cache if available
    fetch_since = last_cached if last_cached else cutoff_date

    new_leads = []
    offset = 0
    page = 0
    while True:
        page += 1
        result = podio_post_with_retry(
            f"https://api.podio.com/item/app/{PODIO_APP_ID}/filter/{SMS_VIEW_ID}/",
            {"limit": 200, "offset": offset, "sort_by": "created_on", "sort_desc": True},
            token=ptoken,
            timeout=60
        )
        items = result.get("items", [])
        if not items: break
        stop = False
        for item in items:
            created = item.get("created_on", "")[:10]
            if created <= fetch_since and cached_leads:
                # We've reached leads we already have
                stop = True
                break
            if created < cutoff_date:
                stop = True
                break
            new_leads.append(parse_lead(item))
        print(f"  SMS page {page}: {len(new_leads)} new leads, last={items[-1].get('created_on','')[:10]}")
        if stop or len(items) < 200: break
        offset += 200
        _time.sleep(0.3)

    # Merge new leads with cache (new leads first, then cached)
    all_leads = new_leads + cached_leads
    # Deduplicate by Podio item_id
    seen = set()
    deduped = []
    for l in all_leads:
        key = l.get("item_id")
        if key is None or key not in seen:
            if key is not None:
                seen.add(key)
            deduped.append(l)

    # Only save cache if we successfully fetched (even 0 new leads is fine)
    save_sms_cache(deduped)

    print(f"  SMS total: {len(deduped)} leads ({len(new_leads)} new + {len(cached_leads)} cached)")
    return deduped

print("Pulling FB leads...")
all_leads     = fetch_leads_from_view(FB_VIEW_ID)
print(f"  FB: {len(all_leads)} leads")

print("Pulling PPC leads...")
all_ppc_leads = fetch_leads_from_view(PPC_VIEW_ID)
print(f"  PPC: {len(all_ppc_leads)} leads")

sms_ok = False
all_sms_leads = []
if not FB_PPC_ONLY:
    print("Pulling SMS leads (incremental)...")
    try:
        all_sms_leads = fetch_sms_leads_direct(cutoff_date="2026-01-01")
        sms_ok = True
    except Exception as e:
        print(f"  ⚠️ SMS fetch failed ({e}), SMS tab will not be updated this run")
else:
    print("SMS fetch skipped (--fb-ppc-only mode)")

# ── Step 3: Get Sheets access token ──
data = urllib.parse.urlencode({
    "client_id": SH_CLIENT_ID, "client_secret": SH_CLIENT_SECRET,
    "refresh_token": SHEETS_REFRESH, "grant_type": "refresh_token"
}).encode()
req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data,
    headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
with urllib.request.urlopen(req) as r:
    access = json.loads(r.read())["access_token"]

# ── Step 3b: Whiteboard auth — for Pending Income + Closed Income ──
print("Authenticating with Whiteboard...")
wb_token = None
try:
    wb_auth = podio_post("https://podio.com/oauth/token", {
        "grant_type": "app", "app_id": WHITEBOARD_APP_ID, "app_token": WHITEBOARD_APP_TOKEN,
        "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
    })
    wb_token = wb_auth["access_token"]
    print("  Whiteboard auth OK")
except Exception as e:
    print(f"  ⚠️ Whiteboard auth failed ({e}) — income columns will be skipped")

def fetch_closings_dates(wb_item_ids):
    """Fetch Closings records and return {whiteboard_item_id: close_date} dict.
    Uses closing-date field if populated, falls back to Closings record created_on.
    Fetches all closings from the view and matches by whiteboard reference field."""
    if not wb_item_ids:
        return {}
    try:
        cl_body = urllib.parse.urlencode({
            "grant_type": "app", "app_id": CLOSINGS_APP_ID, "app_token": CLOSINGS_APP_TOKEN,
            "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
        }).encode()
        cl_req = urllib.request.Request("https://podio.com/oauth/token", data=cl_body,
            headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
        with urllib.request.urlopen(cl_req, timeout=20) as r:
            cl_token = json.loads(r.read())["access_token"]
    except Exception as e:
        print(f"  ⚠️ Closings auth failed ({e}), skipping close date fallback")
        return {}

    result = {}
    target_ids = set(wb_item_ids)
    offset = 0
    while True:
        body = json.dumps({"limit": 200, "offset": offset}).encode()
        req = urllib.request.Request(
            f"https://api.podio.com/item/app/{CLOSINGS_APP_ID}/filter/{CLOSINGS_VIEW_ID}/",
            data=body, headers={"Content-Type": "application/json",
            "Authorization": f"OAuth2 {cl_token}", "Accept-Encoding": "gzip"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip": raw = _gzip.decompress(raw)
            page = json.loads(raw)
        items = page.get("items", [])
        for item in items:
            wb_ref_id = None
            closing_date = ""
            created_on = item.get("created_on", "")[:10]
            for f in item.get("fields", []):
                ext = f.get("external_id", "")
                vals = f.get("values", [])
                if ext == "whiteboard" and vals:
                    wb_ref_id = vals[0].get("value", {}).get("item_id")
                elif ext == "closing-date" and vals:
                    v = vals[0].get("value", {})
                    closing_date = (v.get("start", "") if isinstance(v, dict) else str(v))[:10]
            if wb_ref_id and wb_ref_id in target_ids:
                result[wb_ref_id] = closing_date or created_on
        if len(items) < 200:
            break
        offset += 200
    return result

def fetch_wb_data(item_ids):
    """Given a list of Whiteboard item IDs, return {item_id: {gross_profit, closed_on}} dict.
    closed_on is pulled from 'closed-on' field (date). Skips on error."""
    if not wb_token or not item_ids:
        return {}
    result = {}
    import time as _time
    for iid in item_ids:
        if not iid:
            continue
        try:
            req = urllib.request.Request(
                f"https://api.podio.com/item/{iid}",
                headers={"Authorization": f"OAuth2 {wb_token}", "Accept-Encoding": "gzip, deflate"}
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                raw = r.read()
                enc = r.headers.get("Content-Encoding", "")
                if enc == "gzip":
                    raw = _gzip.decompress(raw)
                wb = json.loads(raw)
            gp = 0.0
            closed_on = ""
            for f in wb.get("fields", []):
                ext_id = f.get("external_id", "")
                vals = f.get("values", [])
                if ext_id == "gross-profit-2" and vals:
                    gp = float(vals[0].get("value", 0) or 0)
                elif ext_id == "closed-on" and vals:
                    # Podio date fields store start_date directly on the values object
                    v = vals[0]
                    closed_on = (v.get("start_date") or v.get("start", ""))[:10]
            result[iid] = {"gross_profit": gp, "closed_on": closed_on}
        except Exception as e:
            print(f"  ⚠️ Whiteboard item {iid} fetch failed: {e}")
        _time.sleep(0.1)
    return result

# ── Step 4: Build sheet updates ──
updates = []

def build_tab_updates(tab, leads_list, row_map, num_map, agents,
                      spend_col=None, spend_by_month=None,
                      col_offset=0, skip_agent_leads_active=False,
                      pending_income_col=None, closed_income_col=None, ltc_col=None):
    """Generate sheet updates for a tab.

    Income columns (pending_income_col, closed_income_col) are column letters WITHOUT offset —
    they are passed as absolute columns (e.g. 'S', 'T' for FB/PPC, 'Z', 'AA' for SMS).
    ltc_col is also absolute (e.g. 'U' for FB/PPC, 'AB' for SMS).
    """
    by_month = {}
    for l in leads_list:
        by_month.setdefault(l["month"], []).append(l)

    # Pre-fetch all whiteboard data (gross profit + close date) and Closings fallback dates
    all_wb_ids = set()
    closed_wb_ids = set()
    for leads in by_month.values():
        for l in leads:
            if l["roa"] in PENDING_ROA + CLOSED_ROA and l.get("disp_item_id"):
                all_wb_ids.add(l["disp_item_id"])
            if l["roa"] in CLOSED_ROA and l.get("disp_item_id"):
                closed_wb_ids.add(l["disp_item_id"])
    wb_data = fetch_wb_data(list(all_wb_ids)) if all_wb_ids else {}
    if all_wb_ids:
        print(f"  Whiteboard data fetched: {len(wb_data)}/{len(all_wb_ids)} items")
    # Fetch Closings dates for closed leads whose WB closed-on is blank
    wb_ids_needing_fallback = [iid for iid in closed_wb_ids
                                if not wb_data.get(iid, {}).get("closed_on")]
    closings_dates = fetch_closings_dates(wb_ids_needing_fallback) if wb_ids_needing_fallback else {}
    if wb_ids_needing_fallback:
        print(f"  Closings fallback dates: {len(closings_dates)}/{len(wb_ids_needing_fallback)} found")

    for month_name, rows in row_map.items():
        month_num = num_map.get(month_name)
        if not month_num:
            continue
        if month_num > current_month_num:
            continue

        leads = by_month.get(month_num, [])
        total  = len(leads)
        active = len([l for l in leads if l["roa"] not in ACTIVE_OPP_EXCLUDE])
        team_row = rows["team"]

        if spend_col and spend_by_month:
            spend = spend_by_month.get(month_name, 0)
            updates.append({"range": f"{tab}!{spend_col}{team_row}", "values": [[round(spend, 2)]]})

        agent_rows = [rows[a] for a in agents if rows.get(a)]
        def sum_formula(c):
            parts = "+".join(f"{c}{r}" for r in agent_rows)
            return f'=IFERROR({parts},"")'

        updates.append({"range": f"{tab}!{col('D', col_offset)}{team_row}", "values": [[total]]})
        updates.append({"range": f"{tab}!{col('E', col_offset)}{team_row}", "values": [[active]]})
        for c in ['G', 'I', 'K', 'N', 'O', 'P', 'Q']:
            updates.append({"range": f"{tab}!{col(c, col_offset)}{team_row}",
                            "values": [[sum_formula(col(c, col_offset))]]})
        # Income + LTC TEAM rows also sum from agents
        if pending_income_col:
            updates.append({"range": f"{tab}!{pending_income_col}{team_row}",
                            "values": [[sum_formula(pending_income_col)]]})
        if closed_income_col:
            updates.append({"range": f"{tab}!{closed_income_col}{team_row}",
                            "values": [[sum_formula(closed_income_col)]]})

        for agent in agents:
            agent_leads = [l for l in leads if l["agent"] == agent]
            agent_row = rows.get(agent)
            if not agent_row:
                continue

            if not skip_agent_leads_active:
                updates.append({"range": f"{tab}!{col('D',col_offset)}{agent_row}", "values": [[len(agent_leads)]]})
                updates.append({"range": f"{tab}!{col('E',col_offset)}{agent_row}", "values": [[len([l for l in agent_leads if l["roa"] not in ACTIVE_OPP_EXCLUDE])]]})

            updates.append({"range": f"{tab}!{col('G',col_offset)}{agent_row}", "values": [[len([l for l in agent_leads if l["roa"] not in BOOKED_EXCLUDE])]]})
            updates.append({"range": f"{tab}!{col('I',col_offset)}{agent_row}", "values": [[len([l for l in agent_leads if l["roa"] not in OFFERED_EXCLUDE])]]})
            updates.append({"range": f"{tab}!{col('K',col_offset)}{agent_row}", "values": [[len([l for l in agent_leads if l["roa"] not in ACCEPTED_EXCLUDE])]]})
            updates.append({"range": f"{tab}!{col('N',col_offset)}{agent_row}", "values": [[len([l for l in agent_leads if l["roa"] in AVAILABLE_ROA])]]})
            updates.append({"range": f"{tab}!{col('O',col_offset)}{agent_row}", "values": [[len([l for l in agent_leads if l["roa"] in PENDING_ROA])]]})
            updates.append({"range": f"{tab}!{col('P',col_offset)}{agent_row}", "values": [[len([l for l in agent_leads if l["roa"] in DEAD_INCLUDE])]]})

            closed_leads  = [l for l in agent_leads if l["roa"] in CLOSED_ROA]
            pending_leads = [l for l in agent_leads if l["roa"] in PENDING_ROA]
            updates.append({"range": f"{tab}!{col('Q',col_offset)}{agent_row}", "values": [[len(closed_leads)]]})

            # Pending Income — sum Whiteboard gross profit for assigned leads
            if pending_income_col:
                pending_income = sum(wb_data.get(l.get("disp_item_id"), {}).get("gross_profit", 0) for l in pending_leads)
                updates.append({"range": f"{tab}!{pending_income_col}{agent_row}",
                                "values": [[round(pending_income, 2) if pending_income else ""]]})

            # Closed Income — sum Whiteboard gross profit for closed leads
            if closed_income_col:
                closed_income = sum(wb_data.get(l.get("disp_item_id"), {}).get("gross_profit", 0) for l in closed_leads)
                updates.append({"range": f"{tab}!{closed_income_col}{agent_row}",
                                "values": [[round(closed_income, 2) if closed_income else ""]]})

            # Length to Close — avg days from lead created_on to close date
            # Priority: Whiteboard closed-on → Closings record date → skip (never average blanks)
            if ltc_col and closed_leads:
                day_counts = []
                for cl in closed_leads:
                    lead_created = cl.get("created_on", "")
                    disp_id = cl.get("disp_item_id")
                    wb_closed = wb_data.get(disp_id, {}).get("closed_on", "")
                    closings_fallback = closings_dates.get(disp_id, "") if not wb_closed else ""
                    close_date = wb_closed or closings_fallback
                    if close_date and lead_created:
                        try:
                            d1 = datetime.strptime(lead_created[:10], "%Y-%m-%d")
                            d2 = datetime.strptime(close_date[:10], "%Y-%m-%d")
                            day_counts.append((d2 - d1).days)
                        except: pass
                if day_counts:
                    avg_days = round(sum(day_counts) / len(day_counts))
                    updates.append({"range": f"{tab}!{ltc_col}{agent_row}", "values": [[avg_days]]})

# SMS tab has 4 rows/month (TEAM, Julian, Charles, Nestor) — build its own map
def build_sms_row_map():
    months = ["January","February","March","April","May","June",
              "July","Aug","Sept","Oct","Nov","Dec"]
    nums   = ["2026-01","2026-02","2026-03","2026-04","2026-05","2026-06",
              "2026-07","2026-08","2026-09","2026-10","2026-11","2026-12"]
    row_map, num_map = {}, {}
    for i, (m, n) in enumerate(zip(months, nums)):
        base = 2 + i * 4  # 4 rows per month
        row_map[m] = {"team": base, "Julian": base+1, "Charles": base+2}  # Nestor=base+3, skip
        num_map[m] = n
    return row_map, num_map

SMS_ROW_MAP, SMS_NUM_MAP = build_sms_row_map()

# Facebook — 3 rows/month, pipeline D-U, spend in C, income S/T, LTC U
# FB columns: D=Leads, E=Active, G=Booked, I=Offered, K=Accepted, N=Available, O=Pending, P=Dead, Q=Closed, S=PendingIncome, T=ClosedIncome, U=LTC
build_tab_updates("Facebook", all_leads, MONTH_ROW_MAP, MONTH_NUM_MAP,
                  agents=["Charles", "Julian"],
                  spend_col="C", spend_by_month=meta_spend_by_month, col_offset=0,
                  pending_income_col="S", closed_income_col="T", ltc_col="U")

# PPC — same structure as Facebook, spend from Google Ads API
build_tab_updates("PPC", all_ppc_leads, MONTH_ROW_MAP, MONTH_NUM_MAP,
                  agents=["Charles", "Julian"],
                  spend_col="C", spend_by_month=google_spend_by_month, col_offset=0,
                  pending_income_col="S", closed_income_col="T", ltc_col="U")

# SMS — 4 rows/month, pipeline starts at K (offset +7)
# SMS columns: K=Leads, L=Active, N=Booked, P=Offered, R=Accepted, U=Available, V=Pending, W=Dead, X=Closed, Z=PendingIncome, AA=ClosedIncome, AB=LTC
if sms_ok:
    build_tab_updates("SMS", all_sms_leads, SMS_ROW_MAP, SMS_NUM_MAP,
                      agents=["Charles", "Julian"],
                      col_offset=SMS_OFFSET, skip_agent_leads_active=True,
                      pending_income_col="Z", closed_income_col="AA", ltc_col="AB")
elif FB_PPC_ONLY:
    print("  SMS tab skipped (fb-ppc-only mode)")
else:
    print("  Skipping SMS tab update (fetch failed)")

# ── Step 5: Write to sheet ──
body = json.dumps({"valueInputOption": "USER_ENTERED", "data": updates}).encode()
req2 = urllib.request.Request(
    f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values:batchUpdate",
    data=body,
    headers={"Authorization": f"Bearer {access}", "Content-Type": "application/json"},
    method="POST"
)
with urllib.request.urlopen(req2) as r:
    result = json.loads(r.read())

sms_status = f"{len(all_sms_leads)} leads" if sms_ok else ("skipped (fb-ppc-only)" if FB_PPC_ONLY else "⚠️ skipped (fetch failed)")
mode_label = " [FB+PPC only]" if FB_PPC_ONLY else ""
print(f"KPI Sheet updated{mode_label}: {result.get('totalUpdatedCells')} cells | {current_month} spend=${month_spend:,.2f} | FB: {len(all_leads)} leads | SMS: {sms_status} ✅")
