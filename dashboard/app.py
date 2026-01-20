import os
import csv
import json
import time
import math
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import requests
from flask import Flask, request, jsonify

# --- dotenv (optional) ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# =========================
# CONFIG
# =========================
PORT = int(os.getenv("PORT", "5000"))
DB_PATH = (os.getenv("DB_PATH") or "inspire_dashboard.db").strip()

SHOPIFY_API_VERSION = (os.getenv("SHOPIFY_API_VERSION") or "2025-10").strip()
DEFAULT_SHOP = (os.getenv("SHOPIFY_SHOP_DOMAIN") or "").strip()
SHOPIFY_TOKEN = (os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN") or "").strip()

ALLOWED_SHOPS = [s.strip() for s in (os.getenv("SHOPIFY_ALLOWED_SHOPS") or "").split(",") if s.strip()]

WHOLESALE_CSV_PATH = (os.getenv("WHOLESALE_CSV_PATH") or "").strip()
ADMIN_KEY = (os.getenv("DASHBOARD_ADMIN_KEY") or "").strip()

# Saved domains (your list)
DOMAINS = [
    {"domain": "mg.wwwinspire.com", "status": "Primary/Connected"},
    {"domain": "0ipndq-vn.myshopify.com", "status": "Connected"},
    {"domain": "inspire-original-lc-2.myshopify.com", "status": "Connected"},
    {"domain": "original-inspiration-2.myshopify.com", "status": "Connected"},
]

# =========================
# APP + STATE
# =========================
app = Flask(__name__)

STATE = {
    "shop": DEFAULT_SHOP or (ALLOWED_SHOPS[0] if ALLOWED_SHOPS else None),
    "logs": ["Boot ✅"],
    "last_error": None,
    "last_sync": None,
    "sync_running": False,
    "recommendations": [],
    "top50": [],
}

def log(msg: str):
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    STATE["logs"].append(f"[{t}] {msg}")
    STATE["logs"] = STATE["logs"][-400:]

def require_admin():
    """Optional protection for write endpoints."""
    if not ADMIN_KEY:
        return True
    key = request.headers.get("X-Admin-Key") or (request.get_json(silent=True) or {}).get("admin_key")
    return key == ADMIN_KEY

def shop_allowed(shop: str) -> bool:
    if not shop:
        return False
    if not ALLOWED_SHOPS:
        return True
    return shop in ALLOWED_SHOPS

# =========================
# DB
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()

    # Sales aggregated daily by variant
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sales_daily (
      shop TEXT NOT NULL,
      day TEXT NOT NULL,
      variant_id INTEGER NOT NULL,
      sku TEXT,
      title TEXT,
      qty INTEGER NOT NULL DEFAULT 0,
      revenue REAL NOT NULL DEFAULT 0,
      orders INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (shop, day, variant_id)
    )
    """)

    # Inventory snapshot per variant (latest wins)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory_latest (
      shop TEXT NOT NULL,
      variant_id INTEGER NOT NULL,
      sku TEXT,
      available INTEGER,
      updated_ts TEXT NOT NULL,
      PRIMARY KEY (shop, variant_id)
    )
    """)

    # External signals (trend, marketplace, ads) normalized 0..1
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
      shop TEXT NOT NULL,
      ts TEXT NOT NULL,
      source TEXT NOT NULL,
      key TEXT NOT NULL,
      score REAL NOT NULL,
      meta TEXT,
      PRIMARY KEY (shop, ts, source, key)
    )
    """)

    # Wholesale catalog candidates (from CSV)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wholesale_candidates (
      shop TEXT NOT NULL,
      key TEXT NOT NULL,
      category TEXT,
      supplier TEXT,
      cost REAL,
      min_price REAL,
      max_price REAL,
      shipping REAL,
      notes TEXT,
      updated_ts TEXT NOT NULL,
      PRIMARY KEY (shop, key)
    )
    """)

    conn.commit()
    conn.close()

init_db()

# =========================
# SHOPIFY CLIENT
# =========================
def shopify_request(method: str, path: str, shop: Optional[str] = None, params=None, json_body=None) -> Dict[str, Any]:
    shop = shop or STATE["shop"]
    if not shop_allowed(shop):
        raise ValueError(f"Shop not allowed: {shop}")
    if not SHOPIFY_TOKEN:
        raise ValueError("Missing SHOPIFY_ADMIN_ACCESS_TOKEN in .env (Admin API token usually starts with shpat_)")

    url = f"https://{shop}/admin/api/{SHOPIFY_API_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    r = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=35)

    limit = r.headers.get("x-shopify-shop-api-call-limit")
    if limit:
        log(f"Shopify rate {limit}")

    if not r.ok:
        STATE["last_error"] = {"status": r.status_code, "body": r.text[:800]}
        raise RuntimeError(f"Shopify {r.status_code}: {r.text[:300]}")
    return r.json()

def shopify_get_shop() -> Dict[str, Any]:
    return shopify_request("GET", "/shop.json").get("shop", {})

def shopify_get_products(limit=250) -> List[Dict[str, Any]]:
    data = shopify_request("GET", "/products.json", params={"limit": int(limit)})
    return data.get("products", [])

def shopify_get_orders_since(created_at_min_iso: str, limit=250) -> List[Dict[str, Any]]:
    params = {
        "status": "any",
        "financial_status": "paid",
        "created_at_min": created_at_min_iso,
        "limit": int(limit),
        "fields": "id,created_at,total_price,line_items",
    }
    data = shopify_request("GET", "/orders.json", params=params)
    return data.get("orders", [])

def shopify_get_inventory_levels(inventory_item_ids: List[int]) -> Dict[int, int]:
    """
    Returns inventory by inventory_item_id aggregated across locations.
    Inventory Levels endpoint requires location; to keep turn-key, we estimate availability from product variants 'inventory_quantity'
    if available. If you want per-location accuracy, we’ll extend with locations + inventory_levels.
    """
    # TURN-KEY PATH: rely on variants inventory_quantity where Shopify provides it in product variants.
    # (Many stores have it; if not, you’ll use locations+inventory_levels.)
    return {}

# =========================
# WHOLESALE CSV INGEST
# =========================
def ingest_wholesale_csv(shop: str) -> int:
    """
    CSV columns supported (case-insensitive):
      key, category, supplier, cost, min_price, max_price, shipping, notes
    'key' can be sku/keyword/product name.
    """
    if not WHOLESALE_CSV_PATH or not os.path.exists(WHOLESALE_CSV_PATH):
        return 0

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    count = 0
    conn = db()
    cur = conn.cursor()

    with open(WHOLESALE_CSV_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            norm = { (k or "").strip().lower(): (v or "").strip() for k, v in row.items() }
            key = norm.get("key") or norm.get("sku") or norm.get("keyword") or ""
            if not key:
                continue
            def fnum(x): 
                try: return float(x)
                except: return None

            cur.execute("""
              INSERT INTO wholesale_candidates
              (shop, key, category, supplier, cost, min_price, max_price, shipping, notes, updated_ts)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(shop, key) DO UPDATE SET
                category=excluded.category,
                supplier=excluded.supplier,
                cost=excluded.cost,
                min_price=excluded.min_price,
                max_price=excluded.max_price,
                shipping=excluded.shipping,
                notes=excluded.notes,
                updated_ts=excluded.updated_ts
            """, (
                shop, key,
                norm.get("category"),
                norm.get("supplier"),
                fnum(norm.get("cost")),
                fnum(norm.get("min_price")),
                fnum(norm.get("max_price")),
                fnum(norm.get("shipping")),
                norm.get("notes"),
                ts
            ))
            count += 1

    conn.commit()
    conn.close()
    log(f"Wholesale CSV ingested ✅ rows={count}")
    return count

# =========================
# SIGNAL INGEST (official APIs plug in later)
# =========================
def ingest_signals(shop: str, source: str, signals: List[Dict[str, Any]]) -> int:
    """
    signals: [{ "key":"keyword-or-sku", "score":0..1, "meta":{...} }, ...]
    """
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    conn = db()
    cur = conn.cursor()
    n = 0
    for s in signals:
        key = (s.get("key") or "").strip()
        if not key:
            continue
        score = float(s.get("score") or 0.0)
        score = max(0.0, min(1.0, score))
        meta = s.get("meta")
        cur.execute("""
          INSERT OR REPLACE INTO signals (shop, ts, source, key, score, meta)
          VALUES (?, ?, ?, ?, ?, ?)
        """, (shop, ts, source, key, score, json.dumps(meta) if meta is not None else None))
        n += 1
    conn.commit()
    conn.close()
    log(f"Signals ingested ✅ source={source} n={n}")
    return n

def read_signal_avg(shop: str, key: str, hours: int = 72) -> float:
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat(timespec="seconds") + "Z"
    conn = db()
    cur = conn.cursor()
    cur.execute("""
      SELECT AVG(score) as avg_score
      FROM signals
      WHERE shop=? AND key=? AND ts>=?
    """, (shop, key, since))
    row = cur.fetchone()
    conn.close()
    return float(row["avg_score"] or 0.0) if row else 0.0

# =========================
# SALES + INVENTORY AGGREGATION
# =========================
def record_sales_from_orders(shop: str, orders: List[Dict[str, Any]]):
    conn = db()
    cur = conn.cursor()

    for o in orders:
        day = (o.get("created_at") or "")[:10]
        try:
            total_price = float(o.get("total_price") or 0)
        except:
            total_price = 0.0

        items = o.get("line_items") or []
        denom = 0.0
        vals = []
        for li in items:
            qty = int(li.get("quantity") or 0)
            price = float(li.get("price") or 0) if li.get("price") is not None else 0.0
            val = qty * price
            vals.append((li, qty, val))
            denom += val

        for li, qty, val in vals:
            vid = li.get("variant_id")
            if not vid or qty <= 0:
                continue
            sku = li.get("sku")
            title = li.get("title")
            revenue_alloc = (val / denom * total_price) if denom > 0 else 0.0

            cur.execute("""
              INSERT INTO sales_daily (shop, day, variant_id, sku, title, qty, revenue, orders)
              VALUES (?, ?, ?, ?, ?, ?, ?, 1)
              ON CONFLICT(shop, day, variant_id) DO UPDATE SET
                qty = qty + excluded.qty,
                revenue = revenue + excluded.revenue,
                orders = orders + 1,
                sku = COALESCE(excluded.sku, sales_daily.sku),
                title = COALESCE(excluded.title, sales_daily.title)
            """, (shop, day, int(vid), sku, title, int(qty), float(revenue_alloc)))

    conn.commit()
    conn.close()

def snapshot_inventory_from_products(shop: str, products: List[Dict[str, Any]]):
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    conn = db()
    cur = conn.cursor()

    count = 0
    for p in products:
        for v in (p.get("variants") or []):
            vid = v.get("id")
            if not vid:
                continue
            sku = v.get("sku")
            # Shopify often provides inventory_quantity in variants
            available = v.get("inventory_quantity")
            if available is None:
                continue
            cur.execute("""
              INSERT INTO inventory_latest (shop, variant_id, sku, available, updated_ts)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT(shop, variant_id) DO UPDATE SET
                sku=excluded.sku,
                available=excluded.available,
                updated_ts=excluded.updated_ts
            """, (shop, int(vid), sku, int(available), ts))
            count += 1

    conn.commit()
    conn.close()
    log(f"Inventory snapshot ✅ variants={count}")

def load_sales_agg(shop: str, days: int = 14) -> List[Dict[str, Any]]:
    since = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
    conn = db()
    cur = conn.cursor()
    cur.execute("""
      SELECT variant_id, sku, title,
             SUM(qty) as qty,
             SUM(revenue) as revenue,
             SUM(orders) as orders
      FROM sales_daily
      WHERE shop=? AND day>=?
      GROUP BY variant_id, sku, title
    """, (shop, since))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def load_inventory(shop: str) -> Dict[int, Dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("""
      SELECT variant_id, sku, available, updated_ts
      FROM inventory_latest
      WHERE shop=?
    """, (shop,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return { int(r["variant_id"]): r for r in rows }

def load_wholesale(shop: str) -> List[Dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("""
      SELECT key, category, supplier, cost, min_price, max_price, shipping, notes, updated_ts
      FROM wholesale_candidates
      WHERE shop=?
    """, (shop,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

# =========================
# RANKING + ROTATION ENGINE
# =========================
@dataclass
class Weights:
    sales_velocity: float = 0.35
    demand_signal: float = 0.25
    profit: float = 0.25
    competition: float = 0.15  # subtract

DEFAULT_WEIGHTS = Weights()

def clamp(x: float, a: float, b: float) -> float:
    return max(a, min(b, x))

def normalize_max(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return clamp(value / max_value, 0.0, 1.0)

def estimate_profit_score(cost: Optional[float], sell: Optional[float], shipping: Optional[float]) -> float:
    """
    profit score normalized 0..1 based on margin%.
    """
    if cost is None or sell is None:
        return 0.0
    ship = shipping or 0.0
    landed = cost + ship
    if sell <= 0:
        return 0.0
    margin = (sell - landed) / sell
    # map 0..0.6 margin into 0..1
    return clamp(margin / 0.60, 0.0, 1.0)

def compute_rotation_actions(shop: str, weights: Weights = DEFAULT_WEIGHTS) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
      recommendations: per-variant actions based on your store truth
      top50: wholesale/candidate product opportunities ranked by projected success
    """
    sales = load_sales_agg(shop, days=14)
    inv = load_inventory(shop)
    wholesale = load_wholesale(shop)

    # --- Store recommendations (existing variants) ---
    max_qty = max([int(x.get("qty") or 0) for x in sales], default=0)
    recs: List[Dict[str, Any]] = []

    for s in sales:
        vid = int(s["variant_id"])
        sku = (s.get("sku") or "").strip()
        title = (s.get("title") or "").strip()
        qty_14d = int(s.get("qty") or 0)
        rev_14d = float(s.get("revenue") or 0.0)
        velocity = qty_14d / 14.0

        available = None
        if vid in inv and inv[vid].get("available") is not None:
            available = int(inv[vid]["available"])
        else:
            available = 0

        # demand signal avg for sku/title (you can choose your own key mapping)
        key = sku or title or str(vid)
        demand = read_signal_avg(shop, key, hours=72)  # 0..1

        sales_norm = normalize_max(qty_14d, max_qty)

        # competition proxy (turn-key): if demand is high but your sales are low -> you're losing to market
        competition = clamp((demand - sales_norm) if demand > sales_norm else 0.0, 0.0, 1.0)

        # score
        score = (
            weights.sales_velocity * sales_norm +
            weights.demand_signal * demand -
            weights.competition * competition
        )

        # action rules
        stockout_risk = (available <= max(3, int(velocity * 2)))
        if velocity >= 1.0 and stockout_risk:
            action = "RESTOCK/REORDER"
        elif score >= 0.55:
            action = "PUSH"
        elif velocity < 0.15 and available > 10:
            action = "DISCOUNT/BUNDLE"
        else:
            action = "NORMAL"

        recs.append({
            "variant_id": vid,
            "sku": sku,
            "title": title,
            "qty_14d": qty_14d,
            "rev_14d": round(rev_14d, 2),
            "velocity_per_day": round(velocity, 3),
            "available": available,
            "demand_signal": round(demand, 3),
            "competition_proxy": round(competition, 3),
            "score": round(score, 4),
            "action": action,
        })

    recs.sort(key=lambda x: x["score"], reverse=True)

    # --- Top 50 wholesale opportunities ---
    # turn-key: ranks wholesale list using signals + profit estimate
    top: List[Dict[str, Any]] = []
    for w in wholesale:
        key = (w.get("key") or "").strip()
        if not key:
            continue
        cost = w.get("cost")
        min_p = w.get("min_price")
        max_p = w.get("max_price")
        ship = w.get("shipping") or 0.0
        # choose a mid sell estimate
        sell_est = None
        if min_p is not None and max_p is not None:
            sell_est = (float(min_p) + float(max_p)) / 2.0
        elif max_p is not None:
            sell_est = float(max_p)
        elif min_p is not None:
            sell_est = float(min_p)

        demand = read_signal_avg(shop, key, hours=72)  # 0..1
        profit = estimate_profit_score(cost, sell_est, ship)  # 0..1

        # competition placeholder: if demand high but profit low -> probably saturated/price war
        competition = clamp(demand * (1.0 - profit), 0.0, 1.0)

        final = (
            0.45 * demand +
            0.40 * profit -
            0.15 * competition
        )

        top.append({
            "key": key,
            "category": w.get("category"),
            "supplier": w.get("supplier"),
            "cost": cost,
            "sell_est": round(sell_est, 2) if sell_est is not None else None,
            "shipping_est": ship,
            "demand_signal": round(demand, 3),
            "profit_score": round(profit, 3),
            "competition_proxy": round(competition, 3),
            "final_score": round(final, 4),
            "notes": w.get("notes"),
        })

    top.sort(key=lambda x: x["final_score"], reverse=True)
    top50 = top[:50]

    return recs, top50

# =========================
# SYNC JOB (TURN-KEY)
# =========================
def run_sync_job():
    if STATE["sync_running"]:
        return
    STATE["sync_running"] = True
    try:
        shop = STATE["shop"]
        if not shop:
            raise ValueError("No shop selected")

        # 1) Shopify test
        info = shopify_get_shop()
        log(f"Shopify connected ✅ {info.get('name','?')}")

        # 2) Pull last 14 days paid orders
        since = (datetime.utcnow() - timedelta(days=14)).isoformat(timespec="seconds") + "Z"
        orders = shopify_get_orders_since(since)
        log(f"Orders pulled ✅ count={len(orders)} since={since}")
        record_sales_from_orders(shop, orders)

        # 3) Pull products + snapshot inventory (turn-key version uses inventory_quantity)
        products = shopify_get_products(limit=250)
        log(f"Products pulled ✅ count={len(products)}")
        snapshot_inventory_from_products(shop, products)

        # 4) Wholesale ingest (if CSV exists)
        ingest_wholesale_csv(shop)

        # 5) Compute recommendations + Top50
        recs, top50 = compute_rotation_actions(shop)
        STATE["recommendations"] = recs
        STATE["top50"] = top50
        log(f"Rotation computed ✅ recs={len(recs)} top50={len(top50)}")

        STATE["last_sync"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        STATE["last_error"] = None
    except Exception as e:
        STATE["last_error"] = {"error": str(e)}
        log(f"SYNC FAILED ❌ {e}")
    finally:
        STATE["sync_running"] = False

# =========================
# ROUTES
# =========================
@app.get("/")
def root():
    return "INSPIRE Vision Dashboard running ✅"

@app.get("/api/status")
def api_status():
    return jsonify({
        "ok": True,
        "shop": STATE["shop"],
        "time": datetime.now().strftime("%H:%M:%S"),
        "domains": DOMAINS,
        "allowed_shops": ALLOWED_SHOPS,
        "sync_running": STATE["sync_running"],
        "last_sync": STATE["last_sync"],
        "last_error": STATE["last_error"],
        "logs": STATE["logs"][-30:],
        "top50_preview": STATE["top50"][:10],
        "recs_preview": STATE["recommendations"][:10],
    })

@app.get("/api/stores")
def api_stores():
    stores = ALLOWED_SHOPS[:] if ALLOWED_SHOPS else [d["domain"] for d in DOMAINS]
    stores = sorted(set([s for s in stores if s]))
    return jsonify({"ok": True, "stores": stores, "selected": STATE["shop"]})

@app.post("/api/store/select")
def api_store_select():
    data = request.get_json(silent=True) or {}
    shop = (data.get("shop") or "").strip()
    if not shop_allowed(shop):
        return jsonify({"ok": False, "error": "Shop not allowed"}), 400
    STATE["shop"] = shop
    log(f"Selected shop ✅ {shop}")
    return jsonify({"ok": True, "shop": shop})

@app.post("/api/sync/run")
def api_sync_run():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if STATE["sync_running"]:
        return jsonify({"ok": False, "error": "sync already running"}), 409

    threading.Thread(target=run_sync_job, daemon=True).start()
    return jsonify({"ok": True, "started": True})

@app.get("/api/recommendations")
def api_recommendations():
    return jsonify({"ok": True, "shop": STATE["shop"], "recommendations": STATE["recommendations"]})

@app.get("/api/top50")
def api_top50():
    return jsonify({"ok": True, "shop": STATE["shop"], "top50": STATE["top50"]})

@app.post("/api/signals/ingest")
def api_signals_ingest():
    """
    Plug official APIs later. For now you can send signals in.
    Body:
      { "source":"google_trends", "signals":[{"key":"hat","score":0.83,"meta":{...}}, ...] }
    """
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    source = (payload.get("source") or "unknown").strip()
    signals = payload.get("signals") or []
    shop = STATE["shop"]
    n = ingest_signals(shop, source, signals)
    # Recompute after new signals
    recs, top50 = compute_rotation_actions(shop)
    STATE["recommendations"] = recs
    STATE["top50"] = top50
    return jsonify({"ok": True, "ingested": n, "recomputed": True})

@app.post("/api/wholesale/ingest_csv")
def api_wholesale_ingest_csv():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    shop = STATE["shop"]
    n = ingest_wholesale_csv(shop)
    recs, top50 = compute_rotation_actions(shop)
    STATE["recommendations"] = recs
    STATE["top50"] = top50
    return jsonify({"ok": True, "rows": n})

@app.get("/test")
def test():
    # quick Shopify sanity
    try:
        info = shopify_get_shop()
        return jsonify({"ok": True, "shop": info})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "last_error": STATE["last_error"]}), 400

# =========================
# RUN
# =========================
if __name__ == "__main__":
    if not SHOPIFY_TOKEN:
        log("WARNING: SHOPIFY_ADMIN_ACCESS_TOKEN missing; Shopify calls will fail until set.")
    app.run(host="127.0.0.1", port=PORT, debug=False)
