"""
QuickBooks Management Dashboard — Flask app (Phase 2A).

New in this build:
- Login page with username/password
- Session management (30-day persistent login)
- User management (admin only) — 2 admins + 5 viewers max
- Password change flow (forced on first login)
- Login audit log
- Rate-limiting / account lockout after 5 failed attempts
"""
import os
import json
import secrets
import sqlite3
from datetime import datetime, date, timedelta

from flask import (Flask, render_template, jsonify, request, redirect,
                   url_for, flash, session)
from apscheduler.schedulers.background import BackgroundScheduler

import qb_connector
import etl
import mock_data
import reports
import search as search_mod
import auth

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
DB_FILE = os.path.join(BASE_DIR, "qb_cache.db")
SECRET_FILE = os.path.join(BASE_DIR, ".secret_key")

app = Flask(__name__)


def _load_or_create_secret():
    """Persistent random secret for session signing."""
    if os.path.exists(SECRET_FILE):
        with open(SECRET_FILE) as f:
            return f.read().strip()
    key = secrets.token_hex(32)
    with open(SECRET_FILE, "w") as f:
        f.write(key)
    os.chmod(SECRET_FILE, 0o600)
    return key


app.secret_key = _load_or_create_secret()
app.permanent_session_lifetime = timedelta(days=auth.SESSION_DAYS)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"


# ---------- Config ----------
DEFAULT_CONFIG = {
    "dsn_name": "",
    "sync_interval_minutes": 15,
    "demo_mode": True,
    "company_label": "Demo Company",
    "fy_start_month": 1,
    "last_sync": None,
    "last_sync_status": "Never synced",
}


def load_config():
    if not os.path.exists(CONFIG_FILE):
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    return cfg


def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


# ---------- DB ----------
def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db_if_needed():
    fresh = not os.path.exists(DB_FILE)
    conn = get_db()
    from schema import create_schema
    create_schema(conn)
    auth.init_auth_schema(conn)
    auth.ensure_bootstrap_admin(conn)
    if fresh:
        print("[init] First run — seeding demo data…")
        mock_data.seed(conn)
    conn.commit()
    conn.close()


# ---------- Sync ----------
scheduler = BackgroundScheduler(daemon=True)


def do_sync():
    cfg = load_config()
    conn = get_db()
    try:
        if cfg["demo_mode"]:
            mock_data.seed(conn, refresh=True)
            cfg["last_sync_status"] = "Demo data refreshed"
        else:
            dsn = cfg["dsn_name"].strip()
            if not dsn:
                cfg["last_sync_status"] = "No DSN configured — skipping sync"
            else:
                etl.run_full_sync(conn, dsn)
                cfg["last_sync_status"] = "Success"
        cfg["last_sync"] = datetime.now().isoformat()
        save_config(cfg)
    except Exception as e:
        cfg["last_sync_status"] = f"Error: {e}"
        cfg["last_sync"] = datetime.now().isoformat()
        save_config(cfg)
        print(f"[sync] Error: {e}")
    finally:
        conn.close()


def start_scheduler():
    cfg = load_config()
    interval = max(5, int(cfg["sync_interval_minutes"]))
    scheduler.add_job(do_sync, "interval", minutes=interval,
                      id="qb_sync", replace_existing=True)
    scheduler.start()


def restart_scheduler():
    cfg = load_config()
    interval = max(5, int(cfg["sync_interval_minutes"]))
    try:
        scheduler.reschedule_job("qb_sync", trigger="interval", minutes=interval)
    except Exception:
        scheduler.add_job(do_sync, "interval", minutes=interval,
                          id="qb_sync", replace_existing=True)


# ---------- Helpers ----------
def _parse_request_range():
    preset = request.args.get("preset")
    if preset:
        return reports.preset_range(preset)
    from_s = request.args.get("from")
    to_s = request.args.get("to")
    if from_s and to_s:
        try:
            return (reports.parse_date(from_s), reports.parse_date(to_s))
        except Exception:
            pass
    return reports.preset_range("this_month")


def _compare_mode():
    return request.args.get("compare", "previous_period")


def _client_ip():
    if request.headers.get("X-Forwarded-For"):
        return request.headers["X-Forwarded-For"].split(",")[0].strip()
    return request.remote_addr


# ---------- Auth pages ----------
@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("uid"):
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        conn = get_db()
        try:
            user, err = auth.authenticate(
                conn,
                request.form.get("username"),
                request.form.get("password"),
                _client_ip(),
                request.headers.get("User-Agent", "")[:200]
            )
            if err:
                flash(err, "error")
                return render_template("login.html",
                                       username=request.form.get("username", ""))
            auth.login_session(user)
            nxt = request.args.get("next")
            if user["must_change_password"]:
                return redirect(url_for("change_password"))
            if nxt and nxt.startswith("/"):
                return redirect(nxt)
            return redirect(url_for("dashboard"))
        finally:
            conn.close()
    return render_template("login.html", username="")


@app.route("/logout")
def logout():
    auth.logout_session()
    return redirect(url_for("login"))


@app.route("/change-password", methods=["GET", "POST"])
@auth.login_required
def change_password():
    if request.method == "POST":
        conn = get_db()
        try:
            user = auth.get_user_by_id(conn, session["uid"])
            current = request.form.get("current_password", "")
            new = request.form.get("new_password", "")
            confirm = request.form.get("confirm_password", "")
            from werkzeug.security import check_password_hash
            if not check_password_hash(user["password_hash"], current):
                flash("Current password is incorrect.", "error")
            elif new != confirm:
                flash("New passwords don't match.", "error")
            elif len(new) < 6:
                flash("Password must be at least 6 characters.", "error")
            else:
                ok, msg = auth.change_password(conn, user["id"], new)
                if ok:
                    session["must_change"] = False
                    flash("Password changed successfully.", "success")
                    return redirect(url_for("dashboard"))
                else:
                    flash(msg, "error")
        finally:
            conn.close()
    return render_template("change_password.html")


# ---------- User management (admin only) ----------
@app.route("/users")
@auth.admin_required
def users_page():
    conn = get_db()
    try:
        users_list = auth.list_users(conn)
        counts = auth.count_by_role(conn)
        audit_rows = auth.recent_audit(conn, 30)
        return render_template("users.html",
                               users=users_list, counts=counts,
                               audit=audit_rows,
                               max_admins=auth.MAX_ADMINS,
                               max_viewers=auth.MAX_VIEWERS)
    finally:
        conn.close()


@app.route("/users/create", methods=["POST"])
@auth.admin_required
def users_create():
    conn = get_db()
    try:
        ok, msg = auth.create_user(
            conn,
            request.form.get("username"),
            request.form.get("name"),
            request.form.get("email"),
            request.form.get("password"),
            request.form.get("role")
        )
        flash(msg, "success" if ok else "error")
        return redirect(url_for("users_page"))
    finally:
        conn.close()


@app.route("/users/<int:uid>/delete", methods=["POST"])
@auth.admin_required
def users_delete(uid):
    conn = get_db()
    try:
        ok, msg = auth.delete_user(conn, uid, session["uid"])
        flash(msg, "success" if ok else "error")
        return redirect(url_for("users_page"))
    finally:
        conn.close()


@app.route("/users/<int:uid>/toggle", methods=["POST"])
@auth.admin_required
def users_toggle(uid):
    conn = get_db()
    try:
        ok, msg = auth.toggle_active(conn, uid, session["uid"])
        flash(msg, "success" if ok else "error")
        return redirect(url_for("users_page"))
    finally:
        conn.close()


@app.route("/users/<int:uid>/reset-password", methods=["POST"])
@auth.admin_required
def users_reset_pwd(uid):
    new_pwd = request.form.get("new_password", "").strip()
    if len(new_pwd) < 6:
        flash("Password must be at least 6 characters.", "error")
        return redirect(url_for("users_page"))
    conn = get_db()
    try:
        target = auth.get_user_by_id(conn, uid)
        ok, msg = auth.reset_password(conn, uid, new_pwd)
        flash(f"Password reset for '{target['username']}'. They must change on next login.",
              "success" if ok else "error")
        return redirect(url_for("users_page"))
    finally:
        conn.close()


# ---------- Pages ----------
@app.route("/")
@auth.login_required
def dashboard():
    cfg = load_config()
    return render_template("dashboard.html", cfg=cfg,
                           current_user_name=session.get("name"),
                           current_user_role=session.get("role"))


@app.route("/settings", methods=["GET", "POST"])
@auth.admin_required
def settings():
    cfg = load_config()
    message = None
    if request.method == "POST":
        cfg["dsn_name"] = request.form.get("dsn_name", "").strip()
        cfg["sync_interval_minutes"] = int(request.form.get("sync_interval_minutes", 15))
        cfg["demo_mode"] = request.form.get("demo_mode") == "on"
        cfg["company_label"] = request.form.get("company_label", "My Company").strip()
        save_config(cfg)
        restart_scheduler()
        message = "Settings saved."
    return render_template("settings.html", cfg=cfg, message=message,
                           current_user_name=session.get("name"),
                           current_user_role=session.get("role"))


# ---------- APIs — all require login ----------
@app.route("/api/test-connection", methods=["POST"])
@auth.admin_required
def api_test_connection():
    dsn = (request.json or {}).get("dsn_name", "").strip()
    if not dsn:
        return jsonify({"ok": False, "message": "Enter a DSN name."})
    try:
        info = qb_connector.test_connection(dsn)
        return jsonify({"ok": True, "message": "Connected!", "info": info})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/sync-now", methods=["POST"])
@auth.login_required
def api_sync_now():
    do_sync()
    cfg = load_config()
    return jsonify({"last_sync": cfg["last_sync"], "status": cfg["last_sync_status"]})


@app.route("/api/status")
@auth.login_required
def api_status():
    cfg = load_config()
    return jsonify({
        "demo_mode": cfg["demo_mode"],
        "last_sync": cfg["last_sync"],
        "last_sync_status": cfg["last_sync_status"],
        "company_label": cfg["company_label"],
        "sync_interval_minutes": cfg["sync_interval_minutes"],
        "user": {"name": session.get("name"), "role": session.get("role")}
    })


@app.route("/api/kpi")
@auth.login_required
def api_kpi():
    from_d, to_d = _parse_request_range()
    conn = get_db()
    try:
        return jsonify(reports.kpi_with_comparison(conn, from_d, to_d, _compare_mode()))
    finally:
        conn.close()


@app.route("/api/pnl")
@auth.login_required
def api_pnl():
    from_d, to_d = _parse_request_range()
    conn = get_db()
    try:
        primary = reports.profit_loss(conn, from_d, to_d)
        comp_from, comp_to = reports.compute_comparison_range(from_d, to_d, _compare_mode())
        comparison = reports.profit_loss(conn, comp_from, comp_to)
        return jsonify({"primary": primary, "comparison": comparison,
                        "compare_mode": _compare_mode()})
    finally:
        conn.close()


@app.route("/api/balance-sheet")
@auth.login_required
def api_balance_sheet():
    as_of = request.args.get("as_of")
    d = reports.parse_date(as_of) if as_of else reports.today()
    conn = get_db()
    try:
        return jsonify(reports.balance_sheet(conn, d))
    finally:
        conn.close()


@app.route("/api/cash-flow")
@auth.login_required
def api_cash_flow():
    from_d, to_d = _parse_request_range()
    conn = get_db()
    try:
        primary = reports.cash_flow(conn, from_d, to_d)
        comp_from, comp_to = reports.compute_comparison_range(from_d, to_d, _compare_mode())
        comparison = reports.cash_flow(conn, comp_from, comp_to)
        return jsonify({"primary": primary, "comparison": comparison})
    finally:
        conn.close()


@app.route("/api/monthly-trend")
@auth.login_required
def api_monthly_trend():
    from_d, to_d = _parse_request_range()
    conn = get_db()
    try:
        return jsonify(reports.monthly_trend(conn, from_d, to_d))
    finally:
        conn.close()


@app.route("/api/top-customers")
@auth.login_required
def api_top_customers():
    from_d, to_d = _parse_request_range()
    n = int(request.args.get("n", 10))
    conn = get_db()
    try:
        return jsonify(reports.top_customers(conn, from_d, to_d, n))
    finally:
        conn.close()


@app.route("/api/top-vendors")
@auth.login_required
def api_top_vendors():
    from_d, to_d = _parse_request_range()
    n = int(request.args.get("n", 10))
    conn = get_db()
    try:
        return jsonify(reports.top_vendors(conn, from_d, to_d, n))
    finally:
        conn.close()


@app.route("/api/top-items")
@auth.login_required
def api_top_items():
    from_d, to_d = _parse_request_range()
    n = int(request.args.get("n", 10))
    conn = get_db()
    try:
        return jsonify(reports.top_items(conn, from_d, to_d, n))
    finally:
        conn.close()


@app.route("/api/ar-aging")
@auth.login_required
def api_ar_aging():
    conn = get_db()
    try:
        return jsonify(reports.ar_aging(conn))
    finally:
        conn.close()


@app.route("/api/ap-aging")
@auth.login_required
def api_ap_aging():
    conn = get_db()
    try:
        return jsonify(reports.ap_aging(conn))
    finally:
        conn.close()


@app.route("/api/cash-accounts")
@auth.login_required
def api_cash_accounts():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT full_name, account_type, balance FROM account
            WHERE account_type IN ('Bank','OtherCurrentAsset') AND is_active=1
            ORDER BY balance DESC
        """)
        return jsonify([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()


@app.route("/api/expense-categories")
@auth.login_required
def api_expense_categories():
    from_d, to_d = _parse_request_range()
    conn = get_db()
    try:
        breakdown = reports.expense_by_account(conn, from_d, to_d)
        sorted_items = sorted(breakdown.items(), key=lambda x: -x[1])[:12]
        return jsonify([{"category": k, "amount": round(v, 2)}
                        for k, v in sorted_items])
    finally:
        conn.close()


@app.route("/api/recent-transactions")
@auth.login_required
def api_recent_transactions():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 'Invoice' AS type, txn_number AS number, customer_name AS party,
                   txn_date, subtotal AS amount, txn_id AS id FROM invoice
            UNION ALL
            SELECT 'Bill' AS type, txn_number AS number, vendor_name AS party,
                   txn_date, amount, txn_id AS id FROM bill
            ORDER BY txn_date DESC LIMIT 25
        """)
        return jsonify([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()


@app.route("/api/customer/<path:customer_name>/invoices")
@auth.login_required
def api_customer_invoices(customer_name):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT txn_id, txn_number, txn_date, due_date, subtotal,
                   balance_remaining, is_paid
            FROM invoice WHERE customer_name = ? ORDER BY txn_date DESC
        """, (customer_name,))
        return jsonify({"customer": customer_name,
                        "invoices": [dict(r) for r in cur.fetchall()]})
    finally:
        conn.close()


@app.route("/api/invoice/<txn_id>/lines")
@auth.login_required
def api_invoice_lines(txn_id):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM invoice WHERE txn_id = ?", (txn_id,))
        inv = cur.fetchone()
        if not inv:
            return jsonify({"error": "Invoice not found"}), 404
        cur.execute("""SELECT item_name, description, quantity, rate, amount
                       FROM invoice_line WHERE txn_id = ?""", (txn_id,))
        return jsonify({"invoice": dict(inv),
                        "lines": [dict(r) for r in cur.fetchall()]})
    finally:
        conn.close()


@app.route("/api/aging-bucket/<bucket>")
@auth.login_required
def api_aging_bucket(bucket):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT txn_id, txn_number, customer_name, txn_date, due_date,
                   balance_remaining FROM invoice WHERE is_paid = 0
        """)
        today_d = reports.today()
        out = []
        for r in cur.fetchall():
            if not r["due_date"]:
                continue
            try:
                due = reports.parse_date(r["due_date"])
                days = (today_d - due).days
            except Exception:
                continue
            in_b = False
            if bucket == "Current (0-30)" and days <= 30: in_b = True
            elif bucket == "31-60 days" and 30 < days <= 60: in_b = True
            elif bucket == "61-90 days" and 60 < days <= 90: in_b = True
            elif bucket == "90+ days" and days > 90: in_b = True
            if in_b:
                d = dict(r)
                d["days_overdue"] = days
                out.append(d)
        out.sort(key=lambda x: -x["balance_remaining"])
        return jsonify({"bucket": bucket, "invoices": out})
    finally:
        conn.close()


@app.route("/api/expense-category/<path:category>/bills")
@auth.login_required
def api_expense_category_bills(category):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.txn_id, b.txn_number, b.vendor_name, b.txn_date, b.amount,
                   b.balance_remaining, bl.description, bl.amount AS line_amount
            FROM bill_line bl JOIN bill b ON b.txn_id = bl.txn_id
            WHERE bl.expense_account = ? ORDER BY b.txn_date DESC
        """, (category,))
        return jsonify({"category": category,
                        "bills": [dict(r) for r in cur.fetchall()]})
    finally:
        conn.close()


@app.route("/api/search")
@auth.login_required
def api_search():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify([])
    conn = get_db()
    try:
        return jsonify(search_mod.search_all(conn, q))
    finally:
        conn.close()


@app.route("/api/diagnostics")
@auth.admin_required
def api_diagnostics():
    """Admin-only: dump raw account + transaction data for debugging."""
    conn = get_db()
    try:
        return jsonify(reports.diagnostics(conn))
    finally:
        conn.close()


@app.route("/diagnostics")
@auth.admin_required
def diagnostics_page():
    """Human-readable page for admins to quickly see data health."""
    conn = get_db()
    try:
        diag = reports.diagnostics(conn)
        return render_template("diagnostics.html", diag=diag,
                               current_user_name=session.get("name"),
                               current_user_role=session.get("role"))
    finally:
        conn.close()


# ---------- Entry point ----------
if __name__ == "__main__":
    init_db_if_needed()
    start_scheduler()
    print("=" * 60)
    print("  QuickBooks Management Dashboard — Phase 2A (Auth)")
    print("=" * 60)
    print("  Open:    http://localhost:5000")
    print("  Login:   username: admin   password: admin   (change on first login)")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
