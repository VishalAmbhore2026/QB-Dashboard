"""
ETL — sync all QB tables into the local SQLite cache.

Each table is wrapped in its own try/except. A failure on one table
doesn't block the others. Sync status is recorded in sync_log.
"""
import traceback
from datetime import datetime
import qb_connector
from schema import create_schema, wipe_data


def _fmt_date(val):
    if val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return str(val)[:10]


def _safe(val, default=None):
    if val is None or val == "":
        return default
    return val


def _float(val):
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def _int_bool(val):
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, str):
        return 1 if val.lower() in ("true", "1", "yes") else 0
    return 1 if val else 0


def _log_sync(sqlite_conn, table_name, rows, started_at, status, message=""):
    cur = sqlite_conn.cursor()
    cur.execute("""INSERT INTO sync_log (table_name, rows, started_at, finished_at, status, message)
                   VALUES (?,?,?,?,?,?)""",
                (table_name, rows, started_at.isoformat(),
                 datetime.now().isoformat(), status, message[:500]))
    sqlite_conn.commit()


def _run_one(label, func, sqlite_conn, qb):
    started = datetime.now()
    try:
        print(f"[sync] {label}...")
        count = func(sqlite_conn, qb)
        print(f"[sync] {label} OK ({count} rows)")
        _log_sync(sqlite_conn, label, count, started, "OK")
        return None
    except Exception as e:
        err = str(e)
        print(f"[sync] {label} FAILED -> {err}")
        traceback.print_exc()
        _log_sync(sqlite_conn, label, 0, started, "FAILED", err)
        return f"{label}: {err}"


# ============ per-table loaders ============

def _etl_customers(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM customer")
    rows = qb_connector.fetch_customers(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO customer VALUES (?,?,?,?,?,?,?)",
                    (r["list_id"], _safe(r["name"], "(no name)"),
                     _safe(r["company"]), _safe(r["phone"]), _safe(r["email"]),
                     _float(r["balance"]), _int_bool(r["is_active"])))
    conn.commit()
    return len(rows)


def _etl_vendors(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM vendor")
    rows = qb_connector.fetch_vendors(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO vendor VALUES (?,?,?,?,?,?,?)",
                    (r["list_id"], _safe(r["name"], "(no name)"),
                     _safe(r["company"]), _safe(r["phone"]), _safe(r["email"]),
                     _float(r["balance"]), _int_bool(r["is_active"])))
    conn.commit()
    return len(rows)


def _etl_accounts(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM account")
    rows = qb_connector.fetch_accounts(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO account VALUES (?,?,?,?,?)",
                    (r["list_id"], _safe(r["name"], "(no name)"),
                     _safe(r["account_type"]), _float(r["balance"]),
                     _int_bool(r["is_active"])))
    conn.commit()
    return len(rows)


def _etl_items(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM item")
    rows = qb_connector.fetch_items(qb)
    for r in rows:
        cur.execute("""INSERT OR REPLACE INTO item
            (list_id, full_name, item_type, description, sales_price,
             purchase_cost, quantity_on_hand, is_active,
             income_account, expense_account, cogs_account)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (r["list_id"], _safe(r["name"], "(no name)"),
                     _safe(r["item_type"]), _safe(r["description"]),
                     _float(r["sales_price"]), _float(r["purchase_cost"]),
                     _float(r["qty_on_hand"]), _int_bool(r["is_active"]),
                     _safe(r.get("income_account")),
                     _safe(r.get("expense_account")),
                     _safe(r.get("cogs_account"))))
    conn.commit()
    return len(rows)


def _build_item_account_map(conn):
    """Map item_name -> best_expense_account (prefer COGS, then expense)."""
    cur = conn.cursor()
    cur.execute("SELECT full_name, cogs_account, expense_account FROM item")
    m = {}
    for name, cogs, exp in cur.fetchall():
        m[name] = cogs or exp or None
    return m


def _etl_classes(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM class")
    rows = qb_connector.fetch_classes(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO class VALUES (?,?,?)",
                    (r["list_id"], _safe(r["name"]), _int_bool(r["is_active"])))
    conn.commit()
    return len(rows)


def _etl_invoices(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM invoice")
    rows = qb_connector.fetch_invoices(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO invoice VALUES (?,?,?,?,?,?,?,?)",
                    (r["txn_id"], str(_safe(r["number"], "")), _safe(r["customer"]),
                     _fmt_date(r["txn_date"]), _fmt_date(r["due_date"]),
                     _float(r["subtotal"]), _float(r["balance"]),
                     _int_bool(r["is_paid"])))
    conn.commit()
    return len(rows)


def _auto_line(r, counter):
    txn = r["txn_id"] or ""
    lid = r.get("line_id")
    if lid is None or lid == "":
        counter[txn] = counter.get(txn, 0) + 1
        return txn, f"auto-{counter[txn]}"
    return txn, str(lid)


def _etl_invoice_lines(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM invoice_line")
    rows = qb_connector.fetch_invoice_lines(qb)
    counter = {}
    count = 0
    for r in rows:
        txn, lid = _auto_line(r, counter)
        try:
            cur.execute("INSERT OR IGNORE INTO invoice_line VALUES (?,?,?,?,?,?,?,?)",
                        (txn, lid, _safe(r["item"]), _safe(r["description"]),
                         _float(r["quantity"]), _float(r["rate"]),
                         _float(r["amount"]), _safe(r.get("class_name"))))
            count += 1
        except Exception:
            pass
    conn.commit()
    return count


def _etl_bills(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM bill")
    rows = qb_connector.fetch_bills(qb)
    for r in rows:
        amount_due = _float(r["amount_due"])
        amount_paid = _float(r["amount_paid"])
        cur.execute("INSERT OR REPLACE INTO bill VALUES (?,?,?,?,?,?,?,?,?)",
                    (r["txn_id"], str(_safe(r["number"], "")), _safe(r["vendor"]),
                     _fmt_date(r["txn_date"]), _fmt_date(r["due_date"]),
                     amount_due, amount_paid, amount_due - amount_paid,
                     _int_bool(r["is_paid"])))
    conn.commit()
    return len(rows)


def _etl_bill_lines(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM bill_line")
    total = 0

    # Expense lines
    expense_rows = qb_connector.fetch_bill_expense_lines(qb)
    counter = {}
    for r in expense_rows:
        txn, lid = _auto_line(r, counter)
        try:
            cur.execute("""INSERT OR IGNORE INTO bill_line
                (txn_id, txn_line_id, expense_account, description, amount,
                 source_type, item_name)
                VALUES (?,?,?,?,?,?,?)""",
                (txn, f"exp-{lid}", _safe(r["account"]), _safe(r["memo"]),
                 _float(r["amount"]), "expense", None))
            total += 1
        except Exception:
            pass

    # Item lines — resolve the item's expense/COGS account
    try:
        item_map = _build_item_account_map(conn)
        item_rows = qb_connector.fetch_bill_item_lines(qb)
        counter2 = {}
        for r in item_rows:
            txn, lid = _auto_line(r, counter2)
            item_name = _safe(r.get("item"))
            resolved = item_map.get(item_name) if item_name else None
            # If we can't resolve the item, still store it with item name
            # as the "account" (better than dropping, and reports filter later)
            account = resolved or (f"(item: {item_name})" if item_name else None)
            try:
                cur.execute("""INSERT OR IGNORE INTO bill_line
                    (txn_id, txn_line_id, expense_account, description, amount,
                     source_type, item_name)
                    VALUES (?,?,?,?,?,?,?)""",
                    (txn, f"itm-{lid}", account, _safe(r.get("memo")),
                     _float(r["amount"]), "item", item_name))
                total += 1
            except Exception:
                pass
    except Exception as e:
        print(f"[sync] Bill Item Lines not available: {e}")

    conn.commit()
    return total


def _etl_checks(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM check_txn")
    rows = qb_connector.fetch_checks(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO check_txn VALUES (?,?,?,?,?,?,?)",
                    (r["txn_id"], str(_safe(r["number"], "")),
                     _safe(r["bank_account"]), _safe(r["payee"]),
                     _fmt_date(r["txn_date"]), _float(r["amount"]),
                     _safe(r["memo"])))
    conn.commit()
    return len(rows)


def _etl_check_lines(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM check_line")
    total = 0

    # Expense lines
    expense_rows = qb_connector.fetch_check_expense_lines(qb)
    counter = {}
    for r in expense_rows:
        txn, lid = _auto_line(r, counter)
        try:
            cur.execute("""INSERT OR IGNORE INTO check_line
                (txn_id, txn_line_id, expense_account, amount, memo,
                 source_type, item_name)
                VALUES (?,?,?,?,?,?,?)""",
                (txn, f"exp-{lid}", _safe(r["account"]), _float(r["amount"]),
                 _safe(r["memo"]), "expense", None))
            total += 1
        except Exception:
            pass

    # Item lines
    try:
        item_map = _build_item_account_map(conn)
        item_rows = qb_connector.fetch_check_item_lines(qb)
        counter2 = {}
        for r in item_rows:
            txn, lid = _auto_line(r, counter2)
            item_name = _safe(r.get("item"))
            resolved = item_map.get(item_name) if item_name else None
            account = resolved or (f"(item: {item_name})" if item_name else None)
            try:
                cur.execute("""INSERT OR IGNORE INTO check_line
                    (txn_id, txn_line_id, expense_account, amount, memo,
                     source_type, item_name)
                    VALUES (?,?,?,?,?,?,?)""",
                    (txn, f"itm-{lid}", account, _float(r["amount"]),
                     _safe(r.get("memo")), "item", item_name))
                total += 1
            except Exception:
                pass
    except Exception as e:
        print(f"[sync] Check Item Lines not available: {e}")

    conn.commit()
    return total


def _etl_deposits(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM deposit_txn")
    rows = qb_connector.fetch_deposits(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO deposit_txn VALUES (?,?,?,?,?)",
                    (r["txn_id"], _safe(r["bank_account"]),
                     _fmt_date(r["txn_date"]), _float(r["amount"]),
                     _safe(r["memo"])))
    conn.commit()
    return len(rows)


def _etl_cc_charges(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM cc_charge")
    rows = qb_connector.fetch_cc_charges(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO cc_charge VALUES (?,?,?,?,?,?,?)",
                    (r["txn_id"], str(_safe(r["number"], "")),
                     _safe(r["cc_account"]), _safe(r["payee"]),
                     _fmt_date(r["txn_date"]), _float(r["amount"]),
                     _safe(r["memo"])))
    conn.commit()
    return len(rows)


def _etl_cc_charge_lines(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM cc_charge_line")
    total = 0

    # Expense lines
    expense_rows = qb_connector.fetch_cc_charge_lines(qb)
    counter = {}
    for r in expense_rows:
        txn, lid = _auto_line(r, counter)
        try:
            cur.execute("""INSERT OR IGNORE INTO cc_charge_line
                (txn_id, txn_line_id, expense_account, amount, memo,
                 source_type, item_name)
                VALUES (?,?,?,?,?,?,?)""",
                (txn, f"exp-{lid}", _safe(r["account"]), _float(r["amount"]),
                 _safe(r["memo"]), "expense", None))
            total += 1
        except Exception:
            pass

    # Item lines
    try:
        item_map = _build_item_account_map(conn)
        item_rows = qb_connector.fetch_cc_charge_item_lines(qb)
        counter2 = {}
        for r in item_rows:
            txn, lid = _auto_line(r, counter2)
            item_name = _safe(r.get("item"))
            resolved = item_map.get(item_name) if item_name else None
            account = resolved or (f"(item: {item_name})" if item_name else None)
            try:
                cur.execute("""INSERT OR IGNORE INTO cc_charge_line
                    (txn_id, txn_line_id, expense_account, amount, memo,
                     source_type, item_name)
                    VALUES (?,?,?,?,?,?,?)""",
                    (txn, f"itm-{lid}", account, _float(r["amount"]),
                     _safe(r.get("memo")), "item", item_name))
                total += 1
            except Exception:
                pass
    except Exception as e:
        print(f"[sync] CC Charge Item Lines not available: {e}")

    conn.commit()
    return total


def _etl_journals(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM journal_entry")
    rows = qb_connector.fetch_journal_entries(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO journal_entry VALUES (?,?,?,?)",
                    (r["txn_id"], str(_safe(r["number"], "")),
                     _fmt_date(r["txn_date"]), _safe(r["memo"])))
    conn.commit()
    return len(rows)


def _etl_journal_lines(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM journal_line")
    rows = qb_connector.fetch_journal_lines(qb)
    counter = {}
    count = 0
    with_account = 0
    for r in rows:
        txn, lid = _auto_line(r, counter)
        debit = _float(r.get("debit"))
        credit = _float(r.get("credit"))
        # If only "amount" is present, use its sign to infer debit/credit
        if debit == 0 and credit == 0 and r.get("amount") is not None:
            amt = _float(r["amount"])
            if amt >= 0:
                debit = amt
            else:
                credit = -amt
        account = _safe(r.get("account"))
        if account:
            with_account += 1
        try:
            cur.execute("INSERT OR IGNORE INTO journal_line VALUES (?,?,?,?,?,?)",
                        (txn, lid, account, debit, credit, _safe(r.get("memo"))))
            count += 1
        except Exception:
            pass
    conn.commit()
    if count > 0 and with_account < count * 0.5:
        print(f"[sync] WARNING: Journal Lines inserted {count} rows, but only "
              f"{with_account} have account names. Expenses from JEs will be missed.")
    return count


def _etl_receive_payments(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM receive_payment")
    rows = qb_connector.fetch_receive_payments(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO receive_payment VALUES (?,?,?,?,?,?)",
                    (r["txn_id"], _safe(r["customer"]),
                     _fmt_date(r["txn_date"]), _float(r["amount"]),
                     _safe(r["deposit_account"]), _safe(r["memo"])))
    conn.commit()
    return len(rows)


def _etl_bill_payments(conn, qb):
    cur = conn.cursor()
    cur.execute("DELETE FROM bill_payment")
    rows = qb_connector.fetch_bill_payments(qb)
    for r in rows:
        cur.execute("INSERT OR REPLACE INTO bill_payment VALUES (?,?,?,?,?)",
                    (r["txn_id"], _safe(r["vendor"]),
                     _fmt_date(r["txn_date"]), _float(r["amount"]),
                     _safe(r["bank_account"])))
    conn.commit()
    return len(rows)


# ============ public entry point ============

ALL_TABLES = [
    ("Customers",          _etl_customers),
    ("Vendors",            _etl_vendors),
    ("Accounts",           _etl_accounts),
    ("Items",              _etl_items),
    ("Classes",            _etl_classes),
    ("Invoices",           _etl_invoices),
    ("Invoice Lines",      _etl_invoice_lines),
    ("Bills",              _etl_bills),
    ("Bill Lines",         _etl_bill_lines),
    ("Checks",             _etl_checks),
    ("Check Lines",        _etl_check_lines),
    ("Deposits",           _etl_deposits),
    ("Credit Card Charges", _etl_cc_charges),
    ("Credit Card Lines",  _etl_cc_charge_lines),
    ("Journal Entries",    _etl_journals),
    ("Journal Lines",      _etl_journal_lines),
    ("Receive Payments",   _etl_receive_payments),
    ("Bill Payments",      _etl_bill_payments),
]


def run_full_sync(sqlite_conn, dsn_name: str):
    """Sync all QB tables. Partial failures don't abort the whole run."""
    qb = qb_connector.get_connection(dsn_name)
    errors = []
    try:
        for label, func in ALL_TABLES:
            errors.append(_run_one(label, func, sqlite_conn, qb))
    finally:
        qb.close()

    errs = [e for e in errors if e]
    if errs:
        raise RuntimeError(f"Partial sync ({len(errs)} of {len(ALL_TABLES)} failed): "
                           + " | ".join(errs))
