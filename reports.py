"""
Financial reports — clean rewrite using Virtual General Ledger approach.

The core idea: build ONE function (`compute_gl_movements`) that pulls every
debit/credit movement from every transaction type. Both P&L and Balance Sheet
derive from that single GL. If the GL is correct, double-entry math makes
BS balance automatically and P&L matches QB's own report to the penny.

Movement sources (each with proper debit/credit split):
  1. Journal lines (direct)
  2. Invoice lines (credit income via item) + Invoice header (debit AR)
  3. Bill lines - expense (debit expense) + Bill lines - item (debit
     item's cogs/expense account) + Bill header (credit AP)
  4. Check lines - expense + item (debit expense) + Check header (credit bank)
  5. CC Charge lines - expense + item (debit expense) + CC header (credit CC)
  6. Deposits (debit bank)
  7. Receive Payment (debit deposit_account, credit AR)
  8. Bill Payment (debit AP, credit bank)

Sign convention:
  - Debit-balance accounts (Asset, Expense): balance = debits - credits
  - Credit-balance accounts (Liability, Equity, Income): balance = credits - debits
"""
import sqlite3
from datetime import datetime, date, timedelta
from calendar import monthrange


# ===================== Period helpers =====================

def parse_date(s):
    if isinstance(s, (date, datetime)):
        return s if isinstance(s, date) and not isinstance(s, datetime) else s.date()
    if not s:
        return None
    return datetime.strptime(s[:10], "%Y-%m-%d").date()


def today():
    return date.today()


def first_day_of_month(d):
    return d.replace(day=1)


def last_day_of_month(d):
    _, last = monthrange(d.year, d.month)
    return d.replace(day=last)


def add_months(d, months):
    m = d.month - 1 + months
    year = d.year + m // 12
    month = m % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)


def preset_range(preset: str):
    t = today()
    if preset == "today":            return (t, t)
    if preset == "yesterday":        return (t - timedelta(days=1), t - timedelta(days=1))
    if preset == "this_week":        return (t - timedelta(days=t.weekday()), t)
    if preset == "this_month":       return (first_day_of_month(t), t)
    if preset == "this_quarter":
        q = (t.month - 1) // 3
        return (date(t.year, q * 3 + 1, 1), t)
    if preset == "this_year":        return (date(t.year, 1, 1), t)
    if preset == "last_7":           return (t - timedelta(days=6), t)
    if preset == "last_30":          return (t - timedelta(days=29), t)
    if preset == "last_90":          return (t - timedelta(days=89), t)
    if preset == "last_month":
        first_this = first_day_of_month(t)
        last_prev = first_this - timedelta(days=1)
        return (first_day_of_month(last_prev), last_prev)
    if preset == "last_quarter":
        q = (t.month - 1) // 3
        start_this = date(t.year, q * 3 + 1, 1)
        end_prev = start_this - timedelta(days=1)
        start_prev = date(end_prev.year, ((end_prev.month - 1) // 3) * 3 + 1, 1)
        return (start_prev, end_prev)
    if preset == "last_year":        return (date(t.year - 1, 1, 1), date(t.year - 1, 12, 31))
    if preset == "ytd":              return (date(t.year, 1, 1), t)
    if preset == "mtd":              return (first_day_of_month(t), t)
    if preset == "qtd":
        q = (t.month - 1) // 3
        return (date(t.year, q * 3 + 1, 1), t)
    if preset == "all":              return (date(1900, 1, 1), t)
    return (first_day_of_month(t), t)


def compute_comparison_range(from_date, to_date, mode):
    from_d = parse_date(from_date) if isinstance(from_date, str) else from_date
    to_d = parse_date(to_date) if isinstance(to_date, str) else to_date
    if mode == "previous_year":
        try:
            return (from_d.replace(year=from_d.year - 1),
                    to_d.replace(year=to_d.year - 1))
        except ValueError:
            return (from_d - timedelta(days=365), to_d - timedelta(days=365))
    if mode == "previous_month":
        return (add_months(from_d, -1), add_months(to_d, -1))
    if mode == "previous_quarter":
        return (add_months(from_d, -3), add_months(to_d, -3))
    span = (to_d - from_d).days + 1
    new_to = from_d - timedelta(days=1)
    new_from = new_to - timedelta(days=span - 1)
    return (new_from, new_to)


# ===================== Account type classification =====================

INCOME_TYPES = {"Income", "OtherIncome"}
EXPENSE_TYPES = {"Expense", "OtherExpense", "CostOfGoodsSold"}
COGS_TYPES = {"CostOfGoodsSold"}
ASSET_TYPES = {"Bank", "AccountsReceivable", "OtherCurrentAsset",
               "FixedAsset", "OtherAsset"}
LIAB_TYPES = {"AccountsPayable", "CreditCard", "OtherCurrentLiability",
              "LongTermLiability"}
EQUITY_TYPES = {"Equity"}
DEBIT_NORMAL_TYPES = ASSET_TYPES | EXPENSE_TYPES  # balance = dr - cr
CREDIT_NORMAL_TYPES = LIAB_TYPES | EQUITY_TYPES | INCOME_TYPES  # balance = cr - dr


# ===================== Helpers =====================

def _scalar(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else 0.0


def _get_account_types_map(conn):
    cur = conn.cursor()
    cur.execute("SELECT full_name, account_type FROM account")
    return {row[0]: row[1] for row in cur.fetchall()}


def _first_account_of_type(conn, account_type):
    """Return the full_name of the first active account of a given type, or None."""
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name FROM account
        WHERE account_type = ? AND is_active = 1
        ORDER BY full_name LIMIT 1
    """, (account_type,))
    row = cur.fetchone()
    return row[0] if row else None


# ===================== THE VIRTUAL GENERAL LEDGER =====================

def _gl_sql_parts(ar_account, ap_account):
    """
    Build the list of (name, sql, needs_ar_ap_params) tuples for every
    GL movement source. Each source returns rows of:
      (account_name, debit, credit, txn_date, source_tag, txn_id)
    """
    parts = []

    # ---- 1. Journal Entry lines (direct debit/credit) ----
    parts.append(("je_lines", """
        SELECT jl.account_name, jl.debit, jl.credit, je.txn_date,
               'je_line' AS source, jl.txn_id
        FROM journal_line jl
        JOIN journal_entry je ON je.txn_id = jl.txn_id
        WHERE jl.account_name IS NOT NULL AND jl.account_name != ''
    """))

    # ---- 2a. Invoice LINES — credit income via item.income_account ----
    parts.append(("invoice_line_income", """
        SELECT i.income_account, 0 AS debit, il.amount AS credit,
               inv.txn_date, 'invoice_line' AS source, il.txn_id
        FROM invoice_line il
        JOIN invoice inv ON inv.txn_id = il.txn_id
        LEFT JOIN item i ON i.full_name = il.item_name
        WHERE i.income_account IS NOT NULL AND i.income_account != ''
    """))

    # ---- 2b. Invoice HEADER — debit AR (offset to income lines) ----
    if ar_account:
        parts.append(("invoice_header_ar", f"""
            SELECT ? AS account_name, inv.subtotal AS debit, 0 AS credit,
                   inv.txn_date, 'invoice_ar' AS source, inv.txn_id
            FROM invoice inv
        """, "ar"))

    # ---- 3a. Bill EXPENSE lines — debit expense account directly ----
    parts.append(("bill_line_expense", """
        SELECT bl.expense_account, bl.amount AS debit, 0 AS credit,
               b.txn_date, 'bill_line_exp' AS source, bl.txn_id
        FROM bill_line bl
        JOIN bill b ON b.txn_id = bl.txn_id
        WHERE bl.expense_account IS NOT NULL AND bl.expense_account != ''
    """))

    # ---- 3b. Bill ITEM lines with NO resolved expense_account — resolve via item
    parts.append(("bill_line_item", """
        SELECT COALESCE(i.cogs_account, i.expense_account) AS account_name,
               bl.amount AS debit, 0 AS credit,
               b.txn_date, 'bill_line_item' AS source, bl.txn_id
        FROM bill_line bl
        JOIN bill b ON b.txn_id = bl.txn_id
        LEFT JOIN item i ON i.full_name = bl.item_name
        WHERE (bl.expense_account IS NULL OR bl.expense_account = '')
          AND bl.item_name IS NOT NULL
          AND COALESCE(i.cogs_account, i.expense_account) IS NOT NULL
    """))

    # ---- 3c. Bill HEADER — credit AP ----
    if ap_account:
        parts.append(("bill_header_ap", f"""
            SELECT ? AS account_name, 0 AS debit, b.amount AS credit,
                   b.txn_date, 'bill_ap' AS source, b.txn_id
            FROM bill b
        """, "ap"))

    # ---- 4a. Check EXPENSE lines — debit expense ----
    parts.append(("check_line_expense", """
        SELECT cl.expense_account, cl.amount AS debit, 0 AS credit,
               c.txn_date, 'check_line_exp' AS source, cl.txn_id
        FROM check_line cl
        JOIN check_txn c ON c.txn_id = cl.txn_id
        WHERE cl.expense_account IS NOT NULL AND cl.expense_account != ''
    """))

    # ---- 4b. Check ITEM lines — resolve via item ----
    parts.append(("check_line_item", """
        SELECT COALESCE(i.cogs_account, i.expense_account) AS account_name,
               cl.amount AS debit, 0 AS credit,
               c.txn_date, 'check_line_item' AS source, cl.txn_id
        FROM check_line cl
        JOIN check_txn c ON c.txn_id = cl.txn_id
        LEFT JOIN item i ON i.full_name = cl.item_name
        WHERE (cl.expense_account IS NULL OR cl.expense_account = '')
          AND cl.item_name IS NOT NULL
          AND COALESCE(i.cogs_account, i.expense_account) IS NOT NULL
    """))

    # ---- 4c. Check HEADER — credit bank ----
    parts.append(("check_header_bank", """
        SELECT c.bank_account, 0 AS debit, c.amount AS credit,
               c.txn_date, 'check_bank' AS source, c.txn_id
        FROM check_txn c
        WHERE c.bank_account IS NOT NULL AND c.bank_account != ''
    """))

    # ---- 5a. CC Charge EXPENSE lines — debit expense ----
    parts.append(("cc_line_expense", """
        SELECT ccl.expense_account, ccl.amount AS debit, 0 AS credit,
               cc.txn_date, 'cc_line_exp' AS source, ccl.txn_id
        FROM cc_charge_line ccl
        JOIN cc_charge cc ON cc.txn_id = ccl.txn_id
        WHERE ccl.expense_account IS NOT NULL AND ccl.expense_account != ''
    """))

    # ---- 5b. CC Charge ITEM lines — resolve via item ----
    parts.append(("cc_line_item", """
        SELECT COALESCE(i.cogs_account, i.expense_account) AS account_name,
               ccl.amount AS debit, 0 AS credit,
               cc.txn_date, 'cc_line_item' AS source, ccl.txn_id
        FROM cc_charge_line ccl
        JOIN cc_charge cc ON cc.txn_id = ccl.txn_id
        LEFT JOIN item i ON i.full_name = ccl.item_name
        WHERE (ccl.expense_account IS NULL OR ccl.expense_account = '')
          AND ccl.item_name IS NOT NULL
          AND COALESCE(i.cogs_account, i.expense_account) IS NOT NULL
    """))

    # ---- 5c. CC Charge HEADER — credit CC account ----
    parts.append(("cc_header_account", """
        SELECT cc.credit_card_account, 0 AS debit, cc.amount AS credit,
               cc.txn_date, 'cc_account' AS source, cc.txn_id
        FROM cc_charge cc
        WHERE cc.credit_card_account IS NOT NULL AND cc.credit_card_account != ''
    """))

    # ---- 6. Deposits — debit bank (offset typically from receive_payment or JE) ----
    parts.append(("deposit", """
        SELECT d.bank_account, d.amount AS debit, 0 AS credit,
               d.txn_date, 'deposit' AS source, d.txn_id
        FROM deposit_txn d
        WHERE d.bank_account IS NOT NULL AND d.bank_account != ''
    """))

    # ---- 7a. Receive Payment — debit deposit account ----
    parts.append(("rp_debit_bank", """
        SELECT rp.deposit_account, rp.amount AS debit, 0 AS credit,
               rp.txn_date, 'rp_bank' AS source, rp.txn_id
        FROM receive_payment rp
        WHERE rp.deposit_account IS NOT NULL AND rp.deposit_account != ''
    """))

    # ---- 7b. Receive Payment — credit AR ----
    if ar_account:
        parts.append(("rp_credit_ar", f"""
            SELECT ? AS account_name, 0 AS debit, rp.amount AS credit,
                   rp.txn_date, 'rp_ar' AS source, rp.txn_id
            FROM receive_payment rp
        """, "ar"))

    # ---- 8a. Bill Payment — credit bank ----
    parts.append(("bp_credit_bank", """
        SELECT bp.bank_account, 0 AS debit, bp.amount AS credit,
               bp.txn_date, 'bp_bank' AS source, bp.txn_id
        FROM bill_payment bp
        WHERE bp.bank_account IS NOT NULL AND bp.bank_account != ''
    """))

    # ---- 8b. Bill Payment — debit AP ----
    if ap_account:
        parts.append(("bp_debit_ap", f"""
            SELECT ? AS account_name, bp.amount AS debit, 0 AS credit,
                   bp.txn_date, 'bp_ap' AS source, bp.txn_id
            FROM bill_payment bp
        """, "ap"))

    return parts


def _iter_gl_movements(conn, from_date=None, to_date=None, as_of_date=None):
    """
    Yield (account_name, debit, credit, txn_date, source, txn_id) tuples
    for every movement in the virtual GL. Optionally filtered by date.
    """
    ar_account = _first_account_of_type(conn, "AccountsReceivable")
    ap_account = _first_account_of_type(conn, "AccountsPayable")
    parts = _gl_sql_parts(ar_account, ap_account)

    cur = conn.cursor()
    for part in parts:
        name = part[0]
        sql = part[1]
        needs_param = part[2] if len(part) > 2 else None

        # Build WHERE date clause
        where_clauses = []
        extra_params = []
        if from_date is not None:
            where_clauses.append(" AND txn_date >= ?")
            extra_params.append(str(from_date))
        if to_date is not None:
            where_clauses.append(" AND txn_date <= ?")
            extra_params.append(str(to_date))
        if as_of_date is not None:
            where_clauses.append(" AND txn_date <= ?")
            extra_params.append(str(as_of_date))

        # Wrap the inner sql so our additions compose correctly
        wrapped = f"SELECT * FROM ({sql}) WHERE 1=1 {''.join(where_clauses)}"

        # Build param list
        params = []
        if needs_param == "ar" and ar_account:
            params.append(ar_account)
        elif needs_param == "ap" and ap_account:
            params.append(ap_account)
        params.extend(extra_params)

        try:
            cur.execute(wrapped, tuple(params))
            for row in cur.fetchall():
                yield row
        except Exception as e:
            # Don't fail the whole GL for one broken source
            print(f"[reports] GL source '{name}' failed: {e}")
            continue


def compute_gl_balances(conn, as_of_date):
    """
    Return {account_name: net_balance} for all accounts as of the date.
    net_balance = sum(debit) - sum(credit)
    (positive for debit-balance accounts; negative for credit-balance accounts)

    Includes account.balance from the chart-of-accounts as an opening seed
    ONLY for Bank / CreditCard / FixedAsset / OtherCurrentAsset / OtherAsset /
    LongTermLiability / OtherCurrentLiability / Equity accounts whose
    balance field is non-zero AND which have no corresponding GL movements.
    This handles demo-data seeding and QB files where opening balances
    weren't recorded as journal entries.
    """
    balances = {}
    for acc, debit, credit, _dt, _src, _tid in _iter_gl_movements(conn, as_of_date=as_of_date):
        if not acc:
            continue
        balances[acc] = balances.get(acc, 0) + (debit or 0) - (credit or 0)

    # Add seeded opening balances for BS accounts not already covered by GL
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, account_type, balance
        FROM account
        WHERE is_active = 1
          AND balance IS NOT NULL
          AND balance != 0
          AND account_type IN ('Bank', 'CreditCard', 'FixedAsset',
                               'OtherCurrentAsset', 'OtherAsset',
                               'LongTermLiability', 'OtherCurrentLiability',
                               'Equity')
    """)
    for name, atype, stored_bal in cur.fetchall():
        if name in balances and abs(balances[name]) > 0.01:
            continue  # already has GL activity; don't double-count
        # Translate the QB-stored balance into our dr-cr convention
        # Bank/FixedAsset/OtherAsset/OtherCurrentAsset = debit-balance (positive as-is)
        # CreditCard/Liability/Equity = credit-balance (store as negative)
        if atype in ("Bank", "FixedAsset", "OtherCurrentAsset", "OtherAsset"):
            balances[name] = float(stored_bal)
        else:
            # Liabilities / Equity / CC: QB stores as positive; we store negative
            # (QB's CC balance is sometimes already negative when owed money — handle either)
            if stored_bal > 0:
                balances[name] = -float(stored_bal)
            else:
                balances[name] = float(stored_bal)

    return balances


def compute_gl_period(conn, from_date, to_date):
    """
    Return {account_name: {'debit': x, 'credit': y}} aggregated over the period.
    """
    totals = {}
    for acc, debit, credit, _dt, _src, _tid in _iter_gl_movements(conn, from_date=from_date, to_date=to_date):
        if not acc:
            continue
        if acc not in totals:
            totals[acc] = {"debit": 0, "credit": 0}
        totals[acc]["debit"] += (debit or 0)
        totals[acc]["credit"] += (credit or 0)
    return totals


# ===================== P&L =====================

def profit_loss(conn, from_date, to_date):
    """
    P&L derived from the virtual GL. For each account with P&L-type classification,
    sum net movement for the period.
      - Income/OtherIncome: net = credits - debits (positive = revenue)
      - Expense/OtherExpense/CostOfGoodsSold: net = debits - credits (positive = expense)
    """
    acc_types = _get_account_types_map(conn)
    period = compute_gl_period(conn, from_date, to_date)

    revenue_rows = []
    other_income_rows = []
    cogs_rows = []
    opex_rows = []
    other_expense_rows = []

    for acc, totals in period.items():
        atype = acc_types.get(acc)
        if not atype:
            continue
        dr = totals["debit"]
        cr = totals["credit"]

        if atype == "Income":
            amt = cr - dr
            if abs(amt) > 0.01:
                revenue_rows.append({"label": acc, "amount": round(amt, 2)})
        elif atype == "OtherIncome":
            amt = cr - dr
            if abs(amt) > 0.01:
                other_income_rows.append({"label": acc, "amount": round(amt, 2)})
        elif atype == "CostOfGoodsSold":
            amt = dr - cr
            if abs(amt) > 0.01:
                cogs_rows.append({"label": acc, "amount": round(amt, 2)})
        elif atype == "Expense":
            amt = dr - cr
            if abs(amt) > 0.01:
                opex_rows.append({"label": acc, "amount": round(amt, 2)})
        elif atype == "OtherExpense":
            amt = dr - cr
            if abs(amt) > 0.01:
                other_expense_rows.append({"label": acc, "amount": round(amt, 2)})

    # Sort each section by amount descending
    for lst in (revenue_rows, other_income_rows, cogs_rows, opex_rows, other_expense_rows):
        lst.sort(key=lambda r: -r["amount"])

    total_revenue = sum(r["amount"] for r in revenue_rows)
    total_other_income = sum(r["amount"] for r in other_income_rows)
    total_cogs = sum(r["amount"] for r in cogs_rows)
    total_opex = sum(r["amount"] for r in opex_rows)
    total_other_expense = sum(r["amount"] for r in other_expense_rows)

    gross_profit = total_revenue - total_cogs
    net_ordinary = gross_profit - total_opex
    net_income = net_ordinary + total_other_income - total_other_expense

    gross_margin_pct = (gross_profit / total_revenue * 100) if total_revenue else 0
    net_margin_pct = (net_income / total_revenue * 100) if total_revenue else 0

    return {
        "from": str(from_date),
        "to": str(to_date),
        "total_revenue": round(total_revenue, 2),
        "revenue_rows": revenue_rows,
        "cogs_total": round(total_cogs, 2),
        "cogs_rows": cogs_rows,
        "gross_profit": round(gross_profit, 2),
        "opex_total": round(total_opex, 2),
        "opex_rows": opex_rows,
        "total_expense": round(total_cogs + total_opex, 2),
        "net_ordinary_income": round(net_ordinary, 2),
        "other_income": round(total_other_income, 2),
        "other_income_rows": other_income_rows,
        "other_expense": round(total_other_expense, 2),
        "other_expense_rows": other_expense_rows,
        "net_income": round(net_income, 2),
        "gross_margin_pct": round(gross_margin_pct, 2),
        "net_margin_pct": round(net_margin_pct, 2),
    }


# ===================== Revenue / Expense scalar helpers =====================

def revenue_total(conn, from_date, to_date):
    pl = profit_loss(conn, from_date, to_date)
    return pl["total_revenue"] + pl["other_income"]


def expense_total(conn, from_date, to_date):
    pl = profit_loss(conn, from_date, to_date)
    return pl["cogs_total"] + pl["opex_total"] + pl["other_expense"]


def other_income_total(conn, from_date, to_date):
    return profit_loss(conn, from_date, to_date)["other_income"]


def expense_by_account(conn, from_date, to_date):
    """
    Return {account_name: total_amount} for expense-type accounts only.
    Used by the expense-categories endpoint.
    """
    acc_types = _get_account_types_map(conn)
    period = compute_gl_period(conn, from_date, to_date)
    out = {}
    for acc, totals in period.items():
        atype = acc_types.get(acc)
        if atype in EXPENSE_TYPES:
            amt = totals["debit"] - totals["credit"]
            if abs(amt) > 0.01:
                out[acc] = round(amt, 2)
    return out


# ===================== Balance Sheet =====================

def balance_sheet(conn, as_of_date):
    """
    Balance Sheet from the virtual GL.
    Includes ALL non-zero accounts, classified by type.
    Adds synthetic "Net Income (YTD)" equity line (Jan 1 of as-of year to date).
    """
    acc_types = _get_account_types_map(conn)
    gl_balances = compute_gl_balances(conn, as_of_date)

    # Also get the account_type for every account including inactive ones,
    # so we include balances even for accounts we missed above
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, account_type FROM account WHERE is_active = 1
    """)
    active_accounts = {row[0]: row[1] for row in cur.fetchall()}

    sections = {
        "Current Assets": [],
        "Fixed Assets": [],
        "Other Assets": [],
        "Current Liabilities": [],
        "Long-Term Liabilities": [],
        "Equity": [],
    }

    # Process every account we know about, not just those in GL
    all_acc_names = set(gl_balances.keys()) | set(active_accounts.keys())

    for name in all_acc_names:
        atype = acc_types.get(name) or active_accounts.get(name)
        if not atype:
            continue
        if atype in INCOME_TYPES or atype in EXPENSE_TYPES:
            continue  # P&L accounts don't go on BS

        raw = gl_balances.get(name, 0)

        # Display sign: flip for credit-normal accounts so all BS values show positive
        if atype in DEBIT_NORMAL_TYPES:
            display = raw
        else:
            display = -raw

        if abs(display) < 0.01:
            continue

        entry = {"name": name, "type": atype, "balance": round(display, 2)}

        if atype in ("Bank", "AccountsReceivable", "OtherCurrentAsset"):
            sections["Current Assets"].append(entry)
        elif atype == "FixedAsset":
            sections["Fixed Assets"].append(entry)
        elif atype == "OtherAsset":
            sections["Other Assets"].append(entry)
        elif atype in ("AccountsPayable", "CreditCard", "OtherCurrentLiability"):
            sections["Current Liabilities"].append(entry)
        elif atype == "LongTermLiability":
            sections["Long-Term Liabilities"].append(entry)
        elif atype == "Equity":
            sections["Equity"].append(entry)

    # Sort each section alphabetically for stable display
    for lst in sections.values():
        lst.sort(key=lambda x: x["name"])

    # Add synthetic "Net Income (YTD)" line to Equity (QB convention).
    # YTD = Jan 1 of the year of as_of_date through as_of_date.
    try:
        year_start = date(as_of_date.year, 1, 1)
    except AttributeError:
        as_of_date = parse_date(as_of_date)
        year_start = date(as_of_date.year, 1, 1)
    ytd_pnl = profit_loss(conn, year_start, as_of_date)
    ytd_net = ytd_pnl["net_income"]
    if abs(ytd_net) > 0.01:
        sections["Equity"].append({
            "name": "Net Income (YTD)",
            "type": "Equity",
            "balance": round(ytd_net, 2),
        })

    totals = {k: round(sum(a["balance"] for a in v), 2) for k, v in sections.items()}
    total_assets = totals["Current Assets"] + totals["Fixed Assets"] + totals["Other Assets"]
    total_liab = totals["Current Liabilities"] + totals["Long-Term Liabilities"]
    total_equity = totals["Equity"]

    return {
        "as_of": str(as_of_date),
        "sections": sections,
        "totals": totals,
        "total_assets": round(total_assets, 2),
        "total_liabilities": round(total_liab, 2),
        "total_equity": round(total_equity, 2),
        "total_liab_equity": round(total_liab + total_equity, 2),
        "out_of_balance": round(total_assets - (total_liab + total_equity), 2),
        "balances_reconciled": abs(total_assets - (total_liab + total_equity)) < 1.0,
    }


# ===================== Cash Flow =====================

def cash_flow(conn, from_date, to_date):
    params = (str(from_date), str(to_date))
    pmt = _scalar(conn, "SELECT COALESCE(SUM(amount),0) FROM receive_payment WHERE txn_date >= ? AND txn_date <= ?", params)
    dep = _scalar(conn, "SELECT COALESCE(SUM(amount),0) FROM deposit_txn WHERE txn_date >= ? AND txn_date <= ?", params)
    bp = _scalar(conn, "SELECT COALESCE(SUM(amount),0) FROM bill_payment WHERE txn_date >= ? AND txn_date <= ?", params)
    ck = _scalar(conn, "SELECT COALESCE(SUM(amount),0) FROM check_txn WHERE txn_date >= ? AND txn_date <= ?", params)
    cc = _scalar(conn, "SELECT COALESCE(SUM(amount),0) FROM cc_charge WHERE txn_date >= ? AND txn_date <= ?", params)
    inflow = pmt + dep
    outflow = bp + ck + cc
    return {
        "from": str(from_date), "to": str(to_date),
        "inflow": round(inflow, 2),
        "inflow_breakdown": [
            {"label": "Customer Payments", "amount": round(pmt, 2)},
            {"label": "Deposits", "amount": round(dep, 2)},
        ],
        "outflow": round(outflow, 2),
        "outflow_breakdown": [
            {"label": "Bill Payments", "amount": round(bp, 2)},
            {"label": "Checks", "amount": round(ck, 2)},
            {"label": "Credit Card Charges", "amount": round(cc, 2)},
        ],
        "net_cash_flow": round(inflow - outflow, 2),
    }


def cash_in(conn, from_date, to_date):
    return cash_flow(conn, from_date, to_date)["inflow"]


def cash_out(conn, from_date, to_date):
    return cash_flow(conn, from_date, to_date)["outflow"]


# ===================== KPIs =====================

def kpi_snapshot(conn, from_date, to_date):
    """Headline KPI tiles for the dashboard."""
    # Cash from BS-style computation so it's accurate
    gl_bal = compute_gl_balances(conn, to_date)
    acc_types = _get_account_types_map(conn)
    cash = 0
    for name, bal in gl_bal.items():
        t = acc_types.get(name)
        if t == "Bank":
            cash += bal  # bank is debit-normal, balance is positive

    ar = _scalar(conn, "SELECT COALESCE(SUM(balance_remaining), 0) FROM invoice WHERE is_paid = 0")
    ap = _scalar(conn, "SELECT COALESCE(SUM(balance_remaining), 0) FROM bill WHERE is_paid = 0")

    pl = profit_loss(conn, from_date, to_date)
    rev = pl["total_revenue"] + pl["other_income"]
    exp = pl["total_expense"] + pl["other_expense"]
    net = rev - exp
    margin = (net / rev * 100) if rev > 0 else 0

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM invoice WHERE txn_date >= ? AND txn_date <= ?", (str(from_date), str(to_date)))
    invoices_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM bill WHERE txn_date >= ? AND txn_date <= ?", (str(from_date), str(to_date)))
    bills_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM customer WHERE is_active = 1")
    customers_active = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM vendor WHERE is_active = 1")
    vendors_active = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM invoice WHERE is_paid = 0")
    open_invoices = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM bill WHERE is_paid = 0")
    open_bills = cur.fetchone()[0]

    return {
        "from": str(from_date), "to": str(to_date),
        "cash": round(cash, 2),
        "ar": round(ar, 2),
        "ap": round(ap, 2),
        "revenue": round(rev, 2),
        "expenses": round(exp, 2),
        "net_income": round(net, 2),
        "margin_pct": round(margin, 2),
        "invoices_count": invoices_count,
        "bills_count": bills_count,
        "customers_active": customers_active,
        "vendors_active": vendors_active,
        "open_invoices": open_invoices,
        "open_bills": open_bills,
    }


def kpi_with_comparison(conn, from_date, to_date, compare_mode="previous_period"):
    primary = kpi_snapshot(conn, from_date, to_date)
    comp_from, comp_to = compute_comparison_range(from_date, to_date, compare_mode)
    comparison = kpi_snapshot(conn, comp_from, comp_to)

    def delta(key):
        a = primary.get(key) or 0
        b = comparison.get(key) or 0
        abs_change = round(a - b, 2)
        pct_change = round(((a - b) / abs(b) * 100) if b else 0, 2)
        return {"abs": abs_change, "pct": pct_change}

    deltas = {k: delta(k) for k in
              ["cash", "ar", "ap", "revenue", "expenses", "net_income",
               "margin_pct", "invoices_count", "bills_count", "open_invoices",
               "open_bills"]}
    return {"primary": primary, "comparison": comparison,
            "deltas": deltas, "compare_mode": compare_mode}


# ===================== Trends =====================

def monthly_trend(conn, from_date, to_date):
    from_d = parse_date(from_date) if isinstance(from_date, str) else from_date
    to_d = parse_date(to_date) if isinstance(to_date, str) else to_date
    months = []
    cursor_d = first_day_of_month(from_d)
    while cursor_d <= to_d:
        m_start = cursor_d
        m_end = last_day_of_month(cursor_d)
        if m_end > to_d:
            m_end = to_d
        pl = profit_loss(conn, m_start, m_end)
        rev = pl["total_revenue"] + pl["other_income"]
        exp = pl["total_expense"] + pl["other_expense"]
        months.append({
            "month": cursor_d.strftime("%b %Y"),
            "month_key": cursor_d.strftime("%Y-%m"),
            "revenue": round(rev, 2),
            "expenses": round(exp, 2),
            "net": round(rev - exp, 2),
        })
        cursor_d = add_months(cursor_d, 1)
    return months


def daily_trend(conn, from_date, to_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT txn_date, COALESCE(SUM(subtotal), 0) FROM invoice
        WHERE txn_date >= ? AND txn_date <= ? GROUP BY txn_date
    """, (str(from_date), str(to_date)))
    rev_by_day = {row[0]: row[1] or 0 for row in cur.fetchall()}
    cur.execute("""
        SELECT txn_date, COALESCE(SUM(amount), 0) FROM (
            SELECT txn_date, amount FROM bill WHERE txn_date >= ? AND txn_date <= ?
            UNION ALL
            SELECT txn_date, amount FROM check_txn WHERE txn_date >= ? AND txn_date <= ?
            UNION ALL
            SELECT txn_date, amount FROM cc_charge WHERE txn_date >= ? AND txn_date <= ?
        ) GROUP BY txn_date
    """, (str(from_date), str(to_date)) * 3)
    exp_by_day = {row[0]: row[1] or 0 for row in cur.fetchall()}
    from_d = parse_date(from_date) if isinstance(from_date, str) else from_date
    to_d = parse_date(to_date) if isinstance(to_date, str) else to_date
    out = []
    d = from_d
    while d <= to_d:
        key = d.strftime("%Y-%m-%d")
        out.append({"date": key,
                    "revenue": round(rev_by_day.get(key, 0), 2),
                    "expenses": round(exp_by_day.get(key, 0), 2)})
        d += timedelta(days=1)
    return out


# ===================== Rankings =====================

def top_customers(conn, from_date, to_date, n=10):
    cur = conn.cursor()
    cur.execute("""
        SELECT customer_name, SUM(subtotal) AS revenue, COUNT(*) AS inv_count
        FROM invoice
        WHERE txn_date >= ? AND txn_date <= ? AND customer_name IS NOT NULL
        GROUP BY customer_name ORDER BY revenue DESC LIMIT ?
    """, (str(from_date), str(to_date), n))
    return [{"customer": r[0], "revenue": round(r[1] or 0, 2), "invoices": r[2]}
            for r in cur.fetchall()]


def top_vendors(conn, from_date, to_date, n=10):
    cur = conn.cursor()
    cur.execute("""
        SELECT vendor, SUM(amount) AS amount, COUNT(*) AS cnt FROM (
            SELECT vendor_name AS vendor, amount FROM bill
            WHERE txn_date >= ? AND txn_date <= ? AND vendor_name IS NOT NULL
            UNION ALL
            SELECT payee AS vendor, amount FROM check_txn
            WHERE txn_date >= ? AND txn_date <= ? AND payee IS NOT NULL
        ) GROUP BY vendor ORDER BY amount DESC LIMIT ?
    """, (str(from_date), str(to_date), str(from_date), str(to_date), n))
    return [{"vendor": r[0], "amount": round(r[1] or 0, 2), "transactions": r[2]}
            for r in cur.fetchall()]


def top_items(conn, from_date, to_date, n=10):
    cur = conn.cursor()
    cur.execute("""
        SELECT il.item_name, SUM(il.amount) AS revenue, SUM(il.quantity) AS qty
        FROM invoice_line il JOIN invoice i ON i.txn_id = il.txn_id
        WHERE i.txn_date >= ? AND i.txn_date <= ?
          AND il.item_name IS NOT NULL AND il.item_name != ''
        GROUP BY il.item_name ORDER BY revenue DESC LIMIT ?
    """, (str(from_date), str(to_date), n))
    return [{"item": r[0], "revenue": round(r[1] or 0, 2), "quantity": round(r[2] or 0, 2)}
            for r in cur.fetchall()]


# ===================== AR / AP Aging =====================

def ar_aging(conn):
    cur = conn.cursor()
    cur.execute("SELECT due_date, balance_remaining FROM invoice WHERE is_paid = 0")
    buckets = {"Current (0-30)": 0, "31-60 days": 0, "61-90 days": 0, "90+ days": 0}
    today_d = today()
    for due_str, bal in cur.fetchall():
        if not due_str:
            continue
        try:
            days = (today_d - parse_date(due_str)).days
        except Exception:
            continue
        if days <= 30:   buckets["Current (0-30)"] += bal or 0
        elif days <= 60: buckets["31-60 days"] += bal or 0
        elif days <= 90: buckets["61-90 days"] += bal or 0
        else:            buckets["90+ days"] += bal or 0
    return [{"bucket": k, "amount": round(v, 2)} for k, v in buckets.items()]


def ap_aging(conn):
    cur = conn.cursor()
    cur.execute("SELECT due_date, balance_remaining FROM bill WHERE is_paid = 0")
    buckets = {"Current (0-30)": 0, "31-60 days": 0, "61-90 days": 0, "90+ days": 0}
    today_d = today()
    for due_str, bal in cur.fetchall():
        if not due_str:
            continue
        try:
            days = (today_d - parse_date(due_str)).days
        except Exception:
            continue
        if days <= 30:   buckets["Current (0-30)"] += bal or 0
        elif days <= 60: buckets["31-60 days"] += bal or 0
        elif days <= 90: buckets["61-90 days"] += bal or 0
        else:            buckets["90+ days"] += bal or 0
    return [{"bucket": k, "amount": round(v, 2)} for k, v in buckets.items()]


# ===================== DIAGNOSTICS =====================

def diagnostics(conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT account_type, full_name, balance, is_active
        FROM account ORDER BY account_type, full_name
    """)
    accounts = [{"type": r[0], "name": r[1], "balance": r[2] or 0, "active": bool(r[3])}
                for r in cur.fetchall()]

    type_counts = {}
    type_totals = {}
    for a in accounts:
        t = a["type"] or "(null)"
        type_counts[t] = type_counts.get(t, 0) + 1
        type_totals[t] = type_totals.get(t, 0) + (a["balance"] if a["active"] else 0)

    totals = {}
    for table in ["invoice", "bill", "check_txn", "cc_charge", "deposit_txn",
                  "receive_payment", "bill_payment", "journal_entry",
                  "invoice_line", "bill_line", "check_line", "cc_charge_line",
                  "journal_line"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            totals[table] = cur.fetchone()[0]
        except Exception:
            totals[table] = "N/A"

    cur.execute("SELECT MIN(txn_date), MAX(txn_date) FROM invoice")
    inv_range = cur.fetchone()
    cur.execute("SELECT MIN(txn_date), MAX(txn_date) FROM bill")
    bill_range = cur.fetchone()

    cur.execute("""
        SELECT jl.account_name, SUM(jl.debit) AS total_debit,
               SUM(jl.credit) AS total_credit, COUNT(*) AS line_count
        FROM journal_line jl
        WHERE jl.account_name IS NOT NULL AND jl.account_name != ''
        GROUP BY jl.account_name
        ORDER BY total_debit + total_credit DESC
        LIMIT 25
    """)
    je_accounts = [{
        "account": r[0], "total_debit": round(r[1] or 0, 2),
        "total_credit": round(r[2] or 0, 2), "lines": r[3],
    } for r in cur.fetchall()]
    for rec in je_accounts:
        cur.execute("SELECT account_type FROM account WHERE full_name = ?", (rec["account"],))
        row = cur.fetchone()
        rec["acc_master_type"] = row[0] if row else "NOT FOUND IN MASTER"

    cur.execute("""
        SELECT bl.expense_account, bl.item_name, bl.source_type,
               SUM(bl.amount) AS total, COUNT(*) AS line_count
        FROM bill_line bl
        GROUP BY bl.expense_account, bl.item_name, bl.source_type
        ORDER BY total DESC LIMIT 25
    """)
    bill_lines_summary = [{
        "expense_account": r[0], "item_name": r[1], "source_type": r[2],
        "total": round(r[3] or 0, 2), "lines": r[4],
    } for r in cur.fetchall()]

    cur.execute("""
        SELECT COUNT(*) AS total,
            SUM(CASE WHEN income_account IS NOT NULL AND income_account != '' THEN 1 ELSE 0 END) AS w_inc,
            SUM(CASE WHEN expense_account IS NOT NULL AND expense_account != '' THEN 1 ELSE 0 END) AS w_exp,
            SUM(CASE WHEN cogs_account IS NOT NULL AND cogs_account != '' THEN 1 ELSE 0 END) AS w_cogs
        FROM item
    """)
    row = cur.fetchone()
    item_status = {
        "total_items": row[0] or 0,
        "with_income_account": row[1] or 0,
        "with_expense_account": row[2] or 0,
        "with_cogs_account": row[3] or 0,
    }

    cur.execute("""
        SELECT full_name, item_type, income_account, expense_account, cogs_account
        FROM item ORDER BY full_name LIMIT 25
    """)
    item_sample = [{
        "name": r[0], "type": r[1],
        "income_account": r[2], "expense_account": r[3], "cogs_account": r[4]
    } for r in cur.fetchall()]

    cur.execute("""
        SELECT 'bill' AS src, bl.expense_account, bl.item_name,
               SUM(bl.amount) AS total, COUNT(*) AS lines
        FROM bill_line bl
        LEFT JOIN account a ON a.full_name = bl.expense_account
                            AND a.account_type IN ('Expense','OtherExpense','CostOfGoodsSold')
        LEFT JOIN item i ON i.full_name = bl.item_name
        LEFT JOIN account a2 ON a2.full_name = i.cogs_account
                             AND a2.account_type IN ('Expense','OtherExpense','CostOfGoodsSold')
        LEFT JOIN account a3 ON a3.full_name = i.expense_account
                             AND a3.account_type IN ('Expense','OtherExpense','CostOfGoodsSold')
        WHERE a.full_name IS NULL AND a2.full_name IS NULL AND a3.full_name IS NULL
          AND bl.amount != 0
        GROUP BY bl.expense_account, bl.item_name
        ORDER BY total DESC LIMIT 15
    """)
    unresolved_bill = [{
        "source": r[0], "account": r[1], "item": r[2],
        "total": round(r[3] or 0, 2), "lines": r[4]
    } for r in cur.fetchall()]

    exp_by_acc = expense_by_account(conn, date(1900, 1, 1), today())
    top_expenses = sorted(exp_by_acc.items(), key=lambda x: -x[1])[:20]

    # Also include GL movement totals to verify debits == credits
    gl_period = compute_gl_period(conn, date(1900, 1, 1), today())
    total_debits = sum(v["debit"] for v in gl_period.values())
    total_credits = sum(v["credit"] for v in gl_period.values())

    return {
        "account_type_summary": [
            {"type": t, "count": type_counts[t], "total_balance": round(type_totals[t], 2)}
            for t in sorted(type_counts.keys())
        ],
        "accounts": accounts,
        "table_counts": totals,
        "invoice_date_range": {"min": inv_range[0], "max": inv_range[1]},
        "bill_date_range": {"min": bill_range[0], "max": bill_range[1]},
        "top_expense_accounts_all_time": [
            {"account": a, "amount": round(v, 2)} for a, v in top_expenses
        ],
        "je_accounts_top": je_accounts,
        "bill_lines_summary": bill_lines_summary,
        "item_resolution_status": item_status,
        "item_sample": item_sample,
        "unresolved_bill_lines": unresolved_bill,
        "gl_totals": {
            "total_debits": round(total_debits, 2),
            "total_credits": round(total_credits, 2),
            "out_of_balance": round(total_debits - total_credits, 2),
        },
    }
