"""
QuickBooks Desktop connector via QODBC driver.

SCHEMA-AGNOSTIC: does `SELECT * FROM <table>` and maps columns by
matching against known QODBC alias variants. This works across QODBC
versions even as column names shift.

For every QBD table we care about, there's a fetch_X(conn) function
that returns a list of dicts keyed by our canonical field names.
"""


def _import_pyodbc():
    try:
        import pyodbc
        return pyodbc
    except ImportError as e:
        raise RuntimeError(
            "pyodbc is not installed. Run setup.bat on Windows or "
            "`pip install pyodbc`."
        ) from e


def get_connection(dsn_name: str):
    pyodbc = _import_pyodbc()
    return pyodbc.connect(f"DSN={dsn_name};", autocommit=True)


def test_connection(dsn_name: str) -> dict:
    conn = get_connection(dsn_name)
    try:
        cur = conn.cursor()
        info = {}
        for (key, sql) in [
            ("company_name",  "SELECT CompanyName FROM Company"),
        ]:
            try:
                cur.execute(sql)
                r = cur.fetchone()
                info[key] = r[0] if r else None
            except Exception:
                info[key] = None
        for (key, tbl) in [
            ("customer_count", "Customer"),
            ("invoice_count", "Invoice"),
            ("vendor_count", "Vendor"),
            ("bill_count", "Bill"),
        ]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                info[key] = cur.fetchone()[0]
            except Exception:
                info[key] = None
        return info
    finally:
        conn.close()


# ======================================================================
# Schema-agnostic helpers
# ======================================================================

def _fetch_flexible(conn, table_name, aliases, where=None, order_by=None):
    """
    SELECT * FROM <table>, map columns by alias, return list of dicts.

    `aliases` is dict: canonical_name -> list of possible QODBC column names.
    Returns None-valued keys when no alias matches.
    """
    cur = conn.cursor()
    sql = f"SELECT * FROM {table_name}"
    if where:
        sql += f" WHERE {where}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    cur.execute(sql)

    available = {d[0].lower(): d[0] for d in cur.description}
    actual_cols = [d[0] for d in cur.description]
    resolved = {}
    for canonical, alias_list in aliases.items():
        for alias in alias_list:
            if alias.lower() in available:
                resolved[canonical] = actual_cols.index(available[alias.lower()])
                break

    out = []
    for row in cur.fetchall():
        d = {}
        for canonical in aliases:
            d[canonical] = row[resolved[canonical]] if canonical in resolved else None
        out.append(d)
    return out


def _try_tables(conn, table_candidates, aliases, where=None, order_by=None):
    """
    Try each table name in order; use the first that exists.
    Useful because some tables have different names across QODBC versions.
    """
    last_err = None
    for tbl in table_candidates:
        try:
            return _fetch_flexible(conn, tbl, aliases, where, order_by)
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    return []


# ======================================================================
# Master data
# ======================================================================

def fetch_customers(conn):
    return _fetch_flexible(conn, "Customer", {
        "list_id":   ["ListID"],
        "name":      ["Name", "FullName"],
        "company":   ["CompanyName"],
        "phone":     ["Phone"],
        "email":     ["Email"],
        "balance":   ["Balance", "TotalBalance"],
        "is_active": ["IsActive"],
    })


def fetch_vendors(conn):
    return _fetch_flexible(conn, "Vendor", {
        "list_id":   ["ListID"],
        "name":      ["Name", "FullName"],
        "company":   ["CompanyName"],
        "phone":     ["Phone"],
        "email":     ["Email"],
        "balance":   ["Balance"],
        "is_active": ["IsActive"],
    })


def fetch_accounts(conn):
    return _fetch_flexible(conn, "Account", {
        "list_id":      ["ListID"],
        "name":         ["FullName", "Name"],
        "account_type": ["AccountType"],
        "balance":      ["Balance"],
        "is_active":    ["IsActive"],
    })


def fetch_items(conn):
    """
    Items with their associated income/expense/COGS accounts.
    Critical for resolving item lines on bills/checks/CC charges
    into the correct P&L account.

    Tries the main `Item` table first. If key account fields aren't populated,
    also tries the type-specific tables (ItemService, ItemInventory,
    ItemNonInventory, ItemOtherCharge) which often expose these fields
    more reliably.
    """
    aliases = {
        "list_id":         ["ListID"],
        "name":            ["FullName", "Name"],
        "item_type":       ["Type", "ItemType"],
        "description":     ["SalesDesc", "SalesDescription", "PurchaseDesc", "Desc"],
        "sales_price":     ["SalesPrice", "Price", "SalesOrPurchase"],
        "purchase_cost":   ["PurchaseCost", "Cost", "AverageCost"],
        "qty_on_hand":     ["QuantityOnHand", "QOH"],
        "is_active":       ["IsActive"],
        # Generous aliases for account fields — QODBC varies these
        "income_account":  ["IncomeAccountRefFullName",
                            "SalesAndPurchaseIncomeAccountRefFullName",
                            "SalesOrPurchaseIncomeAccountRefFullName",
                            "SalesAccountRefFullName"],
        "expense_account": ["ExpenseAccountRefFullName",
                            "PurchaseAccountRefFullName",
                            "SalesAndPurchaseExpenseAccountRefFullName",
                            "SalesOrPurchaseExpenseAccountRefFullName",
                            "COGSAccountRefFullName"],  # fallback to COGS if Expense missing
        "cogs_account":    ["COGSAccountRefFullName",
                            "CostOfGoodsSoldAccountRefFullName",
                            "SalesAndPurchaseCOGSAccountRefFullName"],
    }

    # Primary source: Item table (combined view)
    primary = _try_tables(conn, ["Item"], aliases)

    # Auxiliary: query type-specific tables and merge any account info
    # that was missing from the combined Item query.
    aux_tables = ["ItemService", "ItemInventory", "ItemNonInventory",
                  "ItemOtherCharge"]
    aux_map = {}  # list_id -> extra account info
    for tbl in aux_tables:
        try:
            rows = _fetch_flexible(conn, tbl, {
                "list_id":         ["ListID"],
                "income_account":  ["IncomeAccountRefFullName",
                                    "SalesAndPurchaseIncomeAccountRefFullName",
                                    "SalesOrPurchaseIncomeAccountRefFullName"],
                "expense_account": ["ExpenseAccountRefFullName",
                                    "PurchaseAccountRefFullName",
                                    "SalesAndPurchaseExpenseAccountRefFullName",
                                    "SalesOrPurchaseExpenseAccountRefFullName"],
                "cogs_account":    ["COGSAccountRefFullName",
                                    "CostOfGoodsSoldAccountRefFullName"],
            })
            for r in rows:
                lid = r.get("list_id")
                if lid:
                    entry = aux_map.setdefault(lid, {})
                    for f in ("income_account", "expense_account", "cogs_account"):
                        if r.get(f) and not entry.get(f):
                            entry[f] = r[f]
        except Exception:
            continue

    # Merge aux into primary
    for rec in primary:
        lid = rec.get("list_id")
        if lid and lid in aux_map:
            for f in ("income_account", "expense_account", "cogs_account"):
                if not rec.get(f) and aux_map[lid].get(f):
                    rec[f] = aux_map[lid][f]

    return primary


def fetch_classes(conn):
    return _try_tables(conn, ["Class"], {
        "list_id":   ["ListID"],
        "name":      ["FullName", "Name"],
        "is_active": ["IsActive"],
    })


# ======================================================================
# Invoices
# ======================================================================

def fetch_invoices(conn):
    return _fetch_flexible(conn, "Invoice", {
        "txn_id":     ["TxnID"],
        "number":     ["RefNumber", "TxnNumber"],
        "customer":   ["CustomerRefFullName", "CustomerName"],
        "txn_date":   ["TxnDate"],
        "due_date":   ["DueDate"],
        "subtotal":   ["Subtotal", "SubTotal"],
        "balance":    ["BalanceRemaining"],
        "is_paid":    ["IsPaid"],
    })


def fetch_invoice_lines(conn):
    return _fetch_flexible(conn, "InvoiceLine", {
        "txn_id":      ["TxnID"],
        "line_id":     ["TxnLineID", "InvoiceLineTxnLineID", "LineNumber",
                        "TxnLineNumber", "SeqNo", "RecordNo"],
        "item":        ["ItemRefFullName", "ItemName", "InvoiceLineItemRefFullName"],
        "description": ["Descrip", "Description", "InvoiceLineDesc"],
        "quantity":    ["Quantity", "InvoiceLineQuantity"],
        "rate":        ["Rate", "InvoiceLineRate"],
        "amount":      ["Amount", "InvoiceLineAmount"],
        "class_name":  ["ClassRefFullName", "ClassName"],
    })


# ======================================================================
# Bills
# ======================================================================

def fetch_bills(conn):
    return _fetch_flexible(conn, "Bill", {
        "txn_id":       ["TxnID"],
        "number":       ["RefNumber", "TxnNumber"],
        "vendor":       ["VendorRefFullName", "VendorName"],
        "txn_date":     ["TxnDate"],
        "due_date":     ["DueDate"],
        "amount_due":   ["AmountDue", "Amount"],
        "amount_paid":  ["AmountPaid"],
        "is_paid":      ["IsPaid"],
    })


def fetch_bill_expense_lines(conn):
    # QODBC standard column is `ExpenseLineAccountRefFullName` — with "Line"
    # in the middle. Without "Line" is a common alias on older builds.
    # AccountRefFullName is the LAST fallback because in some tables it
    # refers to the TRANSACTION's source (bank) account, not the expense.
    return _fetch_flexible(conn, "BillExpenseLine", {
        "txn_id":      ["TxnID"],
        "line_id":     ["TxnLineID", "BillExpenseLineTxnLineID", "LineNumber",
                        "TxnLineNumber", "SeqNo", "RecordNo"],
        "account":     ["ExpenseLineAccountRefFullName",
                        "BillExpenseLineAccountRefFullName",
                        "ExpenseAccountRefFullName",
                        "ExpenseAccountName",
                        "AccountName",
                        "AccountRefFullName"],
        "memo":        ["ExpenseLineMemo", "BillExpenseLineMemo", "Memo"],
        "amount":      ["ExpenseLineAmount", "BillExpenseLineAmount", "Amount"],
    })


def fetch_bill_item_lines(conn):
    """
    Item lines from bills. Each references an Item; the item's associated
    expense/COGS account is resolved separately during ETL.
    Many businesses (esp. trading / job-based) use item lines exclusively
    rather than expense lines.
    """
    return _try_tables(conn, ["BillItemLine"], {
        "txn_id":      ["TxnID"],
        "line_id":     ["TxnLineID", "BillItemLineTxnLineID", "LineNumber",
                        "TxnLineNumber", "SeqNo", "RecordNo"],
        "item":        ["ItemLineItemRefFullName",
                        "BillItemLineItemRefFullName",
                        "ItemRefFullName", "ItemName"],
        "amount":      ["ItemLineAmount", "BillItemLineAmount", "Amount"],
        "quantity":    ["ItemLineQuantity", "Quantity"],
        "memo":        ["ItemLineDesc", "BillItemLineDesc", "Desc", "Memo"],
    })


# ======================================================================
# Checks (payments / purchases without bill)
# ======================================================================

def fetch_checks(conn):
    return _try_tables(conn, ["Check"], {
        "txn_id":       ["TxnID"],
        "number":       ["RefNumber", "CheckNumber", "TxnNumber"],
        "bank_account": ["AccountRefFullName", "BankAccount"],
        "payee":        ["PayeeEntityRefFullName", "PayeeName", "PayeeEntityName"],
        "txn_date":     ["TxnDate"],
        "amount":       ["Amount"],
        "memo":         ["Memo"],
    })


def fetch_check_expense_lines(conn):
    return _try_tables(conn, ["CheckExpenseLine"], {
        "txn_id":      ["TxnID"],
        "line_id":     ["TxnLineID", "LineNumber", "TxnLineNumber", "SeqNo", "RecordNo"],
        "account":     ["ExpenseLineAccountRefFullName",
                        "CheckExpenseLineAccountRefFullName",
                        "ExpenseAccountRefFullName",
                        "ExpenseAccountName",
                        "AccountName",
                        "AccountRefFullName"],
        "amount":      ["ExpenseLineAmount", "CheckExpenseLineAmount", "Amount"],
        "memo":        ["ExpenseLineMemo", "CheckExpenseLineMemo", "Memo"],
    })


def fetch_check_item_lines(conn):
    return _try_tables(conn, ["CheckItemLine"], {
        "txn_id":      ["TxnID"],
        "line_id":     ["TxnLineID", "LineNumber", "TxnLineNumber", "SeqNo"],
        "item":        ["ItemLineItemRefFullName",
                        "CheckItemLineItemRefFullName",
                        "ItemRefFullName", "ItemName"],
        "amount":      ["ItemLineAmount", "CheckItemLineAmount", "Amount"],
        "quantity":    ["ItemLineQuantity", "Quantity"],
        "memo":        ["ItemLineDesc", "CheckItemLineDesc", "Desc", "Memo"],
    })


# ======================================================================
# Deposits
# ======================================================================

def fetch_deposits(conn):
    return _try_tables(conn, ["Deposit"], {
        "txn_id":       ["TxnID"],
        "bank_account": ["DepositToAccountRefFullName", "AccountRefFullName", "BankAccount"],
        "txn_date":     ["TxnDate"],
        "amount":       ["DepositTotal", "TotalAmount", "Amount"],
        "memo":         ["Memo"],
    })


# ======================================================================
# Credit Card
# ======================================================================

def fetch_cc_charges(conn):
    return _try_tables(conn, ["CreditCardCharge"], {
        "txn_id":       ["TxnID"],
        "number":       ["RefNumber", "TxnNumber"],
        "cc_account":   ["AccountRefFullName", "CreditCardAccount"],
        "payee":        ["PayeeEntityRefFullName", "PayeeEntityName"],
        "txn_date":     ["TxnDate"],
        "amount":       ["Amount"],
        "memo":         ["Memo"],
    })


def fetch_cc_charge_lines(conn):
    return _try_tables(conn, ["CreditCardChargeExpenseLine"], {
        "txn_id":      ["TxnID"],
        "line_id":     ["TxnLineID", "LineNumber", "TxnLineNumber", "SeqNo"],
        "account":     ["ExpenseLineAccountRefFullName",
                        "CreditCardChargeExpenseLineAccountRefFullName",
                        "ExpenseAccountRefFullName",
                        "ExpenseAccountName",
                        "AccountName",
                        "AccountRefFullName"],
        "amount":      ["ExpenseLineAmount", "Amount"],
        "memo":        ["ExpenseLineMemo", "Memo"],
    })


def fetch_cc_charge_item_lines(conn):
    return _try_tables(conn, ["CreditCardChargeItemLine"], {
        "txn_id":      ["TxnID"],
        "line_id":     ["TxnLineID", "LineNumber", "TxnLineNumber", "SeqNo"],
        "item":        ["ItemLineItemRefFullName",
                        "CreditCardChargeItemLineItemRefFullName",
                        "ItemRefFullName", "ItemName"],
        "amount":      ["ItemLineAmount", "Amount"],
        "quantity":    ["ItemLineQuantity", "Quantity"],
        "memo":        ["ItemLineDesc", "Desc", "Memo"],
    })


# ======================================================================
# Journal Entries
# ======================================================================

def fetch_journal_entries(conn):
    return _try_tables(conn, ["JournalEntry"], {
        "txn_id":    ["TxnID"],
        "number":    ["RefNumber", "TxnNumber"],
        "txn_date":  ["TxnDate"],
        "memo":      ["Memo"],
    })


def fetch_journal_lines(conn):
    """
    Journal Entry line items.
    QODBC's JE structure varies dramatically across versions:
     - Some expose a combined `JournalEntryLine` table
     - Some split it into `JournalEntryCreditLine` + `JournalEntryDebitLine`
     - Column names vary: `AccountRefFullName` / `JournalLineAccountRefFullName`
       / `JournalEntryLineAccountRefFullName` etc.

    Strategy:
      1. Try the combined table first with generous aliases
      2. Validate the result — if account_name is mostly empty, that query
         failed to find the right column; discard and try split tables
      3. Try several split-table name variants
    """

    combined_aliases = {
        "txn_id":       ["TxnID"],
        "line_id":      ["TxnLineID", "JournalLineTxnLineID",
                         "JournalEntryLineTxnLineID",
                         "LineNumber", "TxnLineNumber", "SeqNo", "RecordNo"],
        "account":      ["JournalLineAccountRefFullName",
                         "JournalEntryLineAccountRefFullName",
                         "AccountRefFullName",
                         "AccountName",
                         "JournalDebitLineAccountRefFullName",
                         "JournalCreditLineAccountRefFullName",
                         "LineAccountRefFullName"],
        "debit":        ["JournalLineDebit", "JournalDebitLineAmount",
                         "DebitAmount", "Debit", "JournalLineDebitAmount"],
        "credit":       ["JournalLineCredit", "JournalCreditLineAmount",
                         "CreditAmount", "Credit", "JournalLineCreditAmount"],
        "amount":       ["JournalLineAmount", "Amount", "JournalEntryLineAmount"],
        "memo":         ["JournalLineMemo", "Memo",
                         "JournalDebitLineMemo", "JournalCreditLineMemo"],
    }

    # --- Attempt 1: Combined line table ---
    for combined_tbl in ["JournalEntryLine", "JournalEntryLineDetail"]:
        try:
            lines = _fetch_flexible(conn, combined_tbl, combined_aliases)
            # Validate — do we have useful account names?
            usable = sum(1 for ln in lines if ln.get("account"))
            if usable >= len(lines) * 0.5:  # 50%+ have account → good
                print(f"[qb] Journal lines from {combined_tbl}: {len(lines)} rows, {usable} with accounts")
                return lines
            else:
                print(f"[qb] {combined_tbl} returned {len(lines)} rows but only {usable} "
                      f"had account names — trying split tables")
        except Exception as e:
            # Table doesn't exist, try next
            continue

    # --- Attempt 2: Split credit/debit tables ---
    all_lines = []
    credit_found = False
    for credit_tbl in ["JournalCreditLine", "JournalEntryCreditLine"]:
        try:
            credits = _fetch_flexible(conn, credit_tbl, {
                "txn_id":  ["TxnID"],
                "line_id": ["TxnLineID",
                            "JournalCreditLineTxnLineID",
                            "LineNumber", "SeqNo"],
                "account": ["JournalCreditLineAccountRefFullName",
                            "AccountRefFullName",
                            "AccountName",
                            "LineAccountRefFullName"],
                "amount":  ["JournalCreditLineAmount", "Amount", "CreditAmount"],
                "memo":    ["JournalCreditLineMemo", "Memo"],
            })
            usable = sum(1 for c in credits if c.get("account"))
            if credits and usable > 0:
                print(f"[qb] Journal credits from {credit_tbl}: {len(credits)} rows, "
                      f"{usable} with accounts")
                for c in credits:
                    all_lines.append({**c, "debit": 0.0,
                                      "credit": c.get("amount") or 0.0})
                credit_found = True
                break
        except Exception:
            continue

    debit_found = False
    for debit_tbl in ["JournalDebitLine", "JournalEntryDebitLine"]:
        try:
            debits = _fetch_flexible(conn, debit_tbl, {
                "txn_id":  ["TxnID"],
                "line_id": ["TxnLineID",
                            "JournalDebitLineTxnLineID",
                            "LineNumber", "SeqNo"],
                "account": ["JournalDebitLineAccountRefFullName",
                            "AccountRefFullName",
                            "AccountName",
                            "LineAccountRefFullName"],
                "amount":  ["JournalDebitLineAmount", "Amount", "DebitAmount"],
                "memo":    ["JournalDebitLineMemo", "Memo"],
            })
            usable = sum(1 for d in debits if d.get("account"))
            if debits and usable > 0:
                print(f"[qb] Journal debits from {debit_tbl}: {len(debits)} rows, "
                      f"{usable} with accounts")
                for d in debits:
                    all_lines.append({**d, "debit": d.get("amount") or 0.0,
                                      "credit": 0.0})
                debit_found = True
                break
        except Exception:
            continue

    if credit_found or debit_found:
        return all_lines

    print("[qb] WARNING: Could not fetch journal lines with recognizable accounts")
    return []


# ======================================================================
# Payments
# ======================================================================

def fetch_receive_payments(conn):
    return _try_tables(conn, ["ReceivePayment"], {
        "txn_id":          ["TxnID"],
        "customer":        ["CustomerRefFullName", "CustomerName"],
        "txn_date":        ["TxnDate"],
        "amount":          ["TotalAmount", "Amount"],
        "deposit_account": ["DepositToAccountRefFullName", "DepositTo"],
        "memo":             ["Memo"],
    })


def fetch_bill_payments(conn):
    # BillPayment splits into CheckPayment and CreditCardPayment in some QODBC
    all_rows = []
    for tbl in ["BillPaymentCheck", "BillPayment"]:
        try:
            rows = _fetch_flexible(conn, tbl, {
                "txn_id":       ["TxnID"],
                "vendor":       ["PayeeEntityRefFullName", "PayeeEntityName", "VendorRefFullName"],
                "txn_date":     ["TxnDate"],
                "amount":       ["Amount"],
                "bank_account": ["BankAccountRefFullName", "AccountRefFullName"],
            })
            all_rows.extend(rows)
            break
        except Exception:
            continue
    try:
        rows = _fetch_flexible(conn, "BillPaymentCreditCard", {
            "txn_id":       ["TxnID"],
            "vendor":       ["PayeeEntityRefFullName", "PayeeEntityName"],
            "txn_date":     ["TxnDate"],
            "amount":       ["Amount"],
            "bank_account": ["CreditCardAccountRefFullName", "AccountRefFullName"],
        })
        all_rows.extend(rows)
    except Exception:
        pass
    return all_rows
