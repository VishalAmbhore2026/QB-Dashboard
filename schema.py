"""
Database schema definition.

Expanded to support full financial reporting:
 - P&L (from Account + GeneralDetail / account-type classification)
 - Balance Sheet (from Account balances by type)
 - Cash Flow (from bank transactions)
 - Sales analysis (Invoice + InvoiceLine + Item)
 - Purchases analysis (Bill + BillLine + Check + CreditCardCharge)
 - Banking (Check + Deposit + Transfer)
 - Items / Inventory (Item + ItemInventory)
"""

SCHEMA_SQL = """
-- ============ Master data ============
CREATE TABLE IF NOT EXISTS customer (
    list_id TEXT PRIMARY KEY,
    full_name TEXT,
    company_name TEXT,
    phone TEXT,
    email TEXT,
    balance REAL,
    is_active INTEGER
);

CREATE TABLE IF NOT EXISTS vendor (
    list_id TEXT PRIMARY KEY,
    full_name TEXT,
    company_name TEXT,
    phone TEXT,
    email TEXT,
    balance REAL,
    is_active INTEGER
);

CREATE TABLE IF NOT EXISTS account (
    list_id TEXT PRIMARY KEY,
    full_name TEXT,
    account_type TEXT,       -- Bank, AccountsReceivable, AccountsPayable, Income, Expense, etc.
    balance REAL,
    is_active INTEGER
);
CREATE INDEX IF NOT EXISTS idx_account_type ON account(account_type);

CREATE TABLE IF NOT EXISTS item (
    list_id TEXT PRIMARY KEY,
    full_name TEXT,
    item_type TEXT,
    description TEXT,
    sales_price REAL,
    purchase_cost REAL,
    quantity_on_hand REAL,
    is_active INTEGER,
    income_account TEXT,
    expense_account TEXT,
    cogs_account TEXT
);

CREATE TABLE IF NOT EXISTS class (
    list_id TEXT PRIMARY KEY,
    full_name TEXT,
    is_active INTEGER
);

-- ============ Invoices ============
CREATE TABLE IF NOT EXISTS invoice (
    txn_id TEXT PRIMARY KEY,
    txn_number TEXT,
    customer_name TEXT,
    txn_date TEXT,
    due_date TEXT,
    subtotal REAL,
    balance_remaining REAL,
    is_paid INTEGER
);
CREATE INDEX IF NOT EXISTS idx_invoice_customer ON invoice(customer_name);
CREATE INDEX IF NOT EXISTS idx_invoice_date ON invoice(txn_date);
CREATE INDEX IF NOT EXISTS idx_invoice_paid ON invoice(is_paid);

CREATE TABLE IF NOT EXISTS invoice_line (
    txn_id TEXT,
    txn_line_id TEXT,
    item_name TEXT,
    description TEXT,
    quantity REAL,
    rate REAL,
    amount REAL,
    class_name TEXT,
    PRIMARY KEY (txn_id, txn_line_id)
);
CREATE INDEX IF NOT EXISTS idx_invoiceline_txn ON invoice_line(txn_id);
CREATE INDEX IF NOT EXISTS idx_invoiceline_item ON invoice_line(item_name);

-- ============ Bills ============
CREATE TABLE IF NOT EXISTS bill (
    txn_id TEXT PRIMARY KEY,
    txn_number TEXT,
    vendor_name TEXT,
    txn_date TEXT,
    due_date TEXT,
    amount REAL,
    amount_paid REAL,
    balance_remaining REAL,
    is_paid INTEGER
);
CREATE INDEX IF NOT EXISTS idx_bill_vendor ON bill(vendor_name);
CREATE INDEX IF NOT EXISTS idx_bill_date ON bill(txn_date);

CREATE TABLE IF NOT EXISTS bill_line (
    txn_id TEXT,
    txn_line_id TEXT,
    expense_account TEXT,
    description TEXT,
    amount REAL,
    PRIMARY KEY (txn_id, txn_line_id)
);
CREATE INDEX IF NOT EXISTS idx_billline_account ON bill_line(expense_account);

-- ============ Checks (payments to vendors / expenses, non-bill) ============
CREATE TABLE IF NOT EXISTS check_txn (
    txn_id TEXT PRIMARY KEY,
    txn_number TEXT,           -- check number / reference number
    bank_account TEXT,
    payee TEXT,                 -- vendor / customer / employee
    txn_date TEXT,
    amount REAL,
    memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_check_date ON check_txn(txn_date);
CREATE INDEX IF NOT EXISTS idx_check_bank ON check_txn(bank_account);

CREATE TABLE IF NOT EXISTS check_line (
    txn_id TEXT,
    txn_line_id TEXT,
    expense_account TEXT,
    amount REAL,
    memo TEXT,
    PRIMARY KEY (txn_id, txn_line_id)
);
CREATE INDEX IF NOT EXISTS idx_checkline_acc ON check_line(expense_account);

-- ============ Deposits (money into bank) ============
CREATE TABLE IF NOT EXISTS deposit_txn (
    txn_id TEXT PRIMARY KEY,
    bank_account TEXT,
    txn_date TEXT,
    amount REAL,
    memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_deposit_date ON deposit_txn(txn_date);

-- ============ Credit Card Charges ============
CREATE TABLE IF NOT EXISTS cc_charge (
    txn_id TEXT PRIMARY KEY,
    txn_number TEXT,
    credit_card_account TEXT,
    payee TEXT,
    txn_date TEXT,
    amount REAL,
    memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_cc_date ON cc_charge(txn_date);

CREATE TABLE IF NOT EXISTS cc_charge_line (
    txn_id TEXT,
    txn_line_id TEXT,
    expense_account TEXT,
    amount REAL,
    memo TEXT,
    PRIMARY KEY (txn_id, txn_line_id)
);

-- ============ Journal Entries ============
CREATE TABLE IF NOT EXISTS journal_entry (
    txn_id TEXT PRIMARY KEY,
    txn_number TEXT,
    txn_date TEXT,
    memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_je_date ON journal_entry(txn_date);

CREATE TABLE IF NOT EXISTS journal_line (
    txn_id TEXT,
    txn_line_id TEXT,
    account_name TEXT,
    debit REAL,
    credit REAL,
    memo TEXT,
    PRIMARY KEY (txn_id, txn_line_id)
);
CREATE INDEX IF NOT EXISTS idx_jeline_acc ON journal_line(account_name);

-- ============ Customer Payments ============
CREATE TABLE IF NOT EXISTS receive_payment (
    txn_id TEXT PRIMARY KEY,
    customer_name TEXT,
    txn_date TEXT,
    amount REAL,
    deposit_account TEXT,
    memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_rp_date ON receive_payment(txn_date);

-- ============ Vendor Payments (Bill Payment) ============
CREATE TABLE IF NOT EXISTS bill_payment (
    txn_id TEXT PRIMARY KEY,
    vendor_name TEXT,
    txn_date TEXT,
    amount REAL,
    bank_account TEXT
);
CREATE INDEX IF NOT EXISTS idx_bp_date ON bill_payment(txn_date);

-- ============ Sync metadata ============
CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT,
    rows INTEGER,
    started_at TEXT,
    finished_at TEXT,
    status TEXT,
    message TEXT
);
"""


def create_schema(conn):
    conn.executescript(SCHEMA_SQL)
    # Auto-migrate: add new columns if existing database was created by an
    # earlier version that didn't have them.
    _migrate(conn)
    conn.commit()


def _migrate(conn):
    """Idempotent column additions for schema upgrades."""
    cur = conn.cursor()
    migrations = [
        ("item", "income_account", "TEXT"),
        ("item", "expense_account", "TEXT"),
        ("item", "cogs_account", "TEXT"),
        # Tag which line records came from item lines vs expense lines so we
        # can tell them apart in diagnostics (but they're treated the same
        # in aggregation since both resolve to a P&L account).
        ("bill_line", "source_type", "TEXT DEFAULT 'expense'"),
        ("check_line", "source_type", "TEXT DEFAULT 'expense'"),
        ("cc_charge_line", "source_type", "TEXT DEFAULT 'expense'"),
        ("bill_line", "item_name", "TEXT"),
        ("check_line", "item_name", "TEXT"),
        ("cc_charge_line", "item_name", "TEXT"),
    ]
    for table, column, col_type in migrations:
        try:
            cur.execute(f"SELECT {column} FROM {table} LIMIT 1")
        except Exception:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            except Exception:
                pass
    conn.commit()


def wipe_data(conn):
    """Delete all rows from every data table (for full re-sync)."""
    cur = conn.cursor()
    for tbl in [
        "customer", "vendor", "account", "item", "class",
        "invoice", "invoice_line",
        "bill", "bill_line",
        "check_txn", "check_line",
        "deposit_txn",
        "cc_charge", "cc_charge_line",
        "journal_entry", "journal_line",
        "receive_payment", "bill_payment",
    ]:
        try:
            cur.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()
