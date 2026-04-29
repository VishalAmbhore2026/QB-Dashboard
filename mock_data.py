"""
Mock data generator for DEMO MODE.

Populates every major table with realistic data so the full dashboard
(P&L, Balance Sheet, Cash Flow, Sales, Purchases, Banking, Items)
has something to show without connecting to QuickBooks.
"""
import random
import uuid
from datetime import datetime, timedelta
from schema import create_schema, wipe_data


CUSTOMERS = [
    "Apex Industries Pvt Ltd", "Blue Sky Exports", "Crown Manufacturing",
    "Delta Logistics", "Evergreen Builders", "Fortune Enterprises",
    "Global Traders", "Horizon Media", "Innovate Solutions",
    "JK Consultants", "Kumar & Associates", "Laxmi Foods",
    "Metro Wholesale", "Nova Tech Systems", "Orion Pharmaceuticals",
    "Prakash Electricals", "Quantum Analytics", "Royal Textiles",
    "Sunrise Hospitality", "Titan Engineering", "United Distributors",
    "Vertex Software", "Western Imports", "Xperia Retail",
    "Zenith Constructions",
]

VENDORS = [
    "Office Supplies Co", "Prime Electricity Board", "Tata Telecom",
    "City Water Utility", "Rajesh Stationery Mart", "Cloud Services Ltd",
    "Kumar Transport", "Priya Cleaning Services", "Metro Rent",
    "Sharma & Co CAs", "Star Insurance", "PetroMax Fuels",
    "Quick Courier", "Modern Printers", "Raj Vehicle Rentals",
    "Peak Advertising", "Tech Parts Supply", "Green Office Plants",
]

BANK_ACCOUNTS = [
    ("HDFC Bank - Current A/c", "Bank"),
    ("ICICI Bank - Savings", "Bank"),
    ("SBI - Cash Credit A/c", "Bank"),
    ("Petty Cash", "OtherCurrentAsset"),
]

CC_ACCOUNTS = [
    ("HDFC Business Card", "CreditCard"),
    ("Axis Corporate Card", "CreditCard"),
]

FIXED_ASSETS = [
    ("Office Equipment", "FixedAsset", 850000),
    ("Computers & Laptops", "FixedAsset", 1250000),
    ("Furniture & Fixtures", "FixedAsset", 450000),
    ("Vehicles", "FixedAsset", 2800000),
]

LIABILITIES = [
    ("GST Payable", "OtherCurrentLiability", 185000),
    ("TDS Payable", "OtherCurrentLiability", 92000),
    ("Bank Loan - HDFC", "LongTermLiability", 4500000),
]

EQUITY_ACCOUNTS = [
    ("Owner's Capital", "Equity", 2500000),
    ("Retained Earnings", "Equity", 1800000),
]

INCOME_ACCOUNTS = [
    ("Consulting Revenue", "Income"),
    ("Product Sales", "Income"),
    ("Service Revenue", "Income"),
    ("Training Revenue", "Income"),
    ("Other Income", "OtherIncome"),
]

EXPENSE_ACCOUNTS = [
    ("Rent Expense", "Expense"),
    ("Salaries & Wages", "Expense"),
    ("Electricity", "Expense"),
    ("Internet & Telephone", "Expense"),
    ("Office Supplies", "Expense"),
    ("Professional Fees", "Expense"),
    ("Travel & Conveyance", "Expense"),
    ("Printing & Stationery", "Expense"),
    ("Repairs & Maintenance", "Expense"),
    ("Insurance", "Expense"),
    ("Fuel Expense", "Expense"),
    ("Courier & Postage", "Expense"),
    ("Software Subscriptions", "Expense"),
    ("Marketing & Advertising", "Expense"),
    ("Bank Charges", "Expense"),
]

COGS_ACCOUNTS = [
    ("Cost of Materials", "CostOfGoodsSold"),
    ("Direct Labor", "CostOfGoodsSold"),
    ("Freight & Shipping", "CostOfGoodsSold"),
]

AR_AP_ACCOUNTS = [
    ("Accounts Receivable", "AccountsReceivable"),
    ("Accounts Payable", "AccountsPayable"),
]

ITEMS = [
    ("Consulting Services", "Service", 5000, 2500),
    ("Software License - Annual", "Service", 25000, 8000),
    ("Training Workshop", "Service", 15000, 6000),
    ("Hardware - Laptop", "Inventory", 65000, 45000),
    ("Cloud Hosting - Monthly", "Service", 3500, 1200),
    ("Implementation Service", "Service", 40000, 20000),
    ("Support Contract - Silver", "Service", 12000, 4000),
    ("Support Contract - Gold", "Service", 30000, 10000),
    ("Custom Development", "Service", 80000, 40000),
    ("Data Migration Service", "Service", 20000, 8000),
    ("Hardware - Monitor", "Inventory", 18000, 13000),
    ("Hardware - Printer", "Inventory", 22000, 15000),
]

CLASSES_LIST = ["Services Division", "Products Division", "Consulting Division"]


def _rand_date_in_range(from_year=None, days_back_max=700):
    """Random date within last N days, or a specific year."""
    if from_year:
        start = datetime(from_year, 1, 1)
        end = min(datetime.now(), datetime(from_year, 12, 31))
        delta = (end - start).days
        return start + timedelta(days=random.randint(0, delta))
    return datetime.now() - timedelta(days=random.randint(0, days_back_max))


def seed(conn, refresh=False):
    """Fill the cache with realistic demo data."""
    create_schema(conn)

    if refresh:
        # Just tweak a few account balances to simulate live motion
        c = conn.cursor()
        c.execute("SELECT list_id, balance, account_type FROM account")
        for list_id, bal, atype in c.fetchall():
            if atype == "Bank":
                delta = random.uniform(-0.02, 0.04) * (bal or 1)
                c.execute("UPDATE account SET balance = ? WHERE list_id = ?",
                          (round((bal or 0) + delta, 2), list_id))
        conn.commit()
        return

    wipe_data(conn)
    c = conn.cursor()

    # --- Customers ---
    for name in CUSTOMERS:
        c.execute("INSERT INTO customer VALUES (?,?,?,?,?,?,?)", (
            str(uuid.uuid4()), name, name,
            f"+91-{random.randint(7000000000, 9999999999)}",
            f"contact@{name.lower().replace(' ', '').replace('&','and')[:20]}.com",
            0, 1
        ))

    # --- Vendors ---
    for name in VENDORS:
        c.execute("INSERT INTO vendor VALUES (?,?,?,?,?,?,?)", (
            str(uuid.uuid4()), name, name,
            f"+91-{random.randint(7000000000, 9999999999)}",
            f"billing@{name.lower().replace(' ', '')[:20]}.com",
            0, 1
        ))

    # --- Accounts (Chart of Accounts) ---
    # Bank accounts with realistic balances
    for name, atype in BANK_ACCOUNTS:
        bal = random.randint(300000, 5000000)
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, float(bal), 1))

    for name, atype in CC_ACCOUNTS:
        bal = -random.randint(50000, 450000)  # CC balance negative
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, float(bal), 1))

    for name, atype, bal in FIXED_ASSETS:
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, float(bal), 1))

    for name, atype, bal in LIABILITIES:
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, float(bal), 1))

    for name, atype, bal in EQUITY_ACCOUNTS:
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, float(bal), 1))

    for name, atype in AR_AP_ACCOUNTS:
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, 0.0, 1))

    for name, atype in INCOME_ACCOUNTS:
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, 0.0, 1))

    for name, atype in EXPENSE_ACCOUNTS:
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, 0.0, 1))

    for name, atype in COGS_ACCOUNTS:
        c.execute("INSERT INTO account VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), name, atype, 0.0, 1))

    # --- Items ---
    for name, itype, price, cost in ITEMS:
        qoh = random.randint(0, 50) if itype == "Inventory" else 0
        # Map item to default accounts so item lines resolve correctly
        inc_acc = "Consulting Income" if "Service" in itype or "Consult" in name \
                  else "Product Sales"
        exp_acc = "Cost of Goods Sold" if itype == "Inventory" else None
        cogs_acc = "Cost of Goods Sold" if itype == "Inventory" else None
        c.execute("""INSERT INTO item
            (list_id, full_name, item_type, description, sales_price,
             purchase_cost, quantity_on_hand, is_active,
             income_account, expense_account, cogs_account)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                  (str(uuid.uuid4()), name, itype,
                   f"{name} - professional grade", float(price), float(cost),
                   float(qoh), 1, inc_acc, exp_acc, cogs_acc))

    # --- Classes ---
    for cn in CLASSES_LIST:
        c.execute("INSERT INTO class VALUES (?,?,?)",
                  (str(uuid.uuid4()), cn, 1))

    # --- Invoices + lines ---
    # Distribute across last 2 full years + current
    invoice_number = 1000
    current_year = datetime.now().year
    # Weight: heavier in current year, lighter in older
    for year_offset in [0, 1, 2]:
        year = current_year - year_offset
        count = random.randint(80 + (2 - year_offset) * 40, 140 + (2 - year_offset) * 40)
        for _ in range(count):
            customer = random.choice(CUSTOMERS)
            txn_id = str(uuid.uuid4())
            invoice_number += 1
            txn_date = _rand_date_in_range(from_year=year)
            due_date = txn_date + timedelta(days=random.choice([15, 30, 45, 60]))

            num_lines = random.randint(1, 4)
            subtotal = 0
            for ln in range(num_lines):
                item_name, itype, base_rate, _ = random.choice(ITEMS)
                qty = random.randint(1, 5)
                rate = base_rate * random.uniform(0.9, 1.15)
                amount = qty * rate
                subtotal += amount
                c.execute("INSERT INTO invoice_line VALUES (?,?,?,?,?,?,?,?)",
                          (txn_id, f"{txn_id}-{ln}", item_name,
                           f"{item_name} - engagement", qty,
                           round(rate, 2), round(amount, 2),
                           random.choice(CLASSES_LIST)))

            # Payment status: older invoices more likely paid
            paid_prob = 0.95 if year_offset > 0 else 0.6
            is_paid = 1 if random.random() < paid_prob else 0
            bal = 0 if is_paid else subtotal * random.choice([1.0, 1.0, 0.5])

            c.execute("INSERT INTO invoice VALUES (?,?,?,?,?,?,?,?)", (
                txn_id, str(invoice_number), customer,
                txn_date.strftime("%Y-%m-%d"),
                due_date.strftime("%Y-%m-%d"),
                round(subtotal, 2), round(bal, 2), is_paid
            ))

    # --- Bills + lines ---
    bill_number = 2000
    for year_offset in [0, 1, 2]:
        year = current_year - year_offset
        count = random.randint(60 + (2 - year_offset) * 30, 100 + (2 - year_offset) * 30)
        for _ in range(count):
            vendor = random.choice(VENDORS)
            txn_id = str(uuid.uuid4())
            bill_number += 1
            txn_date = _rand_date_in_range(from_year=year)
            due_date = txn_date + timedelta(days=random.choice([15, 30]))

            num_lines = random.randint(1, 3)
            amount = 0
            for ln in range(num_lines):
                exp_acc = random.choice([a[0] for a in EXPENSE_ACCOUNTS])
                line_amt = random.randint(2000, 80000)
                amount += line_amt
                c.execute("""INSERT INTO bill_line
                    (txn_id, txn_line_id, expense_account, description, amount,
                     source_type, item_name)
                    VALUES (?,?,?,?,?,?,?)""",
                          (txn_id, f"{txn_id}-{ln}", exp_acc,
                           f"{exp_acc} expense", float(line_amt),
                           "expense", None))

            paid_prob = 0.95 if year_offset > 0 else 0.65
            is_paid = 1 if random.random() < paid_prob else 0
            amount_paid = amount if is_paid else 0

            c.execute("INSERT INTO bill VALUES (?,?,?,?,?,?,?,?,?)", (
                txn_id, str(bill_number), vendor,
                txn_date.strftime("%Y-%m-%d"),
                due_date.strftime("%Y-%m-%d"),
                float(amount), float(amount_paid),
                float(amount - amount_paid), is_paid
            ))

    # --- Checks + lines (non-bill expenses) ---
    check_number = 3000
    bank_names = [b[0] for b in BANK_ACCOUNTS if b[1] == "Bank"]
    for _ in range(random.randint(80, 120)):
        txn_id = str(uuid.uuid4())
        check_number += 1
        txn_date = _rand_date_in_range(days_back_max=720)
        payee = random.choice(VENDORS + ["Salary Payment", "Petty Cash Refill"])
        bank = random.choice(bank_names)
        num_lines = random.randint(1, 2)
        amount = 0
        for ln in range(num_lines):
            exp_acc = random.choice([a[0] for a in EXPENSE_ACCOUNTS + COGS_ACCOUNTS])
            line_amt = random.randint(5000, 150000)
            amount += line_amt
            c.execute("""INSERT INTO check_line
                (txn_id, txn_line_id, expense_account, amount, memo,
                 source_type, item_name)
                VALUES (?,?,?,?,?,?,?)""",
                      (txn_id, f"{txn_id}-{ln}", exp_acc, float(line_amt),
                       f"{exp_acc} payment", "expense", None))
        c.execute("INSERT INTO check_txn VALUES (?,?,?,?,?,?,?)",
                  (txn_id, str(check_number), bank, payee,
                   txn_date.strftime("%Y-%m-%d"), float(amount),
                   "Check payment"))

    # --- Deposits ---
    for _ in range(random.randint(60, 100)):
        txn_id = str(uuid.uuid4())
        txn_date = _rand_date_in_range(days_back_max=720)
        bank = random.choice(bank_names)
        amount = random.randint(20000, 800000)
        c.execute("INSERT INTO deposit_txn VALUES (?,?,?,?,?)",
                  (txn_id, bank, txn_date.strftime("%Y-%m-%d"),
                   float(amount), "Customer deposit batch"))

    # --- CC charges ---
    cc_names = [a[0] for a in CC_ACCOUNTS]
    for _ in range(random.randint(60, 100)):
        txn_id = str(uuid.uuid4())
        txn_date = _rand_date_in_range(days_back_max=720)
        cc_acc = random.choice(cc_names)
        payee = random.choice(VENDORS)
        num_lines = random.randint(1, 2)
        amount = 0
        for ln in range(num_lines):
            exp_acc = random.choice([a[0] for a in EXPENSE_ACCOUNTS])
            line_amt = random.randint(1000, 45000)
            amount += line_amt
            c.execute("""INSERT INTO cc_charge_line
                (txn_id, txn_line_id, expense_account, amount, memo,
                 source_type, item_name)
                VALUES (?,?,?,?,?,?,?)""",
                      (txn_id, f"{txn_id}-{ln}", exp_acc, float(line_amt),
                       f"{exp_acc} via CC", "expense", None))
        c.execute("INSERT INTO cc_charge VALUES (?,?,?,?,?,?,?)",
                  (txn_id, f"CC-{random.randint(1000, 9999)}",
                   cc_acc, payee,
                   txn_date.strftime("%Y-%m-%d"), float(amount),
                   "Credit card charge"))

    # --- Journal Entries ---
    for _ in range(random.randint(20, 35)):
        txn_id = str(uuid.uuid4())
        txn_date = _rand_date_in_range(days_back_max=720)
        amount = random.randint(10000, 500000)
        c.execute("INSERT INTO journal_entry VALUES (?,?,?,?)",
                  (txn_id, f"JE-{random.randint(1000, 9999)}",
                   txn_date.strftime("%Y-%m-%d"),
                   "Adjustment entry"))
        debit_acc = random.choice([a[0] for a in EXPENSE_ACCOUNTS])
        credit_acc = random.choice([a[0] for a in EXPENSE_ACCOUNTS])
        c.execute("INSERT INTO journal_line VALUES (?,?,?,?,?,?)",
                  (txn_id, f"{txn_id}-0", debit_acc, float(amount), 0.0, "debit"))
        c.execute("INSERT INTO journal_line VALUES (?,?,?,?,?,?)",
                  (txn_id, f"{txn_id}-1", credit_acc, 0.0, float(amount), "credit"))

    # --- Customer Payments ---
    for _ in range(random.randint(100, 150)):
        txn_id = str(uuid.uuid4())
        txn_date = _rand_date_in_range(days_back_max=720)
        customer = random.choice(CUSTOMERS)
        amount = random.randint(10000, 300000)
        bank = random.choice(bank_names)
        c.execute("INSERT INTO receive_payment VALUES (?,?,?,?,?,?)",
                  (txn_id, customer, txn_date.strftime("%Y-%m-%d"),
                   float(amount), bank, "Payment received"))

    # --- Bill Payments ---
    for _ in range(random.randint(80, 120)):
        txn_id = str(uuid.uuid4())
        txn_date = _rand_date_in_range(days_back_max=720)
        vendor = random.choice(VENDORS)
        amount = random.randint(5000, 200000)
        bank = random.choice(bank_names)
        c.execute("INSERT INTO bill_payment VALUES (?,?,?,?,?)",
                  (txn_id, vendor, txn_date.strftime("%Y-%m-%d"),
                   float(amount), bank))

    # Update customer / vendor running balances
    c.execute("""
        UPDATE customer SET balance = COALESCE((
            SELECT SUM(balance_remaining) FROM invoice
            WHERE invoice.customer_name = customer.full_name AND is_paid = 0
        ), 0)
    """)
    c.execute("""
        UPDATE vendor SET balance = COALESCE((
            SELECT SUM(balance_remaining) FROM bill
            WHERE bill.vendor_name = vendor.full_name AND is_paid = 0
        ), 0)
    """)

    conn.commit()
