"""
Global search — searches across customers, vendors, invoices, bills,
checks, items, accounts, journal entries.

Returns ranked results with type, label, and URL-like payload for
linking to the detail view.
"""


def search_all(conn, query: str, limit_per_type: int = 5):
    q = (query or "").strip()
    if not q:
        return []

    like = f"%{q}%"
    results = []

    # Try number match too (invoice/bill numbers, amounts)
    cur = conn.cursor()

    # --- Customers ---
    cur.execute("""
        SELECT list_id, full_name, company_name, balance
        FROM customer
        WHERE full_name LIKE ? OR company_name LIKE ? OR phone LIKE ? OR email LIKE ?
        ORDER BY balance DESC LIMIT ?
    """, (like, like, like, like, limit_per_type))
    for r in cur.fetchall():
        results.append({
            "type": "customer", "type_label": "Customer",
            "title": r[1], "subtitle": r[2] or "",
            "extra": f"Balance: {r[3] or 0:,.0f}",
            "payload": {"customer": r[1]}
        })

    # --- Vendors ---
    cur.execute("""
        SELECT list_id, full_name, company_name, balance
        FROM vendor
        WHERE full_name LIKE ? OR company_name LIKE ? OR phone LIKE ? OR email LIKE ?
        ORDER BY balance DESC LIMIT ?
    """, (like, like, like, like, limit_per_type))
    for r in cur.fetchall():
        results.append({
            "type": "vendor", "type_label": "Vendor",
            "title": r[1], "subtitle": r[2] or "",
            "extra": f"Owed: {r[3] or 0:,.0f}",
            "payload": {"vendor": r[1]}
        })

    # --- Invoices: match on number, customer, or amount ---
    cur.execute("""
        SELECT txn_id, txn_number, customer_name, txn_date, subtotal, balance_remaining, is_paid
        FROM invoice
        WHERE txn_number LIKE ? OR customer_name LIKE ? OR CAST(subtotal AS TEXT) LIKE ?
        ORDER BY txn_date DESC LIMIT ?
    """, (like, like, like, limit_per_type))
    for r in cur.fetchall():
        paid = "Paid" if r[6] else "Open"
        results.append({
            "type": "invoice", "type_label": "Invoice",
            "title": f"Invoice #{r[1]}",
            "subtitle": f"{r[2]} · {r[3]}",
            "extra": f"{r[4]:,.0f} · {paid}",
            "payload": {"invoice_id": r[0]}
        })

    # --- Bills ---
    cur.execute("""
        SELECT txn_id, txn_number, vendor_name, txn_date, amount, is_paid
        FROM bill
        WHERE txn_number LIKE ? OR vendor_name LIKE ? OR CAST(amount AS TEXT) LIKE ?
        ORDER BY txn_date DESC LIMIT ?
    """, (like, like, like, limit_per_type))
    for r in cur.fetchall():
        paid = "Paid" if r[5] else "Open"
        results.append({
            "type": "bill", "type_label": "Bill",
            "title": f"Bill #{r[1]}",
            "subtitle": f"{r[2]} · {r[3]}",
            "extra": f"{r[4]:,.0f} · {paid}",
            "payload": {"bill_id": r[0]}
        })

    # --- Items ---
    cur.execute("""
        SELECT list_id, full_name, item_type, sales_price
        FROM item
        WHERE full_name LIKE ? OR description LIKE ?
        LIMIT ?
    """, (like, like, limit_per_type))
    for r in cur.fetchall():
        results.append({
            "type": "item", "type_label": "Item",
            "title": r[1], "subtitle": r[2] or "",
            "extra": f"Price: {r[3] or 0:,.2f}",
            "payload": {"item": r[1]}
        })

    # --- Accounts ---
    cur.execute("""
        SELECT list_id, full_name, account_type, balance
        FROM account
        WHERE full_name LIKE ?
        LIMIT ?
    """, (like, limit_per_type))
    for r in cur.fetchall():
        results.append({
            "type": "account", "type_label": "Account",
            "title": r[1], "subtitle": r[2] or "",
            "extra": f"Balance: {r[3] or 0:,.2f}",
            "payload": {"account": r[1]}
        })

    # --- Checks (payments / expenses outside bills) ---
    cur.execute("""
        SELECT txn_id, txn_number, payee, txn_date, amount
        FROM check_txn
        WHERE txn_number LIKE ? OR payee LIKE ? OR memo LIKE ? OR CAST(amount AS TEXT) LIKE ?
        ORDER BY txn_date DESC LIMIT ?
    """, (like, like, like, like, limit_per_type))
    for r in cur.fetchall():
        results.append({
            "type": "check", "type_label": "Check",
            "title": f"Check #{r[1]}",
            "subtitle": f"{r[2]} · {r[3]}",
            "extra": f"{r[4]:,.0f}",
            "payload": {"check_id": r[0]}
        })

    return results
