"""
Authentication & user management.

Design:
- Users stored in SQLite `users` table (separate from QB data cache)
- Passwords hashed with Werkzeug (bcrypt-compatible pbkdf2)
- Sessions managed by Flask's signed cookies
- Roles: admin (full control, can manage users) / viewer (read-only)
- Limits: 2 admins max, 5 viewers max
- Rate limiting: 5 failed logins → 15-minute lockout per username
- Audit log: every login recorded with IP + user-agent
"""
import os
import time
import sqlite3
from datetime import datetime, timedelta
from functools import wraps
from flask import session, request, redirect, url_for, flash, abort
from werkzeug.security import generate_password_hash, check_password_hash

MAX_ADMINS = 2
MAX_VIEWERS = 5
LOCKOUT_THRESHOLD = 5       # failed attempts
LOCKOUT_MINUTES = 15
SESSION_DAYS = 30


USERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    email TEXT,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('admin','viewer')),
    is_active INTEGER NOT NULL DEFAULT 1,
    must_change_password INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_login_at TEXT,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    locked_until TEXT
);

CREATE TABLE IF NOT EXISTS login_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    success INTEGER NOT NULL,
    ip_address TEXT,
    user_agent TEXT,
    timestamp TEXT NOT NULL,
    note TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_user ON login_audit(username);
CREATE INDEX IF NOT EXISTS idx_audit_time ON login_audit(timestamp);
"""


def init_auth_schema(conn: sqlite3.Connection):
    conn.executescript(USERS_SCHEMA)
    conn.commit()


def ensure_bootstrap_admin(conn: sqlite3.Connection):
    """
    If no users exist yet, create the initial admin:
        username: admin
        password: admin (must be changed on first login)
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("""
            INSERT INTO users (username, name, email, password_hash, role,
                               is_active, must_change_password, created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, ("admin", "Administrator", "", generate_password_hash("admin"),
              "admin", 1, 1, datetime.now().isoformat()))
        conn.commit()
        print("[auth] Bootstrap admin created. Username: admin, Password: admin (CHANGE IMMEDIATELY)")


# ====================== User operations ======================

def get_user_by_username(conn, username):
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username = ? COLLATE NOCASE", (username,))
    row = cur.fetchone()
    return dict(row) if row else None


def get_user_by_id(conn, uid):
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (uid,))
    row = cur.fetchone()
    return dict(row) if row else None


def list_users(conn):
    cur = conn.cursor()
    cur.execute("""SELECT id, username, name, email, role, is_active,
                          must_change_password, created_at, last_login_at
                   FROM users ORDER BY role DESC, username""")
    return [dict(r) for r in cur.fetchall()]


def count_by_role(conn):
    cur = conn.cursor()
    cur.execute("SELECT role, COUNT(*) FROM users WHERE is_active=1 GROUP BY role")
    out = {"admin": 0, "viewer": 0}
    for row in cur.fetchall():
        out[row[0]] = row[1]
    return out


def create_user(conn, username, name, email, password, role):
    """
    Create a new user. Returns (ok: bool, message: str).
    Enforces role limits.
    """
    username = (username or "").strip().lower()
    name = (name or "").strip()
    role = role.lower()

    if role not in ("admin", "viewer"):
        return False, "Role must be 'admin' or 'viewer'."
    if not username or not name or not password:
        return False, "Username, name, and password are required."
    if len(username) < 3:
        return False, "Username must be at least 3 characters."
    if len(password) < 6:
        return False, "Password must be at least 6 characters."
    if " " in username:
        return False, "Username cannot contain spaces."
    if get_user_by_username(conn, username):
        return False, f"Username '{username}' is already taken."

    counts = count_by_role(conn)
    if role == "admin" and counts["admin"] >= MAX_ADMINS:
        return False, f"Maximum {MAX_ADMINS} admins allowed."
    if role == "viewer" and counts["viewer"] >= MAX_VIEWERS:
        return False, f"Maximum {MAX_VIEWERS} viewers allowed."

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (username, name, email, password_hash, role,
                           is_active, must_change_password, created_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (username, name, (email or "").strip(),
          generate_password_hash(password), role,
          1, 1, datetime.now().isoformat()))
    conn.commit()
    return True, f"User '{username}' created."


def delete_user(conn, uid, acting_user_id):
    """Hard-delete a user. Refuses to delete yourself."""
    if uid == acting_user_id:
        return False, "You cannot delete your own account."
    target = get_user_by_id(conn, uid)
    if not target:
        return False, "User not found."
    # Prevent deleting the last admin
    if target["role"] == "admin":
        counts = count_by_role(conn)
        if counts["admin"] <= 1:
            return False, "Cannot delete the last admin."
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = ?", (uid,))
    conn.commit()
    return True, f"User '{target['username']}' deleted."


def toggle_active(conn, uid, acting_user_id):
    if uid == acting_user_id:
        return False, "You cannot deactivate your own account."
    target = get_user_by_id(conn, uid)
    if not target:
        return False, "User not found."
    if target["role"] == "admin" and target["is_active"]:
        counts = count_by_role(conn)
        if counts["admin"] <= 1:
            return False, "Cannot deactivate the last active admin."
    new_state = 0 if target["is_active"] else 1
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_active = ? WHERE id = ?", (new_state, uid))
    conn.commit()
    return True, f"User '{target['username']}' {'activated' if new_state else 'deactivated'}."


def change_password(conn, uid, new_password, clear_must_change=True):
    if len(new_password) < 6:
        return False, "Password must be at least 6 characters."
    cur = conn.cursor()
    cur.execute("""UPDATE users SET password_hash = ?, must_change_password = ?,
                                    failed_attempts = 0, locked_until = NULL
                   WHERE id = ?""",
                (generate_password_hash(new_password),
                 0 if clear_must_change else 1, uid))
    conn.commit()
    return True, "Password updated."


def reset_password(conn, uid, new_password):
    """Admin resets another user's password — forces them to change on next login."""
    cur = conn.cursor()
    cur.execute("""UPDATE users SET password_hash = ?, must_change_password = 1,
                                    failed_attempts = 0, locked_until = NULL
                   WHERE id = ?""",
                (generate_password_hash(new_password), uid))
    conn.commit()
    return True, "Password reset. User must change on next login."


# ====================== Authentication ======================

def authenticate(conn, username, password, ip, user_agent):
    """
    Verify credentials. Returns (user_dict_or_None, error_message).
    Increments failed_attempts on failure, locks account after threshold.
    """
    username = (username or "").strip().lower()
    audit(conn, username, False, ip, user_agent, "attempting")  # pre-log; overwrite on success

    user = get_user_by_username(conn, username)
    if not user:
        return None, "Invalid username or password."
    if not user["is_active"]:
        return None, "Account is disabled. Contact admin."

    # Lockout check
    if user["locked_until"]:
        try:
            locked_until = datetime.fromisoformat(user["locked_until"])
            if locked_until > datetime.now():
                mins = int((locked_until - datetime.now()).total_seconds() / 60) + 1
                return None, f"Account locked. Try again in {mins} minute(s)."
            else:
                # Lock expired; clear it
                cur = conn.cursor()
                cur.execute("UPDATE users SET locked_until = NULL, failed_attempts = 0 WHERE id = ?",
                            (user["id"],))
                conn.commit()
                user["locked_until"] = None
                user["failed_attempts"] = 0
        except Exception:
            pass

    if not check_password_hash(user["password_hash"], password):
        # Wrong password — increment failed_attempts
        cur = conn.cursor()
        new_attempts = user["failed_attempts"] + 1
        locked_until = None
        if new_attempts >= LOCKOUT_THRESHOLD:
            locked_until = (datetime.now() + timedelta(minutes=LOCKOUT_MINUTES)).isoformat()
        cur.execute("UPDATE users SET failed_attempts = ?, locked_until = ? WHERE id = ?",
                    (new_attempts, locked_until, user["id"]))
        conn.commit()
        audit(conn, username, False, ip, user_agent,
              f"wrong password (attempt {new_attempts}/{LOCKOUT_THRESHOLD})")
        if locked_until:
            return None, f"Too many failed attempts. Account locked for {LOCKOUT_MINUTES} minutes."
        return None, f"Invalid username or password. ({LOCKOUT_THRESHOLD - new_attempts} attempts remaining)"

    # Success — reset counters, record login
    cur = conn.cursor()
    cur.execute("""UPDATE users SET failed_attempts = 0, locked_until = NULL,
                                    last_login_at = ? WHERE id = ?""",
                (datetime.now().isoformat(), user["id"]))
    conn.commit()
    audit(conn, username, True, ip, user_agent, "login ok")
    # Return fresh user dict
    return get_user_by_username(conn, username), None


def audit(conn, username, success, ip, user_agent, note):
    cur = conn.cursor()
    cur.execute("""INSERT INTO login_audit (username, success, ip_address, user_agent, timestamp, note)
                   VALUES (?,?,?,?,?,?)""",
                (username, 1 if success else 0, ip, user_agent,
                 datetime.now().isoformat(), note))
    conn.commit()


def recent_audit(conn, limit=50):
    cur = conn.cursor()
    cur.execute("""SELECT username, success, ip_address, user_agent, timestamp, note
                   FROM login_audit ORDER BY id DESC LIMIT ?""", (limit,))
    return [dict(r) for r in cur.fetchall()]


# ====================== Flask session helpers & decorators ======================

def current_user(conn):
    uid = session.get("uid")
    if not uid:
        return None
    return get_user_by_id(conn, uid)


def login_session(user):
    session.permanent = True
    session["uid"] = user["id"]
    session["username"] = user["username"]
    session["name"] = user["name"]
    session["role"] = user["role"]
    session["must_change"] = bool(user["must_change_password"])


def logout_session():
    session.clear()


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("uid"):
            # API requests get JSON 401; page requests get redirect
            if request.path.startswith("/api/"):
                return {"error": "Authentication required"}, 401
            return redirect(url_for("login", next=request.path))
        # If user must change password, force them to password-change page
        if session.get("must_change") and request.endpoint not in ("change_password", "logout", "static"):
            if not request.path.startswith("/api/"):
                return redirect(url_for("change_password"))
        return f(*args, **kwargs)
    return wrapped


def admin_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("uid"):
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            if request.path.startswith("/api/"):
                return {"error": "Admin only"}, 403
            abort(403)
        return f(*args, **kwargs)
    return wrapped
