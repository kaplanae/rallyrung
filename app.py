from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from authlib.integrations.flask_client import OAuth
from werkzeug.middleware.proxy_fix import ProxyFix
import sqlite3
import json
import csv
import io
from datetime import datetime, date, timedelta
from calendar import monthrange
from collections import defaultdict
import os
import uuid
import resend
from dotenv import load_dotenv

load_dotenv()

resend.api_key = os.environ.get('RESEND_API_KEY')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'rallyrung-dev-secret-key-change-in-production')
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Flask-Login setup
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'index'

# OAuth setup
oauth = OAuth(app)
google = None

GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')

if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    google = oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'}
    )

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL', '')
if DATABASE_URL and DATABASE_URL.startswith('postgres'):
    USE_POSTGRES = True
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
else:
    USE_POSTGRES = False
    DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rallyrung.db')


def get_placeholder():
    return '%s' if USE_POSTGRES else '?'


def send_email(to, subject, html):
    """Send an email via Resend. Skips silently if no API key (dev mode)."""
    if not resend.api_key:
        return False
    try:
        resend.Emails.send({
            "from": "RallyRung <noreply@rallyrung.com>",
            "to": [to] if isinstance(to, str) else to,
            "subject": subject,
            "html": html
        })
        return True
    except Exception as e:
        print(f"Email send error: {e}")
        return False


def email_wrap(body_html, footer_text="Texas Tennis Ladder — rallyrung.com"):
    """Wrap email body in a consistent layout."""
    return f'''<div style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 0 auto;">
  <h2 style="color: #e74c3c;">RallyRung</h2>
  {body_html}
  <hr style="border: none; border-top: 1px solid #eee; margin: 24px 0;">
  <p style="color: #999; font-size: 12px;">{footer_text}</p>
</div>'''


def get_db():
    if USE_POSTGRES:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        conn = psycopg2.connect(DATABASE_URL)
        conn.cursor_factory = RealDictCursor
        return conn
    else:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        return conn


# ============ USER CLASS ============

class User(UserMixin):
    def __init__(self, id, username, email=None, google_id=None, profile_picture=None,
                 phone=None, ntrp_rating=None, gender=None, is_admin=False, is_active=True):
        self.id = id
        self.username = username
        self.email = email
        self.google_id = google_id
        self.profile_picture = profile_picture
        self.phone = phone
        self.ntrp_rating = ntrp_rating
        self.gender = gender
        self.is_admin = is_admin
        self._is_active = is_active

    @property
    def is_active(self):
        return self._is_active


@login_manager.user_loader
def load_user(user_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    cur.execute(f'SELECT * FROM users WHERE id = {ph}', (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        row = dict(row)
        return User(
            id=row['id'], username=row['username'], email=row.get('email'),
            google_id=row.get('google_id'), profile_picture=row.get('profile_picture'),
            phone=row.get('phone'), ntrp_rating=row.get('ntrp_rating'),
            gender=row.get('gender'),
            is_admin=bool(row.get('is_admin')), is_active=bool(row.get('is_active', True))
        )
    return None


# ============ DATABASE INIT ============

def init_db():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    if USE_POSTGRES:
        cur.execute('''CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            google_id TEXT UNIQUE,
            email TEXT UNIQUE,
            username TEXT NOT NULL,
            phone TEXT,
            ntrp_rating TEXT,
            gender TEXT,
            profile_picture TEXT,
            is_admin BOOLEAN DEFAULT FALSE,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS ladders (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            sport TEXT DEFAULT 'tennis'
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS ladder_players (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            ladder_id INTEGER NOT NULL REFERENCES ladders(id),
            ranking INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            inactive_months INTEGER DEFAULT 0,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, ladder_id)
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS monthly_groups (
            id SERIAL PRIMARY KEY,
            ladder_id INTEGER NOT NULL REFERENCES ladders(id),
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            group_number INTEGER NOT NULL,
            player1_id INTEGER REFERENCES users(id),
            player2_id INTEGER REFERENCES users(id),
            player3_id INTEGER REFERENCES users(id)
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY,
            group_id INTEGER NOT NULL REFERENCES monthly_groups(id),
            player1_id INTEGER NOT NULL REFERENCES users(id),
            player2_id INTEGER NOT NULL REFERENCES users(id),
            winner_id INTEGER REFERENCES users(id),
            set1_p1 INTEGER, set1_p2 INTEGER,
            set2_p1 INTEGER, set2_p2 INTEGER,
            set3_p1 INTEGER, set3_p2 INTEGER,
            set1_tb TEXT, set2_tb TEXT,
            submitted_by INTEGER REFERENCES users(id),
            confirmed_by INTEGER REFERENCES users(id),
            status TEXT DEFAULT 'pending',
            outcome_type TEXT DEFAULT 'completed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS monthly_results (
            id SERIAL PRIMARY KEY,
            ladder_id INTEGER NOT NULL REFERENCES ladders(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            old_ranking INTEGER,
            new_ranking INTEGER,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            games_won INTEGER DEFAULT 0,
            games_lost INTEGER DEFAULT 0,
            movement TEXT
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS magic_tokens (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            token TEXT UNIQUE NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used BOOLEAN DEFAULT FALSE
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS player_availability (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            available_date TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            end_hour INTEGER NOT NULL
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS match_bookings (
            id SERIAL PRIMARY KEY,
            group_id INTEGER NOT NULL REFERENCES monthly_groups(id),
            requester_id INTEGER NOT NULL REFERENCES users(id),
            opponent_id INTEGER NOT NULL REFERENCES users(id),
            match_date TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            end_hour INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
    else:
        cur.execute('''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_id TEXT UNIQUE,
            email TEXT UNIQUE,
            username TEXT NOT NULL,
            phone TEXT,
            ntrp_rating TEXT,
            gender TEXT,
            profile_picture TEXT,
            is_admin INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS ladders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sport TEXT DEFAULT 'tennis'
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS ladder_players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            ladder_id INTEGER NOT NULL REFERENCES ladders(id),
            ranking INTEGER NOT NULL,
            is_active INTEGER DEFAULT 1,
            inactive_months INTEGER DEFAULT 0,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, ladder_id)
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS monthly_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ladder_id INTEGER NOT NULL REFERENCES ladders(id),
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            group_number INTEGER NOT NULL,
            player1_id INTEGER REFERENCES users(id),
            player2_id INTEGER REFERENCES users(id),
            player3_id INTEGER REFERENCES users(id)
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL REFERENCES monthly_groups(id),
            player1_id INTEGER NOT NULL REFERENCES users(id),
            player2_id INTEGER NOT NULL REFERENCES users(id),
            winner_id INTEGER REFERENCES users(id),
            set1_p1 INTEGER, set1_p2 INTEGER,
            set2_p1 INTEGER, set2_p2 INTEGER,
            set3_p1 INTEGER, set3_p2 INTEGER,
            set1_tb TEXT, set2_tb TEXT,
            submitted_by INTEGER REFERENCES users(id),
            confirmed_by INTEGER REFERENCES users(id),
            status TEXT DEFAULT 'pending',
            outcome_type TEXT DEFAULT 'completed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS monthly_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ladder_id INTEGER NOT NULL REFERENCES ladders(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            old_ranking INTEGER,
            new_ranking INTEGER,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            games_won INTEGER DEFAULT 0,
            games_lost INTEGER DEFAULT 0,
            movement TEXT
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS magic_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            token TEXT UNIQUE NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used INTEGER DEFAULT 0
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS player_availability (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            available_date TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            end_hour INTEGER NOT NULL
        )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS match_bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL REFERENCES monthly_groups(id),
            requester_id INTEGER NOT NULL REFERENCES users(id),
            opponent_id INTEGER NOT NULL REFERENCES users(id),
            match_date TEXT NOT NULL,
            start_hour INTEGER NOT NULL,
            end_hour INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

    # Migrations: add columns if missing (idempotent)
    try:
        cur.execute("ALTER TABLE matches ADD COLUMN outcome_type TEXT DEFAULT 'completed'")
        conn.commit()
    except Exception:
        conn.rollback()
    try:
        cur.execute("ALTER TABLE ladder_players ADD COLUMN inactive_months INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        conn.rollback()
    try:
        cur.execute("ALTER TABLE matches ADD COLUMN set1_tb TEXT")
        conn.commit()
    except Exception:
        conn.rollback()
    try:
        cur.execute("ALTER TABLE matches ADD COLUMN set2_tb TEXT")
        conn.commit()
    except Exception:
        conn.rollback()

    # Seed default ladder (idempotent)
    cur.execute('SELECT id FROM ladders LIMIT 1')
    if not cur.fetchone():
        cur.execute("INSERT INTO ladders (name, sport) VALUES ('Cedar Park', 'tennis')")
    else:
        # Rename legacy ladder name if needed
        cur.execute(f"UPDATE ladders SET name = 'Cedar Park' WHERE name = 'RallyRung Tennis Ladder'")

    conn.commit()

    # Seed players if the users table is empty
    cur.execute('SELECT COUNT(*) as cnt FROM users')
    user_count = dict(cur.fetchone())['cnt']
    if user_count == 0:
        try:
            seed_players(conn)
        except Exception as e:
            conn.rollback()
            print(f"Seed skipped (likely already seeded by another worker): {e}")

    conn.close()


SEED_PLAYERS = [
    (1, 'Jason Niedzwiedz', '4.5', 'M', '512-961-0020', 'jas2514@sbcglobal.net'),
    (2, 'Rudra Panchal', '4.5', 'M', '747-343-9757', 'rudrapan07@gmail.com'),
    (3, 'Suleman Khatri', '4.0', 'M', '832-496-2175', 'suleman_khatri@hotmail.com'),
    (4, 'Hugh Landes', '4.0', 'M', '619-857-1005', 'hclandes@gmail.com'),
    (5, 'Harry Tran', '4.5', 'M', '512-573-4742', 'hhtran1959@gmail.com'),
    (6, 'Matt Larsen', '4.5', 'M', '214-400-9477', 'mattlarsen22@gmail.com'),
    (7, 'Bill Fitzpatrick', '4.5', 'M', '415-271-2098', 'fitzpatrick.bill@gmail.com'),
    (8, 'Prashanth Ramakrishnan', '4.5', 'M', '734-747-3608', 'prashanth.rb1@gmail.com'),
    (9, 'Dasha Karoukina', '4.5', 'F', '512-203-1473', 'dashenka.e@gmail.com'),
    (10, 'Ram Sreenivasan', '4.5', 'M', '737-274-4684', 'ram.sreenivasan@gmail.com'),
    (11, 'Nelson Muck', '4.0', 'M', '361-362-4230', 'nnmuck@yahoo.com'),
    (12, 'Arif Saikat', '4.0', 'M', '512-815-5189', 'arif32@gmail.com'),
    (13, 'Saran Chatterjee', '4.0', 'M', '650-472-7024', 'saranagati@gmail.com'),
    (14, 'Guang Zhang', '4.5', 'M', '858-255-1998', 'grant.itennis.zhang@gmail.com'),
    (15, 'Tony Diaz', '4.0', 'M', '512-431-6990', 'tonyvoss@outlook.com'),
    (16, 'Jose Martinez', '4.0', 'M', '512-745-5318', 'jose.martinez@healthtronics.com'),
    (17, 'Brad Jackson', '4.0', 'M', '512-627-9739', 'b.v.jackson@sbcglobal.net'),
    (18, 'Alex Kaplan', '4.0', 'M', '703-231-6841', 'kaplanae@gmail.com'),
    (19, 'Darrell Park', '4.0', 'M', '512-963-0820', 'darrelljpark@yahoo.com'),
    (20, 'Michael DesJarlais', '4.0', 'M', '775-343-5073', 'medesjarlais@gmail.com'),
    (21, 'Ben Tran', '4.5', 'M', '512-791-9907', 'btran1969@gmail.com'),
    (22, 'Ameya Pai', '4.0', 'M', '352-871-5256', 'ameya.d.pai@gmail.com'),
    (23, 'Sujith Vaddi', '4.0', 'M', '551-574-2607', 'Sujith.ou.cse@gmail.com'),
    (24, 'Shashin Koppula', '4.0', 'M', '510-304-1767', 'shashin.k@gmail.com'),
    (25, 'Sundar Pandia', '4.5', 'M', '714-299-0682', 'sundars_inbox@yahoo.co.uk'),
    (26, 'Avinash Hsaniva', '4.0', 'M', '913-645-5561', 'hsaniva465@gmail.com'),
    (27, 'Agustin Valdivia', '4.0', 'M', '832-754-2794', 'agustin@thinkfd.com'),
    (28, 'Giorgio Nallira', '4.0', 'M', '737-318-6729', 'giorgionallira@gmail.com'),
    (29, 'Hong Kim', '4.0', 'M', '512-596-8942', 'hkim32321@gmail.com'),
    (30, 'Babu Moger', '4.0', 'M', '512-922-3333', 'babumoger@gmail.com'),
    (31, 'Webb Sachdev', '4.0', 'M', '585-775-2669', 'vaibhav.march89@gmail.com'),
    (32, 'Leo Guzman', '4.0', 'M', '915-244-0712', 'Leo.guzman8448@gmail.com'),
    (33, 'Enrique Aldecoa', '4.0', 'M', '817-913-9280', 'ealdecoa@gmail.com'),
    (34, 'Aslesh Kumar Thukaram', '4.0', 'M', '512-769-6802', 'asleshgt@yahoo.com'),
    (35, 'Vinod Jayakumar', '4.0', 'M', '352-870-5428', 'vjayakumar.ca@gmail.com'),
    (36, 'Naresh Taduri', '4.0', 'M', '815-508-1614', 'naresh.taduri@gmail.com'),
    (37, 'Vijay Ganji', '4.0', 'M', '337-842-9426', 'bujjus173@gmail.com'),
    (38, 'John Chen', '4.0', 'M', '512-920-7002', 'chichihchen13@gmail.com'),
    (39, 'Mauricio Somerville', '4.0', 'M', '512-587-9952', 'maugs@live.com'),
    (40, 'Yi Yin', '4.0', 'M', '512-299-8755', 'yin74133@gmail.com'),
    (41, 'Jim Champion', '4.0', 'M', '281-455-6808', 'jimchamp@gmail.com'),
    (42, 'Jin Jung', '4.0', 'M', '949-558-6244', 'jinjung013@gmail.com'),
    (43, 'Bibiana Echeverry', '4.5', 'F', '512-713-5724', 'bibiecheverry@icloud.com'),
    (44, 'Jonathan Hein', '4.0', 'M', '512-905-9248', 'jahein9999@gmail.com'),
    (45, 'Ravi Dokiparty', '4.0', 'M', '312-714-0851', 'ravisesank18@gmail.com'),
    (46, 'Rajesh Rai', '4.0', 'M', '512-947-5919', 'rrai26@gmail.com'),
    (47, 'Idan Edery', '3.5', 'M', '310-437-9008', 'imthesolarguru@gmail.com'),
    (48, 'Santosh Payal', '4.0', 'M', '512-720-2060', 'santoshpayal@gmail.com'),
    (49, 'Ricky Jara', '4.0', 'M', '512-999-8733', 'rpjara01@gmail.com'),
    (50, 'Asok Rambe', '4.0', 'M', '757-634-9436', 'asok.rambe@gmail.com'),
    (51, 'Cade Thompson', '4.0', 'M', '713-305-0986', 'cadezombie@hotmail.com'),
    (52, 'Naveen Reddy Karnati', '4.0', 'M', '815-508-2042', 'naveenreddy36@gmail.com'),
    (53, 'Derek Krause', '3.0', 'M', '512-689-8187', 'dw_krause@yahoo.com'),
    (54, 'Bala Muthuvel', '3.5', 'M', '443-956-5447', 'tmsbalachandran@gmail.com'),
    (55, 'Adi Rao', '4.0', 'M', '404-697-3588', 'aramaraow@gmail.com'),
    (56, 'John Yang', '4.0', 'M', '737-529-7820', 'johnyang827@gmail.com'),
    (57, 'Udi Sherel', '3.5', 'M', '737-340-7732', 'udi.sherel@gmail.com'),
    (58, 'Junior Gabaldon', '3.5', 'M', '512-791-2929', 'jrgabby121@gmail.com'),
    (59, 'Amir Shadlu', '3.5', 'M', '563-340-6810', 'amir.shadlu@gmail.com'),
    (60, 'Ramesh Ramani', '4.0', 'M', '225-281-6162', 'ramani_ramesh_in@yahoo.com'),
    (61, 'Mak Wagner', '3.5', 'M', '563-554-1195', 'komsonmak@gmail.com'),
    (62, 'Arun Bhakthavalsalam', '3.5', 'M', '408-507-7506', 'arunb01@icloud.com'),
    (63, 'Sumant Chhunchha', '3.5', 'M', '408-430-4655', 'mr.chhunchha@gmail.com'),
    (64, 'Steven Sturis', '3.5', 'M', '913-485-2900', 'sturis1@hotmail.com'),
    (65, 'Jorge Serratos', '3.5', 'M', '512-571-0723', 'jorge.serratos@gmail.com'),
    (66, 'Vishwa Halaharvi', '3.5', 'M', '517-505-6542', 'vishwa.adds@gmail.com'),
    (67, 'Young-Hoon Jin', '3.5', 'M', '989-392-5585', 'nmdrjin@gmail.com'),
    (68, 'Anh Truong', '3.5', 'M', '217-974-6039', 'ducanhtt@gmail.com'),
    (69, 'Shankar Jayaraman', '4.0', 'M', '860-881-1667', 'shankar60a@yahoo.com'),
    (70, 'Musab Alomari', '3.5', 'M', '585-351-9379', 'musab.alomari@gmail.com'),
    (71, 'Manoj Becket', '3.5', 'M', '408-246-3975', 'manojbecket@yahoo.com'),
    (72, 'Paavan Mistry', '3.5', 'M', '512-299-6809', 'paavan@pm.me'),
    (73, 'Altaf Mohammed', '3.5', 'M', '609-334-3256', 'altaf_fakhrul@hotmail.com'),
    (74, 'Dee Harris', '3.5', 'M', '512-496-6224', 'dharris39@austin.rr.com'),
    (75, 'Srinivas Magathala', '3.5', 'M', '916-510-9967', 'srinivasalu.mn@gmail.com'),
    (76, 'Erik Tran', '3.5', 'M', '512-694-3885', 'eriktran28@gmail.com'),
    (77, 'Andrew Evans', '3.5', 'M', '817-995-0153', 'andrew.evans@att.net'),
    (78, 'Briggs Milburn', '3.5', 'M', '512-579-1776', 'briggs_milburn@outlook.com'),
    (79, 'Sriram Murali', '3.5', 'M', '818-292-5254', 'sriramm123@gmail.com'),
    (80, 'Shameel Ummer', '3.5', 'M', '646-247-9437', 'sham35351@yahoo.com'),
    (81, 'Ashok Ganesan', '3.5', 'M', '408-477-6489', 'ashok.g@gmail.com'),
    (82, 'Ajeeth Chennadi', '3.5', 'M', '510-516-9855', 'ajeeth_chennadi@yahoo.com'),
    (83, 'Jorge Heredia', '3.5', 'M', '512-656-7674', 'jorgeherediacuenca@gmail.com'),
    (84, 'Madhu Hari', '3.5', 'M', '608-320-5626', 'hex_violist0f@icloud.com'),
    (85, 'Enoc Aguilar', '3.5', 'M', '512-659-5291', 'enocaguilar05@gmail.com'),
    (86, 'Sreekanth Nampalli', '3.5', 'M', '404-988-9832', 'sreekanthnampalli@gmail.com'),
    (87, 'Gordon Chen', '3.0', 'M', '213-204-0437', 'jungangchen.usc@gmail.com'),
    (88, 'Syam Vulavala', '3.5', 'M', '508-507-0088', 'v.syamsundar@gmail.com'),
    (89, 'Ashwani Singh', '3.0', 'M', '651-233-6823', 's.ashwani@gmail.com'),
    (90, 'James Chen', '3.5', 'M', '917-373-9630', 'momuntai@gmail.com'),
    (91, 'Umesh Sunnapu', '3.0', 'M', '469-999-7749', 'umesh.sunnapu@gmail.com'),
    (92, 'Kenneth Wong', '3.0', 'M', '312-371-7385', 'gundamunit1@gmail.com'),
    (93, 'Brad Phillips', '3.5', 'M', '512-366-2520', 'phillipsbradt@gmail.com'),
    (94, 'Gideon McClure', '3.5', 'M', '512-626-9025', 'gideon.mcclure@gmail.com'),
    (95, 'Kelly Cameron', '3.5', 'F', '989-392-3031', 'kmcameron25@gmail.com'),
    (96, 'Dibyajat Mishra', '3.5', 'M', '404-909-0039', 'dibyajat@gmail.com'),
    (97, 'Samin Ahmed', '3.5', 'M', '512-660-8817', 'samin_ahmed@ymail.com'),
    (98, 'Sesha Kuchibhotla', '3.5', 'M', '617-447-9680', 'seshasaisrivatsav@gmail.com'),
    (99, 'Muzzaffar Khan', '3.0', 'M', '845-616-2140', 'muzzaffarahmedkhan@gmail.com'),
    (100, 'Ali Hajjafar', '3.0', 'M', '615-500-5470', 'alihajjafar@gmail.com'),
    (101, 'Pradeep Madduri', '3.0', 'M', '915-929-2575', 'Pradeep.madduri@gmail.com'),
    (102, 'Mohammed Nawaz', '3.0', 'M', '845-706-5010', 'mnakhan01@hotmail.com'),
    (103, 'Arifuzzaman Nayeem', '3.0', 'M', '609-970-9810', 'arifuzzamannayeem.ce14.buet@gmail.com'),
    (104, 'Kathryn Houlton', '3.5', 'F', '512-744-8876', 'kbhoulton@gmail.com'),
    (105, 'Shain Shahid Chowdhury', '3.0', 'M', '435-294-6205', 'escapeoni@gmail.com'),
    (106, 'Jenwei Hsieh', '3.5', 'M', '512-560-2760', 'tmp2jww@gmail.com'),
    (107, 'Nazmul Rashed', '3.5', 'M', '415-216-8089', 'nazmul.nitdgp@gmail.com'),
    (108, 'Sagar Babber', '3.0', 'M', '314-680-5230', 'babbermaven@gmail.com'),
    (109, 'Ehab Dayyat', '3.0', 'M', '502-593-1117', 'ehab.dayyat@gmail.com'),
    (110, 'Arun Kumar Naidu', '3.0', 'M', '858-649-9076', 'arunnaidu1@gmail.com'),
    (111, 'Jensen Sang', '3.0', 'M', '512-998-5899', 'jensensang0920@gmail.com'),
    (112, 'Travis Turner', '3.0', 'M', '512-635-6364', 'travismack70@yahoo.com'),
    (113, 'Geraint Roberts', '3.0', 'M', '512-839-4824', 'grbox39@gmail.com'),
    (114, 'Sriram Srinivasan', '3.0', 'M', '512-578-5034', 'ssriram0@gmail.com'),
    (115, 'Sai Kasireddy', '3.0', 'M', '857-400-6462', 'sairamreddi@gmail.com'),
    (116, 'Jon Bowling', '3.5', 'M', '512-634-6666', 'jbowling@thin-nology.com'),
    (117, 'Alan Gao', '3.0', 'M', '234-352-9502', 'gaoqihang3@gmail.com'),
    (118, 'Rajesh Deb', '3.0', 'M', '307-761-3955', 'rajeshchandradeb@gmail.com'),
    (119, 'Boaz Turyahikayo', '3.0', 'M', '641-233-0323', 'boazturya@gmail.com'),
    (120, 'Riad Hasan', '3.0', 'M', '512-954-1968', 'riad01hasan@gmail.com'),
    (121, 'Prat Kode', '3.0', 'M', '737-999-2414', 'andrawidftw@gmail.com'),
    (122, 'Mark Alaniz', '3.0', 'M', '361-455-8875', 'markalaniz000@gmail.com'),
    (123, 'Juan Valdivia', '3.0', 'M', '512-656-3143', 'javaldivia@gmail.com'),
    (124, 'Nishant Kukadia', '3.0', 'M', '512-658-3119', 'nishant.kukadia@gmail.com'),
    (125, 'Tiago Soares', '3.0', 'M', '512-669-1581', 'tvdsoares+cpsl@gmail.com'),
    (126, 'Cesar Chavez', '3.0', 'M', '571-442-1030', 'avecesar163@gmail.com'),
    (127, 'Julia Wang', '3.0', 'F', '814-880-9026', 'letusforever21@gmail.com'),
    (128, 'Huzaifa Ali', '3.0', 'M', '832-329-8690', 'mhuzaifaali@gmail.com'),
    (129, 'Sandeep Vattikuti', '3.0', 'M', '210-816-1824', 'sunny9vz@gmail.com'),
    (130, 'Vinod Kumar', '3.0', 'M', '469-602-9945', 'vnarapuram@gmail.com'),
    (131, 'Casey Schroeder', '3.0', 'M', '319-431-1608', 'caseylschroeder@outlook.com'),
    (132, 'Nagaraj Sathyanarayan', '3.0', 'M', '503-533-8673', 'PrajwalPranav@gmail.com'),
    (133, 'Tino Manoles', '3.0', 'M', '512-913-3216', '307tino@gmail.com'),
    (134, 'Shantha Ram Musalae Ashok', '2.5', 'M', '678-576-8894', 'musalaeashok@gmail.com'),
    (135, 'Petro Gouws', '3.0', 'F', '737-931-7462', 'petro.gouws@gmail.com'),
    (136, 'Jason Mann', '3.0', 'M', '512-965-9371', 'jasonpmann@hotmail.com'),
    (137, 'Melanie Miranda', '3.0', 'F', '312-285-3366', 'melmrand@gmail.com'),
    (138, 'Jesse Grant', '3.0', 'M', '512-963-1701', 'bob31212@yahoo.com'),
    (139, 'Avinash Bellapu', '3.0', 'M', '214-727-6480', 'avinash.bellapu13@gmail.com'),
    (140, 'Waliur Rahman', '3.0', 'M', '512-921-3613', 'rwaliur111@gmail.com'),
    (141, 'Jeremy Santos', '3.0', 'M', '206-739-8499', 'jeremy.santos.93@gmail.com'),
    (142, 'Gina Quijano', '3.0', 'F', '512-948-9144', 'ginambq@gmail.com'),
    (143, 'Anitra Powell', '2.5', 'F', '512-560-2697', 'anitrapow@gmail.com'),
    (144, 'Ed Perron', '2.5', 'M', '512-922-6999', 'edperron1@gmail.com'),
    (145, 'Grant Carey', '3.0', 'M', '512-423-2033', 'grantdce@hotmail.com'),
    (146, 'Craig Young', '3.0', 'M', '512-293-1627', 'craigmyoung23@yahoo.com'),
    (147, 'Wes Gere', '2.5', 'M', '512-410-0224', 'wesgere@gmail.com'),
    (148, 'Brad Wang', '2.5', 'M', '203-548-1207', 'bradwang60174@gmail.com'),
    (149, 'Uyi Enadeghe', '2.5', 'M', '512-761-0291', 'oenadeghe@gmail.com'),
    (150, 'Zulfiya Gaziev', '2.5', 'F', '832-894-4638', 'hzulfiya@gmail.com'),
    (151, 'Betsy Milton', '2.5', 'F', '512-736-7171', 'betsyamilton@gmail.com'),
]


def seed_players(conn):
    """Seed the database with initial Cedar Park tennis ladder players."""
    cur = conn.cursor()
    ph = get_placeholder()

    # Get ladder id
    cur.execute('SELECT id FROM ladders LIMIT 1')
    ladder_row = cur.fetchone()
    if not ladder_row:
        return
    ladder_id = dict(ladder_row)['id']

    # Make Alex Kaplan and Darrell Park admins
    admin_emails = {'kaplanae@gmail.com', 'darrelljpark@yahoo.com'}

    for ranking, name, ntrp, gender, phone, email in SEED_PLAYERS:
        is_admin = 1 if email in admin_emails else 0
        if USE_POSTGRES:
            is_admin = bool(is_admin)

        cur.execute(f'''
            INSERT INTO users (username, email, phone, ntrp_rating, gender, is_admin)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        ''', (name, email, phone, ntrp, gender, is_admin))

        cur.execute(f'SELECT id FROM users WHERE email = {ph}', (email,))
        user_id = dict(cur.fetchone())['id']

        cur.execute(f'''
            INSERT INTO ladder_players (user_id, ladder_id, ranking)
            VALUES ({ph}, {ph}, {ph})
        ''', (user_id, ladder_id, ranking))

    conn.commit()
    print(f"Seeded {len(SEED_PLAYERS)} players into the ladder.")


# ============ HELPERS ============

def get_current_month_year():
    today = date.today()
    return today.month, today.year


def get_ladder_id():
    """Get the user's active ladder id from session, falling back to their first ladder membership."""
    # Check session first
    ladder_id = session.get('ladder_id')
    if ladder_id:
        return ladder_id

    # If logged in, try to get their first ladder membership
    if current_user.is_authenticated:
        conn = get_db()
        cur = conn.cursor()
        ph = get_placeholder()
        cur.execute(f'SELECT ladder_id FROM ladder_players WHERE user_id = {ph} ORDER BY ladder_id ASC LIMIT 1',
                    (current_user.id,))
        row = cur.fetchone()
        conn.close()
        if row:
            ladder_id = dict(row)['ladder_id']
            session['ladder_id'] = ladder_id
            return ladder_id

    # Final fallback: first ladder in DB
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT id FROM ladders ORDER BY id ASC LIMIT 1')
    row = cur.fetchone()
    conn.close()
    return dict(row)['id'] if row else None


def get_all_ladders():
    """Get all available ladders."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM ladders ORDER BY id ASC')
    ladders = [dict(r) for r in cur.fetchall()]
    conn.close()
    return ladders


def get_ladder_name(ladder_id):
    """Get the name of a ladder by id."""
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    cur.execute(f'SELECT name FROM ladders WHERE id = {ph}', (ladder_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row)['name'] if row else 'Unknown'


def get_user_ladders(user_id):
    """Get all ladders a user belongs to."""
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    cur.execute(f'''
        SELECT l.* FROM ladders l
        JOIN ladder_players lp ON l.id = lp.ladder_id
        WHERE lp.user_id = {ph}
        ORDER BY l.id ASC
    ''', (user_id,))
    ladders = [dict(r) for r in cur.fetchall()]
    conn.close()
    return ladders


def get_or_create_user_by_google(google_id, email, name, picture):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM users WHERE google_id = {ph}', (google_id,))
    user = cur.fetchone()

    if user:
        cur.execute(f'UPDATE users SET profile_picture = {ph} WHERE google_id = {ph}', (picture, google_id))
        conn.commit()
        cur.execute(f'SELECT * FROM users WHERE google_id = {ph}', (google_id,))
        user = cur.fetchone()
    else:
        cur.execute(f'SELECT * FROM users WHERE email = {ph}', (email,))
        existing = cur.fetchone()
        if existing:
            cur.execute(f'UPDATE users SET google_id = {ph}, profile_picture = {ph} WHERE email = {ph}',
                        (google_id, picture, email))
            conn.commit()
            cur.execute(f'SELECT * FROM users WHERE email = {ph}', (email,))
            user = cur.fetchone()
        else:
            cur.execute(f'''INSERT INTO users (username, email, google_id, profile_picture)
                VALUES ({ph}, {ph}, {ph}, {ph})''', (name, email, google_id, picture))
            conn.commit()
            cur.execute(f'SELECT * FROM users WHERE google_id = {ph}', (google_id,))
            user = cur.fetchone()

    conn.close()
    return dict(user)


def require_admin(f):
    """Decorator to require admin access."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Admin access required.')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated


@app.context_processor
def inject_ladder_context():
    """Inject current ladder name into all templates."""
    ladder_id = session.get('ladder_id')
    if ladder_id:
        return {'current_ladder_name': get_ladder_name(ladder_id)}
    return {'current_ladder_name': ''}


def validate_set_score(p1, p2):
    """Validate a single set score follows tennis rules."""
    if p1 is None or p2 is None:
        return True  # optional set
    if not (isinstance(p1, int) and isinstance(p2, int)):
        return False
    if p1 < 0 or p2 < 0:
        return False
    # Valid set scores: 6-0..6-4, 7-5, 7-6, or reversed
    valid_scores = set()
    for w in range(5):  # 6-0 through 6-4
        valid_scores.add((6, w))
        valid_scores.add((w, 6))
    valid_scores.add((7, 5))
    valid_scores.add((5, 7))
    valid_scores.add((7, 6))
    valid_scores.add((6, 7))
    return (p1, p2) in valid_scores


def validate_tiebreak_score(p1, p2):
    """Validate a match tiebreak score (3rd set). First to 10 or 7, win by 2."""
    if p1 is None or p2 is None:
        return True
    if not (isinstance(p1, int) and isinstance(p2, int)):
        return False
    if p1 < 0 or p2 < 0:
        return False
    high, low = max(p1, p2), min(p1, p2)
    diff = high - low
    if diff < 2:
        return False
    # 10-point match tiebreak
    if high == 10 and low <= 8:
        return True
    if high > 10 and low == high - 2 and low >= 9:
        return True
    # 7-point match tiebreak
    if high == 7 and low <= 5:
        return True
    if 7 < high < 10 and low == high - 2 and low >= 6:
        return True
    return False


def calculate_match_games(match):
    """Calculate total games won by each player in a match.
    Forfeits and injury_not_played count as 12-0 for the winner."""
    m = dict(match)
    outcome = m.get('outcome_type', 'completed')

    if outcome in ('forfeit', 'injury_not_played'):
        # Winner gets 12-0 game credit
        winner = m.get('winner_id')
        if winner == m['player1_id']:
            return 12, 0
        elif winner == m['player2_id']:
            return 0, 12
        return 0, 0

    p1_games = 0
    p2_games = 0
    for s in range(1, 4):
        s1 = m.get(f'set{s}_p1')
        s2 = m.get(f'set{s}_p2')
        if s1 is not None and s2 is not None:
            p1_games += s1
            p2_games += s2
    return p1_games, p2_games


def calculate_sets_won_lost(match):
    """Calculate sets won by each player in a match. Returns (p1_sets, p2_sets)."""
    m = dict(match)
    outcome = m.get('outcome_type', 'completed')

    if outcome in ('forfeit', 'injury_not_played'):
        winner = m.get('winner_id')
        if winner == m['player1_id']:
            return 2, 0
        elif winner == m['player2_id']:
            return 0, 2
        return 0, 0

    p1_sets = 0
    p2_sets = 0
    for s in range(1, 4):
        s1 = m.get(f'set{s}_p1')
        s2 = m.get(f'set{s}_p2')
        if s1 is not None and s2 is not None:
            if s1 > s2:
                p1_sets += 1
            elif s2 > s1:
                p2_sets += 1
    return p1_sets, p2_sets


def get_group_standings(group_id):
    """Calculate standings within a group based on confirmed matches."""
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM monthly_groups WHERE id = {ph}', (group_id,))
    group = dict(cur.fetchone())

    player_ids = [group['player1_id'], group['player2_id']]
    if group.get('player3_id'):
        player_ids.append(group['player3_id'])

    cur.execute(f"SELECT * FROM matches WHERE group_id = {ph} AND status = 'confirmed'", (group_id,))
    matches = [dict(r) for r in cur.fetchall()]
    conn.close()

    stats = {}
    for pid in player_ids:
        if pid:
            stats[pid] = {'wins': 0, 'losses': 0, 'games_won': 0, 'games_lost': 0,
                          'sets_won': 0, 'sets_lost': 0}

    for m in matches:
        outcome = m.get('outcome_type', 'completed')
        # Skip matches with no winner (schedule/weather problems)
        if outcome in ('schedule_problem', 'weather_problem'):
            continue

        p1 = m['player1_id']
        p2 = m['player2_id']
        winner = m['winner_id']
        p1_games, p2_games = calculate_match_games(m)
        p1_sets, p2_sets = calculate_sets_won_lost(m)

        if p1 in stats:
            stats[p1]['games_won'] += p1_games
            stats[p1]['games_lost'] += p2_games
            stats[p1]['sets_won'] += p1_sets
            stats[p1]['sets_lost'] += p2_sets
        if p2 in stats:
            stats[p2]['games_won'] += p2_games
            stats[p2]['games_lost'] += p1_games
            stats[p2]['sets_won'] += p2_sets
            stats[p2]['sets_lost'] += p1_sets

        if winner == p1:
            if p1 in stats: stats[p1]['wins'] += 1
            if p2 in stats: stats[p2]['losses'] += 1
        elif winner == p2:
            if p2 in stats: stats[p2]['wins'] += 1
            if p1 in stats: stats[p1]['losses'] += 1

    return stats


DAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


@app.template_filter('format_hour')
def format_hour(hour):
    """Format 24-hour integer to 12-hour display (e.g. 18 -> '6:00 PM')."""
    if hour == 0:
        return '12:00 AM'
    elif hour < 12:
        return f'{hour}:00 AM'
    elif hour == 12:
        return '12:00 PM'
    else:
        return f'{hour - 12}:00 PM'


def compute_bookable_slots(my_avail, opp_avail, all_bookings, my_id, opp_id):
    """Compute available 2-hour booking slots from overlapping date-specific availability."""
    today = date.today()

    my_windows = defaultdict(list)
    for a in my_avail:
        my_windows[a['available_date']].append((a['start_hour'], a['end_hour']))

    opp_windows = defaultdict(list)
    for a in opp_avail:
        opp_windows[a['available_date']].append((a['start_hour'], a['end_hour']))

    # Collect hours already booked for either player
    booked = set()
    for b in all_bookings:
        if b['status'] in ('pending', 'confirmed'):
            if b['requester_id'] in (my_id, opp_id) or b['opponent_id'] in (my_id, opp_id):
                for h in range(b['start_hour'], b['end_hour']):
                    booked.add((b['match_date'], h))

    # Find dates where both players have availability
    common_dates = sorted(set(my_windows.keys()) & set(opp_windows.keys()))

    slots = []
    for date_str in common_dates:
        d = date.fromisoformat(date_str)
        if d <= today:
            continue
        for ms, me in my_windows[date_str]:
            for os_, oe in opp_windows[date_str]:
                start = max(ms, os_)
                end = min(me, oe)
                if end - start >= 2:
                    for h in range(start, end - 1):
                        if (date_str, h) not in booked and (date_str, h + 1) not in booked:
                            slots.append({
                                'date': date_str,
                                'date_display': d.strftime('%a, %b %d'),
                                'start_hour': h,
                                'end_hour': h + 2,
                            })

    return slots


# ============ AUTH ROUTES ============

@app.route('/auth/google')
def google_login():
    if not google:
        flash('Google OAuth not configured.')
        return redirect(url_for('index'))
    redirect_uri = url_for('google_callback', _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route('/auth/google/callback')
def google_callback():
    try:
        token = google.authorize_access_token()
        user_info = token.get('userinfo')
        if user_info:
            full_name = user_info.get('name', user_info['email'].split('@')[0])
            first_name = user_info.get('given_name', full_name.split()[0] if full_name else 'User')
            user_data = get_or_create_user_by_google(
                google_id=user_info['sub'],
                email=user_info['email'],
                name=first_name,
                picture=user_info.get('picture', '')
            )
            user = User(
                id=user_data['id'], username=user_data['username'],
                email=user_data.get('email'), google_id=user_data.get('google_id'),
                profile_picture=user_data.get('profile_picture'),
                phone=user_data.get('phone'), ntrp_rating=user_data.get('ntrp_rating'),
                is_admin=bool(user_data.get('is_admin')),
                is_active=bool(user_data.get('is_active', True))
            )
            login_user(user)
            # Set ladder in session based on membership
            user_ladders = get_user_ladders(user.id)
            if user_ladders:
                session['ladder_id'] = user_ladders[0]['id']
                return redirect(url_for('ladder'))
            else:
                return redirect(url_for('choose_ladder'))
    except Exception as e:
        print(f"OAuth error: {e}")
    return redirect(url_for('index'))


@app.route('/auth/magic/<token>')
def magic_login(token):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM magic_tokens WHERE token = {ph}', (token,))
    row = cur.fetchone()
    if not row:
        conn.close()
        flash('Invalid login link.')
        return redirect(url_for('index'))

    row = dict(row)
    if row.get('used') if USE_POSTGRES else row.get('used'):
        conn.close()
        flash('This login link has already been used.')
        return redirect(url_for('index'))

    if datetime.utcnow() > row['expires_at']:
        conn.close()
        flash('This login link has expired.')
        return redirect(url_for('index'))

    # Mark token as used
    if USE_POSTGRES:
        cur.execute(f'UPDATE magic_tokens SET used = TRUE WHERE id = {ph}', (row['id'],))
    else:
        cur.execute(f'UPDATE magic_tokens SET used = 1 WHERE id = {ph}', (row['id'],))

    # Find the user by email
    cur.execute(f'SELECT * FROM users WHERE email = {ph}', (row['email'],))
    user_row = cur.fetchone()
    conn.commit()
    conn.close()

    if not user_row:
        flash('No account found for this email.')
        return redirect(url_for('index'))

    user_row = dict(user_row)
    user = User(
        id=user_row['id'], username=user_row['username'],
        email=user_row.get('email'), google_id=user_row.get('google_id'),
        profile_picture=user_row.get('profile_picture'),
        phone=user_row.get('phone'), ntrp_rating=user_row.get('ntrp_rating'),
        gender=user_row.get('gender'),
        is_admin=bool(user_row.get('is_admin')),
        is_active=bool(user_row.get('is_active', True))
    )
    login_user(user)
    flash(f'Welcome, {user.username}!')
    # Set ladder in session based on membership
    user_ladders = get_user_ladders(user.id)
    if user_ladders:
        session['ladder_id'] = user_ladders[0]['id']
        return redirect(url_for('ladder'))
    else:
        return redirect(url_for('choose_ladder'))


@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.pop('ladder_id', None)
    return redirect(url_for('index'))


@app.route('/choose-ladder')
@login_required
def choose_ladder():
    ladders = get_all_ladders()
    user_ladders = get_user_ladders(current_user.id)
    user_ladder_ids = [l['id'] for l in user_ladders]
    return render_template('choose_ladder.html', ladders=ladders, user_ladder_ids=user_ladder_ids)


@app.route('/switch-ladder/<int:ladder_id>')
@login_required
def switch_ladder(ladder_id):
    # Verify ladder exists
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    cur.execute(f'SELECT id FROM ladders WHERE id = {ph}', (ladder_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        session['ladder_id'] = ladder_id
        flash(f'Switched to {get_ladder_name(ladder_id)} ladder.')
    return redirect(url_for('ladder'))


# ============ API ROUTES ============

@app.route('/api/me')
def api_me():
    if current_user.is_authenticated:
        return jsonify({
            'logged_in': True,
            'id': current_user.id,
            'username': current_user.username,
            'email': current_user.email,
            'profile_picture': current_user.profile_picture,
            'is_admin': current_user.is_admin
        })
    return jsonify({'logged_in': False})


# ============ PAGE ROUTES ============

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/rules')
def rules():
    return render_template('rules.html')


@app.route('/ladder')
def ladder():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    # Get all active players with their rankings
    cur.execute('''
        SELECT u.id, u.username, u.profile_picture, u.ntrp_rating, u.gender,
               lp.ranking, lp.is_active
        FROM ladder_players lp
        JOIN users u ON lp.user_id = u.id
        WHERE lp.ladder_id = %s AND lp.is_active = %s
        ORDER BY lp.ranking ASC
    '''.replace('%s', ph), (ladder_id, True if USE_POSTGRES else 1))
    players = [dict(r) for r in cur.fetchall()]

    # Build player lookup by id
    player_map = {p['id']: p for p in players}

    # Get current month groups and their matches
    cur.execute(f'''
        SELECT mg.*, u1.username as p1_name, u2.username as p2_name, u3.username as p3_name
        FROM monthly_groups mg
        LEFT JOIN users u1 ON mg.player1_id = u1.id
        LEFT JOIN users u2 ON mg.player2_id = u2.id
        LEFT JOIN users u3 ON mg.player3_id = u3.id
        WHERE mg.ladder_id = {ph} AND mg.month = {ph} AND mg.year = {ph}
        ORDER BY mg.group_number ASC
    ''', (ladder_id, month, year))
    groups = [dict(r) for r in cur.fetchall()]

    # Get match results for each group
    for g in groups:
        cur.execute(f"SELECT * FROM matches WHERE group_id = {ph}", (g['id'],))
        g['matches'] = [dict(r) for r in cur.fetchall()]
        g['standings'] = get_group_standings(g['id'])

    # Build a match lookup: (player1_id, player2_id) -> match (unordered)
    match_lookup = {}
    for g in groups:
        for m in g['matches']:
            key = tuple(sorted([m['player1_id'], m['player2_id']]))
            match_lookup[key] = m

    # Build ladder groups of 3 with one row per player
    # Each player appears once on the left with their ranking,
    # opponent rotates: 1v2, 2v3, 3v1 — covers all 3 matchups
    def get_match_info(p1, p2):
        """Return score string and winner_id for a matchup."""
        key = tuple(sorted([p1['id'], p2['id']]))
        match = match_lookup.get(key)
        if not match or match.get('status') not in ('confirmed', 'pending'):
            return '', None
        sets = []
        if match.get('set1_p1') is not None:
            is_p1 = match['player1_id'] == p1['id']
            for s in range(1, 4):
                sp1 = match.get(f'set{s}_p1')
                sp2 = match.get(f'set{s}_p2')
                if sp1 is None:
                    break
                a, b = (sp1, sp2) if is_p1 else (sp2, sp1)
                score = f"{a}-{b}"
                # Add tiebreak detail for 7-6 sets
                tb = match.get(f'set{s}_tb') if s <= 2 else None
                if tb:
                    parts = tb.split('-')
                    if len(parts) == 2:
                        tb_a, tb_b = (parts[0], parts[1]) if is_p1 else (parts[1], parts[0])
                        score += f"({tb_a})"
                sets.append(score)
        return '  '.join(sets), match.get('winner_id')

    def make_row(p, opp):
        score, winner_id = get_match_info(p, opp) if opp else ('', None)
        return {'player': p, 'opponent': opp, 'score': score, 'winner_id': winner_id}

    ladder_groups = []
    for i in range(0, len(players), 3):
        gp = players[i:i+3]
        rows = []
        if len(gp) == 3:
            rows.append(make_row(gp[0], gp[1]))
            rows.append(make_row(gp[1], gp[2]))
            rows.append(make_row(gp[2], gp[0]))
        elif len(gp) == 2:
            rows.append(make_row(gp[0], gp[1]))
            rows.append(make_row(gp[1], gp[0]))
        elif len(gp) == 1:
            rows.append(make_row(gp[0], None))
        ladder_groups.append({'number': i // 3 + 1, 'rows': rows})

    conn.close()
    ladder_name = get_ladder_name(ladder_id)
    return render_template('ladder.html', players=players, ladder_groups=ladder_groups,
                           groups=groups, month=month, year=year, ladder_name=ladder_name)


@app.route('/my-group')
@login_required
def my_group():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    # Find the user's group for the current month
    cur.execute(f'''
        SELECT * FROM monthly_groups
        WHERE ladder_id = {ph} AND month = {ph} AND year = {ph}
          AND (player1_id = {ph} OR player2_id = {ph} OR player3_id = {ph})
    ''', (ladder_id, month, year, current_user.id, current_user.id, current_user.id))
    group = cur.fetchone()

    if not group:
        conn.close()
        return render_template('my_group.html', group=None, group_players=[], matches=[])

    group = dict(group)

    # Get group players with contact info
    player_ids = [group['player1_id'], group['player2_id']]
    if group.get('player3_id'):
        player_ids.append(group['player3_id'])

    group_players = []
    for pid in player_ids:
        if pid:
            cur.execute(f'SELECT id, username, email, phone, profile_picture, ntrp_rating FROM users WHERE id = {ph}', (pid,))
            p = cur.fetchone()
            if p:
                group_players.append(dict(p))

    # Get all matches in this group
    cur.execute(f'''
        SELECT m.*, u1.username as p1_name, u2.username as p2_name, w.username as winner_name
        FROM matches m
        JOIN users u1 ON m.player1_id = u1.id
        JOIN users u2 ON m.player2_id = u2.id
        LEFT JOIN users w ON m.winner_id = w.id
        WHERE m.group_id = {ph}
        ORDER BY m.created_at DESC
    ''', (group['id'],))
    matches = [dict(r) for r in cur.fetchall()]

    standings = get_group_standings(group['id'])

    # Get availability for all group members (current month only)
    opponent_ids = [pid for pid in player_ids if pid and pid != current_user.id]
    month_prefix = f'{year}-{month:02d}'
    availability = {}
    for pid in player_ids:
        if pid:
            cur.execute(f"SELECT * FROM player_availability WHERE user_id = {ph} AND available_date LIKE {ph} ORDER BY available_date, start_hour",
                        (pid, month_prefix + '%'))
            avail = [dict(r) for r in cur.fetchall()]
            for a in avail:
                d = date.fromisoformat(a['available_date'])
                a['date_display'] = d.strftime('%a %b %d')
            availability[pid] = avail

    # Get bookings for this group
    cur.execute(f'''
        SELECT mb.*, u1.username as requester_name, u2.username as opponent_name
        FROM match_bookings mb
        JOIN users u1 ON mb.requester_id = u1.id
        JOIN users u2 ON mb.opponent_id = u2.id
        WHERE mb.group_id = {ph} AND mb.status IN ('pending', 'confirmed')
        ORDER BY mb.match_date, mb.start_hour
    ''', (group['id'],))
    bookings = [dict(r) for r in cur.fetchall()]

    # Compute bookable slots for each opponent
    my_avail = availability.get(current_user.id, [])
    opponent_slots = {}
    for pid in opponent_ids:
        opp_avail = availability.get(pid, [])
        opponent_slots[pid] = compute_bookable_slots(my_avail, opp_avail, bookings, current_user.id, pid)

    # Build opponent name lookup
    opponent_names = {p['id']: p['username'] for p in group_players}

    conn.close()
    return render_template('my_group.html', group=group, group_players=group_players,
                           matches=matches, standings=standings, availability=availability,
                           bookings=bookings, opponent_slots=opponent_slots,
                           opponent_ids=opponent_ids, opponent_names=opponent_names,
                           DAY_NAMES=DAY_NAMES)


@app.route('/submit-result', methods=['GET', 'POST'])
@login_required
def submit_result():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    # Find the user's group
    cur.execute(f'''
        SELECT * FROM monthly_groups
        WHERE ladder_id = {ph} AND month = {ph} AND year = {ph}
          AND (player1_id = {ph} OR player2_id = {ph} OR player3_id = {ph})
    ''', (ladder_id, month, year, current_user.id, current_user.id, current_user.id))
    group = cur.fetchone()

    if not group:
        conn.close()
        flash('You are not in a group this month.')
        return redirect(url_for('my_group'))

    group = dict(group)

    # Get opponents in this group
    player_ids = [group['player1_id'], group['player2_id']]
    if group.get('player3_id'):
        player_ids.append(group['player3_id'])
    opponent_ids = [pid for pid in player_ids if pid and pid != current_user.id]

    opponents = []
    for pid in opponent_ids:
        cur.execute(f'SELECT id, username FROM users WHERE id = {ph}', (pid,))
        opp = cur.fetchone()
        if opp:
            opponents.append(dict(opp))

    if request.method == 'POST':
        opponent_id = int(request.form.get('opponent_id', 0))
        outcome_type = request.form.get('outcome_type', 'completed')
        valid_outcomes = ('completed', 'forfeit', 'schedule_problem', 'weather_problem',
                          'injury_not_finished', 'injury_not_played')

        if opponent_id not in opponent_ids:
            flash('Invalid opponent.')
            conn.close()
            return redirect(url_for('submit_result'))

        if outcome_type not in valid_outcomes:
            flash('Invalid outcome type.')
            conn.close()
            return redirect(url_for('submit_result'))

        # No-winner outcomes: schedule_problem, weather_problem
        no_winner_outcomes = ('schedule_problem', 'weather_problem')
        # Winner-only outcomes (no scores): forfeit, injury_not_played
        winner_only_outcomes = ('forfeit', 'injury_not_played')

        if outcome_type in no_winner_outcomes:
            winner_id = None
        else:
            winner_id = int(request.form.get('winner_id', 0))
            if winner_id not in (current_user.id, opponent_id):
                flash('Invalid winner.')
                conn.close()
                return redirect(url_for('submit_result'))

        # Parse set scores (skip for winner-only and no-winner outcomes)
        sets = [(None, None), (None, None), (None, None)]
        if outcome_type not in no_winner_outcomes and outcome_type not in winner_only_outcomes:
            for s in range(1, 4):
                p1_score = request.form.get(f'set{s}_p1', '')
                p2_score = request.form.get(f'set{s}_p2', '')
                if p1_score and p2_score:
                    try:
                        p1_val = int(p1_score)
                        p2_val = int(p2_score)
                    except ValueError:
                        flash(f'Set {s} scores must be numbers.')
                        conn.close()
                        return redirect(url_for('submit_result'))
                    if outcome_type == 'completed':
                        if s <= 2 and not validate_set_score(p1_val, p2_val):
                            flash(f'Set {s} score {p1_val}-{p2_val} is not a valid tennis score.')
                            conn.close()
                            return redirect(url_for('submit_result'))
                        if s == 3 and not validate_tiebreak_score(p1_val, p2_val):
                            flash(f'Tiebreak score {p1_val}-{p2_val} is not valid. Must be first to 10 or 7, win by 2.')
                            conn.close()
                            return redirect(url_for('submit_result'))
                    sets[s - 1] = (p1_val, p2_val)

            if outcome_type == 'completed':
                if sets[0] == (None, None) or sets[1] == (None, None):
                    flash('At least 2 sets are required.')
                    conn.close()
                    return redirect(url_for('submit_result'))

        # Check for duplicate submission
        cur.execute(f'''
            SELECT id FROM matches WHERE group_id = {ph}
              AND ((player1_id = {ph} AND player2_id = {ph}) OR (player1_id = {ph} AND player2_id = {ph}))
        ''', (group['id'], current_user.id, opponent_id, opponent_id, current_user.id))
        existing = cur.fetchone()
        if existing:
            flash('A match result already exists for this matchup. It may need confirmation.')
            conn.close()
            return redirect(url_for('my_group'))

        # Parse set tiebreak scores (for 7-6 sets)
        set1_tb = request.form.get('set1_tb', '').strip() or None
        set2_tb = request.form.get('set2_tb', '').strip() or None

        # Order players so player1 is always the lower id for consistency
        if current_user.id < opponent_id:
            p1_id, p2_id = current_user.id, opponent_id
            s1_p1, s1_p2 = sets[0]
            s2_p1, s2_p2 = sets[1]
            s3_p1, s3_p2 = sets[2]
        else:
            p1_id, p2_id = opponent_id, current_user.id
            s1_p1, s1_p2 = (sets[0][1], sets[0][0]) if sets[0][0] is not None else (None, None)
            s2_p1, s2_p2 = (sets[1][1], sets[1][0]) if sets[1][0] is not None else (None, None)
            s3_p1, s3_p2 = (sets[2][1], sets[2][0]) if sets[2][0] is not None else (None, None)
            # Flip tiebreak scores too
            if set1_tb:
                parts = set1_tb.split('-')
                if len(parts) == 2:
                    set1_tb = f"{parts[1]}-{parts[0]}"
            if set2_tb:
                parts = set2_tb.split('-')
                if len(parts) == 2:
                    set2_tb = f"{parts[1]}-{parts[0]}"

        cur.execute(f'''
            INSERT INTO matches (group_id, player1_id, player2_id, winner_id,
                set1_p1, set1_p2, set2_p1, set2_p2, set3_p1, set3_p2,
                set1_tb, set2_tb,
                submitted_by, status, outcome_type)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'pending', {ph})
        ''', (group['id'], p1_id, p2_id, winner_id,
              s1_p1, s1_p2, s2_p1, s2_p2, s3_p1, s3_p2,
              set1_tb, set2_tb, current_user.id, outcome_type))
        conn.commit()
        conn.close()
        flash('Match result submitted! Waiting for opponent to confirm.')
        return redirect(url_for('my_group'))

    conn.close()
    return render_template('submit_result.html', group=group, opponents=opponents)


@app.route('/confirm-match/<int:match_id>', methods=['POST'])
@login_required
def confirm_match(match_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM matches WHERE id = {ph}', (match_id,))
    match = cur.fetchone()
    if not match:
        conn.close()
        flash('Match not found.')
        return redirect(url_for('my_group'))

    match = dict(match)
    if match['status'] != 'pending':
        conn.close()
        flash('This match is not pending confirmation.')
        return redirect(url_for('my_group'))

    # Only the other player (not the submitter) can confirm
    if current_user.id == match['submitted_by']:
        conn.close()
        flash('You cannot confirm your own submission.')
        return redirect(url_for('my_group'))

    if current_user.id not in (match['player1_id'], match['player2_id']):
        conn.close()
        flash('You are not a player in this match.')
        return redirect(url_for('my_group'))

    cur.execute(f"UPDATE matches SET status = 'confirmed', confirmed_by = {ph} WHERE id = {ph}",
                (current_user.id, match_id))
    conn.commit()
    conn.close()
    flash('Match confirmed!')
    return redirect(url_for('my_group'))


@app.route('/dispute-match/<int:match_id>', methods=['POST'])
@login_required
def dispute_match(match_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM matches WHERE id = {ph}', (match_id,))
    match = cur.fetchone()
    if not match:
        conn.close()
        flash('Match not found.')
        return redirect(url_for('my_group'))

    match = dict(match)
    if current_user.id == match['submitted_by']:
        conn.close()
        flash('You cannot dispute your own submission. Delete it instead.')
        return redirect(url_for('my_group'))

    if current_user.id not in (match['player1_id'], match['player2_id']):
        conn.close()
        flash('You are not a player in this match.')
        return redirect(url_for('my_group'))

    cur.execute(f"UPDATE matches SET status = 'disputed' WHERE id = {ph}", (match_id,))
    conn.commit()
    conn.close()
    flash('Match disputed. An admin will review.')
    return redirect(url_for('my_group'))


@app.route('/delete-match/<int:match_id>', methods=['POST'])
@login_required
def delete_match(match_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM matches WHERE id = {ph}', (match_id,))
    match = cur.fetchone()
    if not match:
        conn.close()
        flash('Match not found.')
        return redirect(url_for('my_group'))

    match = dict(match)
    if current_user.id != match['submitted_by'] and not current_user.is_admin:
        conn.close()
        flash('Only the submitter or an admin can delete a match.')
        return redirect(url_for('my_group'))

    if match['status'] == 'confirmed' and not current_user.is_admin:
        conn.close()
        flash('Confirmed matches can only be deleted by an admin.')
        return redirect(url_for('my_group'))

    cur.execute(f'DELETE FROM matches WHERE id = {ph}', (match_id,))
    conn.commit()
    conn.close()
    flash('Match deleted.')
    return redirect(url_for('my_group'))


@app.route('/profile')
@login_required
def profile():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    ladder_id = get_ladder_id()

    # Get player's ladder info
    cur.execute(f'SELECT * FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                (current_user.id, ladder_id))
    ladder_player = cur.fetchone()
    ladder_player = dict(ladder_player) if ladder_player else None

    # Get match history
    cur.execute(f'''
        SELECT m.*, u1.username as p1_name, u2.username as p2_name, w.username as winner_name,
               mg.month, mg.year, mg.group_number
        FROM matches m
        JOIN users u1 ON m.player1_id = u1.id
        JOIN users u2 ON m.player2_id = u2.id
        LEFT JOIN users w ON m.winner_id = w.id
        JOIN monthly_groups mg ON m.group_id = mg.id
        WHERE (m.player1_id = {ph} OR m.player2_id = {ph}) AND m.status = 'confirmed'
        ORDER BY m.created_at DESC
    ''', (current_user.id, current_user.id))
    match_history = [dict(r) for r in cur.fetchall()]

    # Get ranking history
    cur.execute(f'''
        SELECT * FROM monthly_results
        WHERE user_id = {ph} AND ladder_id = {ph}
        ORDER BY year DESC, month DESC
    ''', (current_user.id, ladder_id))
    ranking_history = [dict(r) for r in cur.fetchall()]

    # Win/loss totals
    total_wins = sum(1 for m in match_history if m['winner_id'] == current_user.id)
    total_losses = len(match_history) - total_wins

    conn.close()
    all_ladders = get_all_ladders()
    ladder_name = get_ladder_name(ladder_id)
    return render_template('profile.html', ladder_player=ladder_player,
                           match_history=match_history, ranking_history=ranking_history,
                           total_wins=total_wins, total_losses=total_losses,
                           all_ladders=all_ladders, ladder_name=ladder_name,
                           current_ladder_id=ladder_id)


@app.route('/ladder/join', methods=['POST'])
@login_required
def ladder_join():
    ntrp_rating = request.form.get('ntrp_rating', '').strip()
    if not ntrp_rating:
        flash('Please select an NTRP rating.')
        return redirect(url_for('profile'))

    # Use form-specified ladder_id, or fall back to current active ladder
    ladder_id = request.form.get('ladder_id', type=int) or get_ladder_id()
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    # Check if already on ladder
    cur.execute(f'SELECT id FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                (current_user.id, ladder_id))
    if cur.fetchone():
        conn.close()
        flash('You are already on the ladder.')
        return redirect(url_for('profile'))

    # Update user's NTRP rating
    cur.execute(f'UPDATE users SET ntrp_rating = {ph} WHERE id = {ph}',
                (ntrp_rating, current_user.id))

    # Find insertion point: after the last player with same or higher NTRP rating
    cur.execute(f'''
        SELECT lp.ranking, u.ntrp_rating
        FROM ladder_players lp
        JOIN users u ON lp.user_id = u.id
        WHERE lp.ladder_id = {ph}
        ORDER BY lp.ranking ASC
    ''', (ladder_id,))
    players = [dict(r) for r in cur.fetchall()]

    submitted_rating = float(ntrp_rating)
    max_rank_at_or_above = 0
    for p in players:
        try:
            player_rating = float(p['ntrp_rating']) if p['ntrp_rating'] else 0
        except (ValueError, TypeError):
            player_rating = 0
        if player_rating >= submitted_rating:
            max_rank_at_or_above = max(max_rank_at_or_above, p['ranking'])

    if max_rank_at_or_above > 0:
        new_ranking = max_rank_at_or_above + 1
    else:
        new_ranking = 1

    # Shift everyone at or below the new ranking down by 1
    cur.execute(f'''
        UPDATE ladder_players SET ranking = ranking + 1
        WHERE ladder_id = {ph} AND ranking >= {ph}
    ''', (ladder_id, new_ranking))

    cur.execute(f'''
        INSERT INTO ladder_players (user_id, ladder_id, ranking)
        VALUES ({ph}, {ph}, {ph})
    ''', (current_user.id, ladder_id, new_ranking))

    conn.commit()
    conn.close()
    session['ladder_id'] = ladder_id
    ladder_name = get_ladder_name(ladder_id)
    flash(f'Welcome to the {ladder_name} ladder! You have been placed at rank #{new_ranking}.')
    return redirect(url_for('profile'))


@app.route('/ladder/leave', methods=['POST'])
@login_required
def ladder_leave():
    ladder_id = get_ladder_id()
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT ranking FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                (current_user.id, ladder_id))
    row = cur.fetchone()
    if row:
        old_ranking = dict(row)['ranking']
        cur.execute(f'DELETE FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                    (current_user.id, ladder_id))
        cur.execute(f'''
            UPDATE ladder_players SET ranking = ranking - 1
            WHERE ladder_id = {ph} AND ranking > {ph}
        ''', (ladder_id, old_ranking))
        conn.commit()
        flash('You have left the ladder.')
    else:
        flash('You are not on the ladder.')

    conn.close()
    return redirect(url_for('profile'))


@app.route('/ladder/pause', methods=['POST'])
@login_required
def ladder_pause():
    ladder_id = get_ladder_id()
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    if USE_POSTGRES:
        cur.execute(f'UPDATE ladder_players SET is_active = FALSE WHERE user_id = {ph} AND ladder_id = {ph}',
                    (current_user.id, ladder_id))
    else:
        cur.execute(f'UPDATE ladder_players SET is_active = 0 WHERE user_id = {ph} AND ladder_id = {ph}',
                    (current_user.id, ladder_id))
    conn.commit()
    conn.close()
    flash('Your ladder participation is paused. You keep your ranking but won\'t be placed in groups.')
    return redirect(url_for('profile'))


@app.route('/ladder/unpause', methods=['POST'])
@login_required
def ladder_unpause():
    ladder_id = get_ladder_id()
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    if USE_POSTGRES:
        cur.execute(f'UPDATE ladder_players SET is_active = TRUE WHERE user_id = {ph} AND ladder_id = {ph}',
                    (current_user.id, ladder_id))
    else:
        cur.execute(f'UPDATE ladder_players SET is_active = 1 WHERE user_id = {ph} AND ladder_id = {ph}',
                    (current_user.id, ladder_id))
    conn.commit()
    conn.close()
    flash('Welcome back! You are active on the ladder again.')
    return redirect(url_for('profile'))


@app.route('/profile/edit', methods=['POST'])
@login_required
def edit_profile():
    phone = request.form.get('phone', '').strip()
    ntrp = request.form.get('ntrp_rating', '').strip()

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    cur.execute(f'UPDATE users SET phone = {ph}, ntrp_rating = {ph} WHERE id = {ph}',
                (phone, ntrp, current_user.id))
    conn.commit()
    conn.close()
    flash('Profile updated.')
    return redirect(url_for('profile'))


# ============ AVAILABILITY & BOOKING ROUTES ============

@app.route('/availability')
@login_required
def availability():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    month, year = get_current_month_year()
    _, days_in_month = monthrange(year, month)
    month_prefix = f'{year}-{month:02d}'

    cur.execute(f"SELECT * FROM player_availability WHERE user_id = {ph} AND available_date LIKE {ph} ORDER BY available_date, start_hour",
                (current_user.id, month_prefix + '%'))
    windows = [dict(r) for r in cur.fetchall()]
    for w in windows:
        d = date.fromisoformat(w['available_date'])
        w['date_display'] = d.strftime('%a, %b %d')

    # Generate remaining dates for the date picker
    today = date.today()
    remaining_dates = []
    for day_num in range(1, days_in_month + 1):
        d = date(year, month, day_num)
        if d > today:
            remaining_dates.append({
                'date': d.isoformat(),
                'display': d.strftime('%a, %b %d'),
            })

    conn.close()
    return render_template('availability.html', windows=windows, remaining_dates=remaining_dates,
                           month=month, year=year, DAY_NAMES=DAY_NAMES)


@app.route('/availability/add', methods=['POST'])
@login_required
def availability_add():
    available_date = request.form.get('available_date', '').strip()
    try:
        start_hour = int(request.form.get('start_hour', -1))
        end_hour = int(request.form.get('end_hour', -1))
    except (ValueError, TypeError):
        flash('Invalid input.')
        return redirect(url_for('availability'))

    if not available_date:
        flash('Please select a date.')
        return redirect(url_for('availability'))
    if start_hour < 0 or start_hour > 23 or end_hour < 1 or end_hour > 23:
        flash('Invalid time.')
        return redirect(url_for('availability'))
    if end_hour <= start_hour:
        flash('End time must be after start time.')
        return redirect(url_for('availability'))

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    # Check for duplicate
    cur.execute(f'''SELECT id FROM player_availability
        WHERE user_id = {ph} AND available_date = {ph} AND start_hour = {ph} AND end_hour = {ph}''',
                (current_user.id, available_date, start_hour, end_hour))
    if cur.fetchone():
        conn.close()
        flash('This window already exists.')
        return redirect(url_for('availability'))

    cur.execute(f'''INSERT INTO player_availability (user_id, available_date, start_hour, end_hour)
        VALUES ({ph}, {ph}, {ph}, {ph})''',
                (current_user.id, available_date, start_hour, end_hour))
    conn.commit()
    conn.close()
    flash('Availability added.')
    return redirect(url_for('availability'))


@app.route('/availability/quick-fill', methods=['POST'])
@login_required
def availability_quick_fill():
    pattern = request.form.get('pattern', '')
    try:
        start_hour = int(request.form.get('start_hour', -1))
        end_hour = int(request.form.get('end_hour', -1))
    except (ValueError, TypeError):
        flash('Invalid input.')
        return redirect(url_for('availability'))

    valid_patterns = ('weekdays', 'weekends', 'all', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')
    day_map = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}
    if pattern not in valid_patterns:
        flash('Invalid pattern.')
        return redirect(url_for('availability'))
    if end_hour <= start_hour:
        flash('End time must be after start time.')
        return redirect(url_for('availability'))

    month, year = get_current_month_year()
    _, days_in_month = monthrange(year, month)
    today = date.today()

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    added = 0
    for day_num in range(1, days_in_month + 1):
        d = date(year, month, day_num)
        if d <= today:
            continue
        dow = d.weekday()  # 0=Mon, 6=Sun
        if pattern == 'weekdays' and dow >= 5:
            continue
        if pattern == 'weekends' and dow < 5:
            continue
        if pattern in day_map and dow != day_map[pattern]:
            continue

        date_str = d.isoformat()
        # Skip if already exists
        cur.execute(f'''SELECT id FROM player_availability
            WHERE user_id = {ph} AND available_date = {ph} AND start_hour = {ph} AND end_hour = {ph}''',
                    (current_user.id, date_str, start_hour, end_hour))
        if not cur.fetchone():
            cur.execute(f'''INSERT INTO player_availability (user_id, available_date, start_hour, end_hour)
                VALUES ({ph}, {ph}, {ph}, {ph})''',
                        (current_user.id, date_str, start_hour, end_hour))
            added += 1

    conn.commit()
    conn.close()
    flash(f'Added {added} availability windows.')
    return redirect(url_for('availability'))


@app.route('/availability/clear', methods=['POST'])
@login_required
def availability_clear():
    month, year = get_current_month_year()
    month_prefix = f'{year}-{month:02d}'
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    cur.execute(f"DELETE FROM player_availability WHERE user_id = {ph} AND available_date LIKE {ph}",
                (current_user.id, month_prefix + '%'))
    conn.commit()
    conn.close()
    flash('All availability cleared for this month.')
    return redirect(url_for('availability'))


@app.route('/availability/delete/<int:avail_id>', methods=['POST'])
@login_required
def availability_delete(avail_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    cur.execute(f'DELETE FROM player_availability WHERE id = {ph} AND user_id = {ph}',
                (avail_id, current_user.id))
    conn.commit()
    conn.close()
    flash('Availability removed.')
    return redirect(url_for('availability'))


@app.route('/book-match', methods=['POST'])
@login_required
def book_match():
    try:
        opponent_id = int(request.form.get('opponent_id', 0))
        match_date = request.form.get('match_date', '').strip()
        start_hour = int(request.form.get('start_hour', -1))
    except (ValueError, TypeError):
        flash('Invalid booking data.')
        return redirect(url_for('my_group'))

    end_hour = start_hour + 2
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    # Verify both players are in the same group
    cur.execute(f'''
        SELECT id FROM monthly_groups
        WHERE ladder_id = {ph} AND month = {ph} AND year = {ph}
          AND (player1_id = {ph} OR player2_id = {ph} OR player3_id = {ph})
          AND (player1_id = {ph} OR player2_id = {ph} OR player3_id = {ph})
    ''', (ladder_id, month, year,
          current_user.id, current_user.id, current_user.id,
          opponent_id, opponent_id, opponent_id))
    group_row = cur.fetchone()
    if not group_row:
        conn.close()
        flash('You are not in the same group as this player.')
        return redirect(url_for('my_group'))

    group_id = dict(group_row)['id']

    # Validate date is in current month and in the future
    try:
        booking_date = date.fromisoformat(match_date)
    except ValueError:
        conn.close()
        flash('Invalid date.')
        return redirect(url_for('my_group'))

    if booking_date.month != month or booking_date.year != year:
        conn.close()
        flash('Date must be in the current month.')
        return redirect(url_for('my_group'))

    if booking_date <= date.today():
        conn.close()
        flash('Date must be in the future.')
        return redirect(url_for('my_group'))

    # Check for conflicting bookings for either player
    cur.execute(f'''
        SELECT id FROM match_bookings
        WHERE match_date = {ph} AND status IN ('pending', 'confirmed')
          AND ((requester_id = {ph} OR opponent_id = {ph}) OR (requester_id = {ph} OR opponent_id = {ph}))
          AND NOT (end_hour <= {ph} OR start_hour >= {ph})
    ''', (match_date, current_user.id, current_user.id, opponent_id, opponent_id,
          start_hour, end_hour))
    if cur.fetchone():
        conn.close()
        flash('One of you already has a booking at that time.')
        return redirect(url_for('my_group'))

    cur.execute(f'''
        INSERT INTO match_bookings (group_id, requester_id, opponent_id, match_date, start_hour, end_hour)
        VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})
    ''', (group_id, current_user.id, opponent_id, match_date, start_hour, end_hour))
    conn.commit()
    conn.close()

    flash(f'Match time proposed! Waiting for opponent to confirm.')
    return redirect(url_for('my_group'))


@app.route('/booking/<int:booking_id>/confirm', methods=['POST'])
@login_required
def booking_confirm(booking_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM match_bookings WHERE id = {ph}', (booking_id,))
    booking = cur.fetchone()
    if not booking:
        conn.close()
        flash('Booking not found.')
        return redirect(url_for('my_group'))

    booking = dict(booking)
    if booking['opponent_id'] != current_user.id:
        conn.close()
        flash('Only the invited player can confirm.')
        return redirect(url_for('my_group'))

    cur.execute(f"UPDATE match_bookings SET status = 'confirmed' WHERE id = {ph}", (booking_id,))
    conn.commit()
    conn.close()
    flash('Match time confirmed!')
    return redirect(url_for('my_group'))


@app.route('/booking/<int:booking_id>/decline', methods=['POST'])
@login_required
def booking_decline(booking_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM match_bookings WHERE id = {ph}', (booking_id,))
    booking = cur.fetchone()
    if not booking:
        conn.close()
        flash('Booking not found.')
        return redirect(url_for('my_group'))

    booking = dict(booking)
    if booking['opponent_id'] != current_user.id:
        conn.close()
        flash('Only the invited player can decline.')
        return redirect(url_for('my_group'))

    cur.execute(f"UPDATE match_bookings SET status = 'declined' WHERE id = {ph}", (booking_id,))
    conn.commit()
    conn.close()
    flash('Booking declined.')
    return redirect(url_for('my_group'))


@app.route('/booking/<int:booking_id>/cancel', methods=['POST'])
@login_required
def booking_cancel(booking_id):
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT * FROM match_bookings WHERE id = {ph}', (booking_id,))
    booking = cur.fetchone()
    if not booking:
        conn.close()
        flash('Booking not found.')
        return redirect(url_for('my_group'))

    booking = dict(booking)
    if booking['requester_id'] != current_user.id:
        conn.close()
        flash('Only the requester can cancel.')
        return redirect(url_for('my_group'))

    cur.execute(f"UPDATE match_bookings SET status = 'cancelled' WHERE id = {ph}", (booking_id,))
    conn.commit()
    conn.close()
    flash('Booking cancelled.')
    return redirect(url_for('my_group'))


# ============ ADMIN ROUTES ============

@app.route('/admin')
@login_required
@require_admin
def admin():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    # Get all users
    cur.execute('SELECT * FROM users ORDER BY username')
    all_users = [dict(r) for r in cur.fetchall()]

    # Get all ladder players with rankings
    cur.execute(f'''
        SELECT lp.*, u.username, u.email, u.phone, u.ntrp_rating, u.google_id
        FROM ladder_players lp
        JOIN users u ON lp.user_id = u.id
        WHERE lp.ladder_id = {ph}
        ORDER BY lp.ranking ASC
    ''', (ladder_id,))
    ladder_players = [dict(r) for r in cur.fetchall()]

    # Get current groups
    cur.execute(f'''
        SELECT mg.*, u1.username as p1_name, u2.username as p2_name, u3.username as p3_name
        FROM monthly_groups mg
        LEFT JOIN users u1 ON mg.player1_id = u1.id
        LEFT JOIN users u2 ON mg.player2_id = u2.id
        LEFT JOIN users u3 ON mg.player3_id = u3.id
        WHERE mg.ladder_id = {ph} AND mg.month = {ph} AND mg.year = {ph}
        ORDER BY mg.group_number ASC
    ''', (ladder_id, month, year))
    groups = [dict(r) for r in cur.fetchall()]

    # Count disputed matches
    cur.execute(f"SELECT COUNT(*) as cnt FROM matches WHERE status = 'disputed'")
    disputed_count = dict(cur.fetchone())['cnt']

    # Find which emails have successfully used a magic login link
    cur.execute("SELECT DISTINCT email FROM magic_tokens WHERE used = TRUE")
    magic_logged_in_emails = {row['email'] for row in cur.fetchall()}

    # Mark users/players who have logged in (via Google OR magic link)
    for u in all_users:
        u['has_logged_in'] = bool(u.get('google_id')) or (u.get('email') in magic_logged_in_emails)
    for lp in ladder_players:
        lp['has_logged_in'] = bool(lp.get('google_id')) or (lp.get('email') in magic_logged_in_emails)

    conn.close()
    all_ladders = get_all_ladders()
    ladder_name = get_ladder_name(ladder_id)
    return render_template('admin.html', all_users=all_users, ladder_players=ladder_players,
                           groups=groups, month=month, year=year, disputed_count=disputed_count,
                           all_ladders=all_ladders, ladder_name=ladder_name,
                           current_ladder_id=ladder_id)


@app.route('/admin/generate-login-link', methods=['POST'])
@login_required
@require_admin
def admin_generate_login_link():
    user_id = int(request.form.get('user_id', 0))
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT email, username FROM users WHERE id = {ph}', (user_id,))
    user_row = cur.fetchone()
    if not user_row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('admin'))

    user_row = dict(user_row)
    email = user_row['email']
    if not email:
        conn.close()
        flash('User has no email address.')
        return redirect(url_for('admin'))

    token = str(uuid.uuid4())
    expires_at = datetime(2099, 12, 31)  # Effectively never expires; links are single-use

    cur.execute(f'''
        INSERT INTO magic_tokens (email, token, expires_at)
        VALUES ({ph}, {ph}, {ph})
    ''', (email, token, expires_at))
    conn.commit()
    conn.close()

    link = url_for('magic_login', token=token, _external=True)
    flash(f'Login link for {user_row["username"]}: {link}')
    return redirect(url_for('admin'))


@app.route('/admin/add-to-ladder', methods=['POST'])
@login_required
@require_admin
def admin_add_to_ladder():
    user_id = int(request.form.get('user_id', 0))
    ranking = int(request.form.get('ranking', 0))
    ladder_id = get_ladder_id()

    if not user_id or not ranking:
        flash('User ID and ranking are required.')
        return redirect(url_for('admin'))

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    # Check if already on ladder
    cur.execute(f'SELECT id FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                (user_id, ladder_id))
    if cur.fetchone():
        flash('Player is already on the ladder.')
        conn.close()
        return redirect(url_for('admin'))

    # Shift rankings down for players at or below this ranking
    cur.execute(f'''
        UPDATE ladder_players SET ranking = ranking + 1
        WHERE ladder_id = {ph} AND ranking >= {ph}
    ''', (ladder_id, ranking))

    cur.execute(f'''
        INSERT INTO ladder_players (user_id, ladder_id, ranking)
        VALUES ({ph}, {ph}, {ph})
    ''', (user_id, ladder_id, ranking))
    conn.commit()

    # Send welcome email
    cur.execute(f'SELECT email, username FROM users WHERE id = {ph}', (user_id,))
    user_row = cur.fetchone()
    cur.execute(f'SELECT name FROM ladders WHERE id = {ph}', (ladder_id,))
    ladder_row = cur.fetchone()
    if user_row and ladder_row:
        user_row, ladder_row = dict(user_row), dict(ladder_row)
        if user_row.get('email'):
            ladder_name = ladder_row['name']
            send_email(
                user_row['email'],
                f"Welcome to the {ladder_name} Tennis Ladder!",
                email_wrap(f'''<p>Hi {user_row['username']},</p>
<p>You've been added to the <strong>{ladder_name} Singles Tennis Ladder</strong> at rank #{ranking}.</p>
<p>Each month you'll be placed in a group of 2-3 players. Play your matches, submit scores, and climb the ladder!</p>
<p><a href="https://rallyrung.com/my-group" style="display: inline-block; padding: 10px 20px; background: #e74c3c; color: #fff; text-decoration: none; border-radius: 4px;">View Your Group</a></p>
<p><a href="https://rallyrung.com/rules" style="color: #e74c3c;">Read the Rules</a></p>''',
                    f"{ladder_name} Singles Tennis Ladder — rallyrung.com"))

    conn.close()
    flash('Player added to ladder.')
    return redirect(url_for('admin'))


@app.route('/admin/remove-from-ladder', methods=['POST'])
@login_required
@require_admin
def admin_remove_from_ladder():
    user_id = int(request.form.get('user_id', 0))
    ladder_id = get_ladder_id()

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT ranking FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                (user_id, ladder_id))
    row = cur.fetchone()
    if row:
        old_ranking = dict(row)['ranking']
        cur.execute(f'DELETE FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                    (user_id, ladder_id))
        # Close the gap in rankings
        cur.execute(f'''
            UPDATE ladder_players SET ranking = ranking - 1
            WHERE ladder_id = {ph} AND ranking > {ph}
        ''', (ladder_id, old_ranking))
        conn.commit()
        flash('Player removed from ladder.')
    else:
        flash('Player not found on ladder.')

    conn.close()
    return redirect(url_for('admin'))


@app.route('/admin/toggle-admin', methods=['POST'])
@login_required
@require_admin
def admin_toggle_admin():
    user_id = int(request.form.get('user_id', 0))
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    if USE_POSTGRES:
        cur.execute(f'UPDATE users SET is_admin = NOT is_admin WHERE id = {ph}', (user_id,))
    else:
        cur.execute(f'UPDATE users SET is_admin = CASE WHEN is_admin = 1 THEN 0 ELSE 1 END WHERE id = {ph}',
                    (user_id,))
    conn.commit()
    conn.close()
    flash('Admin status toggled.')
    return redirect(url_for('admin'))


@app.route('/admin/generate-groups', methods=['POST'])
@login_required
@require_admin
def admin_generate_groups():
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    # Delete existing groups for this month (and their matches)
    cur.execute(f'''
        SELECT id FROM monthly_groups
        WHERE ladder_id = {ph} AND month = {ph} AND year = {ph}
    ''', (ladder_id, month, year))
    old_groups = [dict(r)['id'] for r in cur.fetchall()]
    for gid in old_groups:
        cur.execute(f'DELETE FROM matches WHERE group_id = {ph}', (gid,))
    cur.execute(f'''
        DELETE FROM monthly_groups
        WHERE ladder_id = {ph} AND month = {ph} AND year = {ph}
    ''', (ladder_id, month, year))

    # Get active players ordered by ranking
    cur.execute(f'''
        SELECT user_id, ranking FROM ladder_players
        WHERE ladder_id = {ph} AND is_active = {ph}
        ORDER BY ranking ASC
    ''', (ladder_id, True if USE_POSTGRES else 1))
    players = [dict(r) for r in cur.fetchall()]

    if len(players) < 2:
        conn.commit()
        conn.close()
        flash('Need at least 2 players to generate groups.')
        return redirect(url_for('admin'))

    # Group players into 3s from top of ladder
    groups = []
    i = 0
    while i < len(players):
        remaining = len(players) - i
        if remaining == 4:
            # Split 4 into two groups of 2
            groups.append(players[i:i+2])
            groups.append(players[i+2:i+4])
            i += 4
        elif remaining == 2:
            groups.append(players[i:i+2])
            i += 2
        else:
            groups.append(players[i:i+3])
            i += 3

    # Insert groups
    for idx, group_players in enumerate(groups):
        p1 = group_players[0]['user_id']
        p2 = group_players[1]['user_id'] if len(group_players) > 1 else None
        p3 = group_players[2]['user_id'] if len(group_players) > 2 else None
        cur.execute(f'''
            INSERT INTO monthly_groups (ladder_id, month, year, group_number, player1_id, player2_id, player3_id)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        ''', (ladder_id, month, year, idx + 1, p1, p2, p3))

    conn.commit()
    conn.close()
    flash(f'Generated {len(groups)} groups for {month}/{year}.')
    return redirect(url_for('admin'))


@app.route('/admin/monthly-reset', methods=['POST'])
@login_required
@require_admin
def admin_monthly_reset():
    """Process end-of-month: calculate standings, apply movement, archive results."""
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    # Get current month groups
    cur.execute(f'''
        SELECT * FROM monthly_groups
        WHERE ladder_id = {ph} AND month = {ph} AND year = {ph}
        ORDER BY group_number ASC
    ''', (ladder_id, month, year))
    groups = [dict(r) for r in cur.fetchall()]

    if not groups:
        conn.close()
        flash('No groups found for this month.')
        return redirect(url_for('admin'))

    # Calculate movement for each group
    movements = {}  # user_id -> movement direction

    for group in groups:
        standings = get_group_standings(group['id'])
        player_ids = [group['player1_id'], group['player2_id']]
        if group.get('player3_id'):
            player_ids.append(group['player3_id'])
        player_ids = [pid for pid in player_ids if pid]

        if len(player_ids) == 3:
            # Three-player group
            for pid in player_ids:
                s = standings.get(pid, {'wins': 0, 'losses': 0, 'games_won': 0, 'games_lost': 0})
                if s['wins'] == 2 and s['losses'] == 0:
                    movements[pid] = 'up'
                elif s['wins'] == 0 and s['losses'] == 2:
                    movements[pid] = 'down'
                elif s['wins'] == 1 and s['losses'] == 1:
                    movements[pid] = 'stay'
                else:
                    movements[pid] = 'stay'

            # Handle three-way 1-1 ties with CPSTL tiebreaker rules
            tied = [pid for pid in player_ids if movements.get(pid) == 'stay'
                    and standings.get(pid, {}).get('wins', 0) == 1]
            if len(tied) == 3:
                # All three are 1-1 — apply tiebreaker cascade:
                # 1) Head-to-head (only useful for 2-way sub-ties)
                # 2) Sets won minus sets lost
                # 3) Games won minus games lost (was the old tiebreaker)
                # 4) Highest current ranking (lower number = better)
                def tiebreak_key(pid):
                    s = standings[pid]
                    set_diff = s['sets_won'] - s['sets_lost']
                    game_diff = s['games_won'] - s['games_lost']
                    # Lower ranking number = better, so negate for sorting
                    rank_tiebreak = -rankings.get(pid, 999)
                    return (set_diff, game_diff, rank_tiebreak)

                tied.sort(key=tiebreak_key, reverse=True)
                movements[tied[0]] = 'up'    # best tiebreak moves up
                movements[tied[2]] = 'down'  # worst tiebreak moves down
                # middle stays

        elif len(player_ids) == 2:
            # Two-player group: winner goes up, loser goes down
            for pid in player_ids:
                s = standings.get(pid, {'wins': 0, 'losses': 0})
                if s['wins'] > s['losses']:
                    movements[pid] = 'up'
                elif s['losses'] > s['wins']:
                    movements[pid] = 'down'
                else:
                    movements[pid] = 'stay'

    # Get current rankings
    cur.execute(f'''
        SELECT user_id, ranking FROM ladder_players
        WHERE ladder_id = {ph}
        ORDER BY ranking ASC
    ''', (ladder_id,))
    rankings = {dict(r)['user_id']: dict(r)['ranking'] for r in cur.fetchall()}

    # Archive monthly results
    for uid, move in movements.items():
        s = {}
        for group in groups:
            gs = get_group_standings(group['id'])
            if uid in gs:
                s = gs[uid]
                break
        old_rank = rankings.get(uid, 0)
        cur.execute(f'''
            INSERT INTO monthly_results (ladder_id, user_id, month, year, old_ranking, new_ranking,
                wins, losses, games_won, games_lost, movement)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        ''', (ladder_id, uid, month, year, old_rank, old_rank,
              s.get('wins', 0), s.get('losses', 0),
              s.get('games_won', 0), s.get('games_lost', 0), move))

    # Apply movements by processing groups from top to bottom
    # Within each group, handle swaps
    for group in groups:
        player_ids = [group['player1_id'], group['player2_id']]
        if group.get('player3_id'):
            player_ids.append(group['player3_id'])
        player_ids = [pid for pid in player_ids if pid]

        # Sort by current ranking (ascending = top first)
        player_ids.sort(key=lambda pid: rankings.get(pid, 999))

        top_player = player_ids[0]
        bottom_player = player_ids[-1]

        top_rank = rankings.get(top_player, 1)
        bottom_rank = rankings.get(bottom_player, 1)

        # Player moving up swaps with the person above them (outside the group)
        if movements.get(top_player) == 'up' and top_rank > 1:
            # Swap with person at top_rank - 1
            above_rank = top_rank - 1
            cur.execute(f'''
                SELECT user_id FROM ladder_players
                WHERE ladder_id = {ph} AND ranking = {ph}
            ''', (ladder_id, above_rank))
            above_row = cur.fetchone()
            if above_row:
                above_uid = dict(above_row)['user_id']
                cur.execute(f'UPDATE ladder_players SET ranking = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                            (above_rank, top_player, ladder_id))
                cur.execute(f'UPDATE ladder_players SET ranking = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                            (top_rank, above_uid, ladder_id))
                rankings[top_player] = above_rank
                rankings[above_uid] = top_rank

        if movements.get(bottom_player) == 'down':
            cur_bottom_rank = rankings.get(bottom_player, 1)
            below_rank = cur_bottom_rank + 1
            cur.execute(f'''
                SELECT user_id FROM ladder_players
                WHERE ladder_id = {ph} AND ranking = {ph}
            ''', (ladder_id, below_rank))
            below_row = cur.fetchone()
            if below_row:
                below_uid = dict(below_row)['user_id']
                cur.execute(f'UPDATE ladder_players SET ranking = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                            (below_rank, bottom_player, ladder_id))
                cur.execute(f'UPDATE ladder_players SET ranking = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                            (cur_bottom_rank, below_uid, ladder_id))
                rankings[bottom_player] = below_rank
                rankings[below_uid] = cur_bottom_rank

    # Top 10 / Bottom 10 reseeding
    # Reload current rankings from DB after swaps
    cur.execute(f'''
        SELECT user_id, ranking FROM ladder_players
        WHERE ladder_id = {ph} AND is_active = {ph}
        ORDER BY ranking ASC
    ''', (ladder_id, True if USE_POSTGRES else 1))
    active_players = [dict(r) for r in cur.fetchall()]
    total_active = len(active_players)

    # Reseeding pattern: position -> new position (1-indexed within the 10)
    # 1→1, 2→4, 3→7, 4→2, 5→5, 6→8, 7→3, 8→6, 9→10, 10→9
    RESEED_MAP = {1: 1, 2: 4, 3: 7, 4: 2, 5: 5, 6: 8, 7: 3, 8: 6, 9: 10, 10: 9}

    def apply_reseed(player_list, offset):
        """Reseed a group of up to 10 players starting at offset rank.
        player_list: list of (user_id, current_ranking) sorted by ranking.
        offset: the base ranking (e.g., 1 for top-10, total-9 for bottom-10)."""
        count = len(player_list)
        if count < 2:
            return

        # Use negative temp rankings to avoid unique constraint conflicts
        temp_base = -10000 - offset
        for i, (uid, _) in enumerate(player_list):
            new_pos = RESEED_MAP.get(i + 1, i + 1)
            new_rank = offset + new_pos - 1
            cur.execute(f'UPDATE ladder_players SET ranking = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                        (temp_base - i, uid, ladder_id))
            rankings[uid] = new_rank

        # Now set final rankings
        for uid, new_rank in [(uid, rankings[uid]) for uid, _ in player_list]:
            cur.execute(f'UPDATE ladder_players SET ranking = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                        (new_rank, uid, ladder_id))

    if total_active >= 10:
        # Apply top-10 reseeding
        top_10 = [(p['user_id'], p['ranking']) for p in active_players[:10]]
        apply_reseed(top_10, 1)

    if total_active >= 20:
        # Apply bottom-10 reseeding
        bottom_10 = [(p['user_id'], p['ranking']) for p in active_players[-10:]]
        bottom_offset = active_players[-10]['ranking']
        apply_reseed(bottom_10, bottom_offset)

    # Auto-drop: check inactivity for all active players
    cur.execute(f'''
        SELECT lp.user_id, lp.ranking, lp.inactive_months FROM ladder_players lp
        WHERE lp.ladder_id = {ph} AND lp.is_active = {ph}
        ORDER BY lp.ranking ASC
    ''', (ladder_id, True if USE_POSTGRES else 1))
    all_active = [dict(r) for r in cur.fetchall()]

    # Get all players who participated in matches this month
    cur.execute(f'''
        SELECT DISTINCT m.player1_id as pid FROM matches m
        JOIN monthly_groups mg ON m.group_id = mg.id
        WHERE mg.ladder_id = {ph} AND mg.month = {ph} AND mg.year = {ph}
          AND m.status = 'confirmed'
        UNION
        SELECT DISTINCT m.player2_id as pid FROM matches m
        JOIN monthly_groups mg ON m.group_id = mg.id
        WHERE mg.ladder_id = {ph} AND mg.month = {ph} AND mg.year = {ph}
          AND m.status = 'confirmed'
    ''', (ladder_id, month, year, ladder_id, month, year))
    active_this_month = {dict(r)['pid'] for r in cur.fetchall()}

    # Get ladder name for emails
    cur.execute(f'SELECT name FROM ladders WHERE id = {ph}', (ladder_id,))
    ladder_name = dict(cur.fetchone())['name']

    dropped_count = 0
    for player in all_active:
        uid = player['user_id']
        if uid in active_this_month:
            # Reset inactivity counter
            cur.execute(f'UPDATE ladder_players SET inactive_months = 0 WHERE user_id = {ph} AND ladder_id = {ph}',
                        (uid, ladder_id))
        else:
            new_inactive = player['inactive_months'] + 1
            if new_inactive >= 2:
                # Deactivate player and compact rankings
                drop_rank = rankings.get(uid, player['ranking'])
                cur.execute(f'''
                    UPDATE ladder_players SET is_active = {ph}, inactive_months = {ph}
                    WHERE user_id = {ph} AND ladder_id = {ph}
                ''', (False if USE_POSTGRES else 0, new_inactive, uid, ladder_id))
                # Shift everyone below up by 1
                cur.execute(f'''
                    UPDATE ladder_players SET ranking = ranking - 1
                    WHERE ladder_id = {ph} AND ranking > {ph} AND is_active = {ph}
                ''', (ladder_id, drop_rank, True if USE_POSTGRES else 1))
                # Update in-memory rankings
                for k, v in rankings.items():
                    if v > drop_rank:
                        rankings[k] = v - 1
                dropped_count += 1
                # Send dropped email
                cur.execute(f'SELECT email, username FROM users WHERE id = {ph}', (uid,))
                dropped_user = cur.fetchone()
                if dropped_user:
                    dropped_user = dict(dropped_user)
                    if dropped_user.get('email'):
                        send_email(
                            dropped_user['email'],
                            f"You've been removed from the {ladder_name} Tennis Ladder",
                            email_wrap(f'''<p>Hi {dropped_user['username']},</p>
<p>You have been removed from the <strong>{ladder_name} Singles Tennis Ladder</strong> for not playing any matches for 2 consecutive months.</p>
<p>You can rejoin at any time — just contact the ladder admin.</p>''',
                                f"{ladder_name} Singles Tennis Ladder — rallyrung.com"))
            else:
                cur.execute(f'UPDATE ladder_players SET inactive_months = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                            (new_inactive, uid, ladder_id))
                # Send drop warning email (1 month inactive)
                if new_inactive == 1:
                    cur.execute(f'SELECT email, username FROM users WHERE id = {ph}', (uid,))
                    warn_user = cur.fetchone()
                    if warn_user:
                        warn_user = dict(warn_user)
                        if warn_user.get('email'):
                            send_email(
                                warn_user['email'],
                                f"Inactivity warning — {ladder_name} Tennis Ladder",
                                email_wrap(f'''<p>Hi {warn_user['username']},</p>
<p>You did not play any matches this month on the <strong>{ladder_name} Singles Tennis Ladder</strong>.</p>
<p>If you do not play next month, you will be automatically removed from the ladder.</p>
<p><a href="https://rallyrung.com/my-group" style="display: inline-block; padding: 10px 20px; background: #e74c3c; color: #fff; text-decoration: none; border-radius: 4px;">View Your Group</a></p>''',
                                    f"{ladder_name} Singles Tennis Ladder — rallyrung.com"))

    # Update archived new_ranking
    for uid in movements:
        new_rank = rankings.get(uid, 0)
        cur.execute(f'''
            UPDATE monthly_results SET new_ranking = {ph}
            WHERE user_id = {ph} AND ladder_id = {ph} AND month = {ph} AND year = {ph}
        ''', (new_rank, uid, ladder_id, month, year))

    conn.commit()
    conn.close()
    drop_msg = f' {dropped_count} player(s) auto-dropped for inactivity.' if dropped_count else ''
    flash(f'Monthly reset complete for {month}/{year}. Rankings updated.{drop_msg}')
    return redirect(url_for('admin'))


@app.route('/admin/import-csv', methods=['POST'])
@login_required
@require_admin
def admin_import_csv():
    """Import players from CSV: name,email,phone,ntrp,ranking"""
    if 'csv_file' not in request.files:
        flash('No file uploaded.')
        return redirect(url_for('admin'))

    file = request.files['csv_file']
    if not file.filename.endswith('.csv'):
        flash('File must be a CSV.')
        return redirect(url_for('admin'))

    ladder_id = get_ladder_id()
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    content = file.stream.read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(content))

    added = 0
    skipped = 0
    for row in reader:
        # Support both lowercase and title-case column headers
        name = (row.get('name') or row.get('Name') or '').strip()
        email = (row.get('email') or row.get('Email') or '').strip()
        phone = (row.get('phone') or row.get('Phone') or '').strip()
        ntrp = (row.get('ntrp') or row.get('NTRP') or '').strip()
        ranking = (row.get('ranking') or row.get('Rank') or '').strip()
        gender = (row.get('gender') or row.get('Gender') or '').strip()

        if not name or not email:
            skipped += 1
            continue

        # Check if user already exists
        cur.execute(f'SELECT id FROM users WHERE email = {ph}', (email,))
        existing = cur.fetchone()

        if existing:
            user_id = dict(existing)['id']
            cur.execute(f'UPDATE users SET phone = {ph}, ntrp_rating = {ph}, gender = {ph} WHERE id = {ph}',
                        (phone, ntrp, gender or None, user_id))
        else:
            cur.execute(f'''
                INSERT INTO users (username, email, phone, ntrp_rating, gender)
                VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
            ''', (name, email, phone, ntrp, gender or None))
            conn.commit()
            cur.execute(f'SELECT id FROM users WHERE email = {ph}', (email,))
            user_id = dict(cur.fetchone())['id']

        # Add to ladder if ranking provided and not already on ladder
        if ranking:
            cur.execute(f'SELECT id FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                        (user_id, ladder_id))
            if not cur.fetchone():
                cur.execute(f'''
                    INSERT INTO ladder_players (user_id, ladder_id, ranking)
                    VALUES ({ph}, {ph}, {ph})
                ''', (user_id, ladder_id, int(ranking)))
                added += 1
            else:
                skipped += 1
        else:
            added += 1

    conn.commit()
    conn.close()
    flash(f'CSV import complete: {added} added, {skipped} skipped.')
    return redirect(url_for('admin'))


@app.route('/admin/update-ranking', methods=['POST'])
@login_required
@require_admin
def admin_update_ranking():
    user_id = int(request.form.get('user_id', 0))
    new_ranking = int(request.form.get('new_ranking', 0))
    ladder_id = get_ladder_id()

    if not user_id or not new_ranking:
        flash('User and ranking required.')
        return redirect(url_for('admin'))

    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()

    cur.execute(f'SELECT ranking FROM ladder_players WHERE user_id = {ph} AND ladder_id = {ph}',
                (user_id, ladder_id))
    row = cur.fetchone()
    if not row:
        conn.close()
        flash('Player not on ladder.')
        return redirect(url_for('admin'))

    old_ranking = dict(row)['ranking']

    if new_ranking == old_ranking:
        conn.close()
        return redirect(url_for('admin'))

    if new_ranking < old_ranking:
        # Moving up: shift others down
        cur.execute(f'''
            UPDATE ladder_players SET ranking = ranking + 1
            WHERE ladder_id = {ph} AND ranking >= {ph} AND ranking < {ph}
        ''', (ladder_id, new_ranking, old_ranking))
    else:
        # Moving down: shift others up
        cur.execute(f'''
            UPDATE ladder_players SET ranking = ranking - 1
            WHERE ladder_id = {ph} AND ranking > {ph} AND ranking <= {ph}
        ''', (ladder_id, old_ranking, new_ranking))

    cur.execute(f'UPDATE ladder_players SET ranking = {ph} WHERE user_id = {ph} AND ladder_id = {ph}',
                (new_ranking, user_id, ladder_id))
    conn.commit()
    conn.close()
    flash('Ranking updated.')
    return redirect(url_for('admin'))


# ============ INIT & RUN ============

with app.app_context():
    init_db()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
