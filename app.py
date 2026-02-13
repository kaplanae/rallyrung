from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from authlib.integrations.flask_client import OAuth
from werkzeug.middleware.proxy_fix import ProxyFix
import sqlite3
import json
import csv
import io
from datetime import datetime, date, timedelta
from collections import defaultdict
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

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
            submitted_by INTEGER REFERENCES users(id),
            confirmed_by INTEGER REFERENCES users(id),
            status TEXT DEFAULT 'pending',
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
            submitted_by INTEGER REFERENCES users(id),
            confirmed_by INTEGER REFERENCES users(id),
            status TEXT DEFAULT 'pending',
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

    # Seed default ladder
    cur.execute('SELECT id FROM ladders LIMIT 1')
    if not cur.fetchone():
        cur.execute("INSERT INTO ladders (name, sport) VALUES ('RallyRung Tennis Ladder', 'tennis')")

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
    """Get the default ladder id."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT id FROM ladders LIMIT 1')
    row = cur.fetchone()
    conn.close()
    return dict(row)['id'] if row else None


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


def calculate_match_games(match):
    """Calculate total games won by each player in a match."""
    m = dict(match)
    p1_games = 0
    p2_games = 0
    for s in range(1, 4):
        s1 = m.get(f'set{s}_p1')
        s2 = m.get(f'set{s}_p2')
        if s1 is not None and s2 is not None:
            p1_games += s1
            p2_games += s2
    return p1_games, p2_games


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
            stats[pid] = {'wins': 0, 'losses': 0, 'games_won': 0, 'games_lost': 0}

    for m in matches:
        p1 = m['player1_id']
        p2 = m['player2_id']
        winner = m['winner_id']
        p1_games, p2_games = calculate_match_games(m)

        if p1 in stats:
            stats[p1]['games_won'] += p1_games
            stats[p1]['games_lost'] += p2_games
        if p2 in stats:
            stats[p2]['games_won'] += p2_games
            stats[p2]['games_lost'] += p1_games

        if winner == p1:
            if p1 in stats: stats[p1]['wins'] += 1
            if p2 in stats: stats[p2]['losses'] += 1
        elif winner == p2:
            if p2 in stats: stats[p2]['wins'] += 1
            if p1 in stats: stats[p1]['losses'] += 1

    return stats


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
            return redirect(url_for('ladder'))
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
    return redirect(url_for('ladder'))


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))


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


@app.route('/ladder')
def ladder():
    conn = get_db()
    cur = conn.cursor()
    ph = get_placeholder()
    ladder_id = get_ladder_id()
    month, year = get_current_month_year()

    # Get all active players with their rankings
    cur.execute('''
        SELECT u.id, u.username, u.profile_picture, u.ntrp_rating,
               lp.ranking, lp.is_active
        FROM ladder_players lp
        JOIN users u ON lp.user_id = u.id
        WHERE lp.ladder_id = %s AND lp.is_active = %s
        ORDER BY lp.ranking ASC
    '''.replace('%s', ph), (ladder_id, True if USE_POSTGRES else 1))
    players = [dict(r) for r in cur.fetchall()]

    # Get current month groups
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

    conn.close()
    return render_template('ladder.html', players=players, groups=groups, month=month, year=year)


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

    conn.close()
    return render_template('my_group.html', group=group, group_players=group_players,
                           matches=matches, standings=standings)


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
        winner_id = int(request.form.get('winner_id', 0))

        if opponent_id not in opponent_ids:
            flash('Invalid opponent.')
            conn.close()
            return redirect(url_for('submit_result'))

        if winner_id not in (current_user.id, opponent_id):
            flash('Invalid winner.')
            conn.close()
            return redirect(url_for('submit_result'))

        # Parse set scores
        sets = []
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
                if not validate_set_score(p1_val, p2_val):
                    flash(f'Set {s} score {p1_val}-{p2_val} is not a valid tennis score.')
                    conn.close()
                    return redirect(url_for('submit_result'))
                sets.append((p1_val, p2_val))
            else:
                sets.append((None, None))

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

        cur.execute(f'''
            INSERT INTO matches (group_id, player1_id, player2_id, winner_id,
                set1_p1, set1_p2, set2_p1, set2_p2, set3_p1, set3_p2,
                submitted_by, status)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'pending')
        ''', (group['id'], p1_id, p2_id, winner_id,
              s1_p1, s1_p2, s2_p1, s2_p2, s3_p1, s3_p2, current_user.id))
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
    return render_template('profile.html', ladder_player=ladder_player,
                           match_history=match_history, ranking_history=ranking_history,
                           total_wins=total_wins, total_losses=total_losses)


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
        SELECT lp.*, u.username, u.email, u.phone, u.ntrp_rating
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

    conn.close()
    return render_template('admin.html', all_users=all_users, ladder_players=ladder_players,
                           groups=groups, month=month, year=year, disputed_count=disputed_count)


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
    expires_at = datetime.utcnow() + timedelta(hours=24)

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

            # Handle three-way 1-1 ties: tiebreak by games won
            tied = [pid for pid in player_ids if movements.get(pid) == 'stay'
                    and standings.get(pid, {}).get('wins', 0) == 1]
            if len(tied) == 3:
                # All three are 1-1 — tiebreak by total games won
                tied.sort(key=lambda pid: standings[pid]['games_won'], reverse=True)
                movements[tied[0]] = 'up'    # most games won moves up
                movements[tied[2]] = 'down'  # fewest games won moves down
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

    # Update archived new_ranking
    for uid in movements:
        new_rank = rankings.get(uid, 0)
        cur.execute(f'''
            UPDATE monthly_results SET new_ranking = {ph}
            WHERE user_id = {ph} AND ladder_id = {ph} AND month = {ph} AND year = {ph}
        ''', (new_rank, uid, ladder_id, month, year))

    conn.commit()
    conn.close()
    flash(f'Monthly reset complete for {month}/{year}. Rankings updated.')
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
