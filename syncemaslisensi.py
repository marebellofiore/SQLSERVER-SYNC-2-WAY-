import pyodbc
import time
from threading import Thread
import os
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import json # Tambahkan untuk serialisasi parameter
import requests
import uuid
import platform
import socket
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import messagebox

class Logger:
    def log(self, message):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

logger = Logger()

def get_machine_id():
    """Generate a unique machine ID based on hardware and network"""
    try:
        # Combine multiple identifiers for uniqueness
        node = platform.node()
        mac = uuid.getnode()
        serial = f"{node}-{mac}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, serial))
    except:
        return "UNKNOWN_MACHINE"
def send_license_email(machine_id):
    try:
        subject = f"SOFTWARESQLSYNC: {os.getenv('COMPUTERNAME', 'UNKNOWN_PC')}"
        body = f"Kode Aktivasi WhatsApp Gateway:\n\n{machine_id}\n\nHubungi xpert emon 082115113221 untuk aktivasi."
        files = {
            'address': (None, 'pratiftadewa@gmail.com'),
            'subject': (None, subject),
            'body': (None, body),
        }
        response = requests.post("https://email1.xperthotelsystem.com//send_email", files=files)
        logger.log("📧 Kode lisensi terkirim ke email!" if response.ok else f"❌ Gagal kirim email lisensi: {response.text}")
    except Exception as e:
        logger.log(f"❌ Gagal kirim email lisensi: {e}")

def check_license():
    machine_id = get_machine_id()
    try:
        response = requests.get("https://raw.githubusercontent.com/marebellofiore/serial/refs/heads/main/app.txt")
        if response.ok and machine_id in response.text:
            logger.log("✅ Lisensi valid. Melanjutkan program...")
            return True, machine_id
        else:
            send_license_email(machine_id)
            messagebox.showerror("Lisensi Tidak Valid", f"⚠️ Lisensi belum terdaftar.\nHubungi xpert emon 082115113221\n\nKode: {machine_id}")
            return False, machine_id
    except Exception as e:
        logger.log(f"❌ Gagal cek lisensi: {e}")
        return False, machine_id

def monitor_license_and_pause():
    """Monitor lisensi dan hentikan sinkronisasi jika melewati masa demo"""
    global sync_active
    sync_active = True

    while sync_active:
        if LICENSE_EXPIRY and datetime.now() >= LICENSE_EXPIRY:
            logger.log("⏸️ Masa demo telah berakhir. Sinkronisasi dihentikan.")
            messagebox.showwarning("Demo Berakhir", "Waktu demo 1 jam telah berakhir. Hubungi xpert emon 082115113221 untuk aktivasi lisensi.")
            sync_active = False
            # Di sini Anda bisa tambahkan logika untuk matikan semua thread sync
            os._exit(0)  # Keluar paksa (opsional, atau gunakan flag lebih halus)
            break
        time.sleep(10)  # Cek tiap 10 detik

# --- PERUBAHAN: Muat nilai limit dari file ---
LIMIT_FILE = "limit.txt"
try:
    with open(LIMIT_FILE, 'r') as f:
        LIMIT_VALUE = int(f.read().strip())
    print(f"✅ Loaded limit value: {LIMIT_VALUE} from {LIMIT_FILE}")
except Exception as e:
    print(f"⚠️  Error loading limit from {LIMIT_FILE} or invalid value. Defaulting to 1000. Error: {e}")
    LIMIT_VALUE = 1000
# --- AKHIR PERUBAHAN: Muat nilai limit dari file ---

# Load configuration from files
with open('odbc.txt', 'r') as f:
    servers = f.read().splitlines()

# Database configurations
CONFIGS = [
    {
        'server': servers[0],
        'database': servers[1],
        'username': servers[2],
        'password': servers[3]
    },
    {
        'server': servers[4],
        'database': servers[5],
        'username': servers[6],
        'password': servers[7]
    }
]

# Tracker database configuration
with open('odbctracker.txt', 'r') as f:
    tracker_config_lines = [line.strip() for line in f if line.strip()]

TRACKER_CONFIG = {
    'server': tracker_config_lines[0],
    'database': tracker_config_lines[1],
    'username': tracker_config_lines[2],
    'password': tracker_config_lines[3]
}

# Table configuration
TABLE_CONFIG_FILE = "table.txt"
SYNC_TRACKING_TABLE = "synced_records"
PENDING_QUERIES_TABLE = "pending_queries" # <-- TAMBAHAN: Konstanta untuk tabel pending
MAX_RETRY_ATTEMPTS = 5 # <-- TAMBAHAN: Batas maksimal retry
RETRY_INTERVAL_SECONDS = 60 # <-- TAMBAHAN: Interval retry (detik)

def load_table_configs(config_file=TABLE_CONFIG_FILE):
    """Load table configurations from file, parsing primary key types."""
    configs = []
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(':')
                if len(parts) >= 3:
                    table_name = parts[0]
                    pk_part = parts[1]
                    columns_part = parts[2]
                    # Parse primary keys to include identity status
                    parsed_primary_keys = []
                    pk_elements = pk_part.split('+')
                    for pk_elem in pk_elements:
                        if '=' in pk_elem:
                            col_name, pk_type = pk_elem.split('=')
                            is_identity = (pk_type.lower() == 'increment')
                            parsed_primary_keys.append({'name': col_name, 'is_identity': is_identity})
                        else:
                            parsed_primary_keys.append({'name': pk_elem, 'is_identity': False}) # Default to False if not specified
                    columns = columns_part.split(',')
                    configs.append({
                        'table_name': table_name,
                        'primary_keys': parsed_primary_keys, # Now a list of dicts
                        'columns': columns,
                        'force_id_sync': 'force_id' in parts[3:] if len(parts) > 3 else False
                    })
        print(f"✅ Loaded {len(configs)} table configurations from {config_file}")
        return configs
    except Exception as e:
        print(f"❌ Error loading table configurations: {e}")
        return []

class SQLServerSyncTracker:
    def __init__(self, db_config, tracking_table=SYNC_TRACKING_TABLE):
        self.db_config = db_config
        self.tracking_table = tracking_table
        # init_db dipanggil di __main__ setelah objek dibuat
        # self.init_db()

    def _get_connection(self):
        """Create new connection to tracking database"""
        return pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']}"
        )

    def init_db(self):
        """Initialize tracking table in SQL Server"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # --- PERUBAHAN: Ubah tipe data id_server1 dan id_server2 menjadi NVARCHAR(255) ---
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.tracking_table}')
                BEGIN
                    CREATE TABLE {self.tracking_table} (
                        id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        table_name NVARCHAR(128) NOT NULL,
                        -- Ganti BIGINT menjadi NVARCHAR(255) untuk mendukung ID string atau kombinasi ID
                        id_server1 NVARCHAR(255) NOT NULL,
                        id_server2 NVARCHAR(255) NOT NULL,
                        sync_time DATETIME2 DEFAULT GETDATE()
                    )
                END
            """)
            # --- AKHIR PERUBAHAN ---
            cursor.execute(f"""
                IF NOT EXISTS (SELECT name FROM sysindexes WHERE name = 'idx_table_id_server1')
                BEGIN
                    CREATE INDEX idx_table_id_server1 ON {self.tracking_table} (table_name, id_server1)
                END
            """)
            cursor.execute(f"""
                IF NOT EXISTS (SELECT name FROM sysindexes WHERE name = 'idx_table_id_server2')
                BEGIN
                    CREATE INDEX idx_table_id_server2 ON {self.tracking_table} (table_name, id_server2)
                END
            """)

            # --- TAMBAHAN: Buat tabel pending_queries ---
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{PENDING_QUERIES_TABLE}')
                BEGIN
                    CREATE TABLE {PENDING_QUERIES_TABLE} (
                      [id] bigint IDENTITY(1,1) NOT NULL,
                      [table_name] nvarchar(128) NOT NULL,
                      [to_server] nvarchar(255) NOT NULL, -- Identifier server tujuan (misal: server:database)
                      [query_text] nvarchar(max) NOT NULL,
                      [query_params] nvarchar(max) NULL, -- JSON string dari parameter
                      [operation_type] char(1) NOT NULL, -- 'I', 'U', 'D'
                      [source_pk] nvarchar(255) NULL,
                      [target_pk] nvarchar(255) NULL, -- ID di server tujuan (jika sudah ada)
                      [error_message] nvarchar(max) NULL,
                      [created_time] datetime2(7) DEFAULT GETDATE() NULL,
                      [retry_count] int DEFAULT 0 NULL,
                      [status] nvarchar(50) DEFAULT 'PENDING' NULL,
                      CONSTRAINT [PK_{PENDING_QUERIES_TABLE}] PRIMARY KEY CLUSTERED ([id])
                    )
                END
            """)
            # Buat index untuk mempercepat pencarian berdasarkan status dan tabel
            cursor.execute(f"""
                IF NOT EXISTS (SELECT name FROM sysindexes WHERE name = 'idx_pending_status_table')
                BEGIN
                    CREATE INDEX idx_pending_status_table ON {PENDING_QUERIES_TABLE} ([status], [table_name])
                END
            """)
            # --- AKHIR TAMBAHAN ---

            conn.commit()
        except Exception as e:
            print(f"❌ Error initializing tracking database: {e}")
        finally:
            if conn:
                conn.close()

    def add_mapping(self, table_name, id_server1, id_server2):
        """Add ID mapping between servers for specific table"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                IF EXISTS (SELECT 1 FROM {self.tracking_table}
                           WHERE table_name = ? AND id_server1 = ? AND id_server2 = ?)
                BEGIN
                    UPDATE {self.tracking_table}
                    SET sync_time = GETDATE()
                    WHERE table_name = ? AND id_server1 = ? AND id_server2 = ?
                END
                ELSE
                BEGIN
                    INSERT INTO {self.tracking_table} (table_name, id_server1, id_server2)
                    VALUES (?, ?, ?)
                END
            """, (table_name, str(id_server1), str(id_server2), # Konversi ke string untuk konsistensi
                  table_name, str(id_server1), str(id_server2),
                  table_name, str(id_server1), str(id_server2)))
            conn.commit()
        except Exception as e:
            print(f"❌ Error adding mapping for {table_name}, ID1={id_server1}, ID2={id_server2}: {e}")
        finally:
            if conn:
                conn.close()

    def get_mapped_id(self, table_name, source_id, is_from_server1):
        """Get mapped ID based on source ID and direction"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            if is_from_server1:
                cursor.execute(f'SELECT id_server2 FROM {self.tracking_table} WHERE table_name = ? AND id_server1 = ?',
                               (table_name, str(source_id),)) # Konversi ke string
            else:
                cursor.execute(f'SELECT id_server1 FROM {self.tracking_table} WHERE table_name = ? AND id_server2 = ?',
                               (table_name, str(source_id),)) # Konversi ke string
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            # print(f"❌ Error getting mapped ID: {e}") # Too verbose, only print if truly an issue
            return None
        finally:
            if conn:
                conn.close()

    def get_all_mapped_ids(self, table_name, is_from_server1):
        """Get all mapped IDs for a given table and direction."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            if is_from_server1: # Want IDs from server1 that are already mapped
                cursor.execute(f'SELECT id_server1 FROM {self.tracking_table} WHERE table_name = ?', (table_name,))
            else: # Want IDs from server2 that are already mapped
                cursor.execute(f'SELECT id_server2 FROM {self.tracking_table} WHERE table_name = ?', (table_name,))
            return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            print(f"❌ Error getting all mapped IDs for {table_name}: {e}")
            return set()
        finally:
            if conn:
                conn.close()

    def remove_mapping(self, table_name, source_id, is_from_server1):
        """Remove mapping based on source ID and direction"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            if is_from_server1:
                cursor.execute(f'DELETE FROM {self.tracking_table} WHERE table_name = ? AND id_server1 = ?',
                               (table_name, str(source_id),)) # Konversi ke string
            else:
                cursor.execute(f'DELETE FROM {self.tracking_table} WHERE table_name = ? AND id_server2 = ?',
                               (table_name, str(source_id),)) # Konversi ke string
            conn.commit()
        except Exception as e:
            print(f"❌ Error removing mapping: {e}")
        finally:
            if conn:
                conn.close()

    # --- TAMBAHAN: Fungsi untuk menyimpan query yang gagal ---
    def save_pending_query(self, table_name, to_server, query_text, query_params, operation_type, source_pk, target_pk, error_message):
        """
        Menyimpan query yang gagal ke tabel pending_queries.
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Serialisasi parameter menjadi JSON string
            try:
                params_str = json.dumps(query_params, default=str) if query_params else None
            except Exception:
                params_str = str(query_params) # Fallback jika JSON gagal

            cursor.execute(f"""
                INSERT INTO {PENDING_QUERIES_TABLE}
                ([table_name], [to_server], [query_text], [query_params], [operation_type],
                 [source_pk], [target_pk], [error_message])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (table_name, to_server, query_text, params_str, operation_type,
                  str(source_pk) if source_pk is not None else None,
                  str(target_pk) if target_pk is not None else None,
                  str(error_message)[:4000] if error_message else None)) # Batasi panjang error message
            conn.commit()
            print(f"    -> ⏳ Query for {table_name} (Op: {operation_type}, Source PK: {source_pk}) saved to pending queue.")
        except Exception as e:
            print(f"❌ Error saving pending query for {table_name}: {e}")
        finally:
            if conn:
                conn.close()

    # --- AKHIR TAMBAHAN ---

    def get_connection_string(self, db_config):
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']}"
        )

    def initialize_mapping(self, table_config):
        """Initialize ID mapping for existing records, prioritizing the server with more data."""
        table_name = table_config['table_name']
        primary_key_info = table_config['primary_keys'][0] # Assuming single primary key for simplicity in initial sync
        primary_key_col = primary_key_info['name']
        is_pk_identity = primary_key_info['is_identity']
        columns = table_config['columns']
        force_id_sync = table_config['force_id_sync']
        limit_value = LIMIT_VALUE # Gunakan nilai limit yang telah dimuat
        print(f"🔍 Initializing mapping for table {table_name} (Limit: {limit_value})...")
        # --- PERUBAHAN: Logika awal baru ---
        conn_a = None
        conn_b = None
        try:
            conn_a = pyodbc.connect(self.get_connection_string(CONFIGS[0]))
            conn_b = pyodbc.connect(self.get_connection_string(CONFIGS[1]))
            cursor_a = conn_a.cursor()
            cursor_b = conn_b.cursor()
            # --- PERUBAHAN: Ambil TOP N ID terakhir dari masing-masing server ---
            # Get TOP N IDs from Server A (ordered by PK DESC)
            cursor_a.execute(f"SELECT TOP ({limit_value}) {primary_key_col} FROM {table_name} ORDER BY {primary_key_col} DESC")
            pks_a_limited = {str(row[0]) for row in cursor_a.fetchall()}
            print(f"    -> Server A ({CONFIGS[0]['server']}/{CONFIGS[0]['database']}): Retrieved {len(pks_a_limited)} latest PKs.")
            # Get TOP N IDs from Server B (ordered by PK DESC)
            cursor_b.execute(f"SELECT TOP ({limit_value}) {primary_key_col} FROM {table_name} ORDER BY {primary_key_col} DESC")
            pks_b_limited = {str(row[0]) for row in cursor_b.fetchall()}
            print(f"    -> Server B ({CONFIGS[1]['server']}/{CONFIGS[1]['database']}): Retrieved {len(pks_b_limited)} latest PKs.")
            # Hitung statistik berdasarkan ID terbatas
            common_pks_limited = pks_a_limited & pks_b_limited # Ada di keduanya dalam set terbatas
            print(f"    -> Common PKs in top {limit_value} from both servers: {len(common_pks_limited)}")
            mappings_added = 0
            # --- PERUBAHAN: Selalu buat mapping 1:1 untuk common_pks_limited ---
            # Mapping aja langsung untuk ID yang sama dalam 1000 terakhir
            if common_pks_limited:
                print(f"    -> Adding {len(common_pks_limited)} identity mappings for common PKs in top {limit_value}...")
                for common_pk in common_pks_limited:
                    # Mapping 1:1 karena ID-nya sama di kedua server
                    self.add_mapping(table_name, common_pk, common_pk)
                    mappings_added += 1
                print(f"    -> Added {mappings_added} identity mappings for common PKs in top {limit_value}.")
            else:
                 print(f"    -> No common PKs found in the top {limit_value} IDs of both servers. No identity mappings added.")
            # --- AKHIR PERUBAHAN: Selalu buat mapping 1:1 untuk common_pks_limited ---
            # --- PERUBAHAN: Hilangkan logika sinkronisasi awal penuh ---
            # Bagian sinkronisasi penuh tabel (INSERT/DELETE) dihapus karena permintaan "mapping aja langsung"
            # --- AKHIR PERUBAHAN: Hilangkan logika sinkronisasi awal penuh ---
            print(f"✅ Initial mapping for {table_name} complete (based on top {limit_value} PKs).")
            print(f"    - Mappings Added/Updated: {mappings_added}")
            return True
        except Exception as e:
            print(f"❌ Error during initial mapping for {table_name}: {e}")
            return False
        finally:
            if conn_a:
                try:
                    conn_a.close()
                except:
                    pass
            if conn_b:
                try:
                    conn_b.close()
                except:
                    pass
        # --- AKHIR PERUBAHAN: Logika awal baru ---

def table_exists(cursor, table_name):
    """Check if table exists"""
    cursor.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?
    """, table_name)
    return cursor.fetchone()[0] > 0

def enable_change_tracking(server, database, username, password, table_name):
    """Enable Change Tracking for specific table"""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}",
            autocommit=True
        )
        cursor = conn.cursor()
        if not table_exists(cursor, table_name):
            print(f"⚠️  Table '{table_name}' not found in {server}/{database}")
            cursor.close()
            conn.close()
            return False
        cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
        BEGIN
            ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON
            (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
        END
        """)
        cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.change_tracking_tables
            WHERE object_id = OBJECT_ID(?)
        )
        BEGIN
            DECLARE @sql NVARCHAR(MAX)
            SET @sql = 'ALTER TABLE ' + QUOTENAME(?) + ' ENABLE CHANGE_TRACKING'
            EXEC sp_executesql @sql
        END
        """, table_name, table_name)
        print(f"✅ Change Tracking enabled in {server}/{database} for {table_name}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Failed to enable Change Tracking in {server} for {table_name}: {str(e)}")
        return False

# --- PERUBAHAN: Fungsi untuk membandingkan data ---
def data_has_changed(target_cursor, table_name, primary_key_col, target_id, current_row_data, columns_to_check):
    """
    Memeriksa apakah data dalam baris target berbeda dari data yang baru.
    Mengembalikan True jika ada perbedaan, False jika tidak.
    """
    try:
        # Bangun daftar kolom untuk SELECT dan parameter untuk WHERE
        cols_str = ", ".join([f"[{col}]" for col in columns_to_check])
        query = f"SELECT {cols_str} FROM [{table_name}] WHERE [{primary_key_col}] = ?"
        target_cursor.execute(query, (target_id,))
        target_row = target_cursor.fetchone()
        if not target_row:
            # Jika baris tidak ditemukan, anggap saja data berubah (walaupun ini seharusnya tidak terjadi)
            return True
        # Bandingkan data kolom per kolom
        for i, col_name in enumerate(columns_to_check):
            new_value = current_row_data.get(col_name)
            target_value = target_row[i]
            # Penanganan khusus untuk None/NULL
            if new_value is None and target_value is None:
                continue # Sama-sama NULL
            elif new_value is None or target_value is None:
                return True # Salah satu NULL, yang lain tidak
            elif new_value != target_value:
                # Untuk tipe data yang bisa dibandingkan secara langsung
                return True # Ada perbedaan
        # Jika semua kolom sama
        return False
    except Exception as e:
        print(f"⚠️  Error comparing data for {table_name} ID {target_id}: {e}")
        # Jika terjadi error, anggap saja data berubah untuk keamanan
        return True
# --- AKHIR PERUBAHAN: Fungsi untuk membandingkan data ---

# --- TAMBAHAN: Fungsi untuk memproses pending queries ---
def process_pending_queries(tracker_instance, all_server_configs):
    """
    Fungsi yang berjalan dalam thread terpisah untuk memproses ulang query pending.
    """
    print("🔄 Starting Pending Queries Processor thread...")
    # Buat dictionary untuk pencarian konfigurasi server dengan mudah
    # Format: "server:database" -> config_dict
    server_lookup = {}
    for config in all_server_configs:
        key = f"{config['server']}:{config['database']}"
        server_lookup[key] = config

    while True:
        try:
            print("🔍 Checking for pending queries to retry...")
            conn_tracker = None
            conn_target = None
            cursor_tracker = None
            cursor_target = None
            processed_in_batch = 0

            conn_tracker = tracker_instance._get_connection()
            cursor_tracker = conn_tracker.cursor()

            # Ambil query pending, urutkan berdasarkan created_time untuk FIFO
            # Batasi jumlah untuk mencegah beban berlebihan
            cursor_tracker.execute(f"""
                SELECT TOP 10 id, table_name, to_server, query_text, query_params,
                               operation_type, source_pk, target_pk, error_message, retry_count
                FROM {PENDING_QUERIES_TABLE}
                WHERE status = 'PENDING' AND retry_count < ?
                ORDER BY created_time ASC
            """, (MAX_RETRY_ATTEMPTS,))

            pending_queries = cursor_tracker.fetchall()

            if not pending_queries:
                print("    -> No pending queries to process at this time.")
            else:
                print(f"    -> Found {len(pending_queries)} pending queries to process.")

            for query_row in pending_queries:
                pending_id, table_name, to_server, query_text, query_params_json, \
                operation_type, source_pk, target_pk, last_error, retry_count = query_row

                target_config = server_lookup.get(to_server)
                if not target_config:
                    error_msg = f"Unknown target server/database: {to_server}"
                    print(f"⚠️  [{pending_id}] {table_name}: {error_msg}")
                    # Update status ke FAILED karena konfigurasi tidak ditemukan
                    cursor_tracker.execute(f"""
                        UPDATE {PENDING_QUERIES_TABLE}
                        SET status = 'FAILED', error_message = ?, retry_count = retry_count + 1
                        WHERE id = ?
                    """, (error_msg, pending_id))
                    conn_tracker.commit()
                    continue

                try:
                    # Deserialisasi parameter
                    query_params = None
                    if query_params_json:
                        try:
                            # Coba parse sebagai JSON
                            query_params = json.loads(query_params_json)
                        except json.JSONDecodeError:
                             # Jika bukan JSON, anggap itu string biasa (fallback)
                            query_params = [query_params_json]

                    # Buat koneksi ke server target
                    conn_target = pyodbc.connect(
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={target_config['server']};"
                        f"DATABASE={target_config['database']};"
                        f"UID={target_config['username']};"
                        f"PWD={target_config['password']}",
                        timeout=30
                    )
                    cursor_target = conn_target.cursor()

                    # Eksekusi query
                    print(f"    -> [{pending_id}] Executing pending query for {table_name} (Op: {operation_type}, Source PK: {source_pk})...")
                    if query_params:
                         # Pastikan query_params adalah list/tuple
                        if not isinstance(query_params, (list, tuple)):
                            query_params = [query_params]
                        cursor_target.execute(query_text, query_params)
                    else:
                        cursor_target.execute(query_text)

                    # Commit transaksi di server target
                    conn_target.commit()

                    # Update status di tabel pending menjadi SUCCESS
                    cursor_tracker.execute(f"""
                        UPDATE {PENDING_QUERIES_TABLE}
                        SET status = 'SUCCESS', retry_count = retry_count + 1
                        WHERE id = ?
                    """, (pending_id,))
                    conn_tracker.commit()
                    processed_in_batch += 1
                    print(f"    ✅ [{pending_id}] {table_name}: Pending query executed successfully.")

                except Exception as e:
                    # Jika gagal lagi, update retry_count dan error_message
                    new_retry_count = retry_count + 1
                    new_error_msg = f"Retry {new_retry_count} failed: {str(e)}"
                    new_status = 'FAILED' if new_retry_count >= MAX_RETRY_ATTEMPTS else 'PENDING'

                    print(f"    ⚠️  [{pending_id}] {table_name}: Retry failed. {new_error_msg}")

                    cursor_tracker.execute(f"""
                        UPDATE {PENDING_QUERIES_TABLE}
                        SET retry_count = ?, error_message = ?, status = ?
                        WHERE id = ?
                    """, (new_retry_count, new_error_msg, new_status, pending_id))
                    conn_tracker.commit()

                finally:
                    # Tutup koneksi target untuk setiap query
                    if conn_target:
                        try:
                            conn_target.close()
                        except:
                            pass
                        conn_target = None
                        cursor_target = None

            if processed_in_batch > 0:
                print(f"📊 Processed {processed_in_batch} pending queries in this batch.")

        except Exception as e:
            print(f"❌ Error in Pending Queries Processor: {e}")

        # Tidur sebelum iterasi berikutnya
        print(f"    -> Sleeping for {RETRY_INTERVAL_SECONDS} seconds...")
        time.sleep(RETRY_INTERVAL_SECONDS)

    # Bagian finally untuk thread ini (jika thread pernah berhenti)
    if conn_tracker:
        try:
            conn_tracker.close()
        except:
            pass
    print("⏹️  Pending Queries Processor thread ended.")
# --- AKHIR TAMBAHAN: Fungsi untuk memproses pending queries ---

def sync_changes_for_table(source, target, direction_id, is_source_server1, table_config):
    """Sync changes for specific table with reconnection logic and pending queue support"""
    table_name = table_config['table_name']
    columns = table_config['columns']
    primary_key_info = table_config['primary_keys'][0] # Assuming single primary key for simplicity
    primary_key_col = primary_key_info['name']
    is_pk_identity = primary_key_info['is_identity']
    force_id_sync = table_config.get('force_id_sync', False)

    # Inisialisasi koneksi sebagai None
    conn_source = None
    conn_target = None
    source_cursor = None
    target_cursor = None
    source_identifier = f"{source['server']}:{source['database']}"
    target_identifier = f"{target['server']}:{target['database']}"
    print(f"🔄 Starting sync loop: {source_identifier} -> {target_identifier} (Table: {table_name})")

    # --- TAMBAHAN: Fungsi helper untuk membuat koneksi ---
    def create_connection(config):
        return pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']}",
            timeout=30 # Tambahkan timeout untuk koneksi
        )
    # --- AKHIR TAMBAHAN ---

    # --- TAMBAHAN: Fungsi untuk memeriksa apakah koneksi masih hidup ---
    def is_connection_alive(conn):
        if conn is None:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1") # Query sederhana untuk mengecek koneksi
            cursor.fetchone()
            return True
        except pyodbc.Error:
            return False
    # --- AKHIR TAMBAHAN ---

    # Loop utama sinkronisasi
    last_version = 0
    while True:
        try:
            # --- PERUBAHAN: Periksa dan buat ulang koneksi jika perlu ---
            # Cek koneksi ke source
            if not is_connection_alive(conn_source):
                if conn_source:
                    try:
                        conn_source.close()
                    except:
                        pass
                    print(f"    -> [{direction_id}] {table_name}: Source connection was dead, closing.")
                print(f"    -> [{direction_id}] {table_name}: Establishing new connection to source ({source_identifier})...")
                conn_source = create_connection(source)
                source_cursor = conn_source.cursor()
                print(f"    -> [{direction_id}] {table_name}: Connected to source.")

            # Cek koneksi ke target
            if not is_connection_alive(conn_target):
                if conn_target:
                    try:
                        conn_target.close()
                    except:
                        pass
                    print(f"    -> [{direction_id}] {table_name}: Target connection was dead, closing.")
                print(f"    -> [{direction_id}] {table_name}: Establishing new connection to target ({target_identifier})...")
                conn_target = create_connection(target)
                target_cursor = conn_target.cursor()
                print(f"    -> [{direction_id}] {table_name}: Connected to target.")

            # Pastikan tabel ada di source dan target
            if not table_exists(source_cursor, table_name):
                print(f"❌ Table '{table_name}' not found in source: {source_identifier}")
                time.sleep(10) # Tunggu lebih lama jika tabel tidak ditemukan
                continue # Coba lagi di iterasi berikutnya
            if not table_exists(target_cursor, table_name):
                print(f"❌ Table '{table_name}' not found in target: {target_identifier}")
                time.sleep(10)
                continue

            # --- AKHIR PERUBAHAN: Periksa dan buat ulang koneksi jika perlu ---

            # Get the current latest version from the source database directly
            # This is important if Change Tracking was enabled recently or the script restarted
            cursor_current_version = conn_source.cursor()
            cursor_current_version.execute(f"SELECT CHANGE_TRACKING_CURRENT_VERSION()")
            current_latest_version = cursor_current_version.fetchone()[0]
            cursor_current_version.close()

            # If last_version is 0 (first run or reset), initialize it to the current latest version
            if last_version == 0:
                last_version = current_latest_version
                print(f"    -> [{direction_id}] {table_name}: Initializing last_version to {last_version}")

            # Ensure primary_key_col is included in the SELECT from T
            select_columns = [f"T.{col}" for col in columns if col != primary_key_col] # Exclude PK if it's identity and not forced
            if not (is_pk_identity and not force_id_sync): # Always include PK if it's not an auto-generated identity column or if force_id_sync is true
                select_columns.insert(0, f"T.{primary_key_col}")
            column_list = ", ".join(select_columns)

            source_cursor.execute(f"""
            SELECT
                CT.SYS_CHANGE_VERSION,
                CT.SYS_CHANGE_OPERATION,
                CT.{primary_key_col} AS SourcePK, -- Alias to distinguish from T.primary_key_col if included
                {column_list}
            FROM CHANGETABLE(CHANGES {table_name}, ?) AS CT
            LEFT JOIN {table_name} T ON CT.{primary_key_col} = T.{primary_key_col}
            WHERE CT.SYS_CHANGE_VERSION > ?
            ORDER BY CT.SYS_CHANGE_VERSION
            """, (last_version, last_version))

            changes = source_cursor.fetchall()

            if changes:
                processed_count = 0
                # target_cursor sudah dibuat ulang di bagian koneksi
                for row in changes:
                    version = row[0]
                    operation = row[1]
                    source_pk = str(row[2]) # ID dari server sumber (bisa string)
                    raw_data_from_row = row[3:] # Data values from the SQL result
                    current_row_data = {}
                    if not (is_pk_identity and not force_id_sync):
                        current_row_data[primary_key_col] = raw_data_from_row[0]
                        data_values_for_other_cols = raw_data_from_row[1:]
                    else:
                        data_values_for_other_cols = raw_data_from_row # Data starts from the first non-PK column
                    # Map remaining values to the non-PK columns
                    non_pk_columns_in_config = [col for col in columns if col != primary_key_col]
                    for i, col_name in enumerate(non_pk_columns_in_config):
                        if i < len(data_values_for_other_cols):
                            current_row_data[col_name] = data_values_for_other_cols[i]
                        else:
                            current_row_data[col_name] = None # Handle missing data for safety

                    # --- PERUBAHAN: Wrap operasi target dalam try-except untuk pending queue dan reconnection ---
                    try:
                        # Tentukan identifier server tujuan untuk logging/pending queue
                        # target_identifier sudah didefinisikan di awal fungsi

                        if operation == 'I':
                            # --- INSERT ---
                            target_id_from_tracker = sql_sync_tracker.get_mapped_id(table_name, source_pk, is_source_server1)
                            if target_id_from_tracker:
                                print(f"    -> [{direction_id}] {table_name}: Skipping ID {source_pk} (already mapped to {target_id_from_tracker})")
                                continue # Already mapped, no need to insert

                            target_id = None # Inisialisasi target_id

                            if force_id_sync and primary_key_col in columns: # If PK is part of the `columns` list, implies we manage it
                                try:
                                    # --- PERUBAHAN: Siapkan query dan params sebelum eksekusi ---
                                    insert_cols = [primary_key_col] + [col for col in columns if col != primary_key_col]
                                    insert_vals = [source_pk] + [current_row_data.get(col) for col in columns if col != primary_key_col]
                                    placeholders = ", ".join(["?"] * len(insert_cols))
                                    columns_str = ", ".join(insert_cols)
                                    query_text = f"SET IDENTITY_INSERT {table_name} ON; INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders}); SET IDENTITY_INSERT {table_name} OFF;"
                                    query_params = insert_vals # Parameters untuk INSERT
                                    # --- AKHIR PERUBAHAN ---

                                    target_cursor.execute(f"SET IDENTITY_INSERT {table_name} ON")
                                    # --- PERUBAHAN: Eksekusi query yang sudah disiapkan ---
                                    target_cursor.execute(
                                        f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})",
                                        insert_vals
                                    )
                                    # --- AKHIR PERUBAHAN ---
                                    target_id = source_pk
                                    print(f"    -> [{direction_id}] {table_name}: FORCED INSERT with ID: {source_pk}")

                                    # Add mapping after INSERT successfully
                                    if is_source_server1:
                                        sql_sync_tracker.add_mapping(table_name, source_pk, target_id)
                                    else:
                                        sql_sync_tracker.add_mapping(table_name, target_id, source_pk)
                                    processed_count += 1

                                except Exception as e:
                                     # Simpan ke pending queue jika INSERT gagal
                                    error_msg = f"FORCED INSERT FAILED: {str(e)}"
                                    print(f"⚠️  [{direction_id}] {table_name}: {error_msg} for ID {source_pk}")
                                    sql_sync_tracker.save_pending_query(
                                        table_name=table_name,
                                        to_server=target_identifier,
                                        query_text=query_text,
                                        query_params=query_params,
                                        operation_type='I',
                                        source_pk=source_pk,
                                        target_pk=None, # Belum ada karena gagal insert
                                        error_message=error_msg
                                    )
                                    continue # Lanjut ke record berikutnya
                                finally:
                                    try:
                                        target_cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF")
                                    except:
                                        pass # Abaikan error saat mematikan IDENTITY_INSERT

                            else: # Normal insert for identity columns (auto-generated ID) or non-identity PK
                                insert_cols = []
                                insert_vals = []
                                for col_name in columns:
                                    # Only include columns that are NOT identity primary key for auto-generation
                                    if col_name == primary_key_col and is_pk_identity:
                                        continue
                                    insert_cols.append(col_name)
                                    insert_vals.append(current_row_data.get(col_name))

                                if not insert_cols:
                                    print(f"⚠️  [{direction_id}] {table_name}: No non-IDENTITY columns to insert for ID {source_pk}. Skipping.")
                                    continue

                                placeholders_str = ", ".join(["?"] * len(insert_cols))
                                columns_str = ", ".join(insert_cols)
                                # --- PERBAIKAN: Gunakan OUTPUT untuk mendapatkan ID yang di-generate ---
                                # Ini lebih andal daripada SCOPE_IDENTITY() dalam beberapa kasus
                                # --- PERUBAHAN: Siapkan query dan params sebelum eksekusi ---
                                query_text = f"INSERT INTO {table_name} ({columns_str}) OUTPUT INSERTED.{primary_key_col} VALUES ({placeholders_str})"
                                query_params = insert_vals # Parameters untuk INSERT
                                # --- AKHIR PERUBAHAN ---
                                try:
                                    # --- PERUBAHAN: Eksekusi query yang sudah disiapkan ---
                                    target_cursor.execute(
                                        query_text,
                                        query_params
                                    )
                                    # --- AKHIR PERUBAHAN ---
                                    # Ambil hasil dari OUTPUT clause
                                    output_result = target_cursor.fetchone()
                                    if output_result:
                                        target_id = output_result[0]
                                        print(f"    -> [{direction_id}] {table_name}: NORMAL INSERT: {source_pk} -> {target_id}")
                                    else:
                                        # Fallback ke SCOPE_IDENTITY() jika OUTPUT tidak mengembalikan nilai
                                        print(f"⚠️  [{direction_id}] {table_name}: OUTPUT clause failed, falling back to SCOPE_IDENTITY() for ID {source_pk}")
                                        target_cursor.execute(f"SELECT SCOPE_IDENTITY()")
                                        scope_result = target_cursor.fetchone()
                                        target_id = scope_result[0] if scope_result else None
                                        if target_id is not None:
                                            print(f"    -> [{direction_id}] {table_name}: NORMAL INSERT (SCOPE_IDENTITY fallback): {source_pk} -> {target_id}")
                                        else:
                                            print(f"❌ [{direction_id}] {table_name}: Failed to get target ID for source PK {source_pk} using both OUTPUT and SCOPE_IDENTITY(). Skipping mapping.")
                                            # Jika tidak bisa mendapatkan ID, lewati penambahan mapping
                                            continue

                                    # Add mapping after INSERT successfully
                                    if target_id is not None:
                                        if is_source_server1:
                                            sql_sync_tracker.add_mapping(table_name, source_pk, target_id)
                                        else:
                                            sql_sync_tracker.add_mapping(table_name, target_id, source_pk)
                                        processed_count += 1
                                    else:
                                        print(f"⚠️  [{direction_id}] {table_name}: Failed to get target ID after insert for source PK {source_pk}")

                                except Exception as e:
                                    # Simpan ke pending queue jika INSERT gagal
                                    error_msg = f"NORMAL INSERT FAILED: {str(e)}"
                                    print(f"⚠️  [{direction_id}] {table_name}: {error_msg} for ID {source_pk}")
                                    sql_sync_tracker.save_pending_query(
                                        table_name=table_name,
                                        to_server=target_identifier,
                                        query_text=query_text,
                                        query_params=query_params,
                                        operation_type='I',
                                        source_pk=source_pk,
                                        target_pk=None, # Belum ada karena gagal insert
                                        error_message=error_msg
                                    )
                                    continue # Lanjut ke record berikutnya

                        elif operation == 'U':
                            # --- UPDATE ---
                            target_id = sql_sync_tracker.get_mapped_id(table_name, source_pk, is_source_server1)
                            if not target_id:
                                print(f"⚠️  [{direction_id}] {table_name}: No mapping for UPDATE {source_pk}. Attempting to insert new record.")
                                # Logika fallback insert dihapus untuk kesederhanaan penanganan error
                                # Anda bisa menambahkannya kembali jika diperlukan, dengan penanganan error yang sama
                                print(f"ℹ️  [{direction_id}] {table_name}: Fallback insert on UPDATE not implemented in this version for pending queue.")
                                continue # Untuk saat ini, skip jika tidak ada mapping

                            # --- PERUBAHAN: Siapkan kolom untuk pemeriksaan perubahan ---
                            # Tentukan kolom-kolom yang akan diperiksa dan diperbarui
                            columns_to_update_and_check = []
                            update_values = []
                            for col_name in columns:
                                # Jangan coba update kolom PK itu sendiri jika itu identity
                                if col_name == primary_key_col and is_pk_identity:
                                    continue
                                columns_to_update_and_check.append(col_name)
                                update_values.append(current_row_data.get(col_name))

                            if not columns_to_update_and_check: # No non-PK columns to update/check
                                print(f"ℹ️  [{direction_id}] {table_name}: No non-PK columns to update/check for ID {source_pk}. Skipping update.")
                                continue

                            # --- PERUBAHAN: Periksa apakah data benar-benar berubah ---
                            # (Pemeriksaan ini tetap dilakukan sebelum menyimpan ke pending)
                            if data_has_changed(target_cursor, table_name, primary_key_col, target_id, current_row_data, columns_to_update_and_check):
                                # Data berbeda, lakukan UPDATE
                                set_clause_parts = [f"{col_name} = ?" for col_name in columns_to_update_and_check]
                                set_clause = ", ".join(set_clause_parts)
                                # --- PERUBAHAN: Siapkan query dan params sebelum eksekusi ---
                                query_text = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key_col} = ?"
                                query_params = update_values + [target_id] # Data baru + ID target untuk WHERE clause
                                # --- AKHIR PERUBAHAN ---
                                try:
                                    # --- PERUBAHAN: Eksekusi query yang sudah disiapkan ---
                                    target_cursor.execute(
                                        query_text,
                                        query_params
                                    )
                                    # --- AKHIR PERUBAHAN ---
                                    if target_cursor.rowcount > 0:
                                        processed_count += 1
                                        print(f"    -> [{direction_id}] {table_name}: UPDATE: {source_pk} -> {target_id}")
                                    else:
                                        print(f"ℹ️  [{direction_id}] {table_name}: No rows affected for {target_id}")

                                except Exception as e:
                                    # Simpan ke pending queue jika UPDATE gagal
                                    error_msg = f"UPDATE FAILED: {str(e)}"
                                    print(f"⚠️  [{direction_id}] {table_name}: {error_msg} for ID {source_pk} -> {target_id}")
                                    sql_sync_tracker.save_pending_query(
                                        table_name=table_name,
                                        to_server=target_identifier,
                                        query_text=query_text,
                                        query_params=query_params,
                                        operation_type='U',
                                        source_pk=source_pk,
                                        target_pk=target_id, # ID target yang sudah ada
                                        error_message=error_msg
                                    )
                                    continue # Lanjut ke record berikutnya
                            else:
                                # Data sama, lewati UPDATE
                                print(f"ℹ️  [{direction_id}] {table_name}: Data for {target_id} is unchanged. Skipping update.")
                            # --- AKHIR PERUBAHAN: Periksa apakah data benar-benar berubah ---

                        elif operation == 'D':
                            # --- DELETE ---
                            target_id = sql_sync_tracker.get_mapped_id(table_name, source_pk, is_source_server1)
                            if not target_id:
                                print(f"⚠️  [{direction_id}] {table_name}: No mapping for DELETE {source_pk}. Skipping.")
                                continue # Skip jika tidak ada mapping

                            # --- PERUBAHAN: Siapkan query dan params sebelum eksekusi ---
                            query_text = f"DELETE FROM {table_name} WHERE {primary_key_col} = ?"
                            query_params = [target_id]
                            # --- AKHIR PERUBAHAN ---
                            try:
                                # --- PERUBAHAN: Eksekusi query yang sudah disiapkan ---
                                target_cursor.execute(
                                    query_text,
                                    query_params
                                )
                                # --- AKHIR PERUBAHAN ---
                                if target_cursor.rowcount > 0:
                                    sql_sync_tracker.remove_mapping(table_name, source_pk, is_source_server1)
                                    processed_count += 1
                                    print(f"    -> [{direction_id}] {table_name}: DELETE: {source_pk} -> {target_id}")
                                else:
                                    print(f"ℹ️  [{direction_id}] {table_name}: No rows deleted for {target_id}")

                            except Exception as e:
                                 # Simpan ke pending queue jika DELETE gagal
                                error_msg = f"DELETE FAILED: {str(e)}"
                                print(f"⚠️  [{direction_id}] {table_name}: {error_msg} for ID {source_pk} -> {target_id}")
                                sql_sync_tracker.save_pending_query(
                                    table_name=table_name,
                                    to_server=target_identifier,
                                    query_text=query_text,
                                    query_params=query_params,
                                    operation_type='D',
                                    source_pk=source_pk,
                                    target_pk=target_id, # ID target yang akan dihapus
                                    error_message=error_msg
                                )
                                continue # Lanjut ke record berikutnya

                    except Exception as e_outer: # Catch error yang tidak tertangkap di blok spesifik
                        print(f"❌ [{direction_id}] {table_name}: Unexpected error processing record (ID: {source_pk}, Op: {operation}): {str(e_outer)}")
                        # Simpan error umum ke pending queue (opsional, bisa saja error di luar operasi DB)
                        # Misalnya error saat membangun query. Untuk kesederhanaan, kita skip.
                        continue # Lanjut ke record berikutnya
                    # --- AKHIR PERUBAHAN ---

                if processed_count > 0:
                    conn_target.commit()
                    print(f"📊 [{direction_id}] {table_name}: Processed: {processed_count}")
                # Update last_version after all changes for this batch are processed
                last_version = version # Assuming version is the max version in the current batch

            time.sleep(3) # Wait before checking for new changes

        except pyodbc.Error as db_err:
            # Tangkap error koneksi database secara spesifik
            print(f"⚠️  [{direction_id}] {table_name}: Database connection error: {str(db_err)}")
            # Tutup koneksi yang bermasalah
            if conn_source:
                try:
                    conn_source.close()
                except:
                    pass
                conn_source = None
            if conn_target:
                try:
                    conn_target.close()
                except:
                    pass
                conn_target = None
            print(f"    -> [{direction_id}] {table_name}: Connections closed due to error. Will attempt to reconnect...")
            time.sleep(10) # Tunggu lebih lama sebelum mencoba reconnect

        except Exception as e_general:
            # Tangkap error umum lainnya
            print(f"❌ [{direction_id}] {table_name}: General sync loop error: {str(e_general)}")
            time.sleep(5) # Tunggu sejenak sebelum mencoba lagi

    # Bagian finally tidak lagi diperlukan karena koneksi dikelola dalam loop
    # Namun, pastikan koneksi ditutup saat thread berakhir (meskipun daemon)
    if conn_source:
        try:
            conn_source.close()
        except:
            pass
    if conn_target:
        try:
            conn_target.close()
        except:
            pass
    print(f"⏹️  [{direction_id}] {table_name}: Sync loop ended.")

def sync_changes(source, target, direction_id, is_source_server1, table_configs):
    """Sync changes for all tables"""
    threads = []
    for table_config in table_configs:
        thread = Thread(
            target=sync_changes_for_table,
            args=(source, target, f"{direction_id}-{table_config['table_name']}",
                  is_source_server1, table_config),
            daemon=True
        )
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

def initialize_mappings_threaded(table_configs):
    """Initialize mappings for all tables with threading"""
    print("\n🔄 Starting initial mapping for all tables...")
    with ThreadPoolExecutor(max_workers=min(4, len(table_configs))) as executor:
        future_to_table = {
            executor.submit(sql_sync_tracker.initialize_mapping, table_config): table_config['table_name']
            for table_config in table_configs
        }
        completed_tables = 0
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                success = future.result()
                completed_tables += 1
                if success:
                    print(f"    ✅ Initial mapping for {table_name} completed.")
                else:
                    print(f"    ❌ Initial mapping for {table_name} failed.")
            except Exception as exc:
                print(f"    ❌ Initial mapping for {table_name} generated an exception: {exc}")
    print(f"✅ Initial mapping completed for {completed_tables}/{len(table_configs)} tables.")

# --- PERUBAHAN: Hapus fungsi Rekonsiliasi ---
# Fungsi reconcile_table_data dan start_reconciliation_thread dihapus
# --- AKHIR PERUBAHAN: Hapus fungsi Rekonsiliasi ---

if __name__ == "__main__":
    print("🚀 Starting synchronization process...")
    print(f"📊 Using SQL Server tracking: {TRACKER_CONFIG['server']}/{TRACKER_CONFIG['database']}")

    # Load table configurations
    print(f"📂 Loading table configurations from {TABLE_CONFIG_FILE}...")
    table_configs = load_table_configs()
    if not table_configs:
        print("❌ No table configurations found. Ensure table.txt exists and is formatted correctly.")
        exit(1)

    # --- 🔐 CHECK LICENSE ---
    is_licensed, MACHINE_ID = check_license()
    LICENSE_EXPIRY = None

    if not is_licensed:
        LICENSE_EXPIRY = datetime.now() + timedelta(hours=1)
        logger.log(f"⏳ Mode demo diaktifkan. Program akan berhenti sinkronisasi dalam 1 jam ({LICENSE_EXPIRY.strftime('%H:%M:%S')}) kecuali lisensi valid.")
    else:
        logger.log("✅ Program berjalan dengan lisensi penuh.")
    # --- AKHIR PENGECEKAN LISENSI ---

    # Start license monitoring
    Thread(target=monitor_license_and_pause, daemon=True).start()

    # Inisialisasi tracker setelah lisensi dicek
    sql_sync_tracker = SQLServerSyncTracker(TRACKER_CONFIG)
    sql_sync_tracker.init_db()

    # Enable Change Tracking
    # ... (kode asli lanjut)
    print("🚀 Starting synchronization process...")
    print(f"📊 Using SQL Server tracking: {TRACKER_CONFIG['server']}/{TRACKER_CONFIG['database']}")

    # Load table configurations
    print(f"📂 Loading table configurations from {TABLE_CONFIG_FILE}...")
    table_configs = load_table_configs()
    if not table_configs:
        print("❌ No table configurations found. Ensure table.txt exists and is formatted correctly.")
        exit(1)

    # --- PERBAIKAN: Inisialisasi sql_sync_tracker DI SINI ---
    sql_sync_tracker = SQLServerSyncTracker(TRACKER_CONFIG)
    # --- AKHIR PERBAIKAN ---

    # Initialize tracking database
    print(f"🔧 Setting up tracking database...")
    sql_sync_tracker.init_db() # Panggil init_db setelah objek dibuat

    # Enable Change Tracking
    success_count = 0
    for i, config in enumerate(CONFIGS):
        print(f"\n🔧 Configuring Change Tracking for Server {i+1}...")
        table_success_count = 0
        for table_config in table_configs:
            if enable_change_tracking(
                config['server'],
                config['database'],
                config['username'],
                config['password'],
                table_config['table_name']
            ):
                table_success_count += 1
        if table_success_count == len(table_configs):
            success_count += 1
    if success_count < 2:
        print("⚠️  Failed to enable Change Tracking on one or more servers for some tables. Check logs above.")

    # Initialize mappings with threading and initial data sync
    initialize_mappings_threaded(table_configs) # Call the new threaded function

    # Start sync threads
    print("\n🔄 Starting real-time sync threads for all tables...")
    Thread(
        target=sync_changes,
        args=(CONFIGS[0], CONFIGS[1], "A→B", True, table_configs),
        daemon=True
    ).start()
    Thread(
        target=sync_changes,
        args=(CONFIGS[1], CONFIGS[0], "B→A", False, table_configs),
        daemon=True
    ).start()

    # --- PERUBAHAN: Hapus thread rekonsiliasi ---
    # Bagian yang memulai thread rekonsiliasi dihapus
    # --- AKHIR PERUBAHAN: Hapus thread rekonsiliasi ---

    # --- TAMBAHAN: Mulai thread untuk memproses pending queries ---
    print("\n🔄 Starting Pending Queries Processor thread...")
    Thread(
        target=process_pending_queries,
        args=(sql_sync_tracker, CONFIGS), # Kirim instance tracker dan konfigurasi server
        daemon=True
    ).start()
    # --- AKHIR TAMBAHAN ---

    print("✅ Sync is running... Press Ctrl+C to stop")
    print("💡 For testing: INSERT, UPDATE, or DELETE data on either server, wait a few seconds for changes to sync.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⏹️  Program stopped.")
        # Catatan: Thread daemon akan berhenti secara otomatis saat program utama berhenti.
        print(f"📁 Tracking database: {TRACKER_CONFIG['server']}/{TRACKER_CONFIG['database']}")
