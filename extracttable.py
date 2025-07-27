import pyodbc

def read_odbc_config(config_file='odbc.txt'):
    """Membaca konfigurasi ODBC dari file."""
    try:
        with open(config_file, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        if len(lines) < 4:
            raise ValueError(f"File {config_file} tidak memiliki cukup baris konfigurasi untuk satu server.")
            
        return {
            'server': lines[0],
            'database': lines[1],
            'username': lines[2],
            'password': lines[3]
        }
    except FileNotFoundError:
        print(f"❌ File konfigurasi '{config_file}' tidak ditemukan.")
        return None
    except Exception as e:
        print(f"❌ Error membaca '{config_file}': {e}")
        return None

def get_user_tables(cursor):
    """Mendapatkan daftar nama tabel pengguna."""
    try:
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            AND TABLE_SCHEMA = 'dbo' -- Sesuaikan schema jika perlu
            ORDER BY TABLE_NAME
        """)
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Error mendapatkan daftar tabel: {e}")
        return []

def get_primary_keys_with_identity_status(cursor, table_name, database_name):
    """
    Mendapatkan daftar kolom primary key beserta status incrementnya (is_identity).
    Mengembalikan list of tuple: (column_name, is_identity_boolean)
    """
    try:
        # Perbaikan query: Menggunakan objek_id dan schema_id untuk sys.columns join
        query = f"""
        SELECT 
            kcu.COLUMN_NAME,
            col.is_identity
        FROM 
            {database_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN 
            {database_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu 
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        JOIN
            {database_name}.sys.tables AS t
            ON t.name = tc.TABLE_NAME
            AND SCHEMA_NAME(t.schema_id) = tc.TABLE_SCHEMA -- Pastikan schema cocok
        JOIN
            {database_name}.sys.columns AS col
            ON col.object_id = t.object_id
            AND col.name = kcu.COLUMN_NAME
        WHERE 
            tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
            AND tc.TABLE_NAME = ?
        ORDER BY 
            kcu.ORDINAL_POSITION
        """
        
        cursor.execute(query, (table_name,))
        results = cursor.fetchall()
        
        # --- Baris debugging: Cetak hasil query mentah ---
        print(f"    [DEBUG] Hasil query PK + Identity untuk '{table_name}': {results}")
        # --- Akhir baris debugging ---
        
        # Mengembalikan list of tuple: (column_name, is_identity_boolean)
        return [(row[0], bool(row[1])) for row in results]
    except Exception as e:
        print(f"❌ Error mendapatkan primary key dan status identity untuk tabel '{table_name}': {e}")
        # Fallback: Coba metode INFORMATION_SCHEMA.KEY_COLUMN_USAGE jika query sys.columns gagal
        try:
            print("    -> Mencoba metode fallback untuk PK tanpa status identity...")
            fallback_query = f"""
            SELECT kcu.COLUMN_NAME
            FROM {database_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            WHERE kcu.TABLE_NAME = ?
              AND EXISTS (
                SELECT 1
                FROM {database_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                WHERE tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              )
            ORDER BY kcu.ORDINAL_POSITION
            """
            cursor.execute(fallback_query, (table_name,))
            results = cursor.fetchall()
            
            # --- Baris debugging untuk fallback ---
            print(f"    [DEBUG] Hasil query fallback PK untuk '{table_name}': {results}")
            # --- Akhir baris debugging ---

            if results:
                print(f"    -> Primary key ditemukan dengan metode fallback untuk '{table_name}'.")
                # Jika menggunakan fallback, kita tidak bisa menentukan is_identity, maka set False
                return [(row[0], False) for row in results] 
            else:
                print(f"    -> Primary key tidak ditemukan dengan metode fallback untuk '{table_name}'.")
                return []
        except Exception as e2:
            print(f"    -> Error metode fallback: {e2}")
            return []

def get_all_columns(cursor, table_name):
    """Mendapatkan daftar semua kolom untuk sebuah tabel."""
    try:
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (table_name,))
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Error mendapatkan kolom untuk tabel '{table_name}': {e}")
        return []

def main():
    """Fungsi utama."""
    print("🔍 Membaca konfigurasi ODBC...")
    config = read_odbc_config('odbc.txt')
    
    if not config:
        return

    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']}"
    )
    
    output_file = 'table.txt'
    processed_tables = []

    try:
        print(f"🔌 Mencoba koneksi ke {config['server']}/{config['database']}...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("✅ Koneksi berhasil.")

        print("📋 Mengambil daftar tabel...")
        tables = get_user_tables(cursor)
        
        if not tables:
            print("⚠️  Tidak ada tabel pengguna ditemukan.")
            return

        print(f"✅ Ditemukan {len(tables)} tabel.")

        for table_name in tables:
            print(f"\n    -> Memproses tabel: {table_name}")
            
            # Dapatkan primary key(s) dengan status identity
            pk_columns_with_identity = get_primary_keys_with_identity_status(cursor, table_name, config['database'])
            
            if not pk_columns_with_identity:
                print(f"      ⚠️  Tidak ditemukan primary key untuk tabel '{table_name}'. Melewati.")
                continue
                
            pk_parts = []
            for col_name, is_identity in pk_columns_with_identity:
                if is_identity:
                    pk_parts.append(f"{col_name}=increment")
                else:
                    pk_parts.append(col_name)
            
            pk_part = "+".join(pk_parts)
            
            # Dapatkan semua kolom
            all_columns = get_all_columns(cursor, table_name)
            
            if not all_columns:
                print(f"      ⚠️  Tidak ditemukan kolom untuk tabel '{table_name}'. Melewati.")
                continue

            # Format baris untuk table.txt
            # Format: table_name:pk_part:column1,column2,...
            line = f"{table_name}:{pk_part}:{','.join(all_columns)}"
            processed_tables.append(line)
            print(f"      ✅ Ditambahkan: {table_name} (PK: {pk_part})")

        conn.close()

        if processed_tables:
            print(f"\n💾 Menyimpan konfigurasi ke '{output_file}'...")
            with open(output_file, 'w') as f:
                for line in processed_tables:
                    f.write(line + '\n')
            print(f"✅ Selesai! {len(processed_tables)} tabel telah disimpan ke '{output_file}'.")
        else:
            print("\n⚠️  Tidak ada tabel yang diproses untuk disimpan.")

    except pyodbc.Error as e:
        print(f"❌ Error koneksi database: {e}")
    except Exception as e:
        print(f"❌ Error tidak terduga: {e}")

if __name__ == "__main__":
    main()
