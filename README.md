# SQL Server Real-Time Sync with Change Tracking

Script Python ini dirancang untuk menyinkronkan data secara real-time antara dua database SQL Server menggunakan fitur **Change Tracking** bawaan SQL Server. Script ini juga mencakup fitur **pemetaan ID** (untuk menangani perbedaan ID antar server) dan **antrian query yang gagal** (pending queries) untuk meningkatkan keandalan sinkronisasi.

## Fitur Utama

*   **Sinkronisasi Real-Time:** Menggunakan `CHANGETABLE(CHANGES ...)` untuk mendeteksi perubahan (`INSERT`, `UPDATE`, `DELETE`) secara real-time.
*   **Pemetaan ID:** Menyimpan dan menggunakan pemetaan ID antara dua server untuk menangani primary key yang berbeda (misalnya, auto-increment).
*   **Sinkronisasi ID Paksa (`force_id`):** Mendukung sinkronisasi nilai primary key secara eksplisit jika diperlukan (misalnya, untuk tabel referensi).
*   **Penanganan Koneksi Ulang:** Mencoba membuat koneksi ulang secara otomatis jika terjadi putus koneksi.
*   **Antrian Query yang Gagal:** Jika sebuah operasi sinkronisasi (`INSERT`, `UPDATE`, `DELETE`) gagal (misalnya karena koneksi atau constraint), query tersebut disimpan ke tabel khusus (`pending_queries`) dan akan diulang secara berkala oleh thread terpisah.
*   **Konfigurasi Fleksibel:** Konfigurasi server, database pelacak, dan tabel yang disinkronkan dimuat dari file teks terpisah.
*   **Batas Sinkronisasi Awal:** Hanya menyinkronkan dan memetakan sejumlah terbatas record terbaru (dikonfigurasi via `limit.txt`) saat inisialisasi untuk meningkatkan kecepatan startup.

## Cara Penggunaan

1.  **Persiapan Lingkungan**
    *   **Python:** Pastikan Anda memiliki Python 3.x terinstal.
    *   **Pustaka Python:** Instal pustaka yang diperlukan.
        ```bash
        pip install pyodbc
        ```
    *   **Driver ODBC:** Pastikan **ODBC Driver 17 for SQL Server** terinstal di mesin tempat script ini dijalankan. Anda bisa mengunduhnya dari situs resmi Microsoft.
    *   **Database SQL Server:** Pastikan Anda memiliki akses ke dua database SQL Server yang ingin disinkronkan dan satu database tambahan untuk menyimpan data pelacakan (tracking database).

2.  **Konfigurasi**

    Buat file-file konfigurasi berikut dalam direktori yang sama dengan script Python (`sync_script.py` atau nama lain yang Anda berikan):

    *   **`odbc.txt`** (Konfigurasi Server Sumber & Target)
        Berisi detail koneksi untuk dua server database yang akan disinkronkan. Formatnya adalah 8 baris berturut-turut:
        ```
        <server1_host_or_ip>
        <server1_database_name>
        <server1_username>
        <server1_password>
        <server2_host_or_ip>
        <server2_database_name>
        <server2_username>
        <server2_password>
        ```
        **Contoh `odbc.txt`:**
        ```
        server1.mycompany.com
        DatabaseA
        sync_user
        sync_pass123
        192.168.1.100
        DatabaseB
        sync_user
        sync_pass456
        ```

    *   **`odbctracker.txt`** (Konfigurasi Database Pelacak)
        Berisi detail koneksi untuk database yang digunakan untuk menyimpan tabel `synced_records` dan `pending_queries`.
        ```
        <tracker_server_host_or_ip>
        <tracker_database_name>
        <tracker_username>
        <tracker_password>
        ```
        **Contoh `odbctracker.txt`:**
        ```
        localhost
        SyncTrackerDB
        tracker_user
        tracker_pass789
        ```

    *   **`table.txt`** (Daftar Tabel yang Disinkronkan)
        Berisi daftar tabel yang ingin disinkronkan beserta konfigurasi primary key dan kolomnya. Setiap baris mewakili satu tabel.
        Format:
        ```
        <table_name>:<primary_key_info>:<column1,column2,...>[:force_id]
        ```
        *   `<table_name>`: Nama tabel.
        *   `<primary_key_info>`:
            *   Jika PK bukan identity: cukup nama kolom (e.g., `id`).
            *   Jika PK identity: `nama_kolom=increment` (e.g., `id=increment`).
            *   Untuk composite PK: `pk1+pk2=increment` (format umum, meskipun script utama mungkin mengasumsikan PK tunggal untuk beberapa operasi).
        *   `<column1,column2,...>`: Daftar kolom yang akan disinkronkan, dipisahkan koma. Sertakan primary key jika Anda ingin menyinkronkannya secara eksplisit (lihat `force_id`).
        *   `:force_id` (opsional): Flag untuk memberi tahu script bahwa nilai primary key harus disinkronkan secara eksplisit (menggunakan `SET IDENTITY_INSERT ... ON`).
        **Contoh `table.txt`:**
        ```
        # Tabel dengan PK identity, tidak disinkronkan secara eksplisit
        Users:id=increment:id,name,email,created_at

        # Tabel referensi dengan PK non-identity, disinkronkan secara eksplisit
        Roles:id:id,name,description:force_id

        # Tabel dengan PK non-identity, disinkronkan secara eksplisit
        Countries:country_code:country_code,country_name:force_id
        ```

    *   **`limit.txt`** (Batas Record untuk Sinkronisasi Awal)
        Berisi angka yang menentukan berapa banyak record terakhir (berdasarkan primary key) yang akan diproses untuk pemetaan awal.
        **Contoh `limit.txt`:**
        ```
        1000
        ```
        Jika file ini tidak ada atau isinya tidak valid, script akan menggunakan nilai default `1000`.

3.  **Menjalankan Script**

    Buka terminal atau command prompt, arahkan ke direktori tempat script dan file konfigurasi berada, lalu jalankan script Python.

    ```bash
    python sync_script.py
    ```

    Script akan:
    *   Membaca file konfigurasi.
    *   Menginisialisasi database pelacak dan membuat tabel `synced_records` serta `pending_queries` jika belum ada.
    *   Mengaktifkan Change Tracking di kedua server untuk tabel-tabel yang terdaftar.
    *   Melakukan inisialisasi pemetaan awal berdasarkan `limit.txt`.
    *   Memulai thread sinkronisasi real-time untuk setiap tabel dan arah (A ke B, B ke A).
    *   Memulai thread untuk memproses ulang query yang tertunda di `pending_queries`.

4.  **Memantau Sinkronisasi**

    *   **Output Console:** Script akan mencetak log ke console untuk menunjukkan status koneksi, deteksi perubahan, operasi sinkronisasi, dan error.
    *   **Database Pelacak:**
        *   Tabel `synced_records`: Berisi pemetaan ID antara server 1 dan server 2 untuk setiap tabel.
        *   Tabel `pending_queries`: Berisi daftar query yang gagal dijalankan, alasan error, jumlah percobaan ulang, dan statusnya (`PENDING`, `SUCCESS`, `FAILED`). Anda bisa memantau tabel ini untuk melihat query mana yang perlu diperhatikan.

## Catatan Penting

*   **Change Tracking:** Fitur Change Tracking harus diaktifkan di tingkat database dan tabel di kedua server sumber dan target. Script mencoba mengaktifkannya secara otomatis, tetapi pastikan pengguna yang digunakan memiliki cukup hak akses.
*   **Primary Key:** Script ini sebagian besar mengasumsikan primary key tunggal. Penanganan composite primary key mungkin memerlukan modifikasi.
*   **Penanganan Error:** Meskipun script memiliki mekanisme antrian ulang, error struktural (misalnya, constraint pelanggaran yang tidak bisa diperbaiki otomatis, perbedaan skema yang signifikan) perlu ditangani secara manual.
*   **Keamanan:** Jangan menyimpan file konfigurasi yang berisi password dalam repository publik. Pertimbangkan untuk menggunakan variabel lingkungan atau sistem manajemen secret.
*   **Performa:** Sinkronisasi real-time bisa memberikan beban pada database. Pastikan server dan jaringan Anda mampu menanganinya, terutama jika jumlah tabel dan volume data tinggi.


alat tambahan
# SQL Server Table Config Generator

Alat bantu untuk membuat file `table.txt` secara otomatis untuk script sinkronisasi database.

## Fungsi
Membaca struktur tabel dari database SQL Server (nama tabel, primary key, status identity, kolom) dan menghasilkan file konfigurasi `table.txt`.

## Cara Pakai

1.  **Siapkan Lingkungan:**
    *   Pasang Python 3.x
    *   Pasang pyodbc: `pip install pyodbc`
    *   Pasang ODBC Driver 17 for SQL Server

2.  **Buat File Konfigurasi:**
    *   Buat file `odbc.txt` dengan format:
        ```
        <server>
        <database>
        <username>
        <password>
        ```
    *   Contoh:
        ```
        localhost
        MyDatabase
        my_user
        my_pass
        ```

3.  **Jalankan Script:**
    ```bash
    python table_config_generator.py
    ```

4.  **Hasil:**
    *   File `table.txt` akan dibuat dengan daftar tabel dan konfigurasinya.
    *   Contoh isi `table.txt`:
      ```
      Users:id=increment:id,name,email,created_at
      Roles:role_id:id,name,description
      ```

5.  **Lanjutkan:**
    *   Salin `table.txt` ke folder script sinkronisasi utama.
    *   Edit `table.txt` jika perlu (hapus tabel, ubah kolom, tambah `:force_id`).

## Catatan
*   Hanya membaca tabel dari skema `dbo`.
*   Perlu hak akses baca struktur database.
