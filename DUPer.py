#!/usr/bin/env python3
import os
import hashlib
import sqlite3
import time
from datetime import datetime

# Configuration
SCRIPT_VERSION = "0.1-alpha"  # Starting with a new version sequence for Python
DEBUG_MODE = False
FILE_DIRECTORY = "/run/media/deck/EXT-512/Emulation/roms/n64/"
DATABASE_FILE = "DUPer_working/file_info.sqlite"
WORKING_DIRECTORY = "DUPer_working"
PROGRESS_INTERVAL = 10

def debug_print(message):
    if DEBUG_MODE:
        print(f"DEBUG: {message}")

def check_and_create_dirs():
    """Checks and creates necessary directories."""
    debug_print("Checking and creating necessary directories...")
    if not os.path.exists(WORKING_DIRECTORY):
        debug_print(f"Creating working directory: {WORKING_DIRECTORY}")
        try:
            os.makedirs(WORKING_DIRECTORY)
        except OSError as e:
            print(f"Error: Could not create working directory '{WORKING_DIRECTORY}'. {e}")
            exit(1)
    db_dir = os.path.dirname(DATABASE_FILE)
    if not os.path.exists(db_dir):
        debug_print(f"Creating directory for database file: {db_dir}")
        try:
            os.makedirs(db_dir)
        except OSError as e:
            print(f"Error: Could not create directory for database file. {e}")
            exit(1)

def initialize_database():
    """Initializes the SQLite database."""
    debug_print("Initializing SQLite database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Create files table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            filepath TEXT PRIMARY KEY,
            filename TEXT,
            md5 TEXT,
            simplified_filename TEXT,
            size_mb REAL,
            created_time TEXT,
            modified_time TEXT,
            extension TEXT,
            is_potential_duplicate INTEGER DEFAULT 0
        )
    """)

    # Create metrics table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            start_time TEXT PRIMARY KEY,
            end_time TEXT,
            scan_duration_seconds INTEGER,
            scan_duration_verbose TEXT,
            errors_encountered INTEGER,
            error_log TEXT,
            script_version TEXT,
            scan_directory TEXT,
            user TEXT,
            database_path TEXT,
            files_processed INTEGER
        )
    """)

    # Create file_statistics table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS file_statistics (
            scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_start_time TEXT,
            total_files INTEGER,
            potential_duplicates INTEGER,
            duplicate_file_info TEXT,
            scan_directory TEXT,
            FOREIGN KEY (scan_start_time) REFERENCES metrics(start_time)
        )
    """)

    # Create scan_history table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_history (
            directory TEXT PRIMARY KEY,
            last_scan_time TEXT
        )
    """)

    conn.commit()
    debug_print("Database initialized or already exists.")
    return conn

def calculate_md5(filepath):
    """Calculates the MD5 hash of a file."""
    if os.path.isfile(filepath):
        try:
            with open(filepath, 'rb') as f:
                file_hash = hashlib.md5()
                while chunk := f.read(8192):
                    file_hash.update(chunk)
            return file_hash.hexdigest()
        except OSError as e:
            debug_print(f"Warning: Could not read file '{filepath}' to calculate MD5: {e}")
            return ""
    else:
        debug_print(f"Warning: File not found: {filepath}")
        return ""

def get_file_size_mb(filepath):
    """Gets the file size in megabytes."""
    if os.path.isfile(filepath):
        try:
            size_bytes = os.path.getsize(filepath)
            return size_bytes / (1024 * 1024)
        except OSError as e:
            debug_print(f"Warning: Could not get size for file '{filepath}': {e}")
            return 0.000
    else:
        return 0.000

def get_file_create_time(filepath):
    """Gets the file creation time as a human-readable string."""
    if os.path.isfile(filepath):
        try:
            timestamp = os.path.getctime(filepath)
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except OSError as e:
            debug_print(f"Warning: Could not get creation time for file '{filepath}': {e}")
            return None
    else:
        return None

def get_file_mod_time(filepath):
    """Gets the file modification time as a human-readable string."""
    if os.path.isfile(filepath):
        try:
            timestamp = os.path.getmtime(filepath)
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except OSError as e:
            debug_print(f"Warning: Could not get modification time for file '{filepath}': {e}")
            return None
    else:
        return None

def process_and_log_file(filepath, conn):
    """Processes a single file and logs its information to the database."""
    filename = os.path.basename(filepath)
    if filename == os.path.basename(__file__):  # Skip the script itself
        return

    simplified_filename = os.path.splitext(filename)[0]
    extension = os.path.splitext(filename)[1].lstrip('.')

    md5 = calculate_md5(filepath)
    size_mb = get_file_size_mb(filepath)
    create_time = get_file_create_time(filepath)
    mod_time = get_file_mod_time(filepath)

    debug_print(f"Processing file: \"{filename}\"")

    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO files (filepath, filename, md5, simplified_filename, size_mb, created_time, modified_time, extension, is_potential_duplicate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
        """, (filepath, filename, md5, simplified_filename, size_mb, create_time, mod_time, extension))
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error interacting with database for file '{filename}': {e}")
        return False

def scan_and_log_directory(conn):
    """Scans the directory and logs file information to the database."""
    debug_print("Scanning directory and logging file information...")
    start_time = time.time()
    error_count = 0
    error_log = ""
    processed_files = 0

    for root, _, files in os.walk(FILE_DIRECTORY):
        if root == FILE_DIRECTORY: # Only process files in the top-level directory
            for filename in files:
                file_path = os.path.join(root, filename)
                if process_and_log_file(file_path, conn):
                    processed_files += 1
                else:
                    error_count += 1
                    error_log += f"{datetime.now()} - Error processing file '{file_path}'\n"

                if processed_files % PROGRESS_INTERVAL == 0:
                    print(f"\rScanning: Processed {processed_files} files...", end="", flush=True)

    end_time = time.time()
    duration = int(end_time - start_time)
    debug_print(f"Scanning and logging completed in {duration} seconds.")
    print()  # Newline after scan

    if error_count > 0:
        print(f"Encountered {error_count} errors during file processing.")
        if DEBUG_MODE:
            print("--- Error Log ---")
            print(error_log)
        print()

    return duration, error_count, error_log, processed_files

def has_scanned_before(conn):
    """Checks if the directory has been scanned before."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM scan_history WHERE directory=?", (FILE_DIRECTORY,))
    count = cursor.fetchone()[0]
    return count > 0

def update_scan_history(conn):
    """Updates the scan history."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO scan_history (directory, last_scan_time) VALUES (?, ?)", (FILE_DIRECTORY, now))
    conn.commit()
    debug_print(f"Updated scan history for '{FILE_DIRECTORY}' to '{now}'.")

def update_database(conn):
    """Updates the database based on current directory contents."""
    debug_print(f"Updating database for directory '{FILE_DIRECTORY}'...")
    start_time = time.time()
    current_files = set(os.path.join(FILE_DIRECTORY, f) for f in os.listdir(FILE_DIRECTORY) if os.path.isfile(os.path.join(FILE_DIRECTORY, f)))

    cursor = conn.cursor()
    cursor.execute("SELECT filepath FROM files WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
    db_files = set(row[0] for row in cursor.fetchall())

    files_to_add = current_files - db_files
    files_to_remove = db_files - current_files
    processed_count = 0

    print("\nUpdating database:")
    print(f"Adding {len(files_to_add)} new files.")
    for file_path in files_to_add:
        if process_and_log_file(file_path, conn):
            processed_count += 1
            if processed_count % PROGRESS_INTERVAL == 0:
                print(f"\r  Adding: Processed {processed_count} new files...", end="", flush=True)
    print()

    processed_count = 0
    print(f"Removing {len(files_to_remove)} deleted files.")
    for file_path in files_to_remove:
        try:
            cursor.execute("DELETE FROM files WHERE filepath=?", (file_path,))
            conn.commit()
            processed_count += 1
            if processed_count % PROGRESS_INTERVAL == 0:
                print(f"\r  Removing: Processed {processed_count} deleted files...", end="", flush=True)
        except sqlite3.Error as e:
            print(f"Error removing file '{file_path}' from database: {e}")
    print()

    end_time = time.time()
    duration = int(end_time - start_time)
    debug_print(f"Database update completed in {duration} seconds.")

def log_script_metrics(conn, start_time, end_time, scan_duration, errors, error_log, files_processed):
    """Logs script metrics to the database."""
    db_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    scan_start_time_verbose = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
    scan_end_time_verbose = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')

    duration_hours = scan_duration // 3600
    duration_minutes = (scan_duration % 3600) // 60
    duration_seconds = scan_duration % 60
    scan_duration_verbose = f"{duration_hours} hours {duration_minutes} minutes {duration_seconds} seconds"

    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO metrics (start_time, end_time, scan_duration_seconds, scan_duration_verbose, errors_encountered, error_log, script_version, scan_directory, user, database_path, files_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (scan_start_time_verbose, scan_end_time_verbose, scan_duration, scan_duration_verbose, errors, error_log, SCRIPT_VERSION, FILE_DIRECTORY, os.getlogin(), DATABASE_FILE, files_processed))
        conn.commit()
        debug_print(f"Script metrics logged to database at {db_start_time}.")
    except sqlite3.Error as e:
        print(f"Error logging script metrics: {e}")

def mark_duplicates(conn):
    """Marks duplicate files in the database."""
    debug_print("Examining database for duplicate files...")
    cursor = conn.cursor()

    # Reset duplicate flags for the current directory
    cursor.execute("UPDATE files SET is_potential_duplicate = 0 WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
    conn.commit()

    # Mark duplicates by filename (excluding the script itself)
    cursor.execute("""
        UPDATE files
        SET is_potential_duplicate = 1
        WHERE filepath IN (
            SELECT T1.filepath
            FROM files T1, files T2
            WHERE T1.filename = T2.filename
              AND T1.filepath != T2.filepath
              AND T1.filename != ?
              AND T1.filepath LIKE ?
              AND T2.filepath LIKE ?
        )
    """, (os.path.basename(__file__), FILE_DIRECTORY + '%', FILE_DIRECTORY + '%'))
    conn.commit()
    debug_print("Finished marking filename duplicates.")

    # Mark duplicates by MD5 sum (excluding empty MD5s and self-matches)
    cursor.execute("""
        UPDATE files
        SET is_potential_duplicate = 1
        WHERE filepath IN (
            SELECT T1.filepath
            FROM files T1, files T2
            WHERE T1.md5 = T2.md5
              AND T1.filepath != T2.filepath
              AND T1.md5 != ''
              AND T1.filepath LIKE ?
              AND T2.filepath LIKE ?
        )
    """, (FILE_DIRECTORY + '%', FILE_DIRECTORY + '%'))
    conn.commit()
    debug_print("Finished marking MD5 duplicates.")

def analyze_and_log_duplicates(conn):
    """Analyzes duplicates and logs statistics."""
    debug_print("Analyzing duplicates and logging statistics...")
    start_time = time.time()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM files WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
    total_files = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM files WHERE is_potential_duplicate = 1 AND filepath LIKE ?", (FILE_DIRECTORY + '%',))
    potential_duplicates = cursor.fetchone()[0]

    duplicate_info = {}
    cursor.execute("SELECT md5 FROM files WHERE is_potential_duplicate = 1 AND md5 != '' AND filepath LIKE ? GROUP BY md5 HAVING COUNT(*) > 1", (FILE_DIRECTORY + '%',))
    duplicate_md5s = [row[0] for row in cursor.fetchall()]

    for md5 in duplicate_md5s:
        cursor.execute("SELECT filepath FROM files WHERE md5=? AND filepath LIKE ?", (md5, FILE_DIRECTORY + '%'))
        duplicate_info[md5] = [row[0] for row in cursor.fetchall()]

    import json
    duplicate_info_json = json.dumps(duplicate_info)

    scan_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        cursor.execute("""
            INSERT INTO file_statistics (scan_start_time, total_files, potential_duplicates, duplicate_file_info, scan_directory)
            VALUES (?, ?, ?, ?, ?)
        """, (scan_start_time, total_files, potential_duplicates, duplicate_info_json, FILE_DIRECTORY))
        conn.commit()
        debug_print("Duplicate statistics logged.")
    except sqlite3.Error as e:
        print(f"Error logging duplicate statistics: {e}")

    end_time = time.time()
    duration = int(end_time - start_time)
    debug_print(f"Duplicate analysis and statistics logging completed in {duration} seconds.")
    print(f"Found {potential_duplicates} potential duplicate files (marked in database).")
    debug_print(f"Duplicate file information: {duplicate_info_json}")

def main():
    script_start_time = time.time()

    check_and_create_dirs()
    conn = initialize_database()

    print(f"DUPer.py - Version {SCRIPT_VERSION}")
    print(f"Scanning directory: {FILE_DIRECTORY}")
    print()

    if has_scanned_before(conn):
        print("Directory has been scanned before. Updating database...")
        update_database(conn)
        scan_duration = int(time.time() - script_start_time) # Approximate duration for update
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
        processed_files = cursor.fetchone()[0]
        errors = 0 # Assume no errors during update for simplicity
        error_log = ""
    else:
        print("First time scanning this directory. Performing full scan...")
        scan_duration, errors, error_log, processed_files = scan_and_log_directory(conn)

    script_end_time = time.time()
    total_duration = int(script_end_time - script_start_time)

    log_script_metrics(conn, script_start_time, script_end_time, total_duration, errors, error_log, processed_files)
    mark_duplicates(conn)
    analyze_and_log_duplicates(conn)
    update_scan_history(conn)

    conn.close()
    print("--- Script Finished ---")

if __name__ == "__main__":
    main()
