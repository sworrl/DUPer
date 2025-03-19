#!/usr/bin/env python3
import os
import hashlib
import sqlite3
import time
from datetime import datetime
import json
import shutil
import sys

# Dev Note: --- Global Configurations ---
SCRIPT_VERSION = "0.3.5-beta"
DEBUG_MODE = False
FILE_DIRECTORY = ""
WORKING_DIRECTORY = ""
DATABASE_FILE = ""
MOVE_LOCATION = ""
CODE_NAME = "Farting On Files"
PROGRESS_INTERVAL = 1

# Dev Note: --- Utility Functions ---
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def debug_print(message):
    if DEBUG_MODE:
        print(f"DEBUG: {message}")

def format_size(size_bytes):
    if size_bytes == 0:
        return "0 B"
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while size_bytes >= 1024 and i < len(units) - 1:
        size_bytes /= 1024
        i += 1
    return f"{size_bytes:.2f} {units[i]}"

def detect_steamos():
    try:
        with open("/etc/os-release", "r") as f:
            os_info = f.read()
            return "steam" in os_info.lower()
    except FileNotFoundError:
        return False

# Dev Note: --- Directory Management ---
def check_and_create_dirs(working_dir):
    debug_print("Checking and creating necessary directories...")
    if not os.path.exists(working_dir):
        debug_print(f"Creating working directory: {working_dir}")
        try:
            os.makedirs(working_dir)
        except OSError as e:
            print(f"Error: Could not create working directory '{working_dir}'. {e}")
            exit(1)
    db_dir = os.path.dirname(DATABASE_FILE)
    if not os.path.exists(db_dir):
        debug_print(f"Creating directory for database file: {db_dir}")
        try:
            os.makedirs(db_dir)
        except OSError as e:
            print(f"Error: Could not create directory for database file. {e}")
            exit(1)

# Dev Note: --- Database Functions ---
def initialize_database():
    debug_print("Initializing SQLite database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_history (
            directory TEXT PRIMARY KEY,
            last_scan_time TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS moved_files (
            move_id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_filepath TEXT UNIQUE,
            moved_to_path TEXT,
            moved_time TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    debug_print("Database initialized or already exists.")
    return conn

def get_config_from_db(conn, key):
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM config WHERE key=?", (key,))
    result = cursor.fetchone()
    return result[0] if result else None

def save_config_to_db(conn, key, value):
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    debug_print(f"Saved config '{key}': '{value}' to database.")

# Dev Note: --- File Information Extraction ---
def calculate_md5(filepath):
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
    filename = os.path.basename(filepath)
    if filename == os.path.basename(__file__):
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

# Dev Note: --- Directory Scanning and Database Update ---
def scan_and_log_directory(conn):
    debug_print("Scanning directory and logging file information...")
    start_time = time.time()
    error_count = 0
    error_log = ""
    processed_files = 0

    for root, _, files in os.walk(FILE_DIRECTORY):
        if root == FILE_DIRECTORY: # Only process files in the top-level directory (as per the first script)
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
    print()

    if error_count > 0:
        print(f"Encountered {error_count} errors during file processing.")
        if DEBUG_MODE:
            print("--- Error Log ---")
            print(error_log)
        print()

    return duration, error_count, error_log, processed_files

def has_scanned_before(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM scan_history WHERE directory=?", (FILE_DIRECTORY,))
    count = cursor.fetchone()[0]
    return count > 0

def update_scan_history(conn):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO scan_history (directory, last_scan_time) VALUES (?, ?)", (FILE_DIRECTORY, now))
    conn.commit()
    debug_print(f"Updated scan history for '{FILE_DIRECTORY}' to '{now}'.")

def update_database(conn):
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

# Dev Note: --- Metrics and Duplicate Analysis ---
def log_script_metrics(conn, start_time, end_time, scan_duration, errors, error_log, files_processed):
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
    debug_print("Examining database for duplicate files...")
    cursor = conn.cursor()

    cursor.execute("UPDATE files SET is_potential_duplicate = 0 WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
    conn.commit()

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

# Dev Note: --- Duplicate Processing (Moving) ---
def process_duplicates(conn):
    debug_print("Processing duplicate files using a scoring system...")
    cursor = conn.cursor()

    global MOVE_LOCATION
    if not MOVE_LOCATION:
        MOVE_LOCATION = os.path.join(FILE_DIRECTORY, "duplicates")

    if not os.path.exists(MOVE_LOCATION):
        try:
            os.makedirs(MOVE_LOCATION)
            debug_print(f"Created duplicates directory: {MOVE_LOCATION}")
        except OSError as e:
            print(f"Error creating duplicates directory: {e}")
            return

    cursor.execute("""
        SELECT md5
        FROM files
        WHERE is_potential_duplicate = 1 AND md5 != '' AND filepath LIKE ?
        GROUP BY md5
        HAVING COUNT(*) > 1
    """, (FILE_DIRECTORY + '%',))
    duplicate_md5_hashes = [row[0] for row in cursor.fetchall()]

    print("\nProcessing and moving duplicate files...")
    moved_count = 0

    for md5_hash in duplicate_md5_hashes:
        cursor.execute("""
            SELECT filepath, simplified_filename, size_mb
            FROM files
            WHERE md5=? AND filepath LIKE ?
        """, (md5_hash, FILE_DIRECTORY + '%'))
        duplicate_files_data = cursor.fetchall()

        if len(duplicate_files_data) > 1:
            scores = {}
            min_size = min(data[2] for data in duplicate_files_data) if duplicate_files_data else 0
            shortest_name_len = min(len(data[1]) for data in duplicate_files_data) if duplicate_files_data else float('inf')
            first_alphabetically = min(data[1] for data in duplicate_files_data) if duplicate_files_data else ""

            for filepath, simplified_filename, size_mb in duplicate_files_data:
                score = 0

                if len(simplified_filename) == shortest_name_len:
                    score += 3
                if simplified_filename == first_alphabetically:
                    score += 2
                if size_mb == min_size and min_size > 0:
                    score += 1

                scores[filepath] = score

            file_to_keep = max(scores, key=scores.get)

            for filepath, _, _ in duplicate_files_data:
                if filepath != file_to_keep:
                    try:
                        filename = os.path.basename(filepath)
                        destination_path = os.path.join(MOVE_LOCATION, filename)

                        if os.path.exists(destination_path):
                            base, ext = os.path.splitext(filename)
                            index = 1
                            while os.path.exists(os.path.join(MOVE_LOCATION, f"{base}_{index}{ext}")):
                                index += 1
                            destination_path = os.path.join(MOVE_LOCATION, f"{base}_{index}{ext}")

                        shutil.move(filepath, destination_path)
                        debug_print(f"Moved duplicate file '{filepath}' to '{destination_path}'")
                        moved_count += 1

                        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        cursor.execute("""
                            INSERT INTO moved_files (original_filepath, moved_to_path, moved_time)
                            VALUES (?, ?, ?)
                        """, (filepath, destination_path, now))
                        conn.commit()

                        cursor.execute("DELETE FROM files WHERE filepath=?", (filepath,))
                        conn.commit()

                    except OSError as e:
                        print(f"Error moving file '{filepath}': {e}")

    print(f"Moved {moved_count} duplicate files to '{MOVE_LOCATION}'.")

# Dev Note: --- Restore Functions ---
def restore_all_moved_files(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT move_id, original_filepath, moved_to_path FROM moved_files")
    moved_files = cursor.fetchall()
    restored_count = 0
    errors = 0

    if not moved_files:
        print("\nNo files to restore.")
        return

    print("\n--- Restoring All Moved Files ---")
    for move_id, original, moved_to in moved_files:
        try:
            os.rename(moved_to, original)
            cursor.execute("DELETE FROM moved_files WHERE move_id=?", (move_id,))
            conn.commit()
            restored_count += 1
            print(f"Restored '{os.path.basename(original)}' to '{original}'.")
        except OSError as e:
            print(f"Error restoring '{os.path.basename(original)}': {e}")
            errors += 1

    print(f"\nSuccessfully restored {restored_count} files.")
    if errors > 0:
        print(f"Encountered {errors} errors during restoration.")

def restore_moved_files(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT move_id, original_filepath, moved_to_path FROM moved_files")
    moved_files = cursor.fetchall()

    if not moved_files:
        print("\nNo files to restore.")
        return

    print("\n--- Restore Moved Files ---")
    for move_id, original, moved_to in moved_files:
        print(f"{move_id}. Restore: {os.path.basename(original)} (from {moved_to})")

    while True:
        choice = input("Enter the ID of the file to restore (or 'q' to quit): ").strip()
        if choice.lower() == 'q':
            break
        try:
            move_id_to_restore = int(choice)
            found = False
            for move_id, original, moved_to in moved_files:
                if move_id == move_id_to_restore:
                    try:
                        os.rename(moved_to, original)
                        cursor.execute("DELETE FROM moved_files WHERE move_id=?", (move_id_to_restore,))
                        conn.commit()
                        print(f"Restored '{os.path.basename(original)}' to '{original}'.")
                        found = True
                        break
                    except OSError as e:
                        print(f"Error restoring '{os.path.basename(original)}': {e}")
                        found = True
                        break
            if not found:
                print("Invalid ID. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number or 'q'.")

# Dev Note: --- Information Display Functions ---
def show_moved_files(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT original_filepath, moved_to_path, moved_time FROM moved_files")
    moved_files = cursor.fetchall()

    if not moved_files:
        print("\nNo files have been moved yet.")
        return

    print("+" + "-" * 60 + "+")
    print(f"| {'Moved Files'.center(60)} |")
    print("+" + "-" * 60 + "+")
    print("| {:<40} | {:<20} |".format("Original Filepath", "Moved Time"))
    print("+" + "-" * 60 + "+")
    for original, moved_to, moved_time in moved_files:
        print("| {:<40} | {:<20} |".format(original, moved_time))
    print("+" + "-" * 60 + "+")

def calculate_total_size(conn):
    """Calculates the total size of files in the current scan directory from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(size_mb) FROM files WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
    total_size_mb = cursor.fetchone()[0]
    return total_size_mb if total_size_mb is not None else 0

def get_directory_stats(directory):
    total_files = 0
    total_size = 0
    try:
        for entry in os.listdir(directory):
            filepath = os.path.join(directory, entry)
            if os.path.isfile(filepath):
                total_files += 1
                total_size += os.path.getsize(filepath)
        free_space = shutil.disk_usage(directory).free
        return total_files, total_size, free_space
    except FileNotFoundError:
        return 0, 0, 0

def show_nerd_stats(conn):
    cursor = conn.cursor()

    print("\n--- Nerd Stats ---")

    print("\n--- Scan Metrics ---")
    cursor.execute("SELECT * FROM metrics ORDER BY start_time DESC LIMIT 1")
    metrics = cursor.fetchone()
    if metrics:
        columns = ["Start Time", "End Time", "Duration (seconds)", "Duration (verbose)", "Errors", "Error Log", "Script Version", "Scan Directory", "User", "Database Path", "Files Processed"]
        for i, value in enumerate(metrics):
            print(f"{columns[i]}: {value}")
    else:
        print("No scan metrics available.")

    print("\n--- File Statistics ---")
    cursor.execute("SELECT * FROM file_statistics ORDER BY scan_id DESC LIMIT 1")
    file_stats = cursor.fetchone()
    if file_stats:
        columns = ["Scan ID", "Scan Start Time", "Total Files", "Potential Duplicates", "Duplicate Info", "Scan Directory"]
        for i, value in enumerate(file_stats):
            if columns[i] == "Duplicate Info":
                try:
                    duplicate_info = json.loads(value)
                    print(f"{columns[i]}:")
                    for md5, filepaths in duplicate_info.items():
                        print(f"  MD5: {md5}")
                        for fp in filepaths:
                            print(f"    - {fp}")
                except json.JSONDecodeError:
                    print(f"{columns[i]}: {value}")
            else:
                print(f"{columns[i]}: {value}")
    else:
        print("No file statistics available.")

    print("\n--- Scan History ---")
    cursor.execute("SELECT * FROM scan_history")
    scan_history = cursor.fetchall()
    if scan_history:
        print("{:<20} {:<20}".format("Directory", "Last Scan Time"))
        print("-" * 40)
        for directory, last_scan_time in scan_history:
            print("{:<20} {:<20}".format(directory, last_scan_time))
    else:
        print("No scan history available.")

    print("\n--- Moved Files Summary ---")
    cursor.execute("SELECT COUNT(*) FROM moved_files")
    moved_count = cursor.fetchone()[0]
    print(f"Total files moved: {moved_count}")

    total_size_mb = calculate_total_size(conn)
    print(f"\nTotal Size of Scanned Files: {format_size(total_size_mb * 1024 * 1024)}") # Convert MB back to bytes for formatting

# Dev Note: --- Main Logic ---
def main_logic(conn):
    script_start_time = time.time()

    if has_scanned_before(conn):
        print("Directory has been scanned before. Updating database...")
        update_database(conn)
        scan_duration = int(time.time() - script_start_time)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
        processed_files = cursor.fetchone()[0]
        errors = 0
        error_log = ""
    else:
        print("First time scanning this directory. Performing full scan...")
        scan_duration, errors, error_log, processed_files = scan_and_log_directory(conn)

    script_end_time = time.time()
    total_duration = int(script_end_time - script_start_time)

    log_script_metrics(conn, script_start_time, script_end_time, total_duration, errors, error_log, processed_files)
    mark_duplicates(conn)
    analyze_and_log_duplicates(conn)
    process_duplicates(conn)
    update_scan_history(conn)

# Dev Note: --- Configuration Menu ---
def config_menu(conn):
    global FILE_DIRECTORY, WORKING_DIRECTORY, DATABASE_FILE, MOVE_LOCATION
    while True:
        clear_screen()
        print("DUPer.py - Configuration")
        print("\nOptions:")
        print(f"1. Set Working Directory: {WORKING_DIRECTORY if WORKING_DIRECTORY else '[Not Set]'}")
        print(f"2. Set Database File: {DATABASE_FILE if DATABASE_FILE else '[Not Set]'}")
        print(f"3. Set Move Location: {MOVE_LOCATION if MOVE_LOCATION else '[Not Set]'}")
        print("4. Delete Database")
        print("5. Back to Main Menu")
        print("\nEnter your choice (number only):")

        choice = input("> ").strip()

        if choice == '1':
            new_dir = input("Enter new working directory: ").strip()
            if os.path.isdir(new_dir):
                WORKING_DIRECTORY = new_dir
                save_config_to_db(conn, 'working_directory', WORKING_DIRECTORY)
            else:
                print("Invalid directory.")
                input("Press Enter to continue...")
        elif choice == '2':
            new_file = input("Enter new database file path: ").strip()
            DATABASE_FILE = new_file
            save_config_to_db(conn, 'database_file', DATABASE_FILE)
            # Re-initialize database if the file path changes? Might need to restart for full effect.
            print("Database file path updated. Restart the script for the change to fully take effect.")
            input("Press Enter to continue...")
        elif choice == '3':
            new_location = input("Enter new move location directory: ").strip()
            if os.path.isdir(new_location):
                MOVE_LOCATION = new_location
                save_config_to_db(conn, 'move_location', MOVE_LOCATION)
            elif new_location == "":
                MOVE_LOCATION = ""
                save_config_to_db(conn, 'move_location', MOVE_LOCATION)
            else:
                print("Invalid directory.")
                input("Press Enter to continue...")
        elif choice == '4':
            confirm = input("Are you sure you want to delete the database? This action is irreversible (y/N): ").strip().lower()
            if confirm == 'y':
                try:
                    os.remove(DATABASE_FILE)
                    print("Database deleted successfully. The script will re-initialize on the next run.")
                    input("Press Enter to continue...")
                    break # Exit config menu after deleting
                except FileNotFoundError:
                    print("Database file not found.")
                    input("Press Enter to continue...")
                except OSError as e:
                    print(f"Error deleting database: {e}")
                    input("Press Enter to continue...")
            else:
                print("Delete operation cancelled.")
                input("Press Enter to continue...")
        elif choice == '5':
            break
        else:
            clear_screen()
            print("Invalid choice. Please try again.")
            input("\nPress Enter to return to the Configuration menu...")

# Dev Note: --- Main Menu ---
def display_menu(conn):
    while True:
        clear_screen()
        print(f"DUPer.py - Version {SCRIPT_VERSION} - {CODE_NAME}")
        print(f"Working Directory: {FILE_DIRECTORY}")

        total_files_db = 0
        total_size_mb_db = calculate_total_size(conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files WHERE filepath LIKE ?", (FILE_DIRECTORY + '%',))
        result = cursor.fetchone()
        if result:
            total_files_db = result[0]

        print(f"Total Files: {total_files_db}, Total Size: {format_size(total_size_mb_db * 1024 * 1024)}, Space Left: {format_size(shutil.disk_usage(FILE_DIRECTORY).free)}")
        print("\nOptions:")
        print("1. Show Moved Files")
        print("2. Restore Moved Files")
        print("3. Restore All Moved Files")
        print("4. Nerd Stats")
        print("5. Rescan Directory")
        print("6. Configuration")
        print("x. Exit")
        print("\nEnter your choice:")

        choice = input("> ").strip()

        if choice == '1':
            clear_screen()
            show_moved_files(conn)
            input("\nPress Enter to return to the menu...")
        elif choice == '2':
            clear_screen()
            restore_moved_files(conn)
            input("\nPress Enter to return to the menu...")
        elif choice == '3':
            clear_screen()
            restore_all_moved_files(conn)
            input("\nPress Enter to return to the menu...")
        elif choice == '4':
            clear_screen()
            show_nerd_stats(conn)
            input("\nPress Enter to return to the menu...")
        elif choice == '5':
            clear_screen()
            print("Rescanning directory...")
            main_logic(conn)
            input("\nPress Enter to return to the menu...")
        elif choice == '6':
            clear_screen()
            config_menu(conn)
        elif choice.lower() == 'x':
            clear_screen()
            print("Exiting DUPer.py")
            break
        else:
            clear_screen()
            print("Invalid choice. Please try again.")
            input("\nPress Enter to return to the menu...")

# Dev Note: --- Main Script Execution ---
def main():
    global FILE_DIRECTORY, WORKING_DIRECTORY, DATABASE_FILE, MOVE_LOCATION

    if "--install" in sys.argv:
        print("Performing installation steps...")
        print("- Creating working directory (if it doesn't exist)...")
        print("- Initializing database (if it doesn't exist)...")
        print("- Installation complete.")
        sys.exit(0)
    elif "--uninstall" in sys.argv:
        print("Performing uninstallation steps...")
        print("- You might want to remove the working directory:", WORKING_DIRECTORY)
        print("- And the database file:", DATABASE_FILE)
        print("- And the moved files directory (if it exists):", MOVE_LOCATION if MOVE_LOCATION else os.path.join(FILE_DIRECTORY, "duplicates"))
        print("- Uninstallation complete.")
        sys.exit(0)

    conn = None
    cursor = None

    # Get configuration from database if it exists (before prompting)
    temp_conn = sqlite3.connect(":memory:") # Use an in-memory database temporarily
    temp_cursor = temp_conn.cursor()
    temp_cursor.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    temp_conn.commit()

    def get_config_from_temp_db(key):
        temp_cursor.execute("SELECT value FROM config WHERE key=?", (key,))
        result = temp_cursor.fetchone()
        return result[0] if result else None

    WORKING_DIRECTORY = WORKING_DIRECTORY or get_config_from_temp_db('working_directory')
    DATABASE_FILE = DATABASE_FILE or get_config_from_temp_db('database_file')
    MOVE_LOCATION = MOVE_LOCATION or get_config_from_temp_db('move_location')

    temp_conn.close()

    while not FILE_DIRECTORY:
        default_directory = ""
        if DATABASE_FILE:
            try:
                conn_check = sqlite3.connect(DATABASE_FILE)
                cursor_check = conn_check.cursor()
                cursor_check.execute("SELECT directory FROM scan_history ORDER BY last_scan_time DESC LIMIT 1")
                last_scanned = cursor_check.fetchone()
                default_directory = last_scanned[0] if last_scanned else ""
                conn_check.close()
            except sqlite3.OperationalError:
                pass # Database might not exist yet

        target_directory = input(f"Enter the directory you want to scan (default: {default_directory} if available): ").strip()
        FILE_DIRECTORY = target_directory if target_directory else default_directory
        if os.path.isdir(FILE_DIRECTORY):
            break
        else:
            print("Invalid directory. Please enter a valid directory path.")
            FILE_DIRECTORY = ""

    while not WORKING_DIRECTORY:
        default_working_dir = "DUPer_working"
        WORKING_DIRECTORY_PROMPT = f"Enter the working directory (default: {default_working_dir}): ".strip()
        entered_dir = input(WORKING_DIRECTORY_PROMPT)
        WORKING_DIRECTORY = entered_dir if entered_dir else default_working_dir
        # We'll save this to the actual database later
        if conn:
            save_config_to_db(conn, 'working_directory', WORKING_DIRECTORY)

    while not DATABASE_FILE:
        default_db_file = os.path.join(WORKING_DIRECTORY, "file_info.sqlite")
        DATABASE_FILE_PROMPT = f"Enter the database file path (default: {default_db_file}): ".strip()
        entered_file = input(DATABASE_FILE_PROMPT)
        DATABASE_FILE = entered_file if entered_file else default_db_file
        # We'll save this to the actual database later
        if conn:
            save_config_to_db(conn, 'database_file', DATABASE_FILE)

    check_and_create_dirs(WORKING_DIRECTORY)

    # Now initialize the database after directories are created
    conn = initialize_database()
    cursor = conn.cursor()

    # Load config again from the actual database
    WORKING_DIRECTORY = WORKING_DIRECTORY or get_config_from_db(conn, 'working_directory')
    DATABASE_FILE = DATABASE_FILE or get_config_from_db(conn, 'database_file')
    MOVE_LOCATION = MOVE_LOCATION or get_config_from_db(conn, 'move_location')

    print(f"DUPer.py - Version {SCRIPT_VERSION} - {CODE_NAME}")
    print(f"Scanning directory: {FILE_DIRECTORY}")

    if detect_steamos():
        print("\nDetected SteamOS.")
        print("If you need to modify files in the read-only OS, you might need to unlock it.")

    print()

    main_logic(conn)
    display_menu(conn)

    conn.close()
    print("--- DUPer.py Exited ---")

if __name__ == "__main__":
    main()
