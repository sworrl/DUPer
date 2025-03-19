# DUPer.py - Duplicate File Annihilator 
## v0.3.97a-beta (**Code Name: *Dastardly Dog's Dick*)**

[![Version](https://img.shields.io/badge/Version-0.3.97a--beta-blue.svg)](https://github.com/sworrl/DUPer/releases/tag/v0.3.97a-beta)

[![CODENAME](https://img.shields.io/badge/CODENAME-Dastardly_Dogs_Dick-pink.svg)](https://github.com/sworrl/DUPer/releases/tag/v0.3.97a-beta)

## Description

`DUPer.py` is a Python script designed to find and manage duplicate files within a specified directory. It scans the directory, identifies potential duplicates based on filename and MD5 hash, and provides options to move these duplicates to a designated location for review or deletion. The script utilizes a SQLite database to store file information and scan history.

## Features

* **Directory Scanning:** Recursively scans a target directory (with an option for optimized scanning of RetroArch ROM directories).
* **File Information:** Collects file path, name, MD5 hash, size, creation time, modification time, and extension.
* **Duplicate Detection:** Identifies potential duplicates based on matching filenames and MD5 hashes.
* **Ignore Lists:** Allows configuration to ignore specific file extensions for fodder, video, music, and picture files.
* **RetroArch ROMs Mode:** Optimizes scanning for directories containing RetroArch ROMs by only scanning subdirectories with more than 3 files (or the top level if it has files).
* **Duplicate Processing:** Scores duplicate files based on filename length, alphabetical order, and size to determine which file to keep and moves others to a configurable "duplicates" directory.
* **Moved Files Management:** Tracks moved files in the database, allowing users to view and restore them.
* **Configuration Menu:** Provides an interactive menu to set working directory, database file, move location, and toggle ignore settings and RetroArch ROMs mode.
* **Scan History:** Records the history of scanned directories and their last scan times.
* **Metrics and Statistics:** Logs scan duration, errors, and duplicate file statistics.
* **Nerd Stats:** Displays detailed information about the latest scan, file statistics, scan history, and moved files.
* **Install/Uninstall Options:** Includes command-line arguments for basic installation and uninstallation notes.

## Installation

1.  **Ensure Python 3 is installed:** This script requires Python 3 to run. You can check your Python version by running `python3 --version` in your terminal.
2.  **Download the script:** Save the provided Python code as `duper.py`.
3.  **Make it executable (optional):** You can make the script executable by running `chmod +x duper.py` in your terminal.

## Usage

1.  **Navigate to the script's directory:** Open your Linux terminal and navigate to the directory where you saved `duper.py`.
2.  **Run the script:** Execute the script using `python3 duper.py` or `./duper.py` (if you made it executable).
3.  **Enter the target directory:** The script will prompt you to enter the directory you want to scan.
4.  **Main Menu:** After the initial scan (or update if the directory has been scanned before), you will see the main menu with the following options:

    ```
    DUPer.py - Version 0.3.97a-beta - Dastardly Dog's Dick
    Working Directory: /path/to/DUPer_working
    Total Files (Directory): ..., Total Size (Directory): ..., Space Left: ...
    Moved Files Directory: /path/to/DUPer_working/duplicates
        Total Moved Files (Directory): ..., Total Size (Directory): ...
        Total Moved Files (Database): ...

    Ignore Settings:
        Ignore Fodder Files: True
        Ignore Video Files: True
        Ignore Music Files: True
        Ignore Picture Files: True
        RetroArch ROMs Directory Mode: True

    Options:
    1. Show Moved Files
    2. Restore Moved Files
    3. Restore All Moved Files
    4. Nerd Stats
    5. Rescan Directory
    6. Configuration
    x. Exit

    Enter your choice:
    ```

5.  **Follow the menu options:**
    * **1. Show Moved Files:** Displays a list of files that have been moved by the script.
    * **2. Restore Moved Files:** Allows you to select and restore specific moved files back to their original locations.
    * **3. Restore All Moved Files:** Restores all files that have been moved by the script.
    * **4. Nerd Stats:** Shows detailed scan metrics, file statistics, scan history, and moved file information.
    * **5. Rescan Directory:** Forces a rescan of the target directory, updating the database and re-identifying duplicates.
    * **6. Configuration:** Opens the configuration menu to change script settings.
    * **x. Exit:** Closes the script.

## Configuration

The configuration menu (option 6) allows you to modify the following settings:

* **1. Set Working Directory:** The directory where the script will store its working files, including the database and the default location for moved files.
* **2. Set Database File:** The path to the SQLite database file used by the script.
* **3. Set Move Location:** The directory where duplicate files will be moved. If left empty, a `duplicates` directory will be created within the working directory.
* **4. Delete Database:** Deletes the current database file. **Warning:** This action is irreversible and will reset the script's knowledge of scanned files.
* **5. Toggle Ignore Fodder Files (.txt, .ini):** Toggles whether files with `.txt` and `.ini` extensions are ignored during scanning.
* **6. Toggle Ignore Video Files (.mp4, .avi, .mkv, .mov, .wmv, .flv, .webm):** Toggles ignoring video files.
* **7. Toggle Ignore Music Files (.mp3, .wav, .flac, .aac, .ogg, .m4a):** Toggles ignoring music files.
* **8. Toggle Ignore Picture Files (.jpg, .jpeg, .png, .gif, .bmp, .tiff, .webp):** Toggles ignoring picture files.
* **9. Toggle RetroArch ROMs Directory Mode:** Toggles the optimized scanning mode for RetroArch ROM directories.
* **10. Back to Main Menu:** Returns to the main menu.

## Command-line Arguments

The script supports the following command-line arguments:

* `--install`: Prints basic installation steps.
* `--uninstall`: Prints notes on how to perform a basic uninstallation (removing working directory, database, and moved files).

## Ignore Settings

You can configure the script to ignore certain types of files based on their extensions. This can be useful to speed up scanning and avoid processing irrelevant files. The ignore settings can be toggled in the Configuration menu.

## RetroArch ROMs Directory Mode

When enabled, this mode optimizes the scanning process for directories containing RetroArch ROMs. It assumes that ROM files are typically located in subdirectories, and it will only scan subdirectories that contain more than 3 files (or the top-level directory if it contains files directly). This can significantly reduce scanning time for large ROM collections.

## Moved Files

Duplicate files identified by the script are moved to a subdirectory named `duplicates` within the working directory by default. You can configure a different move location in the Configuration menu. When RetroArch ROMs mode is enabled, the script attempts to preserve the subdirectory structure when moving duplicate ROM files.

## Restoration

The script provides options to restore moved files back to their original locations. You can restore specific files by their ID or restore all moved files at once.

## Nerd Stats

The "Nerd Stats" option provides detailed information about the latest scan, including start and end times, duration, errors encountered, total files scanned, potential duplicates found, and a breakdown of duplicate files by their MD5 hash. It also shows the scan history and a summary of moved files.

## Contributing

Contributions to this script are welcome. Feel free to fork the repository, make improvements, and submit pull requests.

## License



This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
