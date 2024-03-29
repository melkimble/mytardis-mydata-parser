"""
mytd_parser python settings
Created By: mkimble
"""
import os

# grab directory of settings.py
parser_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BASE_DIR = parser_dir.replace('\\', '/')

# error logging
LOG_FILE_DIR = BASE_DIR+"/logs/"

# parse_seq_run.py settings
MISEQ_STAGING_DIR = "D:/NGS_Outputs/Staging/"
MISEQ_UPLOAD_DIR = "D:/NGS_Outputs/Upload/"
MISEQ_BACKUP_DIR = "D:/NGS_Outputs/Backup/"

MISEQ_EXTRA_BACKUP_DIRS = []

# Mydata cfg
MISEQ_DATA_DIRECTORY = MISEQ_UPLOAD_DIR
FOLDER_STRUCTURE = 'User Group / Dataset'
MYTARDIS_URL = 'https://mytardis.maine-edna.org'

# parse_server_copy.py settings
SERVER_DOWNLOAD_DIR = "/UMaine2/mytardis_download/CORE/"
SERVER_OUTPUT_DIR = "/UMaine2/Maine_eDNA/"
SERVER_UPLOAD_BR_DIR = "/UMaine2/mytardis_upload/"

# Mydata cfg
SERVER_DATA_DIRECTORY = SERVER_UPLOAD_BR_DIR

# mytd_api cfg
MYTARDIS_API_URL = "https://mytardis.maine-edna.org:443/api/v1/"
MYTARDIS_MODEL_NAME = "dataset"
MAINE_EDNA_EXPERIMENT_ID = 1
MYTARDIS_API_USER = os.environ.get('MYTARDIS_API_USER')
MYTARDIS_API_PASSWORD = os.environ.get('MYTARDIS_API_PASSWORD')
RCLONE_FILTER_FILE_DIR = BASE_DIR+"/configs/"
RCLONE_WASABI_FILTER_FILENAME = "filter-wasabi-files"
MAINE_EDNA_RCLONE_GDRIVE_FILTER_FILENAME = "medna_filter-gdrive-files"

# GDrive FastqUpload
GDRIVE_PRIVATE_KEY = RCLONE_FILTER_FILE_DIR+"your_gdrive_key_file.json"
GSHEETS_SPREADSHEET_URL = "https://your_gsheet_url"
GSHEETS_WORKSHEET_NAME = "your_worksheet_name"
