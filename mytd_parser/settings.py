"""
mytd_parser python settings
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""
import os

## grab directory of settings.py
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