"""
mytd_parser python settings
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""
import os
current_working_dir = os.getcwd()

# error logging
LOG_BASE_DIR = current_working_dir.replace('\\', '/')
LOG_FILE_DIR = LOG_BASE_DIR+'/logs/'

MISEQ_BASE_DIR = os.path.dirname(os.path.dirname(current_working_dir))
MISEQ_BASE_DIR = MISEQ_BASE_DIR.replace('\\', '/')

# parse_seq_run.py settings
MISEQ_STAGING_DIR = MISEQ_BASE_DIR+"NGS_Outputs/Staging/"
MISEQ_UPLOAD_DIR = MISEQ_BASE_DIR+"NGS_Outputs/Upload/"
MISEQ_BACKUP_DIR = MISEQ_BASE_DIR+"NGS_Outputs/Backup/"

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





