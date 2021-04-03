"""
mytd_parser python settings
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""
import os

#current_working_dir = os.getcwd()
#BASE_DIR = os.path.dirname(current_working_dir)
#BASE_DIR = BASE_DIR.replace('\\', '/')

BASE_DIR = "D:/GitHub/mytardis-mydata-parser"

# parse_seq_run.py settings
MISEQ_STAGING_DIR = BASE_DIR+"/NGS_Outputs/Staging/"
MISEQ_UPLOAD_DIR = BASE_DIR+"/NGS_Outputs/Upload/"
MISEQ_BACKUP_DIR = BASE_DIR+"/NGS_Outputs/Backup/"

MISEQ_EXTRA_BACKUP_DIRS = [BASE_DIR+"/NGS_Outputs/Backup2/"]

# error logging
LOG_FILE_DIR = BASE_DIR+'/logs/'

# Mydata cfg
MISEQ_DATA_DIRECTORY = MISEQ_UPLOAD_DIR
FOLDER_STRUCTURE = 'User Group / Dataset'
MYTARDIS_URL = 'https://mytardis.maine-edna.org'

# parse_server_copy.py settings
SERVER_STAGING_DIR = "/UMaine2/mytardis/CORE/"
SERVER_OUTPUT_DIR = "/UMaine2/Maine_eDNA/"
SERVER_OUTPUT_HPC_DIR = "/UMaine2/HPC_Results/"

# Mydata cfg
SERVER_DATA_DIRECTORY = SERVER_OUTPUT_HPC_DIR





