"""
mytd_parser python settings
Created By: mkimble
"""
import os

current_working_dir = os.getcwd()
BASE_DIR = os.path.dirname(current_working_dir)
BASE_DIR = BASE_DIR.replace('\\', '/')

MAIN_INPUT_DIR = BASE_DIR+"/NGS_Outputs/Staging/"
MAIN_COPY_DIR = BASE_DIR+"/NGS_Outputs/Original/"
MAIN_OUTPUT_DIR = BASE_DIR+"/NGS_Outputs/Parsed/"

# error logging
LOG_FILE_DIR = BASE_DIR+'/logs/'

# Mydata cfg settings
DATA_DIRECTORY = MAIN_OUTPUT_DIR
FOLDER_STRUCTURE = 'User Group / Dataset'
MYTARDIS_URL = 'https://mytardis.maine-edna.org'




