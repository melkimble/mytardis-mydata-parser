"""
Run server parse to move fastq files to one directory
Creates filter text file for rclone to only upload project specific
runs to gdrive
* UMaine2/{project}/{run_id}/*.fastq.gz
Created By: mkimble
LAST MODIFIED: 08/31/2021
"""

from mytd_parser.parse_server_copy import server_parse
from mytd_parser.mytardis_api import DatasetFilter
from mytd_parser.settings import MAINE_EDNA_EXPERIMENT_ID
from mytd_parser.settings import MAINE_EDNA_RCLONE_GDRIVE_FILTER_FILENAME as dataset_filter_filename
from mytd_parser.settings import MYTARDIS_MODEL_NAME as model_name

server_parse()

params_dict = {
    "experiments__id": MAINE_EDNA_EXPERIMENT_ID,
}

mytd_df = DatasetFilter(model_name, params_dict, dataset_filter_filename)
mytd_df.write_filter_file()

