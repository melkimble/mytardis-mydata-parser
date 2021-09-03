"""
Run mytardis api creates filter text file for rclone to only upload project specific
runs to gdrive from wasabi
Created By: mkimble
LAST MODIFIED: 09/03/2021
"""
from mytd_parser.mytardis_api import DatasetFilter
from mytd_parser.settings import MAINE_EDNA_EXPERIMENT_ID
from mytd_parser.settings import MAINE_EDNA_RCLONE_GDRIVE_FILTER_FILENAME as dataset_filter_filename
from mytd_parser.settings import MYTARDIS_MODEL_NAME as model_name

# parameters to filter datasets by, to see full list, go to:
# https://mytardis.maine-edna.org/api/v1/swagger/#!/dataset/dataset_list
# experiments__id = 1 is maine-edna
params_dict = {
    "experiments__id": MAINE_EDNA_EXPERIMENT_ID,
}

mytd_df = DatasetFilter(model_name, params_dict, dataset_filter_filename)
mytd_df.write_filter_file()
