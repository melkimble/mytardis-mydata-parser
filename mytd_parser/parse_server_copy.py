"""
parse_server_copy.py
Filter and parse server copies of MiSeq Illumina runs and reformat for bioinformatics pipelines
Created By: mkimble
LAST MODIFIED: 08/31/2021
"""

from . import settings
import os, glob
import pandas as pd
from shutil import copy2, move
from .logger_settings import api_logger
from pathlib import *
import datetime
from datetime import datetime
from mytd_parser.parse_seq_run import get_creation_dt, check_rta_complete, get_run_id_xml, \
    get_run_completion_time_xml


def check_upload_complete(fastq_list_filepath, run_dir):
    api_logger.info('[START] check_upload_complete')
    fastq_list_df = pd.read_csv(fastq_list_filepath)

    fastq_list = fastq_list_df['fastq_path'].tolist()
    complete_fastq_list = [run_dir + s for s in fastq_list]
    # api_logger.info(complete_fastq_list) # too much to have in log
    num_complete = len(complete_fastq_list)
    api_logger.info('Total fastq files from run: ', str(num_complete))

    if all(list(map(os.path.isfile, complete_fastq_list))):
        api_logger.info('[END] check_upload_complete - True')
        # all files exist, so upload is complete
        return True
    else:
        api_logger.info('[END] check_upload_complete - False')
        # some files missing, so upload is not complete
        return False


def server_parse():
    sp = ServerParse()
    sp.move_fastq_files()


class ServerParse:
    def __init__(self, download_dir=settings.SERVER_DOWNLOAD_DIR,
                 output_dir=settings.SERVER_OUTPUT_DIR,
                 data_directory=settings.SERVER_DATA_DIRECTORY,
                 log_file_dir=settings.LOG_FILE_DIR,
                 upload_bioinfo_results_dir=settings.SERVER_UPLOAD_BR_DIR):
        self.download_dir = download_dir
        self.output_dir = output_dir
        self.upload_bioinfo_results_dir = upload_bioinfo_results_dir
        self.log_file_dir = log_file_dir
        # mydata cfgs
        self.data_directory = data_directory
        print(self.download_dir)

    def get_dirs(self, export_csv=True, complete_upload=True):
        """
         get dirs and put in pandas df
        """
        try:
            server_parse_dates = []
            run_ids = []
            run_dirs = []
            fastq_dirs = []
            projects = []
            num_fastq_dirs = []
            rta_completes = []
            num_run_dirs = []
            run_dirs_create_dates = []
            fastq_dirs_create_dates = []
            run_completion_times = []
            upload_completes = []
            # make list of all project folders; e.g., maine-edna
            server_parse_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            project_dirs = glob.glob(os.path.join(self.download_dir, '*/'))
            for project_dir in project_dirs:
                dir_length = len(os.listdir(project_dir))
                # if there are no files in the directory, skip processing it
                if dir_length == 0:
                    continue
                project = Path(project_dir).name
                # grab sequencing folder directory with most recent date for each project
                # run_dir = max(glob.glob(os.path.join(project_dir, '*/')), key=os.path.getmtime)
                # run_dir = run_dir.replace('\\', '/')
                # make list of all sequencing run folders in project directories
                run_dir_list = glob.glob(os.path.join(project_dir, '*/'))
                run_dir_list = [dir_path.replace('\\', '/') for dir_path in run_dir_list]
                # loop through all sequencing run folders
                for run_dir in run_dir_list:
                    # the number of sequencing runs per project
                    num_run_dir = len(run_dir_list)
                    # if rta_complete exists, sets variable to True to be filtered on
                    # to only process rta_complete directories
                    rta_complete = check_rta_complete(run_dir)
                    # grab completion_time from CompleteJobInfo.xml
                    completion_time = get_run_completion_time_xml(run_dir)
                    # grab run_id from CompleteJobInfo.xml
                    run_id = get_run_id_xml(run_dir)
                    run_dir_create_date = datetime.fromtimestamp(get_creation_dt(run_dir)).strftime('%Y-%m-%d %H:%M:%S')
                    if not rta_complete or not completion_time:
                        # if rta_complete is false, then the run is not complete
                        fastq_dir = fastq_dir_create_date = num_fastq_dir = "Upload Incomplete"
                        # if Run Failed, then analysis was never complete
                        upload_complete = False

                        # append to lists
                        run_completion_times.append(completion_time)
                        server_parse_dates.append(server_parse_date)
                        projects.append(project)
                        run_ids.append(run_id)
                        run_dirs.append(run_dir)
                        run_dirs_create_dates.append(run_dir_create_date)
                        num_run_dirs.append(num_run_dir)
                        fastq_dirs.append(fastq_dir)
                        fastq_dirs_create_dates.append(fastq_dir_create_date)
                        num_fastq_dirs.append(num_fastq_dir)
                        rta_completes.append(rta_complete)
                        upload_completes.append(upload_complete)
                    else:
                        fastq_dirs_list = glob.glob(os.path.join(run_dir, '*/'))
                        # convert forward slashes to backwards slashes
                        fastq_dirs_list = [dir_path.replace('\\', '/') for dir_path in fastq_dirs_list]
                        # grab filepaths with "Alignment" in it
                        fastq_dirs_list = [fastqdir for fastqdir in fastq_dirs_list if "Fastq" in fastqdir]
                        if not fastq_dirs_list:
                            # if there are no fastq folders
                            # append to lists
                            fastq_dir = fastq_dir_create_date = num_fastq_dir = "Upload Incomplete"
                            # if no alignment folder, then analysis was not complete
                            upload_complete = False

                            # append to lists
                            run_completion_times.append(completion_time)
                            server_parse_dates.append(server_parse_date)
                            projects.append(project)
                            run_ids.append(run_id)
                            run_dirs.append(run_dir)
                            run_dirs_create_dates.append(run_dir_create_date)
                            num_run_dirs.append(num_run_dir)
                            fastq_dirs.append(fastq_dir)
                            fastq_dirs_create_dates.append(fastq_dir_create_date)
                            num_fastq_dirs.append(num_fastq_dir)
                            rta_completes.append(rta_complete)
                            upload_completes.append(upload_complete)
                        else:
                            # fastq files and RTACompete exist, so proceed
                            num_fastq_dir = len(fastq_dirs_list)
                            for fastq_dir in fastq_dirs_list:
                                fastq_list_filepath = fastq_dir + 'Fastq_filelist.csv'
                                fastq_list_exists = os.path.exists(fastq_list_filepath)
                                if not fastq_list_exists:
                                    # check if all fastq files have been uploaded via rclone
                                    upload_complete = False
                                    # grab date of fastq dir
                                    fastq_dir_create_date = datetime.fromtimestamp(get_creation_dt(fastq_dir)).strftime('%Y-%m-%d %H:%M:%S')
                                    # append to lists
                                    run_completion_times.append(completion_time)
                                    server_parse_dates.append(server_parse_date)
                                    projects.append(project)
                                    run_ids.append(run_id)
                                    run_dirs.append(run_dir)
                                    run_dirs_create_dates.append(run_dir_create_date)
                                    num_run_dirs.append(num_run_dir)
                                    fastq_dirs.append(fastq_dir)
                                    fastq_dirs_create_dates.append(fastq_dir_create_date)
                                    num_fastq_dirs.append(num_fastq_dir)
                                    rta_completes.append(rta_complete)
                                    upload_completes.append(upload_complete)
                                else:
                                    # check if all fastq files have been uploaded via rclone
                                    upload_complete = check_upload_complete(fastq_list_filepath, run_dir)
                                    # grab date of fastq dir
                                    fastq_dir_create_date = datetime.fromtimestamp(get_creation_dt(fastq_dir)).strftime('%Y-%m-%d %H:%M:%S')
                                    # append to lists
                                    run_completion_times.append(completion_time)
                                    server_parse_dates.append(server_parse_date)
                                    projects.append(project)
                                    run_ids.append(run_id)
                                    run_dirs.append(run_dir)
                                    run_dirs_create_dates.append(run_dir_create_date)
                                    num_run_dirs.append(num_run_dir)
                                    fastq_dirs.append(fastq_dir)
                                    fastq_dirs_create_dates.append(fastq_dir_create_date)
                                    num_fastq_dirs.append(num_fastq_dir)
                                    rta_completes.append(rta_complete)
                                    upload_completes.append(upload_complete)

            dirs_df = pd.DataFrame(list(zip(server_parse_dates, projects, run_completion_times, run_ids, run_dirs, run_dirs_create_dates,
                                            num_run_dirs, fastq_dirs, fastq_dirs_create_dates, num_fastq_dirs,
                                            rta_completes, upload_completes)),
                                   columns=['server_parse_date', 'project', 'run_completion_time', 'run_id', 'run_dir',
                                            'run_dir_create_date', 'num_run_dir', 'fastq_dir', 'fastq_dir_create_date',
                                            'num_fastq_dir', 'rta_complete', 'upload_complete'])
            # output dirs to csv
            if export_csv:
                output_csv_filename = self.log_file_dir + 'server_dirlist.csv'
                if os.path.exists(output_csv_filename):
                    dirs_df.to_csv(output_csv_filename, encoding='utf-8', index=False, header=False, mode='a')
                else:
                    dirs_df.to_csv(output_csv_filename, encoding='utf-8', index=False)
            if complete_upload:
                # subset by directories that have RTAComplete.txt; we do not want to process incomplete sequencing runs
                dirs_df_upload_complete = dirs_df[(dirs_df['rta_complete'] == True) &
                                                  (dirs_df['upload_complete'] == True)]
                return dirs_df_upload_complete
            else:
                return dirs_df
        except Exception as err:
            raise RuntimeError("** Error: get_dirs Failed (" + str(err) + ")")

    def create_bioinformatics_results_dir(self, project, run_id):
        """
         parse server copy of fastq files
        """
        try:
            api_logger.info('[START] create_bioinformatics_results_dir')
            upload_bioinfo_results_dir = self.upload_bioinfo_results_dir
            output_hpc_run_dir = upload_bioinfo_results_dir + project + '/' + run_id + '/Bioinformatics_Results/'
            if not os.path.exists(output_hpc_run_dir):
                os.makedirs(output_hpc_run_dir)
                api_logger.info('Created dir: ' + project + ', ' + run_id + ': [' + output_hpc_run_dir + ']')
            api_logger.info('[END] create_bioinformatics_results_dir')
        except Exception as err:
            raise RuntimeError("** Error: create_bioinformatics_results_dir Failed (" + str(err) + ")")

    def move_fastq_files(self):
        """
         parse server copy of fastq files
        """
        try:
            api_logger.info('[START] move_fastq_files')
            output_dir = self.output_dir
            dirs_df = self.get_dirs(export_csv=True, complete_upload=True)
            for index, row in dirs_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                fastq_dir = row['fastq_dir']
                self.create_bioinformatics_results_dir(project, run_id)
                output_fastq_dir = output_dir + run_id + '/'
                if not os.path.exists(output_fastq_dir):
                    os.makedirs(output_fastq_dir)
                fastq_files = glob.glob(os.path.join(fastq_dir, '**/*.fastq.gz'), recursive=True)
                num_fastq_files = len(fastq_files)
                api_logger.info('Start move: ' + str(num_fastq_files) + ' fastq files - ' + project + ', ' + run_id +
                                ', from: [' + fastq_dir + '], to: [' + output_fastq_dir + ']')
                fastq_counter = 0
                for fastq_file in fastq_files:
                    fastq_filename = os.path.basename(fastq_file)
                    output_fastq_filename = output_fastq_dir + fastq_filename
                    if not os.path.exists(output_fastq_filename):
                        # only want to copy/move if file doesn't already exist
                        copy2(fastq_file, output_fastq_dir)
                        api_logger.info(str(fastq_filename) + ' moved to [' + str(output_fastq_dir) + ']')
                        fastq_counter += 1
                if fastq_counter == 0:
                    api_logger.info('[ALL ALREADY EXIST] Moved ' + str(fastq_counter) + ' files')
                else:
                    api_logger.info('[MOVED] Moved ' + str(fastq_counter) + ' files')
            api_logger.info('[END] move_fastq_files')
        except Exception as err:
            raise RuntimeError("** Error: move_fastq_files Failed (" + str(err) + ")")
