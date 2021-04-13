"""
parse_server_copy.py
Filter and parse server copies of MiSeq Illumina runs and reformat for bioinformatics pipelines
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""

from . import settings
import os, glob
import pandas as pd
from shutil import copy2, move
from .logger_settings import api_logger
from pathlib import *
import datetime
from datetime import datetime
from mytd_parser.parse_seq_run import MiSeqParser, get_creation_dt

class ServerParse(MiSeqParser):
    def __init__(self,staging_dir=settings.SERVER_STAGING_DIR,
                 output_dir=settings.SERVER_OUTPUT_DIR,
                 data_directory=settings.SERVER_DATA_DIRECTORY,
                 log_file_dir=settings.LOG_FILE_DIR):
        super().__init__(staging_dir, output_dir, data_directory)
        self.staging_dir = staging_dir
        self.output_dir = output_dir
        self.log_file_dir = log_file_dir
        # mydata cfgs
        self.data_directory = data_directory
        print(self.staging_dir)

    def get_dirs(self, export_csv=True, RTAComplete=True):
        """
         get dirs and put in pandas df
        """
        try:
            run_ids = []
            run_dirs = []
           # align_subdirs = []
            fastq_dirs = []
            projects = []
            #num_align_subdirs = []
            rta_completes = []
            num_run_dirs = []
            run_dirs_create_dates = []
            fastq_dirs_create_dates = []
            #align_subdirs_create_dates = []
            analysis_completes = []
            # make list of all project folders; e.g., maine-edna
            project_dirs = glob.glob(os.path.join(self.staging_dir, '*/'))
            for project_dir in project_dirs:
                dir_length = len(os.listdir(project_dir))
                # if there are no files in the directory, skip processing it
                if dir_length == 0:
                    continue
                project = Path(project_dir).name
                # grab sequencing folder directory with most recent date for each project
                #run_dir = max(glob.glob(os.path.join(project_dir, '*/')), key=os.path.getmtime)
                #run_dir = run_dir.replace('\\', '/')
                # make list of all sequencing run folders in project directories
                run_dir_list = glob.glob(os.path.join(project_dir, '*/'))
                run_dir_list = [dir_path.replace('\\', '/') for dir_path in run_dir_list]
                # loop through all squencing run folders
                for run_dir in run_dir_list:
                    # the number of sequencing runs per project
                    num_run_dir = len(run_dir_list)
                    # if rta_complete exists, sets variable to True to be filtered on
                    # to only process rta_complete directories
                    rta_complete = self.check_rta_complete(run_dir)
                    # grab run_id from CompleteJobInfo.xml
                    run_id = self.get_run_id_xml(run_dir)
                    run_dir_create_date = datetime.fromtimestamp(get_creation_dt(run_dir)).strftime('%Y-%m-%d %H:%M:%S')
                    if not rta_complete:
                        # if rta_complete is false, then the run is not complete
                        fastq_dir = fastq_dir_create_date = "Run Failed"
                        # if Run Failed, then analysis was never complete
                        analysis_complete = False

                        # append to lists
                        projects.append(project)
                        run_ids.append(run_id)
                        run_dirs.append(run_dir)
                        run_dirs_create_dates.append(run_dir_create_date)
                        num_run_dirs.append(num_run_dir)

                        fastq_dirs.append(fastq_dir)
                        fastq_dirs_create_dates.append(fastq_dir_create_date)

                        rta_completes.append(rta_complete)
                        analysis_completes.append(analysis_complete)
                    else:
                        fastq_dirs_list = glob.glob(os.path.join(run_dir, '*/'))
                        # convert forward slashes to backwards slashes
                        fastq_dirs_list = [dir_path.replace('\\', '/') for dir_path in fastq_dirs_list]
                        # grab filepaths with "Alignment" in it
                        fastq_dirs_list = [fastqdir for fastqdir in fastq_dirs_list if "Fastq" in fastqdir]
                        if not fastq_dirs_list:
                            # if there are no fastq folders
                            # append to lists
                            align_subdir = align_subdir_create_date = fastq_dir = fastq_dir_create_date = "Analysis Incomplete"
                            # if no alignment folder, then analysis was not complete
                            analysis_complete = False

                            # append to lists
                            projects.append(project)
                            run_ids.append(run_id)
                            run_dirs.append(run_dir)
                            run_dirs_create_dates.append(run_dir_create_date)
                            num_run_dirs.append(num_run_dir)

                            fastq_dirs.append(fastq_dir)
                            fastq_dirs_create_dates.append(fastq_dir_create_date)
                            rta_completes.append(rta_complete)
                            analysis_completes.append(analysis_complete)
                        else:
                            # fastq files and RTACompete exist, so proceed
                            for fastq_dir in fastq_dirs_list:
                                num_align_subdir = len(fastq_dirs_list)

                                # append to lists
                                projects.append(project)
                                run_ids.append(run_id)
                                run_dirs.append(run_dir)
                                run_dirs_create_dates.append(run_dir_create_date)
                                num_run_dirs.append(num_run_dir)
                                align_subdirs.append(align_subdir)
                                align_subdirs_create_dates.append(align_subdir_create_date)
                                num_align_subdirs.append(num_align_subdir)
                                fastq_dirs.append(fastq_dir)
                                fastq_dirs_create_dates.append(fastq_dir_create_date)
                                rta_completes.append(rta_complete)
                                analysis_completes.append(analysis_complete)
            dirs_df = pd.DataFrame(list(zip(projects, run_ids, run_dirs, run_dirs_create_dates, num_run_dirs,
                                            align_subdirs, align_subdirs_create_dates, num_align_subdirs,
                                            fastq_dirs,fastq_dirs_create_dates,rta_completes, analysis_completes)),
                                   columns=['project', 'run_id', 'run_dir','run_dir_create_date', 'num_run_dir',
                                            'align_subdir', 'align_subdir_create_date', 'num_align_subdir',
                                            'fastq_dir', 'fastq_dir_create_date', 'rta_complete', 'analysis_complete'])
            # output dirs to csv
            if export_csv:
                output_csv_filename = datetime.now().strftime(self.log_file_dir + 'dirlist_%Y%m%d_%H%M%S.csv')
                dirs_df.to_csv(output_csv_filename, encoding='utf-8')
            if RTAComplete:
                # subset by directories that have RTAComplete.txt; we do not want to process incomplete sequencing runs
                dirs_df_rta_complete = dirs_df[dirs_df["rta_complete"] == True]
                return(dirs_df_rta_complete)
            else:
                return(dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: get_dirs Failed (" + str(err) + ")")

    def move_fastq_files(self):
        """
         parse server copy of fastq files
        """
        try:
            api_logger.info('[START] move_fastq_files')
            output_dir = self.output_dir
            dirs_df = self.get_dirs(export_csv=True, RTAComplete=True)
            for index, row in dirs_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                fastq_dir = row['fastq_dir']
                output_fastq_dir = output_dir+run_id+'/'
                if not os.path.exists(output_fastq_dir):
                    os.makedirs(output_fastq_dir)
                fastq_files = glob.glob(os.path.join(fastq_dir, '**/*.fastq.gz'), recursive=True)
                num_fastq_files = len(fastq_files)
                api_logger.info('Start move: '+project+', '+run_id+', '+str(num_fastq_files)+' files from: [' + fastq_dir + '], to: [' + output_fastq_dir + ']')
                fastq_counter=0
                for fastq_file in fastq_files:
                    copy2(fastq_file, output_fastq_dir)
                    fastq_counter+=1
                api_logger.info('End: moved '+str(fastq_counter)+' files')
            api_logger.info('[END] move_fastq_files')
        except Exception as err:
            raise RuntimeError("** Error: move_fastq_files Failed (" + str(err) + ")")