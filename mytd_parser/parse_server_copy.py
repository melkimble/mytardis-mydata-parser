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
from mytd_parser.parse_seq_run import MiSeqParser

class ServerParse(MiSeqParser):
    def __init__(self,input_dir=settings.SERVER_INPUT_DIR,
                 output_dir=settings.SERVER_OUTPUT_DIR,
                 data_directory=settings.MISEQ_DATA_DIRECTORY):
        super().__init__(input_dir, output_dir, data_directory)
        self.input_dir = input_dir
        self.output_dir = output_dir
        # mydata cfgs
        self.data_directory = data_directory
        print(self.input_dir)

    def get_run_id_xml(self, input_run_dir):
        """
         parse CompleteJobInfo.xml to get correct run id
         If data are sent multiple times via MyData, mydata-seq-fac will append
         the folder with "-1"; this way the referenced run id is always the correct run id
         for data on the server.
        """
        try:
            import xml.etree.ElementTree as ET
            complete_file_name = 'CompletedJobInfo.xml'
            # grab newest CompletedJobInfo.xml in directory
            complete_file_path = max(glob.glob(f'{input_run_dir}/**/{complete_file_name}', recursive=True))
            complete_file_path = complete_file_path.replace('\\', '/')
            tree = ET.parse(complete_file_path)
            root = tree.getroot()
            for rid in root.findall('RTARunInfo'):
                RunID = rid.find('RunID').text
            return(RunID)
        except Exception as err:
            raise RuntimeError("** Error: get_run_id_xml Failed (" + str(err) + ")")

    def get_dirs(self):
        """
         get dirs and put in pandas df
        """
        try:
            run_dirs = []
            run_ids = []
            fastq_dirs = []
            projects = []
            num_align_subdirs = []
            rta_completes = []
            num_run_dirs = []
            project_dirs = glob.glob(os.path.join(self.input_dir, '*/'))

            for project_dir in project_dirs:
                dir_length = len(os.listdir(project_dir))
                # if there are no files in the directory, skip processing it
                if dir_length == 0:
                    continue
                project = Path(project_dir).name
                # grab sequencing folder directory with most recent date for each project
                #run_dir = max(glob.glob(os.path.join(project_dir, '*/')), key=os.path.getmtime)
                #run_dir = run_dir.replace('\\', '/')
                # change to all sequencing folders in directory
                run_dir_list = glob.glob(os.path.join(project_dir, '*/'))
                run_dir_list = [dir_path.replace('\\', '/') for dir_path in run_dir_list]
                for run_dir in run_dir_list:
                    # the number of sequencing runs per project
                    num_run_dir = len(run_dir_list)
                    # if rta_complete exists, sets variable to True to be filtered on
                    # to only process rta_complete directories
                    rta_complete = self.check_rta_complete(run_dir)
                    # grab run_id from CompleteJobInfo.xml
                    run_id = self.get_run_id_xml(run_dir)
                    fastq_dirs_list = glob.glob(os.path.join(run_dir, '*/'))
                    # convert forward slashes to backwards slashes
                    fastq_dirs_list = [dir_path.replace('\\', '/') for dir_path in fastq_dirs_list]
                    # grab filepaths with "Alignment" in it
                    fastq_dirs_list = [fastqdir for fastqdir in fastq_dirs_list if "Fastq" in fastqdir]

                    for fastq_dir in fastq_dirs_list:
                        num_align_subdir = len(fastq_dirs_list)

                        # append to lists
                        projects.append(project)
                        run_dirs.append(run_dir)
                        run_ids.append(run_id)
                        fastq_dirs.append(fastq_dir)
                        num_run_dirs.append(num_run_dir)
                        num_align_subdirs.append(num_align_subdir)
                        rta_completes.append(rta_complete)
            dirs_df = pd.DataFrame(list(zip(projects, run_dirs, run_ids, fastq_dirs, num_run_dirs,
                                            num_align_subdirs,rta_completes)),
                                   columns=['project', 'run_dir', 'run_id', 'fastq_dir', 'num_run_dir',
                                            'num_align_subdir','rta_complete'])
            # output dirs to csv
            output_csv_filename = datetime.now().strftime(self.log_file_dir + 'dirlist_%Y%m%d_%H%M%S.csv')
            dirs_df.to_csv(output_csv_filename, encoding='utf-8')
            # subset by directories that have RTAComplete.txt; we do not want to process incomplete sequencing runs
            dirs_df_rta_complete = dirs_df[dirs_df["rta_complete"] == True]
            return(dirs_df_rta_complete)
        except Exception as err:
            raise RuntimeError("** Error: get_dirs Failed (" + str(err) + ")")

    def move_fastq_files(self):
        """
         parse server copy of fastq files
        """
        try:
            output_dir = self.output_dir
            dirs_df = self.get_dirs()
            for index, row in dirs_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                fastq_dir = row['fastq_dir']
                output_fastq_dir = output_dir+run_id+'/'
                if not os.path.exists(output_fastq_dir):
                    os.makedirs(output_fastq_dir)
                fastq_files = glob.glob(os.path.join(fastq_dir, '**/*.fastq.gz'), recursive=True)
                num_fastq_files = len(fastq_files)
                api_logger.info('move_fastq_files: '+project+', '+run_id+', '+str(num_fastq_files)+' files from: [' + fastq_dir + '], to: [' + output_fastq_dir + ']')
                fastq_counter=0
                for fastq_file in fastq_files:
                    copy2(fastq_file, output_fastq_dir)
                    fastq_counter+=1
                api_logger.info('End: copied '+str(fastq_counter)+' files')
        except Exception as err:
            raise RuntimeError("** Error: move_fastq_files Failed (" + str(err) + ")")