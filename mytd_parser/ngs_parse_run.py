from . import settings
import os, glob
from pathlib import Path
import pandas as pd
import numpy as np
from shutil import copyfile

from .logger_settings import api_logger


from pathlib import *


class ParseDirs:
    def __init__(self):
        self.MAIN_INPUT_DIR = settings.MAIN_INPUT_DIR
        self.MAIN_COPY_DIR = settings.MAIN_COPY_DIR
        self.MAIN_OUTPUT_DIR = settings.MAIN_OUTPUT_DIR

    def get_dirs(self):
        try:
            run_dirs = []
            run_ids = []
            align_subdirs = []
            fastq_dirs = []
            projects = []
            project_dirs = glob.glob(os.path.join(self.MAIN_INPUT_DIR, '*/'))

            for project_dir in project_dirs:
                dir_length = len(os.listdir(project_dir))
                # if there are no files in the directory, skip processing it
                if dir_length == 0:
                    continue
                project = Path(project_dir).name
                # grab sequencing folder directory with most recent date for each project
                run_dir = max(glob.glob(os.path.join(project_dir, '*/')), key=os.path.getmtime)

                # grab run_id from path name
                run_id = Path(run_dir).name

                alignment_dirs_list = glob.glob(os.path.join(run_dir, '*/'))
                # grab filepath with "Alignment" in it
                alignment_dir = [algndir for algndir in alignment_dirs_list if "Alignment" in algndir]
                # convert list to string
                alignment_dir = ''.join(alignment_dir)

                align_subdirs_list = glob.glob(os.path.join(alignment_dir, '*/'))
                for align_subdir in align_subdirs_list:
                    fastq_dirs_list = glob.glob(os.path.join(align_subdir, '*/'))
                    # grab filepath with "Fastq" in it
                    fastq_dir = [fqdir for fqdir in fastq_dirs_list if "Fastq" in fqdir]
                    # convert list to string
                    fastq_dir = ''.join(fastq_dir)
                    # grab all other dirs and leave as list
                    # other_dirs = [fqdir for fqdir in fastq_dirs_list if not "Fastq" in fqdir]

                    # convert forward slashes to backwards slashes
                    run_dir = run_dir.replace('\\', '/')
                    align_subdir = align_subdir.replace('\\', '/')
                    fastq_dir = fastq_dir.replace('\\', '/')

                    # append to lists
                    projects.append(project)
                    run_dirs.append(run_dir)
                    run_ids.append(run_id)
                    align_subdirs.append(align_subdir)
                    fastq_dirs.append(fastq_dir)

            dirs_df = pd.DataFrame(list(zip(projects, run_dirs, run_ids, align_subdirs, fastq_dirs)),
                                   columns=['project', 'run_dir', 'run_id', 'align_subdir', 'fastq_dir'])
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: get_dirs Failed (" + str(err) + ")")
    def copy_metadata_dirs(self):
        try:
            dirs_df = self.get_dirs()
            MAIN_OUTPUT_DIR = self.MAIN_OUTPUT_DIR

            for index, row in dirs_df.iterrows():
                print('Starting:', row['project'], row['run_dir'], row['run_id'], row['align_subdir'], row['fastq_dir'])

                project = row['project']
                run_dir = row['run_dir']
                run_id = row['run_id']
                align_subdir = row['align_subdir']
                fastq_dir = row['fastq_dir']

                run_dirs_list = glob.glob(os.path.join(run_dir, '*/'))


                # copy files to run_metadata folder
                # Alignment is here because it will be treated differently
                ignore_list = ["Alignment", "Data", "Thumbnail_Images"]
                # directories in run folder to keep and move to run_metadata folder
                keep_dirs = [rundir for rundir in run_dirs_list if not any(ignore in rundir for ignore in ignore_list)]
                print('Keep Dirs:', keep_dirs)

                run_dir_files = glob.glob(os.path.join(run_dir, '*'))

                # files in run folder to move to run_metadata folder
                file_list = [file for file in run_dir_files if os.path.isfile(file)]

                api_logger.info('Copy starting...')

        except Exception as err:
            raise RuntimeError("** Error: get_dirs Failed (" + str(err) + ")")




