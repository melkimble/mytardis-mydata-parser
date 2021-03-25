"""
parse_seq_run.py
Filter and parse MiSeq Illumina runs and submit data via MyData to MyTardis
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""

from . import settings
import os, glob
import pandas as pd
from shutil import copy2, move
from .logger_settings import api_logger
from pathlib import *
from distutils.dir_util import copy_tree
from subprocess import PIPE, run
import datetime
from datetime import datetime
import pathlib

def unique(list1):
    # insert the list to the set
    list_set = set(list1)
    # convert the set to the list
    unique_list = (list(list_set))
    return(unique_list)

class MiSeqParser:
    def __init__(self, input_dir=settings.MISEQ_INPUT_DIR,
                 copy_original_dir=settings.MISEQ_COPY_ORIGINAL_DIR,
                 copy_parsed_dir=settings.MISEQ_COPY_PARSED_DIR,
                 output_dir=settings.MISEQ_OUTPUT_DIR,
                 log_file_dir=settings.LOG_FILE_DIR,
                 data_directory=settings.MISEQ_DATA_DIRECTORY,
                 folder_structure=settings.FOLDER_STRUCTURE,
                 mytardis_url=settings.MYTARDIS_URL):
        self.input_dir = input_dir
        #self.MISEQ_COPY_DIR = settings.MISEQ_COPY_DIR
        self.copy_original_dir = copy_original_dir
        self.copy_parsed_dir = copy_parsed_dir
        self.output_dir = output_dir
        self.log_file_dir = log_file_dir
        # mydata cfgs
        self.data_directory = data_directory
        self.folder_structure = folder_structure
        self.mytardis_url = mytardis_url

    def check_rta_complete(self, input_run_dir):
        """
         check to see if RTAComplete.txt exists
        """
        try:
            #input_run_dir = input_dir + project + "/" + run_id + "/"
            api_logger.info('check_rta_complete: [' + str(input_run_dir)+']')
            rta_file_name = 'RTAComplete.txt'
            rta_file_name = glob.glob(f'{input_run_dir}/**/{rta_file_name}', recursive=True)
            if rta_file_name:
                print(rta_file_name)
                api_logger.info('End: RTAComplete.txt exists')
                return True
            else:
                print(rta_file_name)
                # if RTAComplete.txt does not exist, then we do not want to proceed with processing
                # the sequencing run.
                api_logger.info('End: RTAComplete.txt does not exist - sequencing run not complete')
                return False

        except Exception as err:
            raise RuntimeError("** Error: check_rta_complete Failed (" + str(err) + ")")

    def get_dirs(self):
        """
         get dirs and put in pandas df
        """
        try:
            run_dirs = []
            run_ids = []
            align_subdirs = []
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
                    # grab run_id from path name
                    run_id = Path(run_dir).name
                    alignment_dirs_list = glob.glob(os.path.join(run_dir, '*/'))
                    # convert forward slashes to backwards slashes
                    alignment_dirs_list = [dir_path.replace('\\', '/') for dir_path in alignment_dirs_list]
                    # grab filepath with "Alignment" in it
                    alignment_dir = [algndir for algndir in alignment_dirs_list if "Alignment" in algndir]
                    # convert list to string
                    alignment_dir = ''.join(alignment_dir)

                    align_subdirs_list = glob.glob(os.path.join(alignment_dir, '*/'))
                    # convert forward slashes to backwards slashes
                    align_subdirs_list = [dir_path.replace('\\', '/') for dir_path in align_subdirs_list]
                    for align_subdir in align_subdirs_list:
                        num_align_subdir = len(align_subdirs_list)
                        fastq_dirs_list = glob.glob(os.path.join(align_subdir, '*/'))
                        # convert forward slashes to backwards slashes
                        fastq_dirs_list = [dir_path.replace('\\', '/') for dir_path in fastq_dirs_list]
                        # grab filepath with "Fastq" in it
                        fastq_dir = [fqdir for fqdir in fastq_dirs_list if "Fastq" in fqdir]
                        # convert list to string
                        fastq_dir = ''.join(fastq_dir)
                        # grab all other dirs and leave as list
                        # other_dirs = [fqdir for fqdir in fastq_dirs_list if not "Fastq" in fqdir]

                        # append to lists
                        projects.append(project)
                        run_dirs.append(run_dir)
                        run_ids.append(run_id)
                        align_subdirs.append(align_subdir)
                        fastq_dirs.append(fastq_dir)
                        num_run_dirs.append(num_run_dir)
                        num_align_subdirs.append(num_align_subdir)
                        rta_completes.append(rta_complete)
            dirs_df = pd.DataFrame(list(zip(projects, run_dirs, run_ids, align_subdirs, fastq_dirs, num_run_dirs,
                                            num_align_subdirs,rta_completes)),
                                   columns=['project', 'run_dir', 'run_id', 'align_subdir', 'fastq_dir', 'num_run_dir',
                                            'num_align_subdir','rta_complete'])

            # output dirs to csv
            output_csv_filename = datetime.now().strftime(self.log_file_dir + 'dirlist_%Y%m%d_%H%M%S.csv')
            dirs_df.to_csv(output_csv_filename, encoding='utf-8')
            # subset by directories that have RTAComplete.txt; we do not want to process incomplete sequencing runs
            dirs_df_rta_complete = dirs_df[dirs_df["rta_complete"] == True]
            return(dirs_df_rta_complete)
        except Exception as err:
            raise RuntimeError("** Error: get_dirs Failed (" + str(err) + ")")

    def copy_metadata_dirs(self,ignore_dirs):
        """
         copy metadata dirs and files
        """
        try:
            dirs_df = self.get_dirs()
            output_dir = self.output_dir
            align_counter = 1
            for index, row in dirs_df.iterrows():
                num_align_subdir = row['num_align_subdir']
                # only need to copy metadata once per align_subdir
                if num_align_subdir == 1:
                    # reset counter to 1 if only 1 subdir
                    align_counter = 1
                if align_counter > 1:
                    # if counter is greater than 1, skip to next
                    continue
                project = row['project']
                run_dir = row['run_dir']
                run_id = row['run_id']
                align_subdir = row['align_subdir']
                fastq_dir = row['fastq_dir']
                # log info
                api_logger.info('copy_metadata_dirs: '+project+', '+run_id+', ['+run_dir+', '+align_subdir+', '+fastq_dir+'], '+str(num_align_subdir))

                output_metadata_dir = output_dir + project + "/" + run_id + "/run_metadata_"+run_id+"/"
                # if directory doesn't exist, create it
                if not os.path.exists(output_metadata_dir):
                    os.makedirs(output_metadata_dir)

                run_dirs_list = glob.glob(os.path.join(run_dir, '*/'))
                run_dirs_list = [dir_path.replace('\\', '/') for dir_path in run_dirs_list]
                if ignore_dirs:
                    # if ignore_dirs = True, do not copy any folders to Upload
                    ignore_list = run_dirs_list
                    api_logger.info('Ignore Dirs: ' + str(ignore_list))
                else:
                    # copy files to run_metadata folder
                    # Alignment is here because it will be treated differently
                    ignore_list = ["Alignment", "Data", "Thumbnail_Images"]
                    api_logger.info('Ignore Dirs: ' + str(ignore_list))
                # directories in run folder to keep and move to run_metadata folder
                keep_dirs_list = [rundir for rundir in run_dirs_list if not any(ignore in rundir for ignore in ignore_list)]
                api_logger.info('Keep Dirs: '+str(keep_dirs_list))
                num_dirs = len(keep_dirs_list)

                run_dir_files_list = glob.glob(os.path.join(run_dir, '*'))
                run_dir_files_list = [dir_path.replace('\\', '/') for dir_path in run_dir_files_list]

                # files in run folder to move to run_metadata folder
                file_list = [file for file in run_dir_files_list if os.path.isfile(file)]
                num_files = len(file_list)
                # log info
                api_logger.info('Start copying ' + str(num_files) + ' files')
                file_count=0
                for file in file_list:
                    copy2(file, output_metadata_dir)
                    file_count+=1
                    # log info
                api_logger.info('End copied ' + str(file_count) + ' files')
                if num_dirs > 0:
                    # if there are directories in list, copy them. Otherwise do nothing.
                    api_logger.info('Start copying ' + str(num_dirs) + ' dirs')
                    dir_count=0
                    for keep_dir in keep_dirs_list:
                        dir_name = Path(keep_dir).name
                        copy_tree(keep_dir,output_metadata_dir+dir_name+'/')
                        dir_count+=1
                    # log info
                    api_logger.info('End copied ' + str(dir_count) + ' dirs')
                if num_align_subdir > 1:
                    # if more than 1 align_subdir, add 1 to counter
                    align_counter+=1
            return(dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: copy_metadata_dirs Failed (" + str(err) + ")")

    def parse_fastq_metadata_dirs(self):
        """
         parse fastq metadata dirs
        """
        try:
            dirs_df = self.copy_metadata_dirs(ignore_dirs=True)
            output_dir = self.output_dir
            for index, row in dirs_df.iterrows():
                num_align_subdir = row['num_align_subdir']
                project = row['project']
                run_id = row['run_id']
                align_subdir = row['align_subdir']
                fastq_dir = row['fastq_dir']
                # log info
                api_logger.info('parse_fastq_metadata_dirs: '+project+', '+run_id+', ['+align_subdir+', '+fastq_dir+'], '+str(num_align_subdir))

                output_metadata_dir = output_dir + project + "/" + run_id + "/run_metadata_" + run_id + "/"
                align_subdir_list = glob.glob(os.path.join(align_subdir, '*/'))
                align_subdir_list = [dir_path.replace('\\', '/') for dir_path in align_subdir_list]
                # copy files to run_metadata folder
                # Alignment is here because it will be treated differently
                ignore_list = ["Fastq"]
                # directories in run folder to keep and move to run_metadata folder
                keep_dirs_list = [subdir for subdir in align_subdir_list if not any(ignore in subdir for ignore in ignore_list)]
                # log info
                api_logger.info('Keep Dirs: '+str(keep_dirs_list))
                num_dirs = len(keep_dirs_list)

                align_subdir_files_list = glob.glob(os.path.join(align_subdir, '*'))
                align_subdir_files_list = [dir_path.replace('\\', '/') for dir_path in align_subdir_files_list]

                # files in run folder to move to run_metadata folder
                file_list = [file for file in align_subdir_files_list if os.path.isfile(file)]
                num_files = len(file_list)
                # files in run folder to move to run_metadata folder

                # log info
                api_logger.info('Start copying ' + str(num_files) + ' files')
                align_subdir_name = Path(align_subdir).name
                output_fastq_metadata_dir = output_metadata_dir + 'Fastq_' + align_subdir_name + '/'
                if not os.path.exists(output_fastq_metadata_dir):
                    os.makedirs(output_fastq_metadata_dir)
                file_count=0
                for file in file_list:
                    copy2(file, output_fastq_metadata_dir)
                    file_count+=1
                # log info
                api_logger.info('End copied ' + str(file_count) + ' files')
                api_logger.info('Start copying ' + str(num_dirs) + ' dirs')
                dir_count=0
                for keep_dir in keep_dirs_list:
                    dir_name = Path(keep_dir).name
                    output_fastq_metadata_subdir = output_fastq_metadata_dir+dir_name+'/'
                    if not os.path.exists(output_fastq_metadata_subdir):
                        os.makedirs(output_fastq_metadata_subdir)
                    copy_tree(keep_dir,output_fastq_metadata_subdir)
                    dir_count+=1
                # log info
                api_logger.info('End copied ' + str(dir_count) + ' dirs')
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: parse_fastq_metadata_dirs Failed (" + str(err) + ")")

    def parse_fastq_files(self):
        """
         parse fastq files
        """
        try:
            dirs_df = self.parse_fastq_metadata_dirs()
            output_dir = self.output_dir
            for index, row in dirs_df.iterrows():
                num_align_subdir = row['num_align_subdir']
                project = row['project']
                run_id = row['run_id']
                align_subdir = row['align_subdir']
                fastq_dir = row['fastq_dir']
                # log info
                api_logger.info('parse_fastq_files: '+project+', '+run_id+', ['+align_subdir+', '+fastq_dir+'], Num Subalign: '+str(num_align_subdir))
                # grab name of Alignment subdir, e.g., "20201224_205858"
                align_subdir_name = Path(align_subdir).name

                output_run_dir = output_dir + project + "/" + run_id + "/"
                output_fastq_dir = output_run_dir + "Fastq_"+align_subdir_name + "/"

                fastq_files_list = glob.glob(os.path.join(fastq_dir, '*.fastq.gz'))
                fastq_files_list = [dir_path.replace('\\', '/') for dir_path in fastq_files_list]
                fastq_summary_file_list = glob.glob(os.path.join(fastq_dir, '*.txt'))
                fastq_summary_file_list = [dir_path.replace('\\', '/') for dir_path in fastq_summary_file_list]

                # files in run folder to move to run_metadata folder
                num_files = len(fastq_files_list)
                # files in run folder to move to run_metadata folder

                # log info
                api_logger.info('Start copying ' + str(num_files) + ' files')

                sample_ids = []
                for fastq_file in fastq_files_list:
                    # parse each fastq filename for the sample_id
                    # leftmost after underscore split
                    fastq_name = Path(fastq_file).name
                    sample_id = fastq_name.split('_', 1)[0]
                    sample_ids.append(sample_id)
                fastq_df = pd.DataFrame(list(zip(sample_ids, fastq_files_list)), columns=['sample_id', 'fastq_path'])
                output_csv_filename = datetime.now().strftime(self.log_file_dir+'Fastq_'+align_subdir_name+'_filelist_%Y%m%d_%H%M%S.csv')
                fastq_df.to_csv(output_csv_filename, encoding='utf-8')
                # unique subset of sample_ids sorted by name

                sample_ids = sorted(unique(sample_ids))
                file_count = 0
                for sample_id in sample_ids:
                    # get filepaths with sampleid in them
                    fastq_sid_df = fastq_df[fastq_df['sample_id'] == sample_id]
                    for index, row in fastq_sid_df.iterrows():
                        fastq_file = row['fastq_path']
                        output_fastq_sid_dir = output_fastq_dir + sample_id + "/"
                        if not os.path.exists(output_fastq_sid_dir):
                            os.makedirs(output_fastq_sid_dir)
                        copy2(fastq_file, output_fastq_sid_dir)
                        file_count+=1
                for summary_file in fastq_summary_file_list:
                    # copy summary file into each fastq sample_id folder
                    copy2(summary_file, output_fastq_dir)
                # log info
                api_logger.info('End copied ' + str(file_count) + ' files')
            return(dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: parse_fastq_files Failed (" + str(err) + ")")

    def move_parsing_backup(self):
        """
         move parsed files from upload to backup; must happen after mydata upload is complete
        """
        try:
            dirs_df = self.get_dirs()
            output_dir = self.output_dir
            copy_parsed_dir = self.copy_parsed_dir
            dirs_group_df = dirs_df.groupby(['project', 'run_id']).size().reset_index().rename(columns={0: 'count'})
            # print(dirs_group_df)

            for index, row in dirs_group_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                api_logger.info('move_parsing_backup: '+project+', '+run_id)
                input_copy_dir = output_dir + project + "/" + run_id + "/"
                output_copy_dir = copy_parsed_dir + project + "/" + run_id + "/"
                api_logger.info('Start: backup move - from: ['+input_copy_dir+'], to: ['+output_copy_dir+']')
                move(input_copy_dir, output_copy_dir)
                api_logger.info('End: parse backup moved')
            return(dirs_group_df)
        except Exception as err:
            raise RuntimeError("** Error: move_parsing_backup Failed (" + str(err) + ")")

    def move_staging_backup(self):
        """
         move data out of staging folder and to backup folder; must happen after parsing is complete
        """
        try:
            dirs_df = self.get_dirs()
            input_dir = self.input_dir
            copy_original_dir = self.copy_original_dir
            dirs_group_df = dirs_df.groupby(['project', 'run_id']).size().reset_index().rename(columns={0: 'count'})

            for index, row in dirs_group_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                api_logger.info('move_staging_backup: '+project+', '+run_id)
                input_copy_dir = input_dir + project + "/" + run_id + "/"
                output_copy_dir = copy_original_dir + project + "/" + run_id + "/"
                api_logger.info('Start: backup move - from: ['+input_copy_dir+'], to: ['+output_copy_dir+']')
                move(input_copy_dir, output_copy_dir)
                api_logger.info('End: original backup moved')
            return(dirs_group_df)
        except Exception as err:
            raise RuntimeError("** Error: move_staging_backup Failed (" + str(err) + ")")

    def complete_backup(self, move_parsing, move_staging):
        """
         Create MYTDComplete.txt files in parsed and original folders in backup directory
        """
        try:
            dirs_df = self.get_dirs()
            copy_parsed_dir = self.copy_parsed_dir
            copy_original_dir = self.copy_original_dir
            dirs_group_df = dirs_df.groupby(['project', 'run_id']).size().reset_index().rename(columns={0: 'count'})
            for index, row in dirs_group_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                api_logger.info('complete_backup: '+project+', '+run_id)

                output_parse_dir = copy_parsed_dir + project + "/" + run_id + "/"
                output_copy_dir = copy_original_dir + project + "/" + run_id + "/"

                if move_parsing and move_staging:
                    self.move_parsing_backup()
                    self.move_staging_backup()
                    complete_filepath = output_parse_dir + "MYTDComplete.txt"
                    with open(complete_filepath, mode='a') as file:
                        file.write('[START]\n mytd_parser completed at [%s]\n Project: %s\n RunID: %s\n Original backup copied: [%s]\n Parsed backup copied: [%s]\n[END]' %
                                   (datetime.now(), project,run_id, output_copy_dir,output_parse_dir))
                    copy2(complete_filepath, output_copy_dir)
                    api_logger.info('Backup Parsed and Original: '+project +', '+run_id+', ['+output_copy_dir+', '+output_parse_dir+']')
                elif move_parsing and not move_staging:
                    self.move_parsing_backup()
                    complete_filepath = output_parse_dir + "MYTDComplete.txt"
                    with open(complete_filepath, mode='a') as file:
                        file.write('[START]\n mytd_parser completed at [%s]\n Project: %s\n RunID: %s\n Original backup copied: [%s]\n[END]' %
                                   (datetime.now(), project,run_id, output_parse_dir))
                    api_logger.info('Backup Parsed: ' + project + ', ' + run_id + ', [' + output_parse_dir+']')
                elif not move_parsing and move_staging:
                    self.move_staging_backup()
                    complete_filepath = output_copy_dir + "MYTDComplete.txt"
                    with open(complete_filepath, mode='a') as file:
                        file.write('[START]\n mytd_parser completed at [%s]\n Project: %s\n RunID: %s\n Original backup copied: [%s]\n[END]' %
                                   (datetime.now(), project,run_id, output_copy_dir))
                    api_logger.info('Backup Original: ' + project + ', ' + run_id+', [' + output_copy_dir+']')
                elif not move_parsing and not move_staging:
                    api_logger.info('No backup created: ' + project + ', ' + run_id)

        except Exception as err:
            raise RuntimeError("** Error: complete_copy_upload Failed (" + str(err) + ")")

## Skip below until mydata-python working - issue submitted on GitHub

    def set_mydata_settings_subprocess(self):
        """
         set config settings
        """
        try:
            dirs_df = self.parse_fastq_files()
            # using subprocess.call method
            api_logger.info('Start: mydata config settings')
            command = ['mydata', 'config', 'set', 'data_directory', self.data_directory]
            result_dd = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info(
                'subprocess result: ' + str(result_dd.returncode) + ', ' + str(result_dd.stdout) + ', ' + str(
                    result_dd.stderr))

            command = ['mydata', 'config', 'set', 'folder_structure', self.folder_structure]
            result_fs = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info(
                'subprocess result: ' + str(result_fs.returncode) + ', ' + str(result_fs.stdout) + ', ' + str(
                    result_fs.stderr))

            command = ['mydata', 'config', 'set', 'mytardis_url', self.mytardis_url]
            result_url = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info(
                'subprocess result: ' + str(result_url.returncode) + ', ' + str(result_url.stdout) + ', ' + str(
                    result_url.stderr))
            api_logger.info('End: mydata config settings')

            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: set_mydata_settings Failed (" + str(err) + ")")

    def scan_mydata_subprocess(self):
        """
         call mydata through subprocess to scan and upload data
        """
        try:
            dirs_df = self.set_mydata_settings_subprocess()

            # call mydata-python and start folder scan
            api_logger.info('Start: mydata scan')
            command = ['mydata', 'scan', '-v']
            result_scan = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info(
                'subprocess result: ' + str(result_scan.returncode) + ', ' + str(result_scan.stdout) + ', ' + str(
                    result_scan.stderr))
            api_logger.info('End: mydata scan')
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: scan_mydata_subprocess Failed (" + str(err) + ")")

    def upload_mydata_subprocess(self):
        """
         call mydata through subprocess to scan and upload data
        """
        try:
            dirs_df = self.scan_mydata_subprocess()
            # call mydata-python and start upload
            api_logger.info('Start: mydata upload')
            command = ['mydata', 'upload', '-v']
            result_upload = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info(
                'subprocess result: ' + str(result_upload.returncode) + ', ' + str(result_upload.stdout) + ', ' + str(
                    result_upload.stderr))
            api_logger.info('End: mydata upload')
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: upload_mydata_subprocess Failed (" + str(err) + ")")

    def set_mydata_settings_py(self):
        """
         set config settings
        """
        try:
            from mydata.models.settings.serialize import save_settings_to_disk, load_settings
            from mydata.conf import settings

            dirs_df = self.parse_fastq_files()
            api_logger.info('Start: set config')
            if settings.data_directory != self.data_directory: settings.data_directory = self.data_directory
            if settings.folder_structure != self.folder_structure: settings.folder_structure = self.folder_structure
            if settings.mytardis_url != self.mytardis_url: settings.mytardis_url = self.mytardis_url
            save_settings_to_disk()
            load_settings()
            api_logger.info(
                'Config updated: ' + str(settings.data_directory) + ', ' + str(settings.folder_structure) + ', ' + str(
                    settings.mytardis_url))
            api_logger.info('End: set config')
            # check if settings were updated
            # assert settings.data_directory == self.data_directory
            # assert settings.folder_structure == self.folder_structure
            # assert settings.mytardis_url == self.mytardis_url
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: set_mydata_settings Failed (" + str(err) + ")")

    def scan_mydata_py(self):
        """
         call mydata through py to scan data
        """
        try:
            from mydata.conf import settings
            from mydata.commands import scan

            dirs_df = self.set_mydata_settings_py()
            # call mydata-python and start folder scan
            api_logger.info('Start: mydata scan')
            # users, groups, exps, folders = scan.scan()
            scan.scan_cmd()
            # api_logger.info('py result: '+str(scan.display_scan_summary(users, groups, exps, folders)))
            api_logger.info('End: mydata scan')
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: scan_mydata_py Failed (" + str(err) + ")")

    def upload_mydata_py(self):
        """
         call mydata through py to upload data
        """
        try:
            from mydata.conf import settings
            from mydata.commands import upload

            dirs_df = self.scan_mydata_py()
            # call mydata-python and start upload
            api_logger.info('Start: mydata upload')
            upload.upload_cmd()
            # api_logger.info('py result: '+str(result_upload.returncode)+', '+str(result_upload.stdout)+', '+str(result_upload.stderr))
            api_logger.info('End: mydata upload')
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: upload_mydata_py Failed (" + str(err) + ")")