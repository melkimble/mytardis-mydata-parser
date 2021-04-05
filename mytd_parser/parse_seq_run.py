"""
parse_seq_run.py
Filter and parse MiSeq Illumina runs and submit data via MyData to MyTardis
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""

from . import settings
import sys
import os, glob
import pandas as pd
from shutil import copy2, move
from .logger_settings import api_logger
from pathlib import *
from distutils.dir_util import copy_tree
from subprocess import PIPE, run
import datetime
from datetime import datetime
import platform
import pathlib

def unique(list1):
    # insert the list to the set
    list_set = set(list1)
    # convert the set to the list
    unique_list = (list(list_set))
    return(unique_list)

def get_creation_dt(directory):
    """
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    """
    if platform.system() == 'Windows':
        return os.path.getctime(directory)
    else:
        stat = os.stat(directory)
        try:
            return stat.st_birthtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            return stat.st_mtime

def change_creation_dt(directory, mod_date):
    """
    change creation date of directory
    source: https://stackoverflow.com/questions/4996405/how-do-i-change-the-file-creation-date-of-a-windows-file
    """
    try:
        if platform.system() == 'Windows':
            import pywintypes
            from win32file import CreateFile, SetFileTime, CloseHandle
            from win32file import GENERIC_WRITE, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_WRITE
            win_time = pywintypes.Time(mod_date)
            fh = CreateFile(directory, GENERIC_WRITE, FILE_SHARE_WRITE, None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, 0)
            SetFileTime(fh, win_time, win_time, win_time)
            CloseHandle(fh)
        else:
            os.utime(directory, (mod_date, mod_date))
    except Exception as err:
        raise RuntimeError("** Error: change_creation_dt Failed (" + str(err) + ")")

def modify_create_date(old_dir, new_dir):
    """
     modify new_dir creation date to match old_dir creation date
    """
    try:
        create_date = get_creation_dt(old_dir)
        change_creation_dt(new_dir, create_date)
    except Exception as err:
        raise RuntimeError("** Error: modify_create_date Failed (" + str(err) + ")")

class MiSeqParser:
    def __init__(self, staging_dir=settings.MISEQ_STAGING_DIR,
                 output_dir=settings.MISEQ_UPLOAD_DIR,
                 backup_dir=settings.MISEQ_BACKUP_DIR,
                 extra_backup_dirs=settings.MISEQ_EXTRA_BACKUP_DIRS,
                 log_file_dir=settings.LOG_FILE_DIR,
                 data_directory=settings.MISEQ_DATA_DIRECTORY,
                 folder_structure=settings.FOLDER_STRUCTURE,
                 mytardis_url=settings.MYTARDIS_URL):
        self.staging_dir = staging_dir
        self.backup_dir = backup_dir
        self.backup_original_dir_name = "Original/"
        self.backup_parsed_dir_name = "Parsed/"
        self.output_dir = output_dir
        self.extra_backup_dirs = extra_backup_dirs
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
            #input_run_dir = staging_dir + project + "/" + run_id + "/"
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

    def get_run_id_xml(self, input_run_dir):
        """
         parse CompleteJobInfo.xml to get correct run id
         If data are sent multiple times via MyData, mydata-seq-fac will append
         the folder with "-1"; this way the referenced run id is always the correct run id
         for data on the server.
        """
        try:
            import xml.etree.ElementTree as ET
            complete_file_name = 'RunInfo.xml'
            # grab newest CompletedJobInfo.xml in directory
            complete_file_path = max(glob.glob(f'{input_run_dir}/**/{complete_file_name}', recursive=True))
            complete_file_path = complete_file_path.replace('\\', '/')
            tree = ET.parse(complete_file_path)
            root = tree.getroot()
            for run in root.findall("Run"):
                RunID = run.get('Id')
            return(RunID)
        except Exception as err:
            raise RuntimeError("** Error: get_run_id_xml Failed (" + str(err) + ")")

    def get_dirs(self, export_csv=True, RTAComplete=True):
        """
         get dirs and put in pandas df
        """
        try:
            run_ids = []
            run_dirs = []
            align_subdirs = []
            fastq_dirs = []
            projects = []
            num_align_subdirs = []
            rta_completes = []
            num_run_dirs = []
            run_dirs_create_dates = []
            fastq_dirs_create_dates = []
            align_subdirs_create_dates = []
            analysis_completes = []
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
                # change to all sequencing folders in directory
                run_dir_list = glob.glob(os.path.join(project_dir, '*/'))
                run_dir_list = [dir_path.replace('\\', '/') for dir_path in run_dir_list]
                for run_dir in run_dir_list:
                    # the number of sequencing runs per project
                    num_run_dir = len(run_dir_list)
                    # if rta_complete exists, sets variable to True to be filtered on
                    # to only process rta_complete directories
                    rta_complete = self.check_rta_complete(run_dir)
                    # grab run_id from RunInfo.xml
                    run_id = self.get_run_id_xml(run_dir)
                    # run_id = Path(run_dir).name
                    run_dir_create_date = datetime.fromtimestamp(get_creation_dt(run_dir)).strftime('%Y-%m-%d %H:%M:%S')
                    # grab run_id from CompleteJobInfo.xml
                    if not rta_complete:
                        # if rta_complete is false, then the run is not complete
                        align_subdir = align_subdir_create_date = num_align_subdir = fastq_dir = fastq_dir_create_date = "Incomplete Run"
                        # if incomplete run, then analysis was never complete
                        analysis_complete = False

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
                    else:
                        alignment_dirs_list = glob.glob(os.path.join(run_dir, '*/'))
                        # convert forward slashes to backwards slashes
                        alignment_dirs_list = [dir_path.replace('\\', '/') for dir_path in alignment_dirs_list]
                        # grab filepath with "Alignment" in it
                        alignment_dir = [algndir for algndir in alignment_dirs_list if "Alignment" in algndir]
                        if not alignment_dir:
                            # if there is no Alignment folder, then the analysis did not complete and there are no Fastq files
                            # append to lists
                            align_subdir = align_subdir_create_date = num_align_subdir = fastq_dir = fastq_dir_create_date = "Analysis Incomplete"
                            # if no alignment folder, then analysis was not complete
                            analysis_complete = False

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
                        else:
                            # convert list to string
                            alignment_dir = ''.join(alignment_dir)
                            # there is an analysis folder, then analysis should be complete
                            analysis_complete = True

                            align_subdirs_list = glob.glob(os.path.join(alignment_dir, '*/'))
                            # convert forward slashes to backwards slashes
                            align_subdirs_list = [dir_path.replace('\\', '/') for dir_path in align_subdirs_list]
                            for align_subdir in align_subdirs_list:
                                align_subdir_create_date = datetime.fromtimestamp(get_creation_dt(align_subdir)).strftime('%Y-%m-%d %H:%M:%S')
                                num_align_subdir = len(align_subdirs_list)
                                fastq_dirs_list = glob.glob(os.path.join(align_subdir, '*/'))
                                # convert forward slashes to backwards slashes
                                fastq_dirs_list = [dir_path.replace('\\', '/') for dir_path in fastq_dirs_list]
                                # grab filepath with "Fastq" in it
                                fastq_dir = [fqdir for fqdir in fastq_dirs_list if "Fastq" in fqdir]
                                # convert list to string
                                fastq_dir = ''.join(fastq_dir)

                                fastq_dir_create_date = datetime.fromtimestamp(get_creation_dt(fastq_dir)).strftime('%Y-%m-%d %H:%M:%S')
                                # grab all other dirs and leave as list
                                # other_dirs = [fqdir for fqdir in fastq_dirs_list if not "Fastq" in fqdir]

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
                dirs_df.to_csv(output_csv_filename, encoding='utf-8', index=False)
            if RTAComplete:
                # subset by directories that have RTAComplete.txt; we do not want to process incomplete sequencing runs
                dirs_df_rta_complete = dirs_df[(dirs_df["rta_complete"] == True) & (dirs_df["analysis_complete"] == True)]
                return(dirs_df_rta_complete)
            else:
                return(dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: get_dirs Failed (" + str(err) + ")")

    def copy_metadata_dirs(self,ignore_dirs):
        """
         copy metadata dirs and files
        """
        try:
            api_logger.info('[START] copy_metadata_dirs')
            dirs_df = self.get_dirs(export_csv=True, RTAComplete=True)
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
                run_dir_create_date = row['run_dir_create_date']
                align_subdir = row['align_subdir']
                fastq_dir = row['fastq_dir']
                # log info
                api_logger.info('copy_metadata_dirs: '+project+', '+run_id+', ['+run_dir+', '+align_subdir+', '+fastq_dir+'], '+str(num_align_subdir))

                output_metadata_dir = output_dir + project + "/" + run_id + "/run_metadata_"+run_id+"/"
                # if directory doesn't exist, create it and modify the run dir create date
                if not os.path.exists(output_metadata_dir):
                    os.makedirs(output_metadata_dir)
                    modify_create_date(run_dir, output_dir + project + "/" + run_id + "/")
                    modify_create_date(run_dir, output_metadata_dir)

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
                        modify_create_date(keep_dir, output_metadata_dir+dir_name+'/')
                        dir_count+=1
                    # log info
                    api_logger.info('End copied ' + str(dir_count) + ' dirs')
                if num_align_subdir > 1:
                    # if more than 1 align_subdir, add 1 to counter
                    align_counter+=1
            api_logger.info('[END] copy_metadata_dirs')
            return(dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: copy_metadata_dirs Failed (" + str(err) + ")")

    def parse_fastq_metadata_dirs(self):
        """
         parse fastq metadata dirs
        """
        try:
            api_logger.info('[START] parse_fastq_metadata_dirs')
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
                    modify_create_date(fastq_dir, output_fastq_metadata_dir)
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
                        modify_create_date(keep_dir, output_fastq_metadata_subdir)
                    copy_tree(keep_dir,output_fastq_metadata_subdir)
                    dir_count+=1
                # log info
                api_logger.info('End copied ' + str(dir_count) + ' dirs')
            api_logger.info('[END] parse_fastq_metadata_dirs')
            return (dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: parse_fastq_metadata_dirs Failed (" + str(err) + ")")

    def parse_fastq_files(self, export_csv=True):
        """
         parse fastq files
        """
        try:
            api_logger.info('[START] parse_fastq_files')
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
                if not os.path.exists(output_fastq_dir):
                    os.makedirs(output_fastq_dir)
                    modify_create_date(fastq_dir, output_fastq_dir)

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
                fastq_create_dates = []
                for fastq_file in fastq_files_list:
                    # parse each fastq filename for the sample_id
                    # leftmost after underscore split
                    fastq_create_date = datetime.fromtimestamp(get_creation_dt(fastq_file)).strftime('%Y-%m-%d %H:%M:%S')
                    fastq_name = Path(fastq_file).name
                    sample_id = fastq_name.split('_', 1)[0]
                    sample_ids.append(sample_id)
                    fastq_create_dates.append(fastq_create_date)
                fastq_df = pd.DataFrame(list(zip(sample_ids, fastq_create_dates, fastq_files_list)), columns=['sample_id', 'fastq_create_date', 'fastq_path'])

                if export_csv:
                    output_csv_filename = datetime.now().strftime(self.log_file_dir+'Fastq_'+align_subdir_name+'_filelist_%Y%m%d_%H%M%S.csv')
                    fastq_df.to_csv(output_csv_filename, encoding='utf-8', index=False)
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
                            modify_create_date(fastq_dir, output_fastq_sid_dir)
                        copy2(fastq_file, output_fastq_sid_dir)
                        file_count+=1
                for summary_file in fastq_summary_file_list:
                    # copy summary file into each fastq sample_id folder
                    copy2(summary_file, output_fastq_dir)
                # log info
                api_logger.info('End copied ' + str(file_count) + ' files')
            api_logger.info('[END] parse_fastq_files')
            return(dirs_df)
        except Exception as err:
            raise RuntimeError("** Error: parse_fastq_files Failed (" + str(err) + ")")

    def set_mydata_settings_subprocess(self):
        """
         set config settings through subprocess call
        """
        try:
            # using subprocess.call method
            api_logger.info('[START] set_mydata_settings_subprocess')
            command = ['python', 'mydata-python/run.py', 'config', 'set', 'data_directory', self.data_directory]
            result_dd = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info('subprocess result:\n returncode: [' + str(result_dd.returncode) + ']\n stdout: [' + str(result_dd.stdout).replace("\n", ", ") + ']\n stderr: [' + str(result_dd.stderr) + ']')

            command = ['python', 'mydata-python/run.py', 'config', 'set', 'folder_structure', self.folder_structure]
            result_fs = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info('subprocess result:\n returncode: [' + str(result_fs.returncode) + ']\n stdout: [' + str(result_fs.stdout).replace("\n", ", ") + ']\n stderr: [' + str(result_fs.stderr) + ']')

            command = ['python', 'mydata-python/run.py', 'config', 'set', 'mytardis_url', self.mytardis_url]
            result_url = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info('subprocess result:\n returncode: [' + str(result_url.returncode) + ']\n stdout: [' + str(result_url.stdout).replace("\n", ", ") + ']\n stderr: [' + str(result_url.stderr) + ']')

            api_logger.info('Config updated: ' + str(self.data_directory) + ', ' + str(self.folder_structure) + ', ' + str(self.mytardis_url))
            api_logger.info('[END] set_mydata_settings_subprocess')
        except Exception as err:
            raise RuntimeError("** Error: set_mydata_settings Failed (" + str(err) + ")")

    def scan_mydata_subprocess(self):
        """
         call mydata through subprocess to scan and upload data
        """
        try:
            api_logger.info('[START] scan_mydata_subprocess')
            # make sure settings are correct
            self.set_mydata_settings_subprocess()
            # call mydata-python and start folder scan
            api_logger.info('Start: mydata scan')
            command = ['python', 'mydata-python/run.py', 'scan', '-v']
            result_scan = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info('subprocess result:\n returncode: [' + str(result_scan.returncode) + ']\n stdout: [' + str(result_scan.stdout).replace("\n",", ") + ']\n stderr: [' + str(result_scan.stderr) + ']')
            api_logger.info('[END] scan_mydata_subprocess')
        except Exception as err:
            raise RuntimeError("** Error: scan_mydata_subprocess Failed (" + str(err) + ")")

    def upload_mydata_subprocess(self):
        """
         call mydata through subprocess to scan and upload data
        """
        try:
            api_logger.info('[START] upload_mydata_subprocess')
            # scan folders first
            self.scan_mydata_subprocess()
            # call mydata-python and start upload
            api_logger.info('Start: mydata upload')
            command = ['python', 'mydata-python/run.py', 'upload', '-v']
            result_upload = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            api_logger.info('subprocess result:\n returncode: [' + str(result_upload.returncode) + ']\n stdout: [' + str(result_upload.stdout).replace("\n",", ") + ']\n stderr: [' + str(result_upload.stderr) + ']')
            api_logger.info('[END] upload_mydata_subprocess')
        except Exception as err:
            raise RuntimeError("** Error: upload_mydata_subprocess Failed (" + str(err) + ")")

    def create_mytd_complete(self, input_dir, mytd_dir, project, run_id):
        mytd_filepath = mytd_dir + "MYTDComplete.txt"
        with open(mytd_filepath, mode='a') as file:
            file.write(
                '[START]\n mytd_parser completed at [%s]\n Project: %s\n RunID: %s\n Backup copied from: [%s]\n Backup copied to: [%s]\n[END]' %
                (datetime.now(), project, run_id, input_dir, mytd_dir))

    def move_parsing_backup(self, dirs_df):
        """
         move parsed files from upload to backup; must happen after mydata upload is complete
         extra_backup_dirs: expects extra backup dirs
        """
        try:
            api_logger.info('[START] move_parsing_backup')
            output_dir = self.output_dir
            backup_parsed_dir = self.backup_dir + self.backup_parsed_dir_name
            extra_backup_dirs = self.extra_backup_dirs

            dirs_group_df = dirs_df.groupby(['project', 'run_id', 'rta_complete']).size().reset_index().rename(columns={0: 'count'})
            # print(dirs_group_df)

            for index, row in dirs_group_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                rta_complete = row['rta_complete']
                api_logger.info('move_parsing_backup: '+project+', '+run_id)
                input_copy_dir = output_dir + project + "/" + run_id + "/"
                # if RTAComplete is false, continue to next item in list
                # Only runs with RTAComplete == True were parsed
                if not rta_complete:
                    api_logger.info('End: RTAComplete.txt does not exist - run was not parsed ['+project+ " - " +run_id + "]")
                else:
                    output_move_dir = backup_parsed_dir + project + "/" + run_id + "/"
                    if extra_backup_dirs:
                        for extra_backup_dir in extra_backup_dirs:
                            output_backup_dir = extra_backup_dir + self.backup_parsed_dir_name + project + "/" + run_id + "/"
                            # copy into each extra backup directory
                            api_logger.info('Start: backup copy - from: [' + input_copy_dir + '], to: [' + output_backup_dir + ']')
                            copy_tree(input_copy_dir, output_backup_dir)
                            api_logger.info('End: backup copy')
                            # generate MYTDComplete.txt in each backup folder
                            self.create_mytd_complete(input_copy_dir, output_backup_dir, project, run_id)
                    # when copy complete if there are extra backup dirs, move to final backup directory
                    api_logger.info('Start: backup move - from: [' + input_copy_dir + '], to: [' + output_move_dir + ']')
                    move(input_copy_dir, output_move_dir)
                    # generate MYTDComplete.txt in final backup location
                    self.create_mytd_complete(input_copy_dir, output_move_dir, project, run_id)
                    api_logger.info('End: parse backup moved')
            api_logger.info('[END] move_parsing_backup')
            return (dirs_group_df)
        except Exception as err:
            raise RuntimeError("** Error: move_parsing_backup Failed (" + str(err) + ")")

    def move_staging_backup(self, dirs_df):
        """
         move data out of staging folder and to backup folder; must happen after parsing is complete
         extra_backup_dirs: expects list of directories
        """
        try:
            api_logger.info('[START] move_staging_backup')
            staging_dir = self.staging_dir
            backup_original_dir = self.backup_dir + self.backup_original_dir_name
            extra_backup_dirs = self.extra_backup_dirs
            dirs_group_df = dirs_df.groupby(['project', 'run_id']).size().reset_index().rename(columns={0: 'count'})

            for index, row in dirs_group_df.iterrows():
                project = row['project']
                run_id = row['run_id']
                input_copy_dir = staging_dir + project + "/" + run_id + "/"
                output_copy_dir = backup_original_dir + project + "/" + run_id + "/"
                api_logger.info('Start: backup move - from: ['+input_copy_dir+'], to: ['+output_copy_dir+']')
                if extra_backup_dirs:
                    for extra_backup_dir in extra_backup_dirs:
                        # copy into each extra backup directory
                        output_backup_dir = extra_backup_dir + self.backup_original_dir_name + project + "/" + run_id + "/"
                        api_logger.info('Start: backup copy - from: [' + input_copy_dir + '], to: [' + output_backup_dir + ']')
                        copy_tree(input_copy_dir, output_backup_dir)
                        api_logger.info('End: backup copy')
                        # generate MYTDComplete.txt in each backup folder
                        self.create_mytd_complete(input_copy_dir, output_backup_dir, project, run_id)
                # when copy complete if there are extra backup dirs, move to final backup directory
                move(input_copy_dir, output_copy_dir)
                # generate MYTDComplete.txt in final backup location
                self.create_mytd_complete(input_copy_dir, output_copy_dir, project, run_id)
                api_logger.info('End: backup moved')
            api_logger.info('[END] move_staging_backup')
        except Exception as err:
            raise RuntimeError("** Error: move_staging_backup Failed (" + str(err) + ")")

    def complete_upload_backup(self, upload_parsing, move_parsing, move_staging):
        """
         Create MYTDComplete.txt files in parsed and original folders in backup directory
        """
        try:
            api_logger.info('[START] complete_upload_backup')
            if upload_parsing:
                self.upload_mydata_subprocess()
            # with RTAComplete=False, all directories will be placed into backup
            # even if run was not successful
            dirs_df = self.get_dirs(export_csv=False, RTAComplete=True)
            backup_parsed_dir = self.backup_dir+self.backup_parsed_dir_name
            backup_original_dir = self.backup_dir+self.backup_original_dir_name
            if move_parsing and move_staging:
                self.move_parsing_backup(dirs_df)
                self.move_staging_backup(dirs_df)
                api_logger.info('Backup made of parsed and original ['+backup_parsed_dir+', '+backup_original_dir+']')
            elif move_parsing and not move_staging:
                self.move_parsing_backup(dirs_df)
                api_logger.info('Backup made of parsed ['+backup_parsed_dir+']')
            elif not move_parsing and move_staging:
                self.move_staging_backup(dirs_df)
                api_logger.info('Backup made of original ['+backup_original_dir+']')
            elif not move_parsing and not move_staging:
                api_logger.info('No backup created ['+backup_parsed_dir+', '+backup_original_dir+']')
            api_logger.info('[END] complete_upload_backup')
        except Exception as err:
            raise RuntimeError("** Error: complete_copy_upload Failed (" + str(err) + ")")

## Skip below - not using

    def set_mydata_settings_py(self):
        """
         set config settings
        """
        try:
            from mydata.models.settings.serialize import save_settings_to_disk, load_settings
            from mydata.conf import settings

            api_logger.info('Start: set config')
            if settings.data_directory != self.data_directory: settings.data_directory = self.data_directory
            if settings.folder_structure != self.folder_structure: settings.folder_structure = self.folder_structure
            if settings.mytardis_url != self.mytardis_url: settings.mytardis_url = self.mytardis_url
            save_settings_to_disk()
            load_settings()
            api_logger.info('Config updated: ' + str(settings.data_directory) + ', ' + str(settings.folder_structure) + ', ' + str(settings.mytardis_url))
            api_logger.info('End: set config')
            # check if settings were updated
            # assert settings.data_directory == self.data_directory
            # assert settings.folder_structure == self.folder_structure
            # assert settings.mytardis_url == self.mytardis_url
        except Exception as err:
            raise RuntimeError("** Error: set_mydata_settings Failed (" + str(err) + ")")

    def scan_mydata_py(self):
        """
         call mydata through py to scan data
        """
        try:
            from mydata.conf import settings
            from mydata.commands import scan

            #self.set_mydata_settings_py()
            # call mydata-python and start folder scan
            api_logger.info('Start: mydata scan')
            # users, groups, exps, folders = scan.scan()
            scan.scan_cmd()
            api_logger.info('End: mydata scan')
        except Exception as err:
            raise RuntimeError("** Error: scan_mydata_py Failed (" + str(err) + ")")

    def upload_mydata_py(self):
        """
         call mydata through py to upload data
        """
        try:
            from mydata.conf import settings
            from mydata.commands import upload

            #self.scan_mydata_py()
            # call mydata-python and start upload
            api_logger.info('Start: mydata upload')
            upload.upload_cmd()
            # api_logger.info('py result: '+str(result_upload.returncode)+', '+str(result_upload.stdout)+', '+str(result_upload.stderr))
            api_logger.info('End: mydata upload')
        except Exception as err:
            raise RuntimeError("** Error: upload_mydata_py Failed (" + str(err) + ")")