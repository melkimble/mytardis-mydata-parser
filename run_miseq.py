"""
1. Run parse on MiSeq sequencing run to re-arrange fastq files and filter metadata
* Fastq files: Upload/{project}/{run_id}/{Fastq_subalign_foldername}/{sample_id}/*.fastq.gz
* Run metadata: Upload/{project}/{run_id}/{run_metadata_run_id}/*
* Fastq metadata: Upload/{project}/{run_id}/{run_metadata_run_id}/{Fastq_subalign_foldername}/*
2. Copy and move backup of original and parsed files
* Original backup: Backup/Original/{project}/{run_id}/*
* Parsed backup: Backup/Parsed/{project}/{run_id}/*
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""

from mytd_parser.parse_seq_run import MiSeqParser

miseq_parser = MiSeqParser()

# parse runs in /Staging directory
miseq_parser.parse_fastq_files()

# Read parsed runs in /Upload directory, upload to MyData, and move parsed files and original files to /Backup directory
miseq_parser.complete_upload_backup(upload_parsing=False, move_parsing=False, move_staging=True)
