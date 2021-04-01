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

miseq_parser.parse_fastq_files()
#parser.complete_backup(move_parsing=False, move_staging=True)

# not using mydata-python; just parsing and triggering upload in mydata.exe on daily timer
# mydata-python breaking on upload - FileNotFoundError: [WinError 2] The system cannot find the file specified
# added issue to mydata-python repo
#miseq_parser.set_mydata_settings_py()
#miseq_parser.scan_mydata_subprocess()
#miseq_parser.upload_mydata_subprocess()

