"""
Run server parse to move fastq files to one directory
* UMaine2/{project}/{run_id}/*.fastq.gz
Created By: mkimble
LAST MODIFIED: 03/24/2021
"""

from mytd_parser.parse_server_copy import ServerParse

server_parser = ServerParse()

server_parser.move_fastq_files()