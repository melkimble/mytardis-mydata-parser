import pandas as pd
from mytd_parser.ngs_parse_run import MiSeqParser

parser = MiSeqParser()

# not using mydata-python; just parsing and triggering upload in mydata.exe on daily timer
parser.parse_fastq_files()
parser.complete_backup(move_parsing=False, move_staging=True)

# mydata-python breaking on upload - FileNotFoundError: [WinError 2] The system cannot find the file specified
# added issue to mydata-python repo
#parser.upload_mydata_py()
#parser.upload_mydata_subprocess()

