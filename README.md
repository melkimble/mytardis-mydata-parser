# mytardis-mydata-parser
`mytardis-mydata-parser` filters and parses Illumina MiSeq run results and uploads them to MyTardis via mydata-python and moves 
parsed and original files to desired backup location.

## Dependencies
* [mydata-python](https://github.com/mytardis/mydata-python) 
* Python>=3.6

Python dependencies are installed from requirements.txt:
`pip install -r requirements.txt`

`git clone https://github.com/mytardis/mydata-python.git`

If on Linux:

`cd mydata-python`

`python setup.py install`

Use `python run.py [command]` to run from Windows. Necessary to get around 
`FileNotFoundError: [WinError 2] The system cannot find the file specified`. 
Files missing from `python setup.py install` for Windows.

