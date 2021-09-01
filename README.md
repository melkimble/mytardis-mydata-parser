# mytardis-mydata-parser
`mytardis-mydata-parser` filters and parses Illumina MiSeq run results and uploads them to MyTardis via mydata-python and moves 
parsed and original files to desired backup location.

## Dependencies
* [mydata-python](https://github.com/mytardis/mydata-python) 
* Python>=3.6

1. Create a virtual environment
2. Python dependencies are installed to your venv from requirements.txt:
`pip install -r requirements.txt`


### Install mydata-python
#### Linux:
3. Clone MyData-Python:
`git clone https://github.com/mytardis/mydata-python.git`
4. `cd mydata-python`
5. `python setup.py install`
6. Use `mydata config generate` to setup a config file with your MyTardis API key and username

Use `python run.py [command]` to run from Windows. Necessary to get around 
`FileNotFoundError: [WinError 2] The system cannot find the file specified`. 
Files missing from `python setup.py install` for Windows. 
To uninstall, run `pip uninstall mydata-python`.

