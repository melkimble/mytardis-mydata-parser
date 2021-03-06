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
7. Install and setup rclone (instructions: https://blog.spatialmsk.com/cloud-storage-sync/)
8. Add environmental variables from `configs/mytd_api_configs.env.txt` to bashrc:

```commandline
# enter VIM text editor for bashrc
sudo vim ~/.profile
```

```text
# Update with your username and password and paste into the bottom of profile
export MYTARDIS_API_USER=your_mytardis_username
export MYTARDIS_API_PASSWORD=your_mytardis_password
# save and exit VIM
:wq!
```

Normally we would use ~/.bashrc, but since we are running a non-interactive shell, bashrc will not source.
To avoid this issue, the environmental variables are set in ~/.profile and sourced from within `rclone-cron.sh`.

Use `python run.py [command]` to run from Windows. Necessary to get around 
`FileNotFoundError: [WinError 2] The system cannot find the file specified`. 
Files missing from `python setup.py install` for Windows. 
To uninstall, run `pip uninstall mydata-python`.