# postgres_dump_to_sqlite
Dirty script to convert postgres dumps to a singular sqlite db

## Usage

Get the python script and at the bottom fill in the:

input_dir: This is the directory where the sql dumps are located ending in **.sq**
output_dir: This is the directory where the postgres to sqlite temp files will be stored (default it remove the files one they are merged into the sqlite db however there is a line to comment out to stop the files being removed)
db_file: This is the final concatenated sqlite db file

Then run the script (Id suggest using **pypy** to improve the speed if you have it installed it doubled the speed generating the sqlite temp files)
