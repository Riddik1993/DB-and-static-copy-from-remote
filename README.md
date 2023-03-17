## Database-copy (postgres) and static-files loader from remote server

This service will help you not to spend time of making copy of the database
and static files of your project on remote server to your local machine

### Script implements the following steps:
1. Connect to server through SSH (using rsa-key-file)
2. Make a postgres full-cluster copy to specified folder on remote server
   The name of file will include current datetime of operation
   in format **ddmmYYYY_HH24MMSS**, for example **04022023_113145.sql**
3. Upload dbcopy file to specified folder on your local machine
4. Delete db-copies from remote, which are older than specified period (days)
5. Finally load static media files from remote to local. Service firstly
   create a folder with dttm-name in specified directory and than uploads
   media from remote to it

### Steps for usage:
1. pip install -r requirements.txt
2. be sure you can connect to your remote server using 
   file with key (/.ssh/id_dsa), not password
3. specify envs listed in config.conf
4. be sure your user has rights to write to REMOTE_DBCOPY_PATH
   (postgres user will be added automatically)
5. run make_db_media_copy.py


