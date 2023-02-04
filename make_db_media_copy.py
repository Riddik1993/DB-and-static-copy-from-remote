import time
from datetime import datetime
import os
import paramiko
import sh
from loguru import logger
from paramiko.client import SSHClient
from paramiko.sftp_client import SFTPClient
from pyhocon import ConfigFactory

conf = ConfigFactory.parse_file('config.conf')
REMOTE_HOST = conf['connection.host']
REMOTE_USER = conf['connection.user']
REMOTE_PORT = conf['connection.port']
REMOTE_DBCOPY_FOLDER = conf['load-params.remote-dbcopy-folder']
LOCAL_DBCOPY_FOLDER = conf['load-params.local-dbcopy-folder']
OLD_DB_COPIES_EXP_PERIOD = conf['load-params.old-db-copies-exp-period']
REMOTE_MEDIA_PATH = conf['load-params.remote-media-path']
LOCAL_MEDIA_PATH = conf['load-params.local-media-path']


def _initialize_ssh_client(remote_host: str, remote_user: str, remote_port: int) -> SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=remote_host, username=remote_user, port=remote_port)
    logger.info(f"Connected to remote host {remote_host} successfully!")
    return client


def _get_db_copy_remote_filename() -> str:
    now_str = datetime.now().strftime("%d%m%Y_%H%M%S")
    dbcopy_filename = f"{now_str}.sql"
    return dbcopy_filename


def _get_db_copy_full_path(remote_dbcopy_folder: str, dbcopy_filename: str) -> str:
    return os.path.join(remote_dbcopy_folder, dbcopy_filename)


def make_db_backup(ssh_client: SSHClient, remote_db_copy_file_path: str):
    cmd = f'sudo -u postgres pg_dumpall -c -f {remote_db_copy_file_path}'
    stdin, stdout, stderr = ssh_client.exec_command(cmd)
    errors_lst = stderr.readlines()
    if len(errors_lst) == 0:
        time.sleep(2)
        logger.info(f"DB copy saved successfully on remote host: {remote_db_copy_file_path}")
    else:
        error_msg = f"Error during creating copy on remote host {' '.join(errors_lst)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def upload_db_backup_to_local_machine(ftp_client: SFTPClient, remote_file_path: str, dbcopy_full_local_path: str):
    ftp_client.get(remote_file_path, dbcopy_full_local_path)
    logger.info(f"DB copy downloaded successfully to {dbcopy_full_local_path}")


def delete_old_copies_on_remote(ssh_client: SSHClient, expiration_period_days: int) -> None:
    cmd = f"find -type f -name '*.sql' -ctime +{expiration_period_days} -delete"
    stdin, stdout, stderr = ssh_client.exec_command(cmd)
    errors_lst = stderr.readlines()
    if len(errors_lst) == 0:
        logger.info(f"Removed db_copies older than {expiration_period_days} days from remote server")
    else:
        error_msg = f"Error during removing old copies from remote server"
        logger.error(error_msg)
        raise Exception(error_msg)


def _create_local_media_folder(local_static_path: str) -> str:
    static_folder_name = datetime.now().strftime("%d%m%Y_%H%M%S")
    static_folder_full_path = os.path.join(local_static_path, static_folder_name)
    cmd = f"mkdir {static_folder_full_path}"
    os.system(cmd)
    logger.info(f"Created local folder for static copy: {static_folder_full_path}")
    return static_folder_full_path


def copy_media_files_to_local(remote_user: str, remote_host: str, remote_media_path: str, local_media_path: str):
    local_media_full_path = _create_local_media_folder(local_media_path)
    cmd_args = f"-r {remote_user}@{remote_host}:{remote_media_path} {local_media_full_path}"
    cmd_args_lst = cmd_args.split()

    logger.info(f"Start loading files from {remote_media_path}")
    sh.scp(*cmd_args_lst)
    logger.info(f"Static files copied successfully to {local_media_full_path}")


if __name__ == '__main__':
    ssh_client = _initialize_ssh_client(REMOTE_HOST, REMOTE_USER, REMOTE_PORT)
    ftp_client = ssh_client.open_sftp()
    try:
        dbcopy_filename = _get_db_copy_remote_filename()
        dbcopy_local_path = _get_db_copy_full_path(LOCAL_DBCOPY_FOLDER, dbcopy_filename)
        remote_dbcopy_full_path = _get_db_copy_full_path(REMOTE_DBCOPY_FOLDER, dbcopy_filename)

        make_db_backup(ssh_client, remote_dbcopy_full_path)
        upload_db_backup_to_local_machine(ftp_client, remote_dbcopy_full_path, dbcopy_local_path)
        delete_old_copies_on_remote(ssh_client, OLD_DB_COPIES_EXP_PERIOD)
        copy_media_files_to_local(REMOTE_USER, REMOTE_HOST, REMOTE_MEDIA_PATH, LOCAL_MEDIA_PATH)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise e
    finally:
        ftp_client = ftp_client.close()
