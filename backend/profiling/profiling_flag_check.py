import sys,os,time,shutil
sys.path.append(os.getcwd())
import pandas as pd
import s3fs
import confs.config as conf
import utils.logs as logs
import profiling.df_profile as df_profile, utils.file_io as file_io
import db.db as db, db.db_datasets_versions as db_datasets_versions, db.db_datasets as db_datasets

LOGGER = logs.get_logger()

def check_profile_flag():
    while True:
        sess = db.get_session()
        try:
            read_data = db.read_datasets(sess)      #obtain dataset info   
            read_data_msg = read_data["msg"]
            dataset_id_list = []
            for info in read_data_msg:
                id_val = info["dataset_id"]
                dataset_id_list.append(id_val)              #list of dataset id
            for dataset_id in dataset_id_list:
                get_dataset_version = db_datasets_versions.get_max_ds_ver_id(sess,dataset_id)
                dataset_vers_id = get_dataset_version["msg"]
                if dataset_vers_id != -1:
                    get_data_vers_table = db_datasets_versions.read_datasets_versions(sess,dataset_id,dataset_vers_id)
                    dataset_info_table = get_data_vers_table["msg"]
                    dataset_info = dataset_info_table[0]                #obtain dataset profiling info
                    dataset_location = dataset_info['location']
                    profiling_status = dataset_info['profiling_done']
                    profile_job_id = dataset_info['profiling_job_id']
                    if profiling_status == False:
                        try:
                            dataset_download = file_io.download_file(dataset_id,dataset_vers_id,dataset_location)
                            print(dataset_download)
                            data_file = pd.read_csv(os.path.join('/home/ubuntu/integration/Pace-ML/backend',dataset_download))
                            profiling = file_io.save_profile(data_file,dataset_id,dataset_vers_id)
                            profiling_completed = db_datasets_versions.update_profiling_details(sess,dataset_id,dataset_vers_id,True, profile_job_id)
                            print("Data Profiling completed for ID = ",dataset_id)
                        except:
                            print("Error during Profiling for ID = ",dataset_id)
                    else:
                        print("Dataset Profile already exists for ID = ",dataset_id)
                else:
                    print("Dataset version not found for ID = ",dataset_id)
        except Exception as e:
            print(e)
        finally:
            sess = db.close_session()
            time.sleep(2)                                         #15 min

if __name__ == '__main__':
    # run app in debug mode on port 8215
    check_profile_flag()