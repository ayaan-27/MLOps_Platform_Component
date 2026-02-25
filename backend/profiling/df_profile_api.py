import sys,os,json,shutil

sys.path.append(os.getcwd())
from io import StringIO
import pandas as pd,s3fs
from flask import Flask, Response, jsonify, render_template, request
from flask.wrappers import Request
import confs.config as conf
import utils.logs as logs
import profiling.df_profile as df_profile, utils.file_io as file_io
import db.db as db,db.db_datasets_versions as db_datasets_versions, db.db_datasets as db_datasets,db.db_pdvu_mapping as db_pdvu_mapping


LOGGER = logs.get_logger()

app = Flask(__name__)


@app.route('/data_profiling', methods = ['POST'])
def profiling():
    result = {"status": 0,"msg": "Some Error occured"}
    json_data = request.json
    project_id = int(json_data["project_id"])   
    user_id = int(json_data["user_id"])  
    sess = db.get_session()
    pdvu_details = db_pdvu_mapping.get_ds_ver(sess,project_id,user_id)
    pdvu_msg = pdvu_details["msg"]
    dataset_details = pdvu_msg[0]
    dataset_id = dataset_details["dataset_id"]
    dataset_vers_id = dataset_details["version_id"]
    if dataset_vers_id != -1:
        get_data_vers_table = db_datasets_versions.read_datasets_versions(sess,dataset_id,dataset_vers_id)
        dataset_info_table = get_data_vers_table["msg"]
        dataset_info = dataset_info_table[0]                #obtain dictionary
        dataset_location = dataset_info['location']
        profile_job_id = dataset_info['profiling_job_id']
        profiling_status = dataset_info['profiling_done']      
        if profiling_status == False:
            dataset_download = file_io.download_file(dataset_id,dataset_vers_id,dataset_location)
            data_file = pd.read_csv(dataset_download)
            profiling = file_io.save_profile(data_file,dataset_id,dataset_vers_id)
            profiling_completed = db_datasets_versions.update_profiling_details(sess,dataset_id,dataset_vers_id,True, profile_job_id)
            result =  {"status": 1,"msg": "Data Profiling completed"}
        else:
            result =  {"status": 1,"msg": "Dataset Profile already exists"}    
    else:
        result = {"status": 0,"msg": "Dataset Version not found"}
    sess = db.close_session()
    return result 

if __name__ == '__main__':
    # run app in debug mode on port 8215
    app.run(host="0.0.0.0",debug=True,port=8215,threaded=True)