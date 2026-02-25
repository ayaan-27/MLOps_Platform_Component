import json
import os
import ast
import pandas as pd
import pickle
from flask import Flask, Response, jsonify, render_template, request
from flask.wrappers import Request
from flask_cors import CORS, cross_origin
from sqlalchemy.dialects import registry
from werkzeug.datastructures import Authorization
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename

from jobs.publish import publish

import adminstration.role_management as role_mgmt
import adminstration.user_management as usr_mgmt
import datasets.datasets as ds
import projects.projects as projects
import jobs.job_management as jm
import model_hub.model_hub_management as mm
import deploy.deployment as dp
from user_auth import user_auth
from utils.logs import get_logger

import confs.config as conf
import utils.logs as logs
import profiling.df_profile as df_profile, utils.file_io as file_io
import db.db as db
import db.db_proj_user as project_user



LOGGER = get_logger("Flask.log")


app = Flask(__name__)

CORS(app)


@app.route("/login", methods=["POST"])
@cross_origin(supports_credentials=True)
def login():
    success = False
    resp = ""
    token = ""
    try:
        if request.method == "POST":
            req_json = request.json
            username = req_json["username"]
            password = req_json["password"]

            result = user_auth.login(username=username, password=password)
            success = True if result["status"] == 1 else False
            resp = result["msg"]
            token = result["token"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp, "token": token})
        resp.status_code = 200 if success is True else 500
        return resp

#dashborad
@app.route("/user_role",methods=["POST"])
def user_role():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            user_id = req_json["user_id"]
            proj_id = req_json["proj_id"]
            sess = db.get_session()
            result = project_user.get_project_users(sess,user_id=user_id,proj_ids=[proj_id])
            sess = db.close_session()
            user_name = []
            role_name = []
            for index,i in enumerate(result["msg"]):
                userid_result = usr_mgmt.get_users(user_id=i['user_id'])
                user_name.append(userid_result["msg"][0]['name'])
                roleid_result = role_mgmt.get_roles()
                for j in roleid_result["msg"]:
                    if j["role_id"] == result["msg"][index]["role_id"] :
                        role_name.append(j["role_name"])        
        success = True if result["status"] == 1 else False
        resp = {"usernamelist":user_name,"rolenamelist":role_name}

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp



#! User Management
@app.route("/get_users", methods=["GET", "POST"])
def get_users():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            user_id = req_json["user_id"]
            result = usr_mgmt.get_users(user_id=user_id)
        else:
            result = usr_mgmt.get_users()
        success = True if result["status"] == 1 else False
        resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_user", methods=["POST"])
def create_user():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            name = req_json["name"]
            email_id = req_json["email_id"]
            login_id = req_json["login_id"]
            pwd = req_json["pwd"]
            persona_id = req_json["persona_id"]
            creation_user_id = req_json["creation_user_id"]

            result = usr_mgmt.create_user(
                name=name,
                email_id=email_id,
                login_id=login_id,
                pwd=pwd,
                persona_id=persona_id,
                creation_user_id=creation_user_id,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/edit_user", methods=["POST"])
def edit_user():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            user_id = req_json["user_id"]
            creation_user_id = req_json["creation_user_id"]
            email_id = req_json["email_id"]
            name = req_json["name"]
            pwd = req_json["pwd"]
            persona_id = req_json["persona_id"]

            result = usr_mgmt.edit_user(
                user_id=user_id,
                creation_user_id=creation_user_id,
                name=name,
                email_id=email_id,
                pwd=pwd,
                persona_id=persona_id,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/delete_user", methods=["POST"])
def delete_user():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            user_ids = req_json["user_ids"]
            result = usr_mgmt.delete_user(user_ids=user_ids)
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


#! Role Management
@app.route("/get_roles", methods=["GET", "POST"])
def get_roles():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            role_id = req_json["role_id"]
            result = role_mgmt.get_role_permission_map(role_id)
        else:
            result = role_mgmt.get_roles()
        success = True if result["status"] == 1 else False
        resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/get_modules", methods=["GET"])
def get_modules():
    success = False
    resp = ""
    try:
        if request.method == "GET":
            result = role_mgmt.get_modules()
        success = True if result["status"] == 1 else False
        resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_role", methods=["POST"])
def create_role():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            role_name = req_json["role_name"]
            creation_user_id = req_json["creation_user_id"]
            module_ids = req_json["module_ids"]
            perm_lvls = req_json["perm_lvls"]
            result = role_mgmt.create_role(
                role_name=role_name,
                creation_user_id=creation_user_id,
                module_ids=module_ids,
                perm_lvls=perm_lvls,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/update_role", methods=["POST"])
def update_role():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            role_id = req_json["role_id"]
            creation_user_id = req_json["creation_user_id"]
            module_ids = req_json["module_ids"]
            perm_lvls = req_json["perm_lvls"]
            result = role_mgmt.update_roles(
                role_id=role_id,
                creation_user_id=creation_user_id,
                module_ids=module_ids,
                perm_lvls=perm_lvls,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/delete_role", methods=["POST"])
def delete_role():
    success = False
    resp = ""

    try:
        if request.method == "POST":
            req_json = request.json
            role_ids = req_json["role_ids"]
            result = role_mgmt.delete_role(role_ids=role_ids)
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

#! Project Management
@app.route("/get_proj", methods=["GET","POST"])
def get_proj():
    success = False
    resp = ""
    try:
        if request.method == "GET":
            result = projects.get_proj()
        else:
            req_json = request.json
            proj_id = req_json["proj_id"]
            user_id = req_json["user_id"]
            result = projects.get_proj(proj_id = proj_id, user_id = user_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/get_proj_users", methods=["GET","POST"])
def get_proj_users():
    success = False
    resp = ""
    try:
        if request.method == "GET":
            result = projects.get_proj_users()
        else:
            req_json = request.json
            proj_ids = req_json["proj_ids"]
            user_id = req_json["user_id"]
            result = projects.get_proj_users(proj_ids = proj_ids, user_id = user_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/create_proj", methods=["POST"])
def create_proj():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json["value"]
            creation_user_id = req_json["creation_user_id"]
            name = req_json["name"]
            desc = req_json["desc"]
            project_user_info = {}
            project_user_info['user_role_ids'] = {}
            for i in range (0,len(req_json['user_info'])):
                project_user_info['user_role_ids'][req_json['user_info'][i]['User_id']] = req_json['user_info'][i]['Role_id']    
            user_role_ids = project_user_info['user_role_ids']
            attach_dataset = req_json["attach_dataset"]
            dataset_id = req_json["dataset_id"]
            version_id = req_json["version_id"]
            result = projects.create_proj(creation_user_id = creation_user_id,\
                            name = name, desc = desc, user_role_ids = user_role_ids,\
                            attach_dataset = attach_dataset, dataset_id = dataset_id,\
                            version_id = version_id)
        
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/del_proj", methods = ["POST"])
def del_proj():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            proj_id = req_json["proj_id"]
            result = projects.del_proj(proj_id= proj_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/edit_proj", methods=["POST"])
def edit_proj():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            proj_id = req_json["proj_id"]
            creation_user_id = req_json["creation_user_id"]
            desc = req_json["desc"]
            user_role_ids = req_json["user_role_ids"]        
            
            result = projects.edit_proj(proj_id= proj_id, creation_user_id= creation_user_id, desc= desc, user_role_ids= user_role_ids)
        
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

# Name and ID mapping   

#! Dataset Management - Create Data
@app.route("/create_data", methods=["POST"])
def create_data():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            files = request.files.getlist("file")
            for file in files:
                filename = secure_filename(file.filename)
                if not os.path.exists('upload_folder'):
                    os.mkdir('upload_folder')
                file.save(os.path.join("upload_folder", filename))
            dataset = pd.read_csv("upload_folder/{}".format(filename))
            name = request.form.get("name")
            ds_desc = request.form.get("description")
            creation_user_id = request.form.get("user_id")
            result = ds.create_dataset(
                name=name,
                ds_desc=ds_desc,
                creation_user_id=creation_user_id,
                dataset=dataset,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_data/get_preview", methods=["POST"])
def get_preview():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]
            version_id = req_json["version_id"]
            result = ds.get_preview(dataset_id=dataset_id, version_id=version_id)
            success = True if result["status"] == 1 else False
            resp = result["msg"].to_dict(orient="records")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_data/save_file_encoding", methods=["POST"])
def save_encoding():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]
            version_id = req_json["version_id"]
            skiprows = int(req_json["skiprows"])
            header_string = req_json["header"]
            header = ast.literal_eval(header_string)
            sep = req_json["sep"]
            encoding = req_json["encoding"]
            result = ds.save_dataset_file_encoding(
                dataset_id=dataset_id,
                version_id=version_id,
                skiprows=skiprows,
                header=header,
                sep=sep,
                encoding=encoding,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_data/get_meta", methods=["POST"])
def get_meta():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]
            version_id = req_json["version_id"]
            result = ds.read_meta(
                dataset_id=dataset_id,
                version_id=version_id
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_data/save_user_schema", methods=["POST"])
def save_user_schema():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]
            version_id = req_json["version_id"]
            columns_include = req_json["columns_include"]
            column_types = req_json["column_types"]
            pii_columns = req_json["pii_columns"]
            result = ds.save_user_provided_schema(
                dataset_id=dataset_id,
                version_id=version_id,
                columns_include=columns_include,
                column_types=column_types,
                pii_columns=pii_columns,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_data/get_proj_list", methods=["POST"])
def get_proj_list():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            req_json = request.json
            user_id = req_json["user_id"]
            result = projects.get_proj(user_id=user_id )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_data/project_access", methods=["POST"])
def project_access():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]
            project_ids = req_json["project_ids"]
            creation_user_id = req_json["creation_user_id"]
            public = ast.literal_eval(req_json["public"].capitalize())
            if public == True:
                project_ids.append(1)
            result = ds.project_access(
                dataset_id=dataset_id,
                project_ids=project_ids,
                creation_user_id=creation_user_id,
                public=public,
            )
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


@app.route("/create_data/submit", methods=["POST"])
def submit():
    success = False
    resp = ""
    result = {"status": 0, "msg": "some eror occured"}

    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]
            result = ds.visibility_change(dataset_id=dataset_id)
            success = True if result["status"] == 1 else False
            resp = result["msg"]

    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp



#! Dataset Management
@app.route("/get_datasets", methods=["POST"])
def get_datasets():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            user_id = req_json["user_id"]
            result = ds.get_datasets(user_id=user_id)
        
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/get_datasets_versions", methods=["POST"])
def get_datasets_versions():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]
            user_id = req_json["user_id"]

            result = ds.get_datasets_versions(dataset_id= dataset_id, user_id= user_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/del_dataset", methods=["POST"])
def del_dataset():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            dataset_id = req_json["dataset_id"]

            result = ds.del_dataset(dataset_id= dataset_id)
        
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/pre_process", methods=["POST"])
def pre_process():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            result = publish(req_json)            
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/data_location", methods=["POST"])
def data_location():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            project_id = req_json["project_id"]
            user_id = req_json["user_id"]  
            result = ds.get_data(project_id=project_id,user_id=user_id)         
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/pre_process_map", methods=["POST"])
def pre_process_map():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            project_id = req_json["project_id"]
            user_id = req_json["user_id"]  
            result = ds.pre_process_map(project_id=project_id,user_id=user_id)         
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/get_target_col", methods=["POST"])
def get_target_col():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            project_id = req_json["project_id"]
            user_id = req_json["user_id"]  
            result = ds.get_target_col(project_id=project_id,user_id=user_id)         
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

#send data profile location to frontend based on given project id and user id

@app.route('/send_profile', methods = ['POST'])
def send_profile_location():
    result = {"status": 0,"msg": "Some Error occured"}
    json_data = request.json
    project_id = int(json_data["project_id"])   
    user_id = int(json_data["user_id"])  
    sess = db.get_session()
    pdvu_details = db.get_ds_ver(sess,project_id,user_id)
    pdvu_msg = pdvu_details["msg"]
    dataset_details = pdvu_msg[0]
    dataset_id = dataset_details["dataset_id"]
    dataset_vers_id = dataset_details["version_id"]
    profile_location = os.path.join(file_io.S3_LOCATION,str(dataset_id),str(dataset_vers_id),"dataset_profile.html")
    result = {"status": 1,"msg":profile_location}
    sess = db.close_session()
    return result

@app.route("/augment", methods=["POST"])
def augment():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            result = publish(req_json)            
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/feature_eng", methods=["POST"])
def feature_eng():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            result = publish(req_json)            
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/job_monitor", methods=["POST"])
def job_monitor():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            job_id = int(req_json["job_id"])   
            result = jm.monitor_job(job_id=job_id)       
        success = True if result["status"] == 1 else False
        resp = {"job status":result["msg"]}
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/list_jobs", methods=["POST"])
def list_jobs():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            project_id = int(req_json["project_id"])
            user_id = int(req_json["user_id"])
            result = jm.list_jobs(project_id=project_id, user_id=user_id)       
        success = True if result["status"] == 1 else False
        resp = {"job status":result["msg"]}
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/auto_ml", methods=["POST"])
def auto_ml():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            result = publish(req_json)         
        success = True if result["status"] == 1 else False
        resp = {"job status":result["msg"]}
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp


#! Model maangement
@app.route("/register_model", methods=["POST"])
def register_model():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            model_name = req_json["model_name"]
            model_version_name = req_json["model_version_name"]
            project_id = req_json["project_id"]
            mlflow_runid = req_json["mlflow_runid"]
            job_id = req_json["job_id"]
            user_id = req_json["user_id"]
            model_param = req_json["model_param"]
            model_hyperparameters = req_json["model_hyperparameters"]
            result=mm.create_model(
                model_name=model_name,model_version_name=model_version_name,
                project_id=project_id, mlflow_runid=mlflow_runid, 
                job_id=job_id, user_id=user_id, model_param=model_param, 
                model_hyperparameters=model_hyperparameters)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/register_model_version", methods=["POST"])
def register_model_version():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            model_id = req_json["model_id"]
            model_version_name = req_json["model_version_name"]
            mlflow_runid = req_json["mlflow_runid"]
            job_id = req_json["job_id"]
            user_id = req_json["user_id"]
            model_param = req_json["model_param"]
            model_hyperparameters = req_json["model_hyperparameters"]
            result=mm.create_model_version(
                model_id=model_id, user_id=user_id,
                model_version_name=model_version_name,mlflow_runid=mlflow_runid, 
                job_id=job_id, model_param=model_param, 
                model_hyperparameters=model_hyperparameters)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/read_models", methods=["POST"])
def read_models():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            user_id = req_json["user_id"]
            result=mm.read_models(user_id=user_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/read_model_versions", methods=["POST"])
def read_model_versions():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            model_id = req_json["model_id"]
            result=mm.read_model_version(model_id=model_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/delete_model", methods=["POST"])
def delete_model():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            model_id = req_json["model_id"]
            result=mm.del_model(
                model_id=model_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/delete_model_versions", methods=["POST"])
def delete_model_versions():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            model_id = req_json["model_id"]
            version_id = req_json["version_id"]
            result=mm.del_model_version(
                model_id=model_id, version_id=version_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

#! Deployment
@app.route("/deploy", methods=["POST"])
def deplopy():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            model_id = req_json["model_id"]
            version_id = req_json["version_id"]
            user_id = req_json["user_id"]
            name = req_json["name"]
            status = req_json["status"]
            access_lvl  = req_json["access_lvl"]

            result=dp.create_deployment(
                model_id=model_id, version_id=version_id, user_id=user_id, name=name,
                status=status, access_lvl=access_lvl
                )
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

@app.route("/list_deployment", methods=["POST"])
def list_deployment():
    success = False
    resp = ""
    try:
        if request.method == "POST":
            req_json = request.json
            user_id = req_json["user_id"]
            result=dp.read_deployments(user_id=user_id)
        success = True if result["status"] == 1 else False
        resp = result["msg"]
    except Exception as e:
        LOGGER.exception(e)
    finally:
        resp = jsonify({"success": success, "details": resp})
        resp.status_code = 200 if success is True else 500
        return resp

# endpoint for inference
@app.route('/inference', methods = ["POST", "GET"])
def inference():
    req_json = request.json
    data_json= pd.DataFrame(req_json['data'])
    model_details_json = req_json['model_details']
    model_id = model_details_json['model_id']
    version_id = model_details_json['version_id']
    models = os.path.join("Models",str(model_id),str(version_id))
    model_pipeline_dict = mm.get_pipline_dict(model_id,version_id)
    model_pipeline_msg = model_pipeline_dict['msg']
    for seq_id,seq_details in sorted(list(model_pipeline_msg.items()), reverse = True):
        processed_data = data_json
        if seq_details['process_name']=='preprocess': 
            file_name = seq_details['file_name']
            artif_loc = os.path.join(models,file_name)
            loaded_model = pickle.load(open(artif_loc, 'rb'))
            processed_data= pd.DataFrame(loaded_model.transform(processed_data),columns=processed_data.columns)
            data_json = processed_data
        if seq_details['process_name']=='feature_eng':
            file_name = seq_details['file_name']
            artif_loc = os.path.join(models,file_name)
            loaded_model = pickle.load(open(artif_loc, 'rb'))
            processed_data= loaded_model.transform(processed_data)
            processed_data = pd.DataFrame(processed_data)
            data_json = processed_data
    new_col = []
    for col in processed_data.columns:
        new_col.append(''.join(e for e in col if e.isalnum()))
    processed_data.columns = new_col
    artif_loc = os.path.join(models,"finalized_model.sav")
    model = pickle.load(open(artif_loc, 'rb'))
    prediction = model.predict(processed_data)

    return jsonify(prediction.tolist())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8201, threaded=True, debug=True)

