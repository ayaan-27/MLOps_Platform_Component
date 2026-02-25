import json
import logging
import os
from pathlib import Path
from threading import Thread

import pandas as pd
import requests
from flask import Flask, Response, jsonify, render_template, request
from flask_cors import CORS
from werkzeug.exceptions import BadRequest

import feast_code.exceptions as Exceptions
import feast_code.feature_store as feature_store
from auto_ml import pycaret_automl_bias_clf as clf
from auto_ml import pycaret_automl_reg as reg
from deployment import sage_endpoint as sagemaker_ep
from feature_engeneering import feature_analysis as fe
from model_hub.db_query_modelhub import return_models_info
from preprocessing import pycaret_preprocessing as pre
from profiling import df_profile

mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI")

app = Flask(__name__)

CORS(app)

app.logger.info("Starting PACE-ML Gunicorn Server...")


@app.route("/ping", methods=["GET"])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    status = 200
    return Response(response="ping success", status=status, mimetype="application/json")


@app.route("/experiment/list", methods=["GET"])
def get_exp_list():
    """Wrapper over Ml flow API and will provide experiments list

    to the UI"""

    try:
        response = requests.get(
            "{}/api/2.0/preview/mlflow/experiments/list".format(mlflow_tracking_uri),
            timeout=5,
        )
    except requests.exceptions.HTTPError as errh:
        return Response(response=str(errh), status=500, mimetype="plain/text")
    except requests.exceptions.ConnectionError as errc:
        return Response(response=str(errc), status=500, mimetype="plain/text")
    except requests.exceptions.Timeout as errt:
        return Response(response=str(errt), status=500, mimetype="plain/text")
    except requests.exceptions.RequestException as err:
        return Response(response=str(err), status=500, mimetype="plain/text")
    if response.status_code == 200:
        return Response(response=response.text, status=200, mimetype="application/json")
    else:
        return Response(
            response="Mlflow service returned 500 response",
            status=response.status,
            mimetype="plain/text",
        )


@app.route("/runs/search", methods=["POST"])
def get_exp_runs():
    """Connects with the Ml flow API and will provide run results
    to the UI
    request : {
                  "experiment_ids":["2","1","0"],
                  "max_results":100
              }
    """

    if request.content_type == "application/json":
        payload = request.json
        if list(payload.keys()) == ["experiment_ids", "max_results"]:
            try:
                url = "{}/api/2.0/mlflow/runs/search".format(mlflow_tracking_uri)
                headers = {"Content-Type": "application/json"}
                response = requests.request(
                    "POST", url, headers=headers, json=payload, timeout=5
                )
            except requests.exceptions.HTTPError as errh:
                return Response(response=str(errh), status=500, mimetype="plain/text")
            except requests.exceptions.ConnectionError as errc:
                return Response(response=str(errc), status=500, mimetype="plain/text")
            except requests.exceptions.Timeout as errt:
                return Response(response=str(errt), status=500, mimetype="plain/text")
            except requests.exceptions.RequestException as err:
                return Response(response=str(err), status=500, mimetype="plain/text")

            if response.status_code == 200 and len(response.text) > 10:
                return Response(
                    response=response.text, status=200, mimetype="application/json"
                )
            else:
                return Response(
                    response="Mlflow service returned 500 response , check your request json",
                    status=response.status,
                    mimetype="plain/text",
                )
        else:
            return Response(
                response="Keys are not proper, should be ['experiment_ids', 'max_results']",
                mimetype="plain/text",
            )
    else:
        return Response(
            response="Please provide appliation/json format", mimetype="plain/text"
        )


@app.route("/preprocessing", methods=["POST"])
def preprocess():
    params = {}
    # Fetched parameters
    payload = request.form
    params["is_znz"] = payload["is_znz"] == "TRUE"
    params["is_club_rare"] = payload["is_club_rare"] == "TRUE"
    params["is_untrained_lvls"] = payload["is_untrained_lvls"] == "TRUE"
    params["is_ordinal_encoding"] = payload["is_ordinal_encoding"] == "TRUE"
    params["is_cardinality_reduction"] = payload["is_cardinality_reduction"] == "TRUE"
    params["is_binning"] = payload["is_binning"] == "TRUE"
    params["imputation_type"] = payload["imputation_type"]
    params["num_imputation_strategy"] = payload["num_imputation_strategy"]
    params["cat_imputation_strategy"] = payload["cat_imputation_strategy"]

    # Fetched target aname
    target = payload["target_variable"]

    # Fetched file
    csv_file = request.files["inputFile"]
    csv_file.save("common_data/input.csv")
    df = pd.read_csv("common_data/input.csv")
    result = pre.preprocess(df, target, params)

    # output_stream = StringIO()
    # result.to_csv(output_stream)
    # output_df = output_stream.getvalue()
    json = result.to_json(orient="records")
    return Response(response=json, status=200, mimetype="application/json")
    # return Response(response=output_df, status=200, mimetype='text/csv')


@app.route("/featureEngineering", methods=["POST"])
def feature_engineering():
    params = {}

    # Fetched parameters
    payload = request.form
    params["is_pca"] = payload["is_pca"] == "TRUE"
    params["is_poly"] = payload["is_poly"] == "TRUE"
    params["is_interaction"] = payload["is_interaction"] == "TRUE"
    params["is_multicol"] = payload["is_multicol"] == "TRUE"
    params["is_cluster"] = payload["is_cluster"] == "TRUE"
    params["is_power"] = payload["is_power"] == "TRUE"

    # Fetched target aname
    target = payload["target_variable"]
    # Fetched file
    csv_file = request.files["inputFile"]
    csv_file.save("input.csv")
    df = pd.read_csv("input.csv")
    result = fe.feat_engg(df, target, params)
    json = result.to_json(orient="records")
    return Response(response=json, status=200, mimetype="application/json")


# automl-pycaret


@app.route("/automlpycaret", methods=["POST"])
def run_automl():
    # if request.content_type == 'application/json':
    sample_input = request.form

    input_csv = sample_input["Rows"]

    sample_file_name = "Input.csv"
    input_df = pd.read_json(input_csv, encoding="utf-8")
    input_df.to_csv("common_data/" + sample_file_name, index=False)
    target_variable = str(sample_input["Target_Variable"])
    optimize_on = str(sample_input["Optimize_On"])
    modelling_techniques = str(sample_input["Modelling_Techniques"])
    bias_detection = str(sample_input["Bias_Detection"])
    bias_metric = str(sample_input["Bias_Metric"])
    sensitive_attribute = str(sample_input["Sensitive_Attribute"])
    tune_model = str(sample_input["Tune_Model"])

    if bias_detection == "True":
        bias_detection = True
    else:
        bias_detection = False

    if tune_model == "True":
        tune_model = True
    else:
        tune_model = False

    bias_metric_dict = {
        "Disparate Impact": "DI",
        "Demographic Parity": "DPD",
        "Equal Opportunity": "EOD",
        "Equalized Odds": "EOR",
    }

    if bias_metric in bias_metric_dict.keys():
        bias_metric = bias_metric_dict[bias_metric]

    if modelling_techniques == "Classification":
        try:
            score_grid = clf.automl_pipeline(
                path="common_data/" + sample_file_name,
                target=target_variable,
                mlflow_tracking_uri=mlflow_tracking_uri,
                experiment_name="Automl_clf",
                optimize=optimize_on,
                sensitive_attribute=sensitive_attribute,
                bias_metric="EOR",
                bias_detection=bias_detection,
                tune=tune_model,
                score_grid=True,
                return_model=False,
            )
            lb_json = score_grid

        except Exception as e:
            return Response(response=str(e), status=200, mimetype="text/plain")

    else:
        try:
            score_grid = reg.automl_pipeline(
                path="common_data/" + sample_file_name,
                target=target_variable,
                mlflow_tracking_uri=mlflow_tracking_uri,
                experiment_name="Automl_reg",
                optimize=optimize_on,
                tune=tune_model,
                score_grid=True,
                return_model=False,
            )
            lb_json = score_grid
        except Exception as e:
            return Response(response=str(e), status=200, mimetype="text/plain")
    return Response(response=json.dumps(lb_json), status=200, mimetype="application/json")


@app.route("/createendpoint", methods=["POST"])
def create():
    data = ""
    if request.content_type == "application/json":
        try:
            data = json.loads(request.data)
        except Exception as e:
            return Response(
                response="Data loading exception", status=500, mimetype="application/json"
            )
        try:
            result = sagemaker_ep.create_endpoint(data)
        except Exception as e:
            return Response(
                response="Endpoint creation Exception", status=500, mimetype="text/plain"
            )
        return Response(response=result, status=200, mimetype="application/json")
    else:
        return Response(
            response="Please send a proper format file", status=500, mimetype="text/plain"
        )


@app.route("/deleteendpoint", methods=["POST"])
def delete():
    data = ""
    if request.content_type == "application/json":
        try:
            data = json.loads(request.data)
        except Exception as e:
            return Response(response=e, status=500, mimetype="application/json")
        try:
            result = sagemaker_ep.delete_endpoint(data)
        except Exception as e:
            return Response(response=e, status=500, mimetype="text/plain")
        return Response(response=result, status=200, mimetype="text/plain")
    else:
        return Response(
            response="Please send a proper format file", status=500, mimetype="text/plain"
        )


@app.route("/generate_profile", methods=["POST", "GET"])
def return_data_profile():
    df_profile.generate_data_profile()
    return render_template("report.html")


@app.route("/input", methods=["POST", "GET"])
def dataingestion():
    sample_input = request.form
    input_csv = sample_input["Rows"]
    sample_file_name = "Input.csv"
    input_df = pd.read_json(input_csv, encoding="utf-8")
    input_df.to_csv("common_data/" + sample_file_name, index=False)
    check = "SAVED SUCCESSFULLY !!"
    thread = Thread(target=df_profile.generate_data_profile)
    thread.start()
    resp = jsonify({"check": check})
    return resp


# TODO rename


@app.route("/name", methods=["POST", "GET"])
def init_function():
    cred = pd.read_csv("common_data/Input.csv", encoding="utf-8")

    data_rows = cred.to_dict("records")
    data_header = list(cred.columns)
    # resp = jsonify({'header':header,'leaderboard':leaderboard})
    resp = jsonify({"Data_Rows": data_rows, "Data_Columns": data_header})
    # resp = jsonify({'Target_Variable':Target_Variable,'method':method,'ml_method':ml_method})
    return resp


@app.route("/modelhub", methods=["GET"])
def modelhub():
    try:
        jsonresult = return_models_info()
        return Response(
            response=json.dumps(jsonresult), status=200, mimetype="application/json"
        )
    except Exception as e:
        print(e)
        return Response(response=e, status=200, mimetype="application/json")


@app.route("/feature_group_persist", methods=["POST"])
def feature_store_persist():
    """Stores features from a dataset into a feature group.
    request fromat : {
                        "Rows":[
                                {
                                    'feature_1':numeric_value_1,
                                    'feature_2':'value_2',
                                    'feature_3':'value_3',
                                },
                                {
                                    'feature_1':numeric_value_2,
                                    'feature_2':'value_5',
                                    'feature_3':'value_6',
                                }
                            ],
                        "entity_name":"Primary key column of df, has to be numeric",
                        "feature_group_name":"Name for the feature group created"
                    }
    response fromat : {
                        "response":"Feature Group Created"
                    }
    """
    input_requirements = request.form
    feature_store_artifacts = str(Path.cwd()) + "/feature_store"
    feature_groups_uri = "file:////{}".format(feature_store_artifacts)

    if input_requirements["entity_name"]:
        entity_column_name = input_requirements["entity_name"]
    else:
        raise (Exceptions.Entity_Key_Empty)

    if input_requirements["feature_group_name"]:
        feature_group_name = input_requirements["feature_group_name"]
    else:
        raise (Exceptions.Feature_Group_Key_Empty)
    if input_requirements["Rows"]:
        input_csv = input_requirements["Rows"]
    else:
        raise (Exceptions.Input_Missing)
    sample_input = pd.read_json(input_csv, encoding="utf-8")
    columns = feature_table_columns = list(sample_input.columns)
    columns.remove(entity_column_name)
    entity_key = feature_store.create_entity(
        entity_column_name, str(sample_input[entity_column_name].dtypes)
    )
    feature_store.client.apply(entity_key)

    features = feature_store.create_features(sample_input, columns)
    # Creates a feature table from the list of features created
    try:
        feature_table = feature_store.create_feature_table(
            feature_group_name, entity_key.name, features, feature_groups_uri
        )
        feature_table_columns = columns.copy()
        feature_table_columns.append(entity_column_name)
        feature_store.ingest_data_for_feature_group(
            sample_input, feature_table_columns, feature_table
        )
        return Response(
            response=json.dumps({"response": "Feature Group Created"}),
            status=200,
            mimetype="application/json",
        )
    except Exceptions.Entity_Key_Empty:
        response_msg = {"response": "Please enter a string value for 'entity_name' key"}
        return Response(
            response=json.dumps(response_msg), status=400, mimetype="application/json"
        )
    except Exceptions.Feature_Group_Key_Empty:
        response_msg = {
            "response": "Please enter a string value for 'feature_group_name' key"
        }
        return Response(
            response=json.dumps(response_msg), status=400, mimetype="application/json"
        )
    except Exceptions.Input_Missing:
        response_msg = {
            "response": "Please enter a string value for 'feature_group_name' key"
        }
        return Response(
            response=json.dumps(response_msg), status=400, mimetype="application/json"
        )
    except Exception as e:
        response_msg = {
            "response": "Please use an alternative name for your feature group"
        }
        return Response(
            response=json.dumps(response_msg), status=422, mimetype="application/json"
        )


@app.route("/feature_groups_list", methods=["GET"])
def feature_groups():
    """Sends a list of feature groups created.
    response fromat : {
                        [feature_grp_1,feature_grp_2]
                    }
    """
    input_requirements = request.form
    feature_store_artifacts = str(Path.cwd()) + "/feature_store"
    feature_groups_uri = "file:////{}".format(feature_store_artifacts)
    feature_table_list = []
    flag = bool(feature_store.client.list_feature_tables())
    if flag:
        for feature_table in feature_store.client.list_feature_tables():
            ft = json.loads(str(feature_table))["spec"]["name"]
            feature_table_list.append(ft)
        return Response(
            response=json.dumps(feature_table_list),
            status=200,
            mimetype="application/json",
        )

    else:
        response_msg = {"response": "No feature groups created"}
        return Response(
            response=json.dumps(response_msg), status=404, mimetype="application/json"
        )


@app.route("/feature_group_return", methods=["POST"])
def feature_store_select():
    """Returns a feature group from the database, cached in redis.
    request fromat : {
                        "feature_group_name":"Name for the feature group created"
                    }
    response fromat : {
                       "values":list_of_df_rows,
                       "header":list_of_df_header
                    }
    """

    input_requirements = request.form
    feature_store_artifacts = str(Path.cwd()) + "/feature_store"
    feature_groups_uri = "file:////{}".format(feature_store_artifacts)

    if input_requirements["feature_group_name"]:
        feature_group_name = input_requirements["feature_group_name"]
    else:
        raise (BadRequest)

    try:
        retreived_feature_group = feature_store.read_parquet(
            feature_groups_uri + "/" + feature_group_name
        )
        output_df = retreived_feature_group.drop(["datetime", "date"], axis=1)
        fs_list = list(output_df.T.to_dict().values())
        fs_header = list(output_df.columns)
        fs_json = {"values": fs_list, "header": fs_header}
        return Response(
            response=json.dumps(fs_json), status=200, mimetype="application/json"
        )
    except BadRequest:
        response_msg = {
            "response": "Please enter a string value for 'feature_group_name' key"
        }
        return Response(
            response=json.dumps(response_msg), status=400, mimetype="application/json"
        )
    except Exception as e:
        response_msg = {"response": "Please select from existing feature groups"}
        return Response(
            response=json.dumps(response_msg), status=404, mimetype="application/json"
        )


if __name__ != "__main__":
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8200, threaded=True, debug=True)
