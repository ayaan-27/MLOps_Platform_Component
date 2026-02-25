import pickle
import time
import os, sys

sys.path.append(os.getcwd())
from confs.config import dbConfig
import mlflow
import pandas as pd
import auto_ml.scoring as scoring
import utils.logs as logs

LOGGER = logs.get_logger()
params = dbConfig(section="mlflow_tracking")
mlflow_tracking_uri = params['mlflow_tracking_uri']

def generate_leaderboard_regression(
    X_train,
    y_train,
    X_test,
    y_test,
    tuning_metric,
    n_passes,
    tuners,
    candidates,
    selector,
    selector_dict,
):
    """
        Generating Leaderboard from all possible candidates.
        Leaderboard will have 'n_passes' number of models

    Args:
        X_train :pandas Dataframe
            training Dataframe, to be passed to model for training

        y_train :pandas Series
            Corresponding true values for X_train

        X_test :pandas Dataframe
            Testing Dataframe

        y_test :pandas Series
            Corresponding true values for X_test

        tuning_metric :String
            Metric to be maximized or minimized for tuning

        n_passes :integer
            number of models to be generated

        tuners : Dictionary
            Dictionary that saves tuners to corresponding models

        candidates :Dictionary
            Dictionary of models

        selector :BTB slection class
            selector type for running BTB

        selector_dict :dictionary
            sictionary with all the candidates respective tuner scores
    """

    # Defining the structure of the leaderborad
    leaderboard_df = pd.DataFrame(
        columns=[
            "name",
            "MSE",
            "RMSE",
            "MAE",
            "R2",
            "MSLE",
            "MAPE",
        ]
    )
    i = 0
    # R2 greater is better
    # Other metrics are error metrics
    # best_run_id = ""
    # if tuning_metric == "R2":
    #     best_score = 0
    # else:
    #     best_score = 10000
    try:
        mlflow.set_tracking_uri(mlflow_tracking_uri)
    except Exception as e:
        LOGGER.error("Failed to set MLFlow tracking URI ")
        raise Exception(str(e))    
    
    exp_obj = mlflow.get_experiment_by_name("auto_ml")
    if exp_obj is None:
        exp_id = mlflow.create_experiment("auto_ml")
    else:
        exp_id = exp_obj.experiment_id
    
    mlflow.start_run(experiment_id=exp_id)
    while i < n_passes:
        # MLFlow
        # mlflow.end_run()
        run_id = mlflow.start_run(nested=True).info.run_id
        # print(run_id)
        # Selecting the candidate
        candidate = selector.select(selector_dict)
        # Selectoing model hyperparmaeters
        parameters = tuners[candidate].propose()
        model = candidates[candidate](**parameters)

        # Model Training
        model.fit(X_train, y_train)
        # Model Scoring. If model performs too poorly(rare cases) might throw
        # error (MSLE/ y_pred or y_true has negative values)
        try:
            model_scores = scoring.regression_score(model, X_test, y_test)
        except Exception as e:
            LOGGER.info(str(e))
            LOGGER.info("Reached rare error, continue execution")
            print("\nError:", candidate)
            continue
        # score based on tuning metric
        score = model_scores[tuning_metric]
        # appending the model scores to the leaderboard dataframe
        leaderboard_df = leaderboard_df.append(model_scores, ignore_index=True)
        tuners[candidate].record(parameters, score)
        del model_scores["name"]
        # Usinf MLFlow to save the file
        # add name
        filename = "finalized_model.sav"

        pickle.dump(model, open(filename, "wb"))
        mlflow.log_metrics(model_scores)
        mlflow.log_params(parameters)
        mlflow.log_artifact("finalized_model.sav")
        mlflow.end_run()

        i += 1
        # if tuning_metric == "R2":
        #     if score > best_score:
        #         best_run_id = run_id
        #         best_score = score
        #         best_model = model
        #         best_params = parameters
        # else:
        #     if score < best_score:
        #         best_run_id = run_id
        #         best_score = score
        #         best_model = model
        #         best_params = parameters
    # add run id for best model
    # sort leaderboard based on metric
    mlflow.end_run()
    if tuning_metric == "R2":
        ascending = False
    else:
        ascending = True
    leaderboard_df.sort_values(by=[tuning_metric], inplace=True, ascending=ascending)

    # Rearranging columns to make the tuning metric first
    reorder = [i for i in leaderboard_df.columns if i != tuning_metric]
    reorder.insert(1, tuning_metric)
    leaderboard_df = leaderboard_df.reindex(columns=reorder)

    # displaying the leaderboard
    print(leaderboard_df)

    return leaderboard_df


def generate_leaderboard_classification(
    X_train,
    y_train,
    X_test,
    y_test,
    tuning_metric,
    n_passes,
    tuners,
    candidates,
    selector,
    selector_dict,
    S_train="",
    bias=False,
):
    """
        Generating Leaderboard from all possible candidates.
        Leaderboard will have 'n_passes' number of models

    Args:
        X_train :pandas Dataframe
            training Dataframe, to be passed to model for training

        y_train :pandas Series
            Corresponding true values for X_train

        X_test :pandas Dataframe
            Testing Dataframe

        y_test :pandas Series
            Corresponding true values for X_test

        tuning_metric :String
            Metric to be maximized or minimized for tuning

        n_passes :integer
            number of models to be generated

        tuners : Dictionary
            Dictionary that saves tuners to corresponding models

        candidates :Dictionary
            Dictionary of models

        selector :BTB slection class
            selector type for running BTB

        selector_dict :dictionary
            dictionary with all the candidates respective tuner scores

        S_train :pandas Series
            Sensitive features column from a dataset to be passed when bias is given

        bias :Boolean
            boolean True value to be passed when bias metric is given as tuning metric
    """
    # define columns of the leaderboard df
    try:
        columns = [
            "name",
            "accuracy",
            "precision",
            "recall",
            "f1",
            #"roc_auc",
            #"jaccard"
        ]
        # check for bias
        if bias is True:
            columns.append("parity_diff")
            columns.append("disparate_impact")
            columns.append("eq_odds_ratio")
            columns.append("equal_opportunity")

        columns.append("tts")
        # best_score = 0
        leaderboard_df = pd.DataFrame(columns=columns)

        i = 0
        # best_run_id = ""
        try:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
        except Exception as e:
            LOGGER.error("Failed to set MLFlow tracking URI ")
            raise Exception(str(e))    
        
        exp_obj = mlflow.get_experiment_by_name("auto_ml")
        if exp_obj is None:
            exp_id = mlflow.create_experiment("auto_ml")
        else:
            exp_id = exp_obj.experiment_id
        
        mlflow.start_run(experiment_id=exp_id)
        while i < n_passes:
            # mlflow.end_run()

            run_id = mlflow.start_run(nested=True, experiment_id = exp_id).info.run_id
            candidate = selector.select(selector_dict)

            # after many tunings tuning some candidates may throw an error
            try:
                parameters = tuners[candidate].propose(allow_duplicates=True)
                model = candidates[candidate](**parameters)
            except Exception as e:
                LOGGER.info(str(e))
                LOGGER.info("Removing candidate")
                selector_dict.pop(candidate)
                continue

            # Train model
            start = time.time()
            model.fit(X_train, y_train)
            end = time.time()

            # scoring func
            model_scores = scoring.classification_score(model, X_test, y_test, S_train, bias)
            model_scores["tts"] = end - start

            # model_scores["run_id"] = run_id
            score = model_scores[tuning_metric]
            if candidate == "cat_boost":
                model_scores["name"] = "cat_boost"

            # add model scores to the leaderboard
            # leaderboard_df = leaderboard_df.append(model_scores, ignore_index=True)

            try:
                tuners[candidate].record(parameters, score)
            except Exception as e:
                LOGGER.error(str(e))
                raise Exception(str(e))
            i += 1
            # Save run details in mlflow
            filename = "finalized_model.sav"
            pickle.dump(model, open(filename, "wb"))
            mlflow.log_artifact("finalized_model.sav")
            
            name = model_scores["name"]
            del model_scores["name"]
            mlflow.log_metrics(model_scores)
            mlflow.log_params(parameters)
            mlflow.end_run()

            model_scores["run_id"] = run_id
            model_scores["name"] = name
            model_scores["hyper_param"] = parameters
            leaderboard_df = leaderboard_df.append(model_scores, ignore_index=True)

            # track best score, runid, model and parameters
            # if score > best_score:
            #     best_run_id = run_id
            #     best_score = score
            #     best_model = model
            #     best_params = parameters

        mlflow.end_run()

        leaderboard_df.sort_values(by=[tuning_metric], inplace=True, ascending=False)
        # reorder leaderboard with tuning metric as first column
        reorder = [i for i in leaderboard_df.columns if i != tuning_metric]
        reorder.insert(1, tuning_metric)
        leaderboard_df = leaderboard_df.reindex(columns=reorder)

        print(leaderboard_df)
        return leaderboard_df
    except Exception as e:
        LOGGER.error(e)
        mlflow.end_run()