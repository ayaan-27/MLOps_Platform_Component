from os import name

import matplotlib.pyplot as plt
import mlflow
import numpy as np
from interpret.ext.blackbox import TabularExplainer
from pycaret.classification import *

val_filename_chars = "-_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


def train_explainer(model, x_train, features, x_test):
    """
    Trains a model-specific explainer using InterpretML's TabularExplainer class.

    Parameters
    ----------

    model: trained estimator
        The model which has been returned by the AutoML pipeline

    x_train: pandas DataFrame
        The training data

    features: list
        The features of the data

    x_test: pandas DataFrame
        The test data

    Returns
    -------

    global_explanation: interpret_community.explanation.explanation.DynamicGlobalExplanation
        The DynamicGlobalExplanation instance that has been fit to the training data

    """

    import warnings

    warnings.filterwarnings("ignore")

    if model is None:
        raise Exception("Model cannot be None.")
    if x_train is None and x_test is None:
        raise Exception(
            "x_train and x_test both cannot be None. One of them have to be supplied."
        )
    if features is None:
        raise Exception("Please provide the list of features.")
    if not isinstance(features, list):
        raise Exception("Please provide the list of features.")
    if x_train is None:
        x_train = x_test
    if x_test is None:
        x_test = x_train

    print("CLEARED PARAMETER CHECKS")

    try:
        row_length, feature_length = np.shape(x_test)
    except BaseException:
        row_length, feature_length = x_test.shape

    print("CLEARED DATA TYPE CHECKS")

    if row_length > 10000:
        raise Exception("Exceedes maximum number of rows(10000).")
    if len(features) != feature_length:
        raise Exception("Features list provide does not match the dataset provided.")

    print("CLEARED DATA LENGTH AND EQUALITY CHECKS")

    explainer = TabularExplainer(
        model=model, initialization_examples=x_train, features=features
    )
    global_explanation = explainer.explain_global(x_test)
    warnings.filterwarnings("default")

    return global_explanation


def get_transformed_data():
    """
    Returns the data that has been transformed by PyCaret in order to get global explanations for the model

    Returns
    -------

    X_train: pd.DataFrame
        The transformed feature set used for training

    X_test: pd.DataFrame
        The transformed feature set used in model testing

    y_train: pd.Series
        The target values associated with the training data

    y_test: pd.Series
        The target values associated with the test data
    """

    # Getting the data
    X_train = get_config("X_train")
    y_train = get_config("y_train")
    X_test = get_config("X_test")
    y_test = get_config("y_test")

    return X_train, X_test, y_train, y_test


def feature_importance(global_explanation, k=5):
    feature_importance = global_explanation.get_feature_importance_dict()
    fig = plt.figure(figsize=(7.5, 5))
    ax = fig.gca()
    plt.xticks(rotation=45)
    ax.bar(list(feature_importance.keys())[:k], list(feature_importance.values())[:k])
    ax.set_title("Top " + str(k) + " Feature Importance")
    ax.set_ylabel("Feature Importance Score")
    plt.savefig("Feature_Importance.png", format="png", bbox_inches="tight")


def dependence_plot(global_explanation, x_test):
    local_explanations = global_explanation.local_importance_values
    if len(local_explanations) <= 3:
        local_explanations = local_explanations[0]
    if not isinstance(x_test, pd.DataFrame):
        x_test = pd.DataFrame(x_test, columns=global_explanation.features)
    data = pd.DataFrame(local_explanations, columns=global_explanation.features)
    feature_importance = global_explanation.get_feature_importance_dict()
    feature_importance = list(feature_importance.keys())[:5]

    x = 5 if len(global_explanation.features) > 4 else len(global_explanation.features)
    names = []
    for i in range(0, x):
        fig = plt.figure(figsize=(7.5, 5))
        ax = fig.gca()
        p = ax.scatter(
            x_test[feature_importance[i]],
            -data[feature_importance[i]],
            c=x_test[feature_importance[1 if i == 0 else 0]],
        )
        cb = plt.colorbar(p, ax=ax)
        cb.set_label(feature_importance[1 if i == 0 else 0])
        ax.set_title("Dependence Plot for " + feature_importance[i])
        ax.set_ylabel("Feature Importance of " + feature_importance[i])
        ax.set_xlabel(feature_importance[i])
        filename = "Dependence_Plot_" + feature_importance[i].replace(" ", "_") + ".png"
        filename = "".join(c if c in val_filename_chars else "_" for c in filename)
        names.append(filename)
        plt.savefig(names[i], format="png", bbox_inches="tight")
    return names


def log_explanation(exp_name, dep_names):
    exp = mlflow.get_experiment_by_name(exp_name)
    runs = mlflow.list_run_infos(exp.experiment_id)
    run = mlflow.start_run(runs[0].run_id)

    # mlflow.log_artifact("Global_Explainer_Pickled.pickle")
    mlflow.log_artifact("Feature_Importance.png")
    for i in dep_names:
        mlflow.log_artifact(i)

    mlflow.end_run()
