import numpy as np
import pandas as pd
from fairlearn.metrics import (
    demographic_parity_difference,
    equalized_odds_ratio,
    true_positive_rate,
)
import utils.logs as logs
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    jaccard_score,
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    mean_squared_log_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)

LOGGER = logs.get_logger()

def regression_score(model, X_test, y_test):
    """
        Funciton that gives all the regression metrics for a given model.

    Args:

        model :
            Model given for testing

        X_test : Pandas dataframe
            X values for the testing dataset

        y_test : Pandas Series
            Corresponding true values for X_test

    Returns:
        Dictionary: Dict with model names and metrics

    """
    # predicting test values
    y_pred = model.predict(X_test)
    y_true = y_test

    # Calculating regression metrics
    mse = mean_squared_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    mae = mean_absolute_error(y_true, y_pred)
    R2 = r2_score(y_true, y_pred)

    msle = mean_squared_log_error(y_true, y_pred)
    mape = mean_absolute_percentage_error(y_true, y_pred)
    # get model name
    model_name = str(model)
    name = model_name.split("(")
    # store model name and metrics in a dictionary and return
    metric_dict = {
        "name": name[0],
        "MSE": mse,
        "RMSE": rmse,
        "MAE": mae,
        "R2": R2,
        "MSLE": msle,
        "MAPE": mape,
    }
    return metric_dict


def classification_score(model, X_test, y_test, S_train, bias=False):
    """
        Funciton that gives all the classification metrics for a given model.

    Args:

        model :
            Model given for testing

        X_test : Pandas dataframe
            X values for the testing dataset

        y_test : Pandas Series
            Corresponding true values for X_test

        S_train : pandas Series
            Sensitive features column from a dataset to be passed when bias is given

        bias : Boolean
            boolean True value to be passed when bias metric is given as tuning metric

    Returns:
        Dictionary: Dict with model names and metrics

    """
    # predict X_test values
    y_pred = model.predict(X_test)
    y_true = y_test
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, average= "weighted")
    recall = recall_score(y_true, y_pred, average= "weighted")
    f1 = f1_score(y_true, y_pred,  average= "weighted")
    # roc_auc = roc_auc_score(y_true, y_pred, multi_class="ovr")
    # jaccard = jaccard_score(y_true, y_pred)

    if precision != 0 and bias:
        disparate_impact = disparate_impact_train(y_true, y_pred, S_train)
        parity_diff = return_parity_difference(y_true, y_pred, S_train)
        eq_odds_ratio = return_eq_odds_ratio(y_true, y_pred, S_train)
        equal_opportunity = equal_opportunity_difference_train(y_true, y_pred, S_train)
    else:
        disparate_impact = 0
        parity_diff = 0
        eq_odds_ratio = 0
        equal_opportunity = 0

    model_name = str(model)
    name = model_name.split("(")
    # remove jaccard
    metric_dict = {
        "name": name[0],
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        # "roc_auc": roc_auc,
        # 'jaccard': jaccard,
        "parity_diff": parity_diff,
        "disparate_impact": disparate_impact,
        "eq_odds_ratio": eq_odds_ratio,
        "equal_opportunity": equal_opportunity,
    }
    # if not bias metric is given remove it from the result
    if bias is False:
        del metric_dict["disparate_impact"]
        del metric_dict["parity_diff"]
        del metric_dict["eq_odds_ratio"]
        del metric_dict["equal_opportunity"]

    return metric_dict


def disparate_impact_train(y_true, y_pred, sensitive_features):
    """
    Calculates the standard disparate impact metric which has been adapted for training purposes.

    Instead of finding the ratio of P[h(x) = 1 | A = a] for A = 'underprivileged' and 'privileged', the score is calculated as:
        min(P[h(x) = 1 | A = a]) / max(P[h(x) = 1 | A = a])

    In this regard, it is very similar to the Demographic Parity Ratio scoring function.

    This is to constrain the value range to between 0 and 1, so that the model can be optimized for a higher score.

    Drawback: the user will not be able to determine in which direction the bias acts; only that there is a bias.

    Determining the direction of the bias will require the user to run the 'true' disparate impact scoring function on the training/
        test predictions once the model has been trained.

    Parameters
    ----------

    y_true: pd.Series
        The true target values (sampled)

    y_pred: array-like
        The predicted values (sampled)

    sensitive_features: pd.Series
        The sensitive attribute data (full)

    Returns
    -------

    result: float
        The disparate impact calculated by dividing the minimum probability of selection over the maximum.
    """

    # y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)
    y_pred = pd.Series(y_pred)
    sensitive_data_sample = sensitive_features
    demographics = list(sensitive_data_sample.unique())
    prob_outcome = []
    for demographic in demographics:

        temp_sf = sensitive_data_sample[sensitive_data_sample == demographic]
        temp_predictions = y_pred.reindex_like(temp_sf)

        dem_probability = len(temp_predictions[temp_predictions == 1]) / len(
            temp_predictions
        )
        prob_outcome.append(dem_probability)

    try:
        result = min(prob_outcome) / max(prob_outcome)
    except Exception as e:
        LOGGER.error(str(e))
        result = 0

    return result


def return_parity_difference(y_true, y_pred, sensitive_features):
    """
    Calculates the demographic parity difference for a sample of true and predicted values along with the sensitive attribute data.

    Parameters
    ----------

    y_true: pd.Series
        The true target values (sampled)

    y_pred: array-like
        The predicted values (sampled)

    sensitive_features: pd.Series
        The sensitive attribute data (full)

    target: str
        The name of the target column

    sensitive_attribute: str
        The name of the sensitive features' column

    Returns
    -------

    difference: float
        The demographic parity ratio of the sample of data
    """

    # y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)
    sensitive_data_sample = sensitive_features
    difference = demographic_parity_difference(
        y_true=y_true, y_pred=y_pred, sensitive_features=sensitive_data_sample
    )
    difference = np.absolute(difference)

    return difference


def return_eq_odds_ratio(y_true, y_pred, sensitive_features):
    """
    Calculates the equalized odds ratio for a sample of true and predicted values along with the sensitive attribute data.

    Parameters
    ----------

    y_true: pd.Series
        The true target values (sampled)

    y_pred: array-like
        The predicted values (sampled)

    sensitive_features: pd.Series
        The sensitive attribute data (full)

    target: str
        The name of the target column

    sensitive_attribute: str
        The name of the sensitive features' column

    Returns
    -------

    ratio: float
        The equalized odds ratio for the sample of data
    """

    # y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)
    sensitive_data_sample = sensitive_features
    ratio = equalized_odds_ratio(
        y_true=y_true, y_pred=y_pred, sensitive_features=sensitive_data_sample
    )

    return ratio


def equal_opportunity_difference_train(y_true, y_pred, sensitive_features):
    """
    Calculates the standard equal opportunity difference metric which has been adapted for training purposes.

    Rather than calculating the true difference between the underprivileged and privileged classes, the true difference between the
        maximum true positive rate and minimum TPR is calculated and returned.

    This is to constrain the value range to between 0 and 1, so that the model can be optimized for a higher score (with 0 being the best).

    Drawback: the user will not be able to determine in which direction the bias acts; only that there is a bias.

    Determining the direction of the bias will require the user to run the 'true' equal opportunity difference function on the training/
        test predictions once the model has been trained.

    Parameters
    ----------

    y_true: pd.Series
        The true target values (sampled)

    y_pred: array-like
        The predicted values (sampled)

    sensitive_features: pd.Series
        The sensitive attribute data (full)

    Returns
    -------

    result: float
        The equal opportunity difference calculated as the true difference between the maximum and minimum TPRs
    """

    # y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)
    sensitive_data_sample = sensitive_features
    demographics = list(sensitive_data_sample.unique())
    true_pos_rates = []
    index_list = [i for i in y_true.index]
    y_pred = pd.Series(y_pred, index=index_list)
    for demographic in demographics:

        temp_sf = sensitive_data_sample[sensitive_data_sample == demographic]
        temp_pred = y_pred.reindex_like(temp_sf)
        temp_true = y_true.reindex_like(temp_sf)
        tpr_curr_dem = true_positive_rate(temp_true, temp_pred)
        true_pos_rates.append(tpr_curr_dem)
    result = max(true_pos_rates) - min(true_pos_rates)

    return result
