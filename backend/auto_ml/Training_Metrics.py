import numpy as np
import pandas as pd
from fairlearn.metrics import (
    demographic_parity_difference,
    demographic_parity_ratio,
    equalized_odds_difference,
    equalized_odds_ratio,
    true_positive_rate,
)


def add_metric_parameters(scoring_method):
    """
    Returns the parameters that have to be passed into pycaret's add_metric function

    Parameters
    ----------

    scoring_method: str
        Must be one of ['DI', 'DPD', 'EOR', 'EOD']

    Returns
    -------

    metric_param: dictionary
        A dictionary of the parameters that can be passed into the add_metric function
    """

    metric_params = {
        "DI": {
            "ID": "disparate_impact",
            "Name": "Disparate Impact",
            "Function": disparate_impact_train,
            "Greater-Better": True,
        },
        "DPD": {
            "ID": "parity_difference",
            "Name": "Parity Difference",
            "Function": return_parity_difference,
            "Greater-Better": False,
        },
        "EOR": {
            "ID": "eq_odds_ratio",
            "Name": "Eq. Odds Ratio",
            "Function": return_eq_odds_ratio,
            "Greater-Better": True,
        },
        "EOD": {
            "ID": "eq_opp_difference",
            "Name": "Eq. Opp. Difference",
            "Function": equal_opportunity_difference_train,
            "Greater-Better": False,
        },
    }

    try:
        metric_param = metric_params[scoring_method]
        return metric_param
    except KeyError:
        raise Exception("{} is not a recognized scoring method".format(scoring_method))


def line_up(y_true, y_pred, sensitive_features):
    """
    Reindexes the sensitive attribute data so that it matches the sampled true and predicted values

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

    y_true: pd.Series
        The true target values (sampled; reindexed)

    y_pred: array-like
        The predicted values (sampled)

    sensitive_features: pd.Series
        The sensitive attribute data (reindexed)
    """

    y_pred = pd.Series(y_pred)
    sensitive_data_sample = sensitive_features.reindex_like(y_true)

    y_true.reset_index(inplace=True, drop=True)
    assert isinstance(y_true, pd.core.series.Series), "True Y: {}".format(y_true)

    sensitive_data_sample.reset_index(inplace=True, drop=True)
    assert isinstance(
        sensitive_data_sample, pd.core.series.Series
    ), "Sensitive Data: {}".format(sensitive_data_sample)

    assert len(y_pred) == len(y_true)
    assert len(y_pred) == len(sensitive_data_sample)

    # y_true = y_true.apply(lambda x: int(x))
    # y_pred = y_pred.apply(lambda x: int(x))
    # sensitive_data_sample = sensitive_data_sample.apply(lambda x: str(x))

    return y_true, y_pred, sensitive_data_sample


def return_parity_ratio(y_true, y_pred, sensitive_features):
    """
    Calculates the demographic parity ratio for a sample of true and predicted values along with the sensitive attribute data.

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
        The demographic parity ratio of the sample of data
    """

    y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)

    ratio = demographic_parity_ratio(
        y_true=y_true, y_pred=y_pred, sensitive_features=sensitive_data_sample
    )

    return ratio


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

    y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)

    difference = demographic_parity_difference(
        y_true=y_true, y_pred=y_pred, sensitive_features=sensitive_data_sample
    )
    difference = np.absolute(difference)

    return difference


def return_eq_odds_difference(y_true, y_pred, sensitive_features):
    """
    Calculates the equalized odds difference for a sample of true and predicted values along with the sensitive attribute data.

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
        The absolute value of the equalized odds difference for the sample of data
    """

    y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)

    difference = equalized_odds_difference(
        y_true=y_true, y_pred=y_pred, sensitive_features=sensitive_data_sample
    )

    return np.absolute(difference)


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

    y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)

    ratio = equalized_odds_ratio(
        y_true=y_true, y_pred=y_pred, sensitive_features=sensitive_data_sample
    )

    return ratio


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

    y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)

    demographics = list(sensitive_data_sample.unique())
    prob_outcome = []

    for demographic in demographics:

        temp_sf = sensitive_data_sample[sensitive_data_sample == demographic]
        temp_predictions = y_pred.reindex_like(temp_sf)

        dem_probability = len(temp_predictions[temp_predictions == 1]) / len(
            temp_predictions
        )
        prob_outcome.append(dem_probability)

    result = min(prob_outcome) / max(prob_outcome)

    return result


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

    y_true, y_pred, sensitive_data_sample = line_up(y_true, y_pred, sensitive_features)

    demographics = list(sensitive_data_sample.unique())
    true_pos_rates = []

    for demographic in demographics:

        temp_sf = sensitive_data_sample[sensitive_data_sample == demographic]
        temp_pred = y_pred.reindex_like(temp_sf)
        temp_true = y_true.reindex_like(temp_sf)

        tpr_curr_dem = true_positive_rate(temp_true, temp_pred)
        true_pos_rates.append(tpr_curr_dem)

    result = max(true_pos_rates) - min(true_pos_rates)

    return result
