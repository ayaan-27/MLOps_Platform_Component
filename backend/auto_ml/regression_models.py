from btb.selection import UCB1
from btb.tuning import GPTuner, Tunable
from btb.tuning import hyperparams as hp
from lightgbm import LGBMRegressor
import utils.logs as logs
from sklearn.ensemble import (
    AdaBoostRegressor,
    ExtraTreesRegressor,
    GradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.linear_model import (
    ARDRegression,
    BayesianRidge,
    ElasticNet,
    HuberRegressor,
    Lars,
    Lasso,
    LassoLars,
    LinearRegression,
    RANSACRegressor,
    Ridge,
)
from sklearn.neighbors import KNeighborsRegressor
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor

LOGGER = logs.get_logger()

def model_definitions(tuning_metric):
    """
        Function for defining all the models, their hyperparameter grids, tuning functions and the selector.

    Args:
        tuning_metric : String
            Metric to be maximized or minimized for tuning

    Returns:
        tuners : Dictionary
            dictionary of respective models GP tuners

        candidates : Dictionary
            dictionary of all possible candidates/models that can be selected

        selector : btb selector
            uses UCB to select models

        selector_dict : dictionary
            mapping of models to their tuning scores

    """
    LOGGER.info("Defining Candidates")
    # Dicitonary of candidates for performing regression
    candidates = {
        "linreg": LinearRegression,
        "lassoreg": Lasso,
        "ridgereg": Ridge,
        "elastic": ElasticNet,
        "LarsReg": Lars,
        "LassoLars": LassoLars,
        "lgbm": LGBMRegressor,
        "xgb": XGBRegressor,
        "gradBoost": GradientBoostingRegressor,
        "adaBoost": AdaBoostRegressor,
        "extraTrees": ExtraTreesRegressor,
        "rf": RandomForestRegressor,
        "dtr": DecisionTreeRegressor,
        "knn": KNeighborsRegressor,
        "svr": SVR,
        "huber": HuberRegressor,
        "ransac": RANSACRegressor,
        "ard": ARDRegression,
        "bayesian_ridge": BayesianRidge,
    }

    # Defining the range of hyperparameters for all the candidates

    linreg_hyperperams = {
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
        "normalize": hp.CategoricalHyperParam([True, False]),
    }

    lassoreg_hyperperams = {
        "alpha": hp.FloatHyperParam(min=0.01, max=10),
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
        "normalize": hp.CategoricalHyperParam([True, False]),
    }
    ridgereg_hyperperams = {
        "alpha": hp.FloatHyperParam(min=0.01, max=10),
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
        "normalize": hp.CategoricalHyperParam([True, False]),
    }
    elastic_hyperperams = {
        "alpha": hp.FloatHyperParam(min=0.01, max=10),
        "l1_ratio": hp.FloatHyperParam(min=0.01, max=1),
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
        "normalize": hp.CategoricalHyperParam([True, False]),
    }
    lars_hyperperams = {
        "eps": hp.FloatHyperParam(min=0.0001, max=0.1),
    }
    lassolars_hyperperams = {
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
        "normalize": hp.CategoricalHyperParam([True, False]),
        "alpha": hp.FloatHyperParam(min=0.00001, max=0.9),
        "eps": hp.FloatHyperParam(min=0.0001, max=0.1),
    }
    lgbm_hyperperams = {
        "num_leaves": hp.CategoricalHyperParam(
            [
                2,
                4,
                6,
                8,
                10,
                20,
                30,
                40,
                50,
                60,
                70,
                80,
                90,
                100,
                150,
                200,
                256,
            ]
        ),
        "learning_rate": hp.FloatHyperParam(min=0.00001, max=0.5),
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "min_split_gain": hp.FloatHyperParam(min=0.00001, max=0.5),
        "reg_alpha": hp.FloatHyperParam(min=0.00001, max=10),
        "reg_lambda": hp.FloatHyperParam(min=0.00001, max=10),
        "feature_fraction": hp.FloatHyperParam(min=0.4, max=1),
        "bagging_fraction": hp.FloatHyperParam(min=0.4, max=1),
        "bagging_freq": hp.IntHyperParam(min=1, max=7, step=1),
        "min_child_samples": hp.IntHyperParam(min=1, max=101, step=5),
    }
    xgb_hyperperams = {
        "learning_rate": hp.FloatHyperParam(min=0.00001, max=0.5),
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "subsample": hp.FloatHyperParam(min=0.2, max=1),
        "max_depth": hp.IntHyperParam(min=1, max=11, step=1),
        "colsample_bytree": hp.FloatHyperParam(min=0.5, max=1),
        "min_child_weight": hp.IntHyperParam(min=1, max=4, step=1),
        "reg_alpha": hp.FloatHyperParam(min=0.000001, max=10),
        "reg_lambda": hp.FloatHyperParam(min=0.000001, max=10),
        "scale_pos_weight": hp.IntHyperParam(min=0, max=50, step=1),
    }
    gradBoost_hyperperams = {
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "learning_rate": hp.FloatHyperParam(min=0.00001, max=0.5),
        "subsample": hp.FloatHyperParam(min=0.2, max=1),
        "min_samples_split": hp.CategoricalHyperParam([2, 4, 5, 7, 9, 10]),
        "min_samples_leaf": hp.IntHyperParam(min=1, max=5, step=1),
        "max_depth": hp.IntHyperParam(min=1, max=11, step=1),
        "min_impurity_decrease": hp.FloatHyperParam(min=0.00001, max=0.5),
        "max_features": hp.CategoricalHyperParam(["sqrt", "log2"]),
    }

    adaBoost_hyperperams = {
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "learning_rate": hp.FloatHyperParam(min=0.00001, max=0.5),
        "loss": hp.CategoricalHyperParam(["linear", "square", "exponential"]),
    }

    extraTrees_hyperperams = {
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "criterion": hp.CategoricalHyperParam(["mse", "mae"]),
        "max_depth": hp.IntHyperParam(min=1, max=11, step=1),
        "min_impurity_decrease": hp.FloatHyperParam(min=0.0001, max=0.5),
        "max_features": hp.CategoricalHyperParam(["sqrt", "log2"]),
        "bootstrap": hp.CategoricalHyperParam([True, False]),
        "min_samples_split": hp.CategoricalHyperParam([2, 5, 7, 9, 10]),
        "min_samples_leaf": hp.IntHyperParam(min=2, max=6, step=1),
    }
    rf_hyperperams = {
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "max_depth": hp.IntHyperParam(min=1, max=11, step=1),
        "min_impurity_decrease": hp.FloatHyperParam(min=0.0001, max=0.5),
        "max_features": hp.CategoricalHyperParam(["sqrt", "log2"]),
        "bootstrap": hp.CategoricalHyperParam([True, False]),
    }

    dtr_hyperperams = {
        "max_depth": hp.IntHyperParam(min=1, max=16, step=1),
        "max_features": hp.CategoricalHyperParam(["sqrt", "log2"]),
        "min_samples_leaf": hp.IntHyperParam(min=2, max=6, step=1),
        "min_samples_split": hp.CategoricalHyperParam([2, 5, 7, 9, 10]),
        "min_impurity_decrease": hp.FloatHyperParam(min=0.0001, max=0.5),
        "criterion": hp.CategoricalHyperParam(["mse", "mae", "friedman_mse"]),
    }
    knn_hyperperams = {
        "n_neighbors": hp.IntHyperParam(min=1, max=51, step=1),
        "weights": hp.CategoricalHyperParam(["uniform", "distance"]),
        "metric": hp.CategoricalHyperParam(["minkowski", "euclidean", "manhattan"]),
    }
    svr_hyperperams = {
        "C": hp.FloatHyperParam(min=0, max=10),
        "epsilon": hp.FloatHyperParam(min=1.1, max=1.9),
    }

    huber_hyperperams = {
        "epsilon": hp.FloatHyperParam(min=1, max=1.9),
        "alpha": hp.FloatHyperParam(min=0.000001, max=0.9),
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
    }
    ransac_hyperperams = {
        "min_samples": hp.FloatHyperParam(min=0, max=1),
        "max_trials": hp.IntHyperParam(min=1, max=20, step=1),
        "max_skips": hp.IntHyperParam(min=1, max=20, step=1),
        "stop_n_inliers": hp.IntHyperParam(min=1, max=25, step=1),
        "stop_probability": hp.FloatHyperParam(min=0, max=1),
        "loss": hp.CategoricalHyperParam(["absolute_loss", "squared_loss"]),
    }
    ard_hyperperams = {
        "alpha_1": hp.FloatHyperParam(min=0.000001, max=0.3),
        "alpha_2": hp.FloatHyperParam(min=0.000001, max=0.3),
        "lambda_1": hp.FloatHyperParam(min=0.0000001, max=0.3),
        "lambda_2": hp.FloatHyperParam(min=0.0000001, max=0.3),
        "threshold_lambda": hp.IntHyperParam(min=5000, max=60000, step=5000),
        "compute_score": hp.CategoricalHyperParam([True, False]),
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
        "normalize": hp.CategoricalHyperParam([True, False]),
    }

    bayesian_ridge_hyperparams = {
        "alpha_1": hp.FloatHyperParam(min=0.0000001, max=0.3),
        "alpha_2": hp.FloatHyperParam(min=0.0000001, max=0.3),
        "lambda_1": hp.FloatHyperParam(min=0.0000001, max=0.3),
        "lambda_2": hp.FloatHyperParam(min=0.0000001, max=0.3),
        "compute_score": hp.CategoricalHyperParam([True, False]),
        "fit_intercept": hp.CategoricalHyperParam([True, False]),
        "normalize": hp.CategoricalHyperParam([True, False]),
    }

    # Creating tunables for the candidates
    linreg_tunables = Tunable(linreg_hyperperams)
    lassoreg_tunables = Tunable(lassoreg_hyperperams)
    ridgereg_tunables = Tunable(ridgereg_hyperperams)
    elastic_tunables = Tunable(elastic_hyperperams)
    lars_tunables = Tunable(lars_hyperperams)
    lassolars_tunables = Tunable(lassolars_hyperperams)
    lgbm_tunables = Tunable(lgbm_hyperperams)
    xgb_tunables = Tunable(xgb_hyperperams)
    gradBoost_tunables = Tunable(gradBoost_hyperperams)
    adaBoost_tunables = Tunable(adaBoost_hyperperams)
    extraTrees_tunables = Tunable(extraTrees_hyperperams)
    rf_tunables = Tunable(rf_hyperperams)
    dtr_tunables = Tunable(dtr_hyperperams)
    knn_tunables = Tunable(knn_hyperperams)
    svr_tunables = Tunable(svr_hyperperams)
    huber_tunables = Tunable(huber_hyperperams)
    ransac_tunables = Tunable(ransac_hyperperams)
    ard_tunables = Tunable(ard_hyperperams)
    bayesian_ridge_tunables = Tunable(bayesian_ridge_hyperparams)

    if tuning_metric == "R2":
        maximize = True
    else:
        maximize = False
    # defining tuner dicitionary for all candidates
    tuners = {
        "linreg": GPTuner(linreg_tunables, maximize=maximize),
        "lassoreg": GPTuner(lassoreg_tunables, maximize=maximize),
        "ridgereg": GPTuner(ridgereg_tunables, maximize=maximize),
        "elastic": GPTuner(elastic_tunables, maximize=maximize),
        "LarsReg": GPTuner(lars_tunables, maximize=maximize),
        "LassoLars": GPTuner(lassolars_tunables, maximize=maximize),
        "lgbm": GPTuner(lgbm_tunables, maximize=maximize),
        "xgb": GPTuner(xgb_tunables, maximize=maximize),
        "gradBoost": GPTuner(gradBoost_tunables, maximize=maximize),
        "adaBoost": GPTuner(adaBoost_tunables, maximize=maximize),
        "extraTrees": GPTuner(extraTrees_tunables, maximize=maximize),
        "rf": GPTuner(rf_tunables, maximize=maximize),
        "dtr": GPTuner(dtr_tunables, maximize=maximize),
        "knn": GPTuner(knn_tunables, maximize=maximize),
        "svr": GPTuner(svr_tunables, maximize=maximize),
        "huber": GPTuner(huber_tunables, maximize=maximize),
        "ransac": GPTuner(ransac_tunables, maximize=maximize),
        "ard": GPTuner(ard_tunables, maximize=maximize),
        "bayesian_ridge": GPTuner(bayesian_ridge_tunables, maximize=maximize),
    }
    LOGGER.info("Defined hyperparameters for Candidates")

    # Defining the selector with all the candidates
    selector = UCB1(
        [
            "linreg",
            "lassoreg",
            "ridgereg",
            "elastic",
            "LarsReg",
            "LassoLars",
            "lgbm",
            "xgb",
            "gradBoost",
            "adaBoost",
            "extraTrees",
            "rf",
            "dtr",
            "knn",
            "svr",
            "huber",
            "ransac",
            "ard",
            "bayesian_ridge",
        ]
    )

    selector_dict = {
        "linreg": tuners["linreg"].scores,
        "lassoreg": tuners["lassoreg"].scores,
        "ridgereg": tuners["ridgereg"].scores,
        "elastic": tuners["elastic"].scores,
        "LarsReg": tuners["LarsReg"].scores,
        "LassoLars": tuners["LassoLars"].scores,
        "lgbm": tuners["lgbm"].scores,
        "xgb": tuners["xgb"].scores,
        "gradBoost": tuners["gradBoost"].scores,
        "adaBoost": tuners["adaBoost"].scores,
        "extraTrees": tuners["extraTrees"].scores,
        "rf": tuners["rf"].scores,
        "dtr": tuners["dtr"].scores,
        "knn": tuners["knn"].scores,
        "svr": tuners["svr"].scores,
        "huber": tuners["huber"].scores,
        "ransac": tuners["ransac"].scores,
        "ard": tuners["ard"].scores,
        "bayesian_ridge": tuners["bayesian_ridge"].scores,
    }

    return (tuners, candidates, selector, selector_dict)
