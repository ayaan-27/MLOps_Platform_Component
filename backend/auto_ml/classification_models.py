from btb.selection import UCB1
from btb.tuning import GPTuner, Tunable
from btb.tuning import hyperparams as hp
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier
from sklearn.discriminant_analysis import (
    LinearDiscriminantAnalysis,
    QuadraticDiscriminantAnalysis,
)
from sklearn.ensemble import (
    AdaBoostClassifier,
    BaggingClassifier,
    ExtraTreesClassifier,
    RandomForestClassifier,
)
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.linear_model import LogisticRegression, RidgeClassifier, SGDClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier


def model_definitions(tuning_metric):
    """
        Function for defining all the models, their hyperparameter grids, tuning functions and the selector.

    Args:
        tuning_metric : metric to maximize or minimize

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
    # Defining all the candidates and their tuning grids
    candidates = {
        "DTC": DecisionTreeClassifier,
        "SGDC": SGDClassifier,
        "LogReg": LogisticRegression,
        "rf": RandomForestClassifier,
        "ada": AdaBoostClassifier,
        "lgbm": LGBMClassifier,
        "xgb": XGBClassifier,
        "knn": KNeighborsClassifier,
        "gnb": GaussianNB,
        "svc": SVC,
        "ridge": RidgeClassifier,
        "gaussian_process": GaussianProcessClassifier,
        "quadratic_discriminant": QuadraticDiscriminantAnalysis,
        "linear_discriminant": LinearDiscriminantAnalysis,
        "extra_trees": ExtraTreesClassifier,
        "cat_boost": CatBoostClassifier,
        "bagging": BaggingClassifier,
    }
    rf_hyperparmas = {
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "max_depth": hp.IntHyperParam(min=1, max=11, step=1),
        "min_impurity_decrease": hp.FloatHyperParam(min=0.01, max=0.5),
        "max_features": hp.CategoricalHyperParam(["sqrt", "log2"]),
        "bootstrap": hp.CategoricalHyperParam([True, False]),
    }

    ada_hyperparams = {
        "n_estimators": hp.IntHyperParam(min=50, max=250, step=50),
        "learning_rate": hp.FloatHyperParam(min=0.05, max=2),
    }

    dtc_hyperparams = {
        "max_depth": hp.IntHyperParam(min=1, max=16, step=1),
        "max_features": hp.CategoricalHyperParam(["sqrt", "log2"]),
        "min_samples_leaf": hp.CategoricalHyperParam([2, 3, 4, 5, 6]),
        "min_samples_split": hp.FloatHyperParam(min=0.01, max=1),
        "criterion": hp.CategoricalHyperParam(["gini", "entropy"]),
        "min_impurity_decrease": hp.FloatHyperParam(min=0.01, max=0.5),
    }

    sgdc_hyperparams = {
        "max_iter": hp.IntHyperParam(min=1, max=5000, default=1000),
        "tol": hp.FloatHyperParam(min=1e-3, max=1, default=1e-3),
    }
    logreg_hyperparams = {
        "max_iter": hp.IntHyperParam(min=1, max=5000, default=1000),
        "C": hp.FloatHyperParam(min=1e-3, max=100, default=1),
        "solver": hp.CategoricalHyperParam(["saga", "newton-cg", "lbfgs", "liblinear"]),
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
        "min_split_gain": hp.FloatHyperParam(min=0, max=0.9),
        "reg_alpha": hp.FloatHyperParam(min=0.000001, max=10),
        "reg_lambda": hp.FloatHyperParam(min=0.000001, max=10),
        "feature_fraction": hp.FloatHyperParam(min=0.4, max=1),
        "bagging_fraction": hp.FloatHyperParam(min=0.4, max=1),
        "bagging_freq": hp.IntHyperParam(min=0, max=7, step=1),
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
    knn_hyperperams = {
        "n_neighbors": hp.IntHyperParam(min=1, max=51, step=1),
        "weights": hp.CategoricalHyperParam(["uniform", "distance"]),
        "metric": hp.CategoricalHyperParam(["minkowski", "euclidean", "manhattan"]),
    }
    gnb_hyperperams = {
        "var_smoothing": hp.FloatHyperParam(min=0.0000001, max=1),
    }
    svc_hyperperams = {
        "C": hp.FloatHyperParam(min=0.001, max=100),
        "kernel": hp.CategoricalHyperParam(["rbf"]),
        "class_weight": hp.CategoricalHyperParam(["balanced"]),
    }

    ridge_hyperperams = {
        "alpha": hp.FloatHyperParam(min=0.001, max=10),
    }
    gaussian_process_hyperperams = {
        "max_iter_predict": hp.IntHyperParam(min=100, max=1000, step=100),
    }

    quadratic_discriminant_hyperperams = {
        "reg_param": hp.FloatHyperParam(min=0, max=1),
    }
    linear_discriminant_hyperperams = {
        "solver": hp.CategoricalHyperParam(["lsqr", "eigen"]),
        "shrinkage": hp.CategoricalHyperParam(
            [
                "empirical",
                "auto",
                0.0001,
                0.001,
                0.01,
                0.0005,
                0.005,
                0.05,
                0.1,
                0.2,
                0.3,
                0.4,
                0.5,
                0.6,
                0.7,
                0.8,
                0.9,
                1,
            ]
        ),
    }

    extra_trees_hyperperams = {
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "criterion": hp.CategoricalHyperParam(["gini", "entropy"]),
        "max_depth": hp.IntHyperParam(min=1, max=11, step=1),
        "min_impurity_decrease": hp.FloatHyperParam(min=0, max=0.5),
        "max_features": hp.CategoricalHyperParam(["sqrt", "log2"]),
        "bootstrap": hp.CategoricalHyperParam([True, False]),
        "min_samples_split": hp.CategoricalHyperParam([2, 5, 7, 9, 10]),
        "min_samples_leaf": hp.IntHyperParam(min=2, max=6, step=1),
        "class_weight": hp.CategoricalHyperParam(["balanced", "balanced_subsample"]),
    }

    cat_boost_hyperperams = {
        "eta": hp.FloatHyperParam(min=0, max=1),
        "depth": hp.IntHyperParam(min=1, max=12, step=1),
        "n_estimators": hp.IntHyperParam(min=10, max=300, step=10),
        "random_strength": hp.FloatHyperParam(min=0, max=0.8),
        "l2_leaf_reg": hp.CategoricalHyperParam(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 50, 100, 200]
        ),
    }

    bagging_hyperperams = {
        "bootstrap": hp.CategoricalHyperParam([True, False]),
        "bootstrap_features": hp.CategoricalHyperParam([True, False]),
        "max_features": hp.FloatHyperParam(min=0.4, max=1),
        "max_samples": hp.FloatHyperParam(min=0.4, max=1),
    }

    dtc_tunable = Tunable(dtc_hyperparams)
    sgdc_tunable = Tunable(sgdc_hyperparams)
    logregs_tunable = Tunable(logreg_hyperparams)
    ada_tunables = Tunable(ada_hyperparams)
    rf_tunables = Tunable(rf_hyperparmas)
    lgbm_tunables = Tunable(lgbm_hyperperams)
    xgb_tunables = Tunable(xgb_hyperperams)
    knn_tunables = Tunable(knn_hyperperams)
    gnb_tunables = Tunable(gnb_hyperperams)
    svc_tunables = Tunable(svc_hyperperams)
    ridge_tunables = Tunable(ridge_hyperperams)
    gaussian_process_tunables = Tunable(gaussian_process_hyperperams)
    quadratic_discriminant_tunables = Tunable(quadratic_discriminant_hyperperams)
    linear_discriminant_tunables = Tunable(linear_discriminant_hyperperams)
    extra_trees_tunables = Tunable(extra_trees_hyperperams)
    cat_boost_tunables = Tunable(cat_boost_hyperperams)
    bagging_tunables = Tunable(bagging_hyperperams)

    tuners = {
        "ada": GPTuner(ada_tunables),
        "DTC": GPTuner(dtc_tunable),
        "SGDC": GPTuner(sgdc_tunable),
        "LogReg": GPTuner(logregs_tunable),
        "rf": GPTuner(rf_tunables),
        "lgbm": GPTuner(lgbm_tunables),
        "xgb": GPTuner(xgb_tunables),
        "knn": GPTuner(knn_tunables),
        "gnb": GPTuner(gnb_tunables),
        "svc": GPTuner(svc_tunables),
        "ridge": GPTuner(ridge_tunables),
        "gaussian_process": GPTuner(gaussian_process_tunables),
        "quadratic_discriminant": GPTuner(quadratic_discriminant_tunables),
        "linear_discriminant": GPTuner(linear_discriminant_tunables),
        "extra_trees": GPTuner(extra_trees_tunables),
        "cat_boost": GPTuner(cat_boost_tunables),
        "bagging": GPTuner(bagging_tunables),
    }
    # declaring the selection algorithm
    selector = UCB1(
        [
            "ada",
            "DTC",
            "SGDC",
            "LogReg",
            "rf",
            "lgbm",
            "xgb",
            "knn",
            "gnb",
            "svc",
            "ridge",
            "gaussian_process",
            "quadratic_discriminant",
            "linear_discriminant",
            "extra_trees",
            "cat_boost",
            "bagging",
        ]
    )
    # defining the dictionary for selection
    selector_dict = {
        "ada": tuners["ada"].scores,
        "rf": tuners["rf"].scores,
        "DTC": tuners["DTC"].scores,
        "SGDC": tuners["SGDC"].scores,
        "lgbm": tuners["lgbm"].scores,
        "xgb": tuners["xgb"].scores,
        "knn": tuners["knn"].scores,
        "gnb": tuners["gnb"].scores,
        "svc": tuners["svc"].scores,
        "gaussian_process": tuners["gaussian_process"].scores,
        "quadratic_discriminant": tuners["quadratic_discriminant"].scores,
        "cat_boost": tuners["cat_boost"].scores,
        "bagging": tuners["bagging"].scores,
        "linear_discriminant": tuners["linear_discriminant"].scores,
        "extra_trees": tuners["extra_trees"].scores,
        "LogReg": tuners["LogReg"].scores,
        "ridge": tuners["ridge"].scores,
    }
    return (tuners, candidates, selector, selector_dict)
