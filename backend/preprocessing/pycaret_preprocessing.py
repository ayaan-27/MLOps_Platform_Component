from pycaret.internal.preprocess import Preprocess_Path_One


def preprocess(df, predictor, options):
    df = df
    imputation_type_str = options["imputation_type"] + " imputer"
    return Preprocess_Path_One(
        train_data=df,
        target_variable=predictor,
        display_types=False,
        imputation_type=imputation_type_str,
        numeric_imputation_strategy=options["num_imputation_strategy"],
        categorical_imputation_strategy=options["cat_imputation_strategy"],
        apply_zero_nearZero_variance=options["is_znz"],
        club_rare_levels=options["is_club_rare"],
        rara_level_threshold_percentage=0.05,
        apply_untrained_levels_treatment=options["is_untrained_lvls"],
        untrained_levels_treatment_method="least frequent",
        apply_ordinal_encoding=options["is_ordinal_encoding"],
        apply_cardinality_reduction=options["is_cardinality_reduction"],
        cardinal_method="cluster",
        apply_binning=options["is_binning"],
    )
