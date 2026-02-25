import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from statsmodels.stats.outliers_influence import variance_inflation_factor


class MulticollinearityRemovalTransformer(BaseEstimator, TransformerMixin):
    # global variable with non-collinear column names
    final_columns = []

    def __init__(self, threshold_value):
        """initialises threshold_value while object creation of transformer.
        Set it with a global scope.

        Args:
            feature_name (str): name of feature that is descripted as date time column
        """
        self.threshold_value = threshold_value

    def fit(self, X, y=None):
        """Fits and calculates the parameters that are used while transforming dataset.
        Find column names that are multicollinearity free.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.
            y (Pandas DataFrame/ Series, optional): Target Column. Defaults to None.

        Returns:
            Self: reference to the instance object on which it was called.
        """

        self.final_columns = self.mutliCollinearityRemoval(X, self.threshold_value)
        return self.final_columns

    def vif_scores(self, independent_features_dframe):
        """Calculates VIF scores for each feature and add the values in a dataframe.
        Dataframe contains independent features and its corresponding VIF Score.
        The column labels are: Independent Features and VIF Score.


        Args:
            independent_features_dframe ([type]): [description]

        Returns:
            Pandas DataFrame: VIF Score and corresponding feature name.
        """
        # creating new dataframe with column labels as 'independent features' and 'vif scores'
        try:
            VIF_Scores = pd.DataFrame()
            VIF_Scores.loc[:, "Independent Features"] = independent_features_dframe.columns.tolist()
            # calculating vif scores using statsmodel packages for all features in dataframe
            VIF_Scores.loc[:, "VIF Scores"] = [variance_inflation_factor(independent_features_dframe.values, i) for i in range(independent_features_dframe.shape[1])]
        except Exception as er:
            raise Exception("Error while calculating VIF values. Error is: {}".format(er))
        return VIF_Scores

    def mutliCollinearityRemoval(self, dframe, threshold):
        """Removes features that show multicollinearity and can be explained using other variables.
        Makes use of variation inflation factor(VIF) value to figure multicollinearity.
        Feature with highest VIF is dropped and VIF score for all remaining features is checked and updated.

        Args:
            dframe (Pandas DataFrame): Entire dataframe in pipeline or input.
            threshold (float): [description]

        Returns:
            list(str): non multi-collinear features.
        """
        try:

            # only numeric columns
            dframe_numeric_features = dframe.select_dtypes(include=np.number)

            # calculating vif_value first time
            df_VIF_Score = self.vif_scores(dframe_numeric_features)
            # soring vif values in ascending order
            df_VIF_Score.sort_values(by=['VIF Scores'], inplace=True)
            # largest vif value among all features
            feature_VIF_score = df_VIF_Score.iloc[-1]['VIF Scores']

        # Iteratively dropping features by highest VIF score and rechecking VIF scores
            while (feature_VIF_score >= threshold):
                feature_name = df_VIF_Score.iloc[-1]['Independent Features']
                # dropping column with largest vif value and also greater than threshold
                dframe_numeric_features.drop(columns=[feature_name], inplace=True)
                df_VIF_Score = self.vif_scores(dframe_numeric_features)
                # sorting vif values in ascending ordre
                df_VIF_Score.sort_values(by=['VIF Scores'], inplace=True)
                # largest vif values among remaining independent features
                feature_VIF_score = df_VIF_Score.iloc[-1]['VIF Scores']
            noncollinear_features = df_VIF_Score['Independent Features'].tolist()

        except Exception as er:
            raise Exception("Error in finding multicollinear features. Error is: {}".format(er))

        return noncollinear_features

    def dropMulticollinearFeature(self, input_dataframe, final_columns):
        """drops multi collinear features and return only independent feature dataframe

        Args:
            input_dataframe (Pandas DataFrame): dataframe in pipeline or read using path provided.
            final_columns (list(str)): list of columns that show no multicollinearity and have VIF values less than threshold

        Returns:
            Pandas DataFrame: data frame with non-collinear features.
        """
        try:
            final_dataframe = pd.DataFrame()
            # masking subset of features with no multicollinearity
            final_dataframe[final_columns] = input_dataframe.loc[:, final_columns]
        except Exception as er:
            raise Exception("Error in removing multicollinear features. Error is: {}".format(er))
        return final_dataframe

    def transform(self, X):
        """Trasforms the dataset based on fit parameters.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.

        Returns:
            Pandas DataFrame: dataframe comprising of columns/features with no multicollinearity
        """
        return self.dropMulticollinearFeature(X, self.final_columns)
