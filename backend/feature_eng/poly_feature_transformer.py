import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import PolynomialFeatures


class PolynomialFeatureTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, polynomial_operation_input):
        """initialises polynomial_operation_input.Set it with a global scope.

        Args:
            polynomial_operation_input (dict): feature name and polynomial degree/power.
            "feature":[col_name]
            "degree": int
        """
        self.polynomial_operation_input = polynomial_operation_input

    def fit(self, X, y=None):
        """Fits and calculates the parameters that are used while transforming dataset.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.
            y (Pandas DataFrame/ Series, optional): Target Column. Defaults to None.

        Returns:
            Self: reference to the instance object on which it was called.
        """
        return self

    def polynomialTransformation(self, data_frame, polynomial_operation_input):
        """Generates new feature of polynomial degree.
        Particular feature gets its new feature added of degree 2 to n.
        Raises exception when degree is < 2 or if feature doesn't exist in dataset.

        Args:
            data_frame (Pandas DataFrame): Dataset in pipeline or input read from path.
            polynomial_operation_input ([type]): [description]

        Raises:
            Exception: Polynomial degree should be greater than 1.

        Returns:
            Pandas DataFrame: original datset with newly genearted polynomial features.
        """
        feature_list = []
        try:
            # iterate over list of dictionary of feature and polynomial degree required
            for feature in polynomial_operation_input:
                # extractinh values using keys of dict
                feature_name = feature.get("feature")
                feature_list.append(feature_name)
                poly_degree = feature.get("degree")
                # poly degree check. should be greater than one
                if poly_degree > 1:
                    # reshaping for sklearn polynomialfeature class/transformation
                    feature_array = data_frame.loc[:, feature_name].values.reshape(-1, 1)
                    poly = PolynomialFeatures(degree=poly_degree)
                    # sklearn fit_transform call
                    poly_result = poly.fit_transform(feature_array)
                    # creating new column label
                    columns_labels = [str(feature_name) + str('^') + str(i) for i in range(poly_degree + 1)]
                    # masking out relevant columns only from returned dataset
                    poly_feature_dataframe = pd.DataFrame(data=poly_result, columns=columns_labels)
                    poly_feature_dataframe.drop([columns_labels[0], columns_labels[1]], axis=1, inplace=True)
                    data_frame.join(poly_feature_dataframe) #This Line has error
                    print(data_frame)
                else:
                    raise Exception("Request polynomial degree should be >1 only.")
        except Exception as er:
            raise Exception("Error in Poly Tranformer. Invalid input. Main error is:  {}".format(er))

        return data_frame

    def transform(self, X):
        """Trasforms the dataset based on fit parameters.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.

        Returns:
            Pandas DataFrame: dataframe comprising of existing and new generated polynomial features
        """
        return self.polynomialTransformation(X, self.polynomial_operation_input)
