
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin

from utils.logs import get_logger

LOGGER = get_logger()


class MathOpsTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, math_ops_input):
        """initialises math_ops_input while object creation of transformer.
        Set it with a global scope

        Args:
            math_ops_input (dict): feature names and the operation to be performed.
        """
        self.math_ops_input = math_ops_input

    def fit(self, X, y=None):
        """Fits and calculates the parameters that are used while transforming dataset.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.
            y (Pandas DataFrame/ Series, optional): Target Column. Defaults to None.

        Returns:
            Self: reference to the instance object on which it was called.
        """
        return self

    def concatOperation(self, data_frame, list_of_features, concat_by):
        """Perform concatenation of features.
        features are of categorical type and get concatenated by character of choice.

        Args:
            data_frame (Pandas DataFrame): dataframe in pipeline or read from local path
            list_of_features (list(str)): feature names that are to be concatenated
            concat_by (str): Feature will be concatenated using this feature only

        Returns:
            Pandas DataFrame: transformed dataset with newly added feature.
        """

        feature_name = ''
        # creating column label based on all input columns/features
        for feature in list_of_features:
            feature_name = str(feature_name) + concat_by + str(feature)
        # concatenating all features/columns using concat_by
        data_frame[feature_name] = data_frame[list_of_features].apply(lambda row: concat_by.join(row.values.astype(str)), axis=1)
        return data_frame

    def productOperation(self, data_frame, list_of_features):
        """Perform multiplication of features

        Args:
            data_frame (Pandas DataFrame): dataframe in pipeline or read from path provided.

            list_of_features (list(str)): feature/column names

        Returns:
            Pandas DataFrame: resulting data frame with new feature  generated after multiplication.
        """
        product_feature_name = 'Product: '
        # creating column label with product and column/feature names
        for feature in list_of_features:
            product_feature_name = product_feature_name + str(' ') + str(feature)
        # getting first feature in product input list
        feature1 = list_of_features[0]
        # copying first feature input in list as new feature
        data_frame[product_feature_name] = data_frame[str(feature1)]
        # multiplying each feature in list iteratively
        for col in range(1, len(list_of_features)):
            data_frame.loc[:, str(product_feature_name)] = data_frame.loc[:, str(list_of_features[col])] * data_frame.loc[:, str(product_feature_name)]

        return data_frame

    def additionOperation(self, data_frame, list_of_features):
        """Perform addition of features

        Args:
            data_frame (Pandas DataFrame): dataframe in pipeline or read from path provided.

            list_of_features (list(str)): feature/column names

        Returns:
            Pandas DataFrame: resulting data frame with new feature  generated after addition.
        """
        addition_feature_name = "Sum: "
        # creating feature name based on input feature list
        for feature in list_of_features:
            addition_feature_name = addition_feature_name + str(' ') + str(feature)
        # adding all feature in input list
        data_frame.loc[:, addition_feature_name] = data_frame.loc[:, list_of_features].sum(axis=1)
        return data_frame

    def subtractOperation(self, data_frame, list_of_features):
        """Perform subtraction of features.
        all features are subtracted from first feature element in the list.

        Args:
            data_frame (Pandas DataFrame): dataframe in pipeline or read from path provided.

            list_of_features (list(str)): list of list of feature values.
                Feature added in list are those that require division.

        Returns:
            data_frame: resulting data frame with new feature  generated after subtraction.
        """
        subtraction_feature_name = "sub: "
        # creating feature name based on input feature list
        for feature in list_of_features:
            subtraction_feature_name = subtraction_feature_name + str(' ') + str(feature)
        feature1 = list_of_features[0]
        # from first feature subtracting all remaining features in list
        data_frame.loc[:, subtraction_feature_name] = data_frame.loc[:, feature1] - data_frame.loc[:, list_of_features[1::]].sum(axis=1)
        return data_frame

    def ratioOperation(self, data_frame, list_of_features):
        """Perform division of features.
        first feature in list is divided by remaining features.

        Args:
            data_frame ([type]): dataframe in pipeline or read from path provided.
            list_of_features (list(str)): list of list of feature values.
            Feature added in list are those that require division.

        Returns:
            data_frame: transformed dataset with newly added feature
        """
        ratio_feature_name = 'Ratio'
        # creating feature name based on input feature list
        for feature in list_of_features:
            ratio_feature_name = ratio_feature_name + str('/') + str(feature)
        # first column in col that will be in numerator
        feature1 = list_of_features[0]
        # creating ratio_feature_name column with value of numerator column
        data_frame.loc[:, str(ratio_feature_name)] = data_frame.loc[:, str(feature1)]
        try:
            # dividing numerator column by remaining columns in list iteratively
            for col in range(1, len(list_of_features)):
                data_frame.loc[:, str(ratio_feature_name)] = np.divide(data_frame.loc[:, str(ratio_feature_name)],
                                                                       data_frame.loc[:, str(list_of_features[col])])
        except Exception as e:
            LOGGER.error("Error while performing ratio operation in MathOps. Error is: {}".format(e))
        return data_frame

    def mathOpsController(self, data_frame, math_ops_input):
        """Controls flow of request.
        Checks for data type of numerical and perform desired operation possible on the features.

        Args:
            data_frame (Pandas DataFrame): Data frame in pipeline or loaded csv file.

            math_ops_input (list of dict): contains the operation to be performed and the list of features which are to be operated.
            Ex.
            [{'feature': [name_feature1, name_feature1], 'operationType': 'concat', 'concat_using': '@'},
             {'feature': [name_feature1, name_feature1], 'operationType': 'addition'}]

        Raises:
            Exception: Concat operation requested but concat_using operator/parameter not provided

        Returns:
            Pandas DataFrame: transformed data frame.
        """
        try:  # iteratively fetching out features list and operation type
            for operation in math_ops_input:
                feature_list = operation.get("feature")
                operationType = operation.get("operationType")

                # concat operation check and get input parameter check
                if operationType.lower() == 'concat':
                    concat_by = operation.get("concat_using")
                    if concat_by is None:
                        raise Exception("Input to MathOps doesn't contain concat_using attribute for concat operation.")
                    # call concatOperation
                    data_frame = self.concatOperation(data_frame, feature_list, concat_by)
                # check addition operation
                if operationType.lower() == 'addition':
                    # call addition operation
                    data_frame = self.additionOperation(data_frame, feature_list)
                # check subtraction operation
                elif operationType.lower() == 'subtraction':
                    # call subtract operation
                    data_frame = self.subtractOperation(data_frame, feature_list)
                # check multiplication
                elif operationType.lower() == 'multiplication':
                    # call multiplication operation
                    data_frame = self.productOperation(data_frame, feature_list)
                # check ratio operation
                elif operationType.lower() == 'ratio':
                    # call ratio operation
                    data_frame = self.ratioOperation(data_frame, feature_list)
        except Exception as er:
            LOGGER.error("Error in Maths Ops controller. Error is: {}".format(er))

        return data_frame

    def transform(self, X):
        """Transforms the dataset based on fit parameters.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.

        Returns:
            Pandas DataFrame: dataframe comprising of existing and new generated date time features
        """
        return self.mathOpsController(X, self.math_ops_input)
