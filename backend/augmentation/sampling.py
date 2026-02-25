from imblearn.over_sampling import SMOTE, SMOTENC, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
import utils.logs as logs
LOGGER = logs.get_logger()


class Sampling:
    def __init__(self, X, Y, sampling_strategy):
        """
        Args:
             X : pandas dataframe
                The dataframe containing original data.

             Y : array
                Corrosponding labels of X.

             sampling_strategy : float, str or dict
                 Sampling information to resample the data set.
                 When 'float' , it corresponds to the desired ratio of the
                 number of samples in the minority class over the number of
                 samples in the majority class after resampling.

                 When str, specify the class targeted by the resampling.
                 For oversampling and SMOTE : ['minority', 'not minority',
                                             'all', 'auto'].
                 For undersampling : ['majority', 'not minority',
                                     'not majority', 'all', 'auto'].
                 Defaults to 'auto'.
                 When dict, the keys correspond to the targeted classes.
                 The values correspond to the desired number of samples for
                 each targeted class.
        """
        self.x = X
        self.y = Y
        self.sampling_strategy = sampling_strategy

    def randomOverSampler(self):
        """
            Function to perform Random Oversampling
        Args:

        Returns:
            X : pandas dataframe
                The dataframe containing resampled data.

            Y : array
                Corrosponding labels of X.
        """
        LOGGER.info("Random oversampling")
        oversampler = RandomOverSampler(sampling_strategy=self.sampling_strategy)
        X, Y = oversampler.fit_resample(self.x, self.y)
        return X, Y

    def randomUnderSampler(self):
        """
            Performs Random Undersampling
        Args:

        Returns:
            X : pandas dataframe
                The dataframe containing resampled data.

            Y : array
                Corrosponding labels of X.
        """
        LOGGER.info("Random undersampling")
        undersampler = RandomUnderSampler(sampling_strategy=self.sampling_strategy)
        X, Y = undersampler.fit_resample(self.x, self.y)
        return X, Y

    def smote(self, k_neighbors=5):
        """
            Performs SMOTE
        Args:
            k_neighbors : int
                Number of nearest neighbours to used to construct synthetic
                samples, defaults to 5.

        Returns:
            X : pandas dataframe
                The dataframe containing resampled data.

            Y : array
                Corrosponding labels of X.
        """
        LOGGER.info("SMOTE")
        over_smote = SMOTE(
            sampling_strategy=self.sampling_strategy, k_neighbors=k_neighbors, n_jobs=-1
        )
        X, Y = over_smote.fit_resample(self.x, self.y)
        return X, Y

    def smotenc(self, categorical_features, k_neighbors=5):
        """
            Performs Categorical SMOTE
        Args:
            categorical_features : list(int)
                Specified which features are categorical

            k_neighbors : int
                Number of nearest neighbours to used to construct synthetic
                samples, defaults to 5.

        Returns:
            X : pandas dataframe
                The dataframe containing resampled data.
            Y : array
                Corrosponding labels of X.
        """
        LOGGER.info("Categorical SMOTE")
        smotenc = SMOTENC(
            categorical_features=categorical_features,
            sampling_strategy=self.sampling_strategy,
            k_neighbors=k_neighbors,
        )
        X, Y = smotenc.fit_resample(self.x, self.y)
        return X, Y
