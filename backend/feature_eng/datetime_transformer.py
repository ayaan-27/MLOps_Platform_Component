
import calendar

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from utils.logs import get_logger

LOGGER = get_logger()


class DateTimeTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, feature_name):
        """initialises feature name while object creation of transformer.
        Set it with a global scope.

        Args:
            feature_name (str): name of feature that is descripted as date time column
        """
        self.feature_name = feature_name

    def quarter(self, month):
        """identifies quarter of the year.
        Args:
            month (int): month of the year in numeric/ integer value.

        Returns:
            str: 1st quarter, 2nd Quarter, 3rd Quarter and 4th Quarter of the yearly cycle.
        """
        quarter = ''
        if(month >= 1 and month <= 3):
            quarter = '1st Quarter'
        elif(month >= 3 and month <= 6):
            quarter = '2nd Quarter'
        elif(month >= 6 and month <= 9):
            quarter = '3rd Quarter'
        elif(month <= 12):
            quarter = '4th Quarter'
        else:
            quarter = 'Null'
        return quarter

    def datetimeFeatureExtraction(self, feature):
        """extracts new features from existing date time value in a feature.

        Parameters
        ----------
        feature: object
            value/instance of feature in any format.

        Returns
        -------
        data_frame : series (pandas)
            pandas series object comprising of newly generated date time features.
            ['Day_of_Month','Month','Year','Day_of_week','Working_Day_Type','Type_of_Quarter','Hour','Minute']
            Also, if feature is NaT type- representing missing Values then, all feature are generated of NaT type.

        """
        try:
            # converting to datetime object
            feature = pd.to_datetime(feature)
        except Exception as e:
            LOGGER.error(
                "Error in convert feature to date time pandas object. Error is: ", e)

        if (pd.notnull(feature)):
            # extract day, month, hour etc
            day = str(feature.day)
            time_hour = feature.hour
            time_minute = feature.minute
            month = feature.month
            monthName = str(feature.strftime("%B"))
            year = str(feature.year)
            dayOfWeek = str(calendar.day_name[feature.weekday()])
            d_week = feature.weekday()
            typeOfWorkingDay = ''
            # checking for working day type
            if int(d_week) < 5:
                typeOfWorkingDay = 'Week Day'
            else:
                typeOfWorkingDay = 'Weekend'
            quarter = self.quarter(int(month))
            return pd.Series([day, monthName, year, dayOfWeek, typeOfWorkingDay, quarter, time_hour, time_minute])
        else:
            return pd.Series([pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT])

    def datetimeTransformation(self, data_frame, col_name):
        """Perform date time tranformation on given date column feature.

        Args:
            data_frame (Pandas DataFrame): data set which is in pipeline or is read from the path provided
            col_name (str): feature name which contains date timestamp in the data set.

        Returns:
            Pandas DataFrame: dataframe comprising of existing and new generated date time features
            feature lables are: ['Day_of_Month','Month','Year','Day_of_week','Working_Day_Type','Type_of_Quarter','Hour','Minutes']
        """
        column_label = ['Day_of_Month', 'Month', 'Year', 'Day_of_week',
                        'Working_Day_Type', 'Type_of_Quarter', 'Hour', 'Minutes']

        try:
            # parsing clean date with format of MM-DD-YY
            data_frame['clean_date'] = data_frame[col_name].apply(
                lambda x: pd.Timestamp(x).strftime('%B-%d-%Y %I:%M %p'))
        except ValueError as ve:
            LOGGER.error("Date time value/format not proper. Error is: ", ve)
        except Exception as e:
            LOGGER.error("Error in formattig timestamp Error is: ", e)
        # apply feature extraction method to each row
        data_frame[column_label] = data_frame.clean_date.apply(
            self.datetimeFeatureExtraction)

        # checking if all null values are not added in column. If yes, then drop that generated column.
        for col in column_label:
            if(data_frame[col].isnull().all()):
                data_frame.drop(columns=[col], inplace=True)

        return data_frame

    def fit(self, X, y=None):
        """Fits and calculates the parameters that are used while transforming dataset.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.
            y (Pandas DataFrame/ Series, optional): Target Column. Defaults to None.

        Returns:
            Self: reference to the instance object on which it was called.
        """
        return self

    def transform(self, X):
        """Trasforms the dataset based on fit parameters.

        Args:
            X (Pandas DataFrame): Dataset with only independent features.

        Returns:
            Pandas DataFrame: dataframe comprising of existing and new generated date time features
        """
        return self.datetimeTransformation(X, self.feature_name)
