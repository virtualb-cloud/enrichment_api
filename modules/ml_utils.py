
import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import Ridge


class Utils:

    def __init__(self) -> None:
        pass

# utils

    def train_validate_test_splitter(self, df:pd.DataFrame, target:str):
        
        length = len(df)

        train_size = 0.8
        validate_size = 0.2
        test_size = 0.1

        # X, y
        X = df.drop(target, axis=1)
        y = df[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X, 
            y, 
            test_size=test_size, 
            random_state=42
        )

        X_train, X_validate, y_train, y_validate = train_test_split(
            X_train, 
            y_train, 
            test_size=validate_size, 
            random_state=42
        )

        return X_train, X_validate, X_test, y_train, y_validate, y_test
    
    def ordinals_scaler(self, df:pd.DataFrame, ordinals:list):

        scaled_df = df

        X = scaled_df[ordinals]

        scaler = MinMaxScaler(feature_range=(0,1))

        scaler.partial_fit(X)

        with open("models/scaler.pkl", 'wb') as path:
            pickle.dump(scaler, path)

        # Generating the standardized values of X
        scaled_df[ordinals] = scaler.transform(X)

        return scaled_df
    
    def nominals_encoder(self, df:pd.DataFrame, nominals:list):
        
        encoded_df = df

        # one hot encoder
        encoder = OneHotEncoder(handle_unknown='ignore', sparse=True)
        encoder.fit(encoded_df[nominals])

        # output columns
        output_categories = []

        for input_category in encoder.categories_:
            input_category = input_category.tolist()
            
            for x in input_category:
                output_categories.append(x)

        transformed = encoder.transform(encoded_df[nominals])
        encoded_df[output_categories] =  transformed.toarray()
        encoded_df = encoded_df.drop(nominals, axis=1)

        with open("models/encoder.pkl", 'wb') as path:
            pickle.dump(encoder, path)

        # get dummies
        # one_hot_df = pd.get_dummies(df, columns=nominals)

        return encoded_df, output_categories

# regressors

    def base_forest_regressor(self, X_train, y_train):

        model = RandomForestRegressor(
            n_estimators=10, 
            criterion='squared_error', 
            max_depth=20,
            min_samples_leaf=10
        )

        model.fit(X_train,y_train)
        
        parameters = {
            'n_estimators' : [100],
            'max_depth' : [15, 20],
            'min_samples_leaf' : [40, 50, 60]
            }

        # Model GridSearchCV
        model_grid = GridSearchCV(
            model,
            parameters,
            cv=3,
            scoring='neg_mean_squared_error',
            verbose=0,
            return_train_score=True
        ).fit(X_train,y_train)

        return model_grid.best_estimator_, model_grid.best_params_

    def base_tree_regressor(self, X_train, y_train):

        model = DecisionTreeRegressor(
            criterion='squared_error', 
            splitter="best",
            max_depth=50,
            min_samples_leaf=1
        )

        model.fit(X_train,y_train)
        
        parameters = {
            'max_depth' : [15, 20],
            'min_samples_leaf' : [45, 50]
            }

        # Model GridSearchCV
        model_grid = GridSearchCV(
            model,
            parameters,
            cv=3,
            scoring='r2',
            verbose=0,
            return_train_score=True
        ).fit(X_train,y_train)

        return model_grid.best_estimator_, model_grid.best_params_

    def base_linear_regressor(self, X_train, y_train):

        model = Ridge()

        model.fit(X_train,y_train)

        return model, model.coef_


    def base_regressor_metrics(self, model, X_test, y_test):
        
        # Model prediction on test set
        y_predict = model.predict(X_test)

        metrics = {
            "mae" : round(mean_absolute_error(y_test, y_predict), 2),
            "mse" : round(mean_squared_error(y_test, y_predict), 2),
            "r2" : round(r2_score(y_test, y_predict), 2)
        }

        return metrics


