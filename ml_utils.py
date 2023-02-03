
import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import recall_score, precision_score, f1_score, accuracy_score, confusion_matrix
from catboost import CatBoostClassifier
# from sklearn.ensemble import RandomForestRegressor
# import tensorflow
# from keras.wrappers.scikit_learn import KerasClassifier

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

        with open("pipelines_ml/models/scaler.pkl", 'wb') as path:
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

        with open("pipelines_ml/models/encoder.pkl", 'wb') as path:
            pickle.dump(encoder, path)

        # get dummies
        # one_hot_df = pd.get_dummies(df, columns=nominals)

        return encoded_df, output_categories

    def imbalanced_class_cutter(self, df:pd.DataFrame, target:str):
        
        lengths = {}
        min_length = len(df)

        values = df[target].unique()

        # find the lengths
        for value in values:
            _df = df[ df[target] == value]
            lengths[value] = len(_df)

            # find the min length
            if len(_df) < min_length: min_length = len(_df)

        # cut and concat
        concats = []
        concatenated_df = pd.DataFrame()


        for value, length in lengths.items():

            fraction = np.abs(min_length / length)
            concats.append(df[df[target] == value].sample(frac=fraction))

        for item in concats:

            concatenated_df = pd.concat([concatenated_df, item], axis=0, ignore_index=True)

        return concatenated_df

# classifiers

    def build_ANN_classifier(self, unit:int):
        
        ann = tensorflow.keras.models.Sequential()
        
        ann.add(tensorflow.keras.layers.Dense(units=unit, activation='relu'))
        ann.add(tensorflow.keras.layers.Dense(units=unit, activation='relu'))
        # softmax or sigmoid
        ann.add(tensorflow.keras.layers.Dense(units=1, activation='softmax'))
        
        ann.compile(optimizer = 'adam', loss = 'binary_crossentropy', metrics = ['accuracy'])

        return ann

    def base_ANN_classifier(self, X_train, y_train):
        
        model = KerasClassifier(build_fn=self.build_ANN_classifier)
        
        params = {
            'batch_size':[100, 50],
            'nb_epoch':[5, 10],
            'unit':[8, 16, 32]
        }

        model = GridSearchCV(
            estimator=model, 
            param_grid=params, 
            scoring='accuracy', 
            cv=3
        )
        
        model = model.fit(X_train, y_train)

        return model.best_estimator_, model.best_params_

    def base_catboost_classifier(self, X_train, y_train):
        
        model = CatBoostClassifier(
            iterations=500, 
            loss_function='Logloss', 
            eval_metric='Accuracy',
            verbose=False,
            early_stopping_rounds=50,
            depth=3,
            random_state=42
        )

        model.fit(X_train,y_train)
        
        parameters = {'depth':[2,3],'iterations':[500,1000]}

        # Model GridSearchCV
        model_grid = GridSearchCV(
            model,
            parameters,
            cv=3,
            scoring='accuracy',
            verbose=0,
            return_train_score=True
        ).fit(X_train,y_train)

        return model_grid.best_estimator_, model_grid.best_params_

    def base_classification_metrics(self, model, X_test, y_test):
        
        # Model prediction on test set
        y_predict = model.predict(X_test)

        metrics = {
            "recall" : recall_score(y_test, y_predict),
            "precision" : precision_score(y_test, y_predict),
            "f1_score" : f1_score(y_test, y_predict),
            "accuracy" : accuracy_score(y_test, y_predict),
            # "confusion_matrix" : confusion_matrix(y_test, y_predict, normalize='true') 
        }

        return metrics

    def base_neighbors_classifier(self, X_train, y_train):
        pass

    def base_classifier_metrics(self):
        pass

# regressors

    def base_forest_regressor(self, X_train, y_train):

        model = RandomForestRegressor(
            n_estimators=100, 
            criterion='Logloss', 
            max_depth=20,
            min_samples_leaf=10
        )

        model.fit(X_train,y_train)
        
        parameters = {
            'criterion' : ["squared_error", "absolute_error", "friedman_mse", "poisson"],
            'max_depth' : [50, 100],
            'min_samples_leaf' : [10, 50, 100]
            }

        # Model GridSearchCV
        model_grid = GridSearchCV(
            model,
            parameters,
            cv=3,
            scoring='accuracy',
            verbose=0,
            return_train_score=True
        ).fit(X_train,y_train)

        return model_grid.best_estimator_, model_grid.best_params_

    def base_regressor_metrics(self):
                # # Features importance
        # feature_importances = model_grid.best_estimator_.named_steps['model'].get_feature_importance()
        # feature_names = x_train.columns
        # important_features = []
        
        # for score, name in sorted(zip(feature_importances, feature_names), reverse=True):
        #     important_features.append(name)

        # return model_grid

        pass

