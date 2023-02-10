from sqlalchemy import create_engine
from ml_utils import Utils
import pandas as pd
import numpy as np
import pickle
import json
import time
from datetime import date


class Train:

    def __init__(self) -> None:
        
        # ml utils object
        self.utils = Utils()

        # db connection
        self.schema_name = "data_enrichment_v3"
        self.engine = create_engine("postgresql://postgres:!vbPostgres@virtualb-rds-data-enrichment.clg6weaheijj.eu-south-1.rds.amazonaws.com:5432/postgres")
        
        # variables
        self.domain = ["age", "gender", "location", "education", "profession"]
        
        self.encoded_columns = [
            'f', 'm', 
            'abruzzo', 'basilicata', 'calabria', 'campania', 
            'emilia romagna', 'friuli venezia giulia', 'lazio', 'liguria', 
            'lombardia', 'marche', 'molise', 'piemonte', 'puglia', 
            'sardegna', 'sicilia', 'toscana', 'trentino alto adige', 
            'umbria', 'valle daosta', 'veneto', 
            'istruzione superiore università', 'master di II livello e PHD', 
            'scuola primaria', 'scuola secondaria di I grado', 'scuola secondaria di II grado', 
            'lavoratore dipendente', 'lavoratore indipendente', 'non occupato', 'pensionato'
        ]
        
        self.gender_map = {
            "m" : 1,
            "f" : 2
        }
        
        self.location_map = {
            'piemonte': 1,
            'valle daosta': 2,
            'lombardia': 3,
            'trentino alto adige': 4,
            'veneto': 5,
            'friuli venezia giulia': 6,
            'liguria': 7,
            'emilia romagna': 8,
            'toscana': 9,
            'umbria': 10,
            'marche': 11,
            'lazio': 12,           
            'abruzzo': 13,
            'molise': 14,
            'campania': 15,
            'puglia': 16,
            'basilicata': 17,
            'calabria': 18,
            'sicilia': 19,
            'sardegna': 20
        }
        
        self.education_map = {
            "scuola primaria" : 1,
            "scuola secondaria di I grado" : 2,
            "scuola secondaria di II grado" : 3,
            "istruzione superiore università" : 4,
            "master di II livello e PHD" : 5
        }
        
        self.profession_map = {
            "non occupato" : 1,
            "lavoratore dipendente" : 2,
            "lavoratore indipendente" : 3,
            "pensionato" : 4
            
        }
        
        self.cultures = [
            "financial_litteracy_index", "financial_experience_index", 
            "objective_risk_propensity_index", 
            "subjective_risk_propensity_index", "esg_propensity_index", "financial_horizon_index", 
            "life_quality_index"]
        
        self.status = [
            "net_income_index", "net_expences_index", "net_savings_index",
            "real_assets_index", "financial_assets_index", "net_liabilities_index",
            "net_wealth_index"]
        
        self.attitudes = [
            "bank_activity_index", "digital_activity_index", 
            "cultural_activity_index", "charity_activity_index"]
        
        self.needs = [
            "capital_accumulation_investment_need", "capital_protection_investment_need", 
            "liquidity_investment_need", "income_investment_need",
            "retirement_investment_need", "heritage_investment_need",
            "health_insurance_need", "home_insurance_need",
            "longterm_care_insurance_need", "payment_financing_need",
            "loan_financing_need", "mortgage_financing_need"]

        # all X
        self.ordinals = ['age']
        self.nominals = ['gender', 'location', 'education', 'profession']
        
        # all y
        self.indexes = self.cultures + self.status + self.attitudes + self.needs


    def check_samples_table(self):
        
        dirty_samples = self.engine.execute(
            f'''
            SELECT sp.sample_id, sp.mean_age, sp.mode_gender, sp.mode_location, 
            sp.mode_education, sp.mode_profession, sp.sample_size, sp.sample_date
            FROM {self.schema_name}.sample_properties as sp;
            '''
        ).fetchall()

        # initiate
        clean_samples = {}
        for sample in dirty_samples:
            clean_samples[sample[0]] = {
                "mean_age" : sample[1],
                "mode_gender" : self.gender_map[sample[2]],
                "mode_location" : self.location_map[sample[3]],
                "mode_education" : self.education_map[sample[4]],
                "mode_profession" : self.profession_map[sample[5]],
                "sample_size" : sample[6],
                "sample_date" : sample[7]
            }
        
        return clean_samples

    def rate_samples(self, body:dict):

        samples = self.check_samples_table()

        new_sample = {
            "mean_age" : body["predict_set"]["mean_age"],
            "mode_gender" : self.gender_map[body["predict_set"]["mode_gender"]],
            "mode_location" : self.location_map[body["predict_set"]["mode_location"]],
            "mode_education" : self.education_map[body["predict_set"]["mode_education"]],
            "mode_profession" : self.profession_map[body["predict_set"]["mode_profession"]]
        }

        ratings = {}
        
        for sample_id, sample_prop in samples.items():

            dist = np.abs(sample_prop["mean_age"] - new_sample["mean_age"]) / 82
            
            dist += np.abs(sample_prop["mode_gender"] - new_sample["mode_gender"]) 
            
            dist += np.abs(sample_prop["mode_location"] - new_sample["mode_location"]) / 20
            
            dist += np.abs(sample_prop["mode_education"] - new_sample["mode_education"]) / 5
            
            dist += np.abs(sample_prop["mode_profession"] - new_sample["mode_profession"]) / 4
            
            sample_year = int(sample_prop["sample_date"].split("/")[2])
            oldest_sample = 2016
            today = date.today()
            year = int(today.strftime("%Y"))
            dist += np.abs( (sample_year - year) / (oldest_sample - year) )
            dist += np.abs(sample_prop["sample_size"]) / 500000

            ratings[sample_id] = 1 - (dist / 7)

        maximum = ratings[1]
        minimum = ratings[1]
        for key, value in ratings.items():
            if value > maximum:
                maximum = value
            elif value < minimum:
                minimum = value

        for key, value in ratings.items():
            if value != minimum:
                ratings[key] = (value - minimum) / (maximum - minimum)
            else:
                ratings[key] = 0.1

        return ratings

    def read_table(self, column:str):

        if column in self.cultures: table_name = "cultures"
        elif column in self.attitudes: table_name = "attitudes"
        elif column in self.status: table_name = "status" 
        elif column in self.needs: table_name = "needs" 

        
        # read status
        query = f'''
        SELECT sd.sample_id, sd.age_, sd.gender_, sd.location_, sd.education_, sd.profession_,
        st.{column}
        
        FROM {self.schema_name}.sociodemographics as sd

            JOIN {self.schema_name}.{table_name} as st
            ON st.person_id = sd.person_id
        
        WHERE st.{column} IS NOT NULL;
        '''

        table = pd.read_sql(sql=query, con=self.engine)

        table.rename(columns={
            "age_" : "age",
            "gender_" : "gender",
            "location_" : "location",
            "education_" : "education",
            "profession_" : "profession",
            f"st.{column}" : column
        }, inplace=True)

        table[column] = table[column].astype("float")

        return table

    def aggregate(self, table:pd.DataFrame, column:str, rates:dict):
        
        # rate the samples 
        weights = 0 
        for sample_id in table["sample_id"].unique():
            weights += rates[sample_id]

        if weights != 1: coef = 1/weights
        else: coef = 1
        table["rating"] = table["sample_id"].map(rates) * coef

        # calculate alpha and beta of beta-binomial distribution 
        table["alpha"] = table[column] * table["rating"]
        table["beta"] = table["rating"] - table["alpha"]

        # sum the alphas and betas in each hypothetical domain cell 
        final_df = table.groupby(
            by=['age', 'gender', 'location', 'education', 'profession']
            )[["alpha", "beta"]].agg(["sum"])
        final_df.columns = final_df.columns.droplevel(level=1)
        final_df.reset_index(inplace=True)

        # find the distribution peak related to aggregated alpha and beta
        final_df["dist_peak"] = final_df["alpha"] / (final_df["alpha"] + final_df["beta"])
        final_df[column] = final_df["dist_peak"]
        final_df.drop(labels=["alpha", "beta", 'dist_peak'], axis=1, inplace=True)

        return final_df

    def build_model(self, final_df:pd.DataFrame, column:str):
        
        minimum = final_df[column].min()
        maximum = final_df[column].max()
        final_df[column] = round((final_df[column]-minimum)/(maximum-minimum) ,2)

        # scale y
        if column in self.status:
            
            mean = np.average(final_df[column])
            stddev = np.std(final_df[column])

            left = mean - (3 * stddev)
            right = mean + (3 * stddev)

            array = final_df[column].apply(lambda x: left if x<=left else right if x>=right else x)

            maximum = array.max()
            minimum = array.min()
            
            print(array.min(), array.max(), np.average(array), np.std(array))
            final_df[column] = (array - minimum) / (maximum - minimum)


        else:
        
            final_df[column] = final_df[column]

        # scale and encode X
        final_df = self.utils.ordinals_scaler(df=final_df, ordinals=self.ordinals)
        
        final_df, output_columns = self.utils.nominals_encoder(df=final_df, nominals=self.nominals)

        ready_df = pd.DataFrame()
        for col in self.ordinals:
            ready_df[col] = final_df[col]

        for col in self.encoded_columns:

            if col in final_df.columns: ready_df[col] = final_df[col]
            elif not col in final_df.columns: ready_df[col] = 0
        
        ready_df[column] = final_df[column]
        
        # train, validate, and test sets
        X_train, X_validate, X_test, y_train, y_validate, y_test = self.utils.train_validate_test_splitter(df=ready_df, target=column)
        
        # train model
        model, parameters = self.utils.base_forest_regressor(X_train=X_train, y_train=y_train)

        train_metrics = self.utils.base_regressor_metrics(model, X_train, y_train)
        validation_metrics = self.utils.base_regressor_metrics(model, X_validate, y_validate)
        test_metrics = self.utils.base_regressor_metrics(model, X_test, y_test)
        metrics = [train_metrics, validation_metrics, test_metrics]

        # save model
        with open(f"models/{column}.pkl", "wb") as write_model:
            pickle.dump(model, write_model)

        return model, parameters, metrics
    
    def metrics_builder(self, column:str, parameters, metrics:list):

        train_metrics = metrics[0]
        validation_metrics = metrics[1]
        test_metrics = metrics[2]

        model_details_df = pd.DataFrame()
        idx = 0

        # save metrics
        model_details_df.loc[idx, "target"] = column
        model_details_df.loc[idx, "model"] = "RandomForestRegressor"
        model_details_df.loc[idx, "parameters"] = str(parameters)
        model_details_df.loc[idx, "parameters_validator"] = "GridSearchCV"
        model_details_df.loc[idx, "train_mae"] = train_metrics["mae"]
        model_details_df.loc[idx, "train_mse"] = train_metrics["mse"]
        model_details_df.loc[idx, "train_r2"] = train_metrics["r2"]
        model_details_df.loc[idx, "validation_mae"] = validation_metrics["mae"]
        model_details_df.loc[idx, "validation_mse"] = validation_metrics["mse"]
        model_details_df.loc[idx, "validation_r2"] = validation_metrics["r2"]
        model_details_df.loc[idx, "test_mae"] = test_metrics["mae"]
        model_details_df.loc[idx, "test_mse"] = test_metrics["mse"]
        model_details_df.loc[idx, "test_r2"] = test_metrics["r2"]

        model_details_df.loc[idx, "execution_date"] = date.today()
        idx += 1

        model_details_df.set_index(keys=["target"],  inplace=True)
        model_details_df.to_sql("regression_metrics", if_exists="append",schema=self.schema_name, con=self.engine)

        return True

    def run_modeling(self, body:dict):
        
        # rate the samples with respect to predict set
        rates = self.rate_samples(body)

        for column in self.indexes:

            # read what we have as information
            table = self.read_table(column=column)

            final_df = self.aggregate(table=table, column=column, rates=rates)

            model, parameters, metrics = self.build_model(final_df=final_df, column=column)

            self.metrics_builder(column=column, parameters=parameters, metrics=metrics)

        return True