import pandas as pd
import numpy as np
import pickle
from modules.ml_utils import Utils

class Predict:

    def __init__(self, purpose:str):

        self.purpose = purpose

        # ml utils object
        self.utils = Utils()

        # all X
        self.ordinals = ['age']
        self.nominals = ['gender', 'location', 'education', 'profession']
        
        # read scaler and encoder
        self.scaler = pickle.load(open('models/scaler.pkl', 'rb'))
        self.encoder = pickle.load(open('models/encoder.pkl', 'rb'))
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
        
        # read regressors
        if purpose == "Needs":
            self.capital_accumulation_investment_need = pickle.load(open('models/capital_accumulation_investment_need.pkl', 'rb'))
            self.capital_protection_investment_need = pickle.load(open('models/capital_protection_investment_need.pkl', 'rb'))
            self.liquidity_investment_need = pickle.load(open('models/liquidity_investment_need.pkl', 'rb'))
            self.income_investment_need = pickle.load(open('models/income_investment_need.pkl', 'rb'))
            self.retirement_investment_need = pickle.load(open('models/retirement_investment_need.pkl', 'rb'))
            self.heritage_investment_need = pickle.load(open('models/heritage_investment_need.pkl', 'rb'))
            self.health_insurance_need = pickle.load(open('models/health_insurance_need.pkl', 'rb'))
            self.home_insurance_need = pickle.load(open('models/home_insurance_need.pkl', 'rb'))
            self.longterm_care_insurance_need = pickle.load(open('models/longterm_care_insurance_need.pkl', 'rb'))
            self.payment_financing_need = pickle.load(open('models/payment_financing_need.pkl', 'rb'))
            self.loan_financing_need = pickle.load(open('models/loan_financing_need.pkl', 'rb'))
            self.mortgage_financing_need = pickle.load(open('models/mortgage_financing_need.pkl', 'rb'))
        
        elif purpose == "Cultures":
            self.financial_horizon_index = pickle.load(open('models/financial_horizon_index.pkl', 'rb'))
            self.financial_litteracy_index = pickle.load(open('models/financial_litteracy_index.pkl', 'rb'))
            self.financial_experience_index = pickle.load(open('models/financial_experience_index.pkl', 'rb'))
            self.objective_risk_propensity_index = pickle.load(open('models/objective_risk_propensity_index.pkl', 'rb'))
            self.subjective_risk_propensity_index = pickle.load(open('models/subjective_risk_propensity_index.pkl', 'rb'))
            self.esg_propensity_index = pickle.load(open('models/esg_propensity_index.pkl', 'rb'))
            self.life_quality_index = pickle.load(open('models/life_quality_index.pkl', 'rb'))
        
        elif purpose == "Attitudes":
            self.bank_activity_index = pickle.load(open('models/bank_activity_index.pkl', 'rb'))
            self.charity_activity_index = pickle.load(open('models/charity_activity_index.pkl', 'rb'))
            self.cultural_activity_index = pickle.load(open('models/cultural_activity_index.pkl', 'rb'))
            self.digital_activity_index = pickle.load(open('models/digital_activity_index.pkl', 'rb'))

        elif purpose == "Status":
            self.real_assets_index = pickle.load(open('models/real_assets_index.pkl', 'rb'))
            self.financial_assets_index = pickle.load(open('models/financial_assets_index.pkl', 'rb'))
            self.net_liabilities_index = pickle.load(open('models/net_liabilities_index.pkl', 'rb'))
            self.net_wealth_index = pickle.load(open('models/net_wealth_index.pkl', 'rb'))
            self.net_income_index = pickle.load(open('models/net_income_index.pkl', 'rb'))
            self.net_savings_index = pickle.load(open('models/net_savings_index.pkl', 'rb'))
            self.net_expences_index = pickle.load(open('models/net_expences_index.pkl', 'rb'))
        

    def Needs(self, mapped_configurations:list):

        # convert to data frame
        temp_df = pd.DataFrame()
        temp_df = temp_df.from_dict(mapped_configurations)
        
        # 1) ordinals scaling
        temp_df[self.ordinals] = self.scaler.transform(temp_df[self.ordinals])

        # 2) nominals encoding
        nominals_output_categories = []

        for input_category in self.encoder.categories_:
            input_category = input_category.tolist()
            
            for x in input_category:
                nominals_output_categories.append(x)

        transformed = self.encoder.transform(temp_df[self.nominals])
        temp_df[nominals_output_categories] =  transformed.toarray()
        temp_df = temp_df.drop(self.nominals, axis=1)
        
        ready_df = pd.DataFrame()
        ready_df["id"] = temp_df["id"]

        for col in self.ordinals:
            ready_df[col] = temp_df[col]

        for col in self.encoded_columns:

            if col in temp_df.columns: ready_df[col] = temp_df[col]
            elif not col in temp_df.columns: ready_df[col] = 0

        temp_df = ready_df[["id"] + self.ordinals + self.encoded_columns]
        X_predict = temp_df[self.ordinals + self.encoded_columns].copy()

        # predict
        
        temp_df["capital_accumulation_investment_need"] = np.round(self.capital_accumulation_investment_need.predict(X_predict), 2)
        temp_df["capital_protection_investment_need"] = np.round(self.capital_protection_investment_need.predict(X_predict), 2)
        temp_df["liquidity_investment_need"] = np.round(self.liquidity_investment_need.predict(X_predict), 2)
        temp_df["income_investment_need"] = np.round(self.income_investment_need.predict(X_predict), 2)
        temp_df["retirement_investment_need"] = np.round(self.retirement_investment_need.predict(X_predict), 2)
        temp_df["heritage_investment_need"] = np.round(self.heritage_investment_need.predict(X_predict), 2)
        temp_df["health_insurance_need"] = np.round(self.health_insurance_need.predict(X_predict), 2)
        temp_df["home_insurance_need"] = np.round(self.home_insurance_need.predict(X_predict), 2)
        temp_df["longterm_care_insurance_need"] = np.round(self.longterm_care_insurance_need.predict(X_predict), 2)
        temp_df["payment_financing_need"] = np.round(self.payment_financing_need.predict(X_predict), 2)
        temp_df["loan_financing_need"] = np.round(self.loan_financing_need.predict(X_predict), 2)
        temp_df["mortgage_financing_need"] = np.round(self.mortgage_financing_need.predict(X_predict), 2)
        
        
        # drop details
        temp_df.drop(columns=self.ordinals + self.encoded_columns, axis=1, inplace=True)

        # convert to dict
        results = temp_df.to_dict(orient="records")

        return results

    def Cultures(self, mapped_configurations:list):

        # convert to data frame
        temp_df = pd.DataFrame()
        temp_df = temp_df.from_dict(mapped_configurations)
        
        # 1) ordinals scaling
        temp_df[self.ordinals] = self.scaler.transform(temp_df[self.ordinals])

        # 2) nominals encoding
        nominals_output_categories = []

        for input_category in self.encoder.categories_:
            input_category = input_category.tolist()
            
            for x in input_category:
                nominals_output_categories.append(x)

        transformed = self.encoder.transform(temp_df[self.nominals])
        temp_df[nominals_output_categories] =  transformed.toarray()
        temp_df = temp_df.drop(self.nominals, axis=1)
        
        ready_df = pd.DataFrame()
        ready_df["id"] = temp_df["id"]

        for col in self.ordinals:
            ready_df[col] = temp_df[col]
        for col in self.encoded_columns:

            if col in temp_df.columns: ready_df[col] = temp_df[col]
            elif not col in temp_df.columns: ready_df[col] = 0

        temp_df = ready_df[["id"] + self.ordinals + self.encoded_columns]
        X_predict = temp_df[self.ordinals + self.encoded_columns].copy()

        # predict
        temp_df["financial_horizon_index"] = np.round(self.financial_horizon_index.predict(X_predict), 2)
        temp_df["financial_litteracy_index"] = np.round(self.financial_litteracy_index.predict(X_predict), 2)
        temp_df["financial_experience_index"] = np.round(self.financial_experience_index.predict(X_predict), 2)
        temp_df["objective_risk_index"] = np.round(self.objective_risk_propensity_index.predict(X_predict), 2)
        temp_df["subjective_risk_index"] = np.round(self.subjective_risk_propensity_index.predict(X_predict), 2)
        temp_df["esg_propensity_index"] = np.round(self.esg_propensity_index.predict(X_predict), 2)
        temp_df["life_quality_index"] = np.round(self.life_quality_index.predict(X_predict), 2)

        # drop details
        temp_df.drop(columns=self.ordinals + self.encoded_columns, axis=1, inplace=True)

        # convert to dict
        results = temp_df.to_dict(orient="records")

        return results

    def Status(self, mapped_configurations:list):

        # convert to data frame
        temp_df = pd.DataFrame()
        temp_df = temp_df.from_dict(mapped_configurations)
        
        # 1) ordinals scaling
        temp_df[self.ordinals] = self.scaler.transform(temp_df[self.ordinals])

        # 2) nominals encoding
        nominals_output_categories = []

        for input_category in self.encoder.categories_:
            input_category = input_category.tolist()
            
            for x in input_category:
                nominals_output_categories.append(x)

        transformed = self.encoder.transform(temp_df[self.nominals])
        temp_df[nominals_output_categories] =  transformed.toarray()
        temp_df = temp_df.drop(self.nominals, axis=1)
        
        ready_df = pd.DataFrame()
        ready_df["id"] = temp_df["id"]

        for col in self.ordinals:
            ready_df[col] = temp_df[col]
        for col in self.encoded_columns:

            if col in temp_df.columns: ready_df[col] = temp_df[col]
            elif not col in temp_df.columns: ready_df[col] = 0

        temp_df = ready_df[["id"] + self.ordinals + self.encoded_columns]
        X_predict = temp_df[self.ordinals + self.encoded_columns].copy()

        # predict
        temp_df["real_assets_index"] = np.round(self.real_assets_index.predict(X_predict), 2)
        temp_df["financial_assets_index"] = np.round(self.financial_assets_index.predict(X_predict), 2)
        temp_df["net_liabilities_index"] = np.round(self.net_liabilities_index.predict(X_predict), 2)
        temp_df["net_wealth_index"] = np.round(self.net_wealth_index.predict(X_predict), 2)
        temp_df["net_income_index"] = np.round(self.net_income_index.predict(X_predict), 2)
        temp_df["net_savings_index"] = np.round(self.net_savings_index.predict(X_predict), 2)
        temp_df["net_expences_index"] = np.round(self.net_expences_index.predict(X_predict), 2)

        # drop details
        temp_df.drop(columns=self.ordinals + self.encoded_columns, axis=1, inplace=True)

        # convert to dict
        results = temp_df.to_dict(orient="records")

        return results

    def Attitudes(self, mapped_configurations:list):

        # convert to data frame
        temp_df = pd.DataFrame()
        temp_df = temp_df.from_dict(mapped_configurations)
        
        # 1) ordinals scaling
        temp_df[self.ordinals] = self.scaler.transform(temp_df[self.ordinals])

        # 2) nominals encoding
        nominals_output_categories = []

        for input_category in self.encoder.categories_:
            input_category = input_category.tolist()
            
            for x in input_category:
                nominals_output_categories.append(x)

        transformed = self.encoder.transform(temp_df[self.nominals])
        temp_df[nominals_output_categories] =  transformed.toarray()
        temp_df = temp_df.drop(self.nominals, axis=1)
        
        ready_df = pd.DataFrame()
        ready_df["id"] = temp_df["id"]

        for col in self.ordinals:
            ready_df[col] = temp_df[col]
        for col in self.encoded_columns:

            if col in temp_df.columns: ready_df[col] = temp_df[col]
            elif not col in temp_df.columns: ready_df[col] = 0

        temp_df = ready_df[["id"] + self.ordinals + self.encoded_columns]
        X_predict = temp_df[self.ordinals + self.encoded_columns].copy()

        # predict
        temp_df["bank_activity_index"] = np.round(self.bank_activity_index.predict(X_predict), 2)
        temp_df["charity_activity_index"] = np.round(self.charity_activity_index.predict(X_predict), 2)
        temp_df["cultural_activity_index"] = np.round(self.cultural_activity_index.predict(X_predict), 2)
        temp_df["digital_activity_index"] = np.round(self.digital_activity_index.predict(X_predict), 2)

        # drop details
        temp_df.drop(columns=self.ordinals + self.encoded_columns, axis=1, inplace=True)

        # convert to dict
        results = temp_df.to_dict(orient="records")

        return results

    def run(self, body:list):

        if self.purpose == "Needs": return self.Needs(body)
        elif self.purpose == "Status": return self.Status(body)
        elif self.purpose == "Cultures": return self.Cultures(body)
        elif self.purpose == "Attitudes": return self.Attitudes(body)
        