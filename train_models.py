from sqlalchemy import create_engine
from ml_utils import Utils
import pandas as pd
import pickle

class Train:

    def __init__(self) -> None:
        
        # ml utils object
        self.utils = Utils()

        # db connection
        self.schema_name = "data_enrichment_v3"
        self.engine = create_engine("postgresql://postgres:!vbPostgres@virtualb-rds-data-enrichment.clg6weaheijj.eu-south-1.rds.amazonaws.com:5432/postgres")

    def read_tables(self):

        # read status
        query = f'''
        SELECT sd.age_, sd.gender_, sd.location_, sd.education_, sd.profession_,
        cr.financial_horizon_index, cr.finacial_littercay_index, 
        cr.financial_experience_index, cr.objective_risk_propensity_index, 
        cr.subjective_risk_propensity_index, cr.esg_propensity_index,
        cr.life_quality_index,
        st.net_income_index, st.net_expences_index, st.net_savings_index,
        st.real_assets_index, st.financial_assets_index, st.net_liabilities_index,
        st.net_wealth_index,
        at.bank_activity_index, at.digital_activity_index, 
        at.cultural_activity_index, at.charity_activity_index,
        nd.capital_accumulation_investment_need, nd.capital_protection_investment_need, 
        nd.liquidity_investment_need, nd.income_investment_need,
        nd.retirement_investment_need, nd.heritage_investment_need,
        nd.health_incurance_need, nd.home_insurance_need,
        nd.longterm_care_insurance_need, nd.payment_financing_need,
        nd.loan_financing_need, nd.mortgage_financing_need
        
        FROM {self.schema_name}.sociodemographics as sd

            JOIN {self.schema_name}.status as st
            ON st.person_id = sd.person_id

            JOIN {self.schema_name}.cultures as cr
            ON cr.person_id = sd.person_id

            JOIN {self.schema_name}.attitudes as at
            ON at.person_id = sd.person_id

            JOIN {self.schema_name}.needs as nd
            ON nd.person_id = sd.person_id
        '''
        table = pd.read_sql(sql=query, con=self.engine)

        table.rename(columns={
            "age_" : "age",
            "gender_" : "gender",
            "location_" : "location",
            "education_" : "education",
            "profession_" : "profession"
        }, inplace=True)

        return table
        
    def models(self):

        domain = ["age", "gender", "location", "education", "profession"]
        cultures = [
            "financial_horizon_index", "finacial_littercay_index", 
            "financial_experience_index", "objective_risk_propensity_index", 
            "subjective_risk_propensity_index", "esg_propensity_index",
            "life_quality_index"]
        status = [
            "net_income_index", "net_expences_index", "net_savings_index",
            "real_assets_index", "financial_assets_index", "net_liabilities_index",
            "net_wealth_index"]
        attitudes = [
            "bank_activity_index", "digital_activity_index", 
            "cultural_activity_index", "charity_activity_index"]
        needs = [
            "capital_accumulation_investment_need", "capital_protection_investment_need", 
            "liquidity_investment_need", "income_investment_need",
            "retirement_investment_need", "heritage_investment_need",
            "health_incurance_need", "home_insurance_need",
            "longterm_care_insurance_need", "payment_financing_need",
            "loan_financing_need", "mortgage_financing_need"]

        # all X
        ordinals = ['age']
        nominals = ['gender', 'location', 'education', 'profession']
        indexes = cultures + status + attitudes + needs

        table = self.read_tables()

        for column in indexes:
            
            temp_df = table.dropna(subset=column)

            # 2) ordinals scaling
            temp_df = self.utils.ordinals_scaler(temp_df, ordinals)

            # 3) nominals encoding
            temp_df, nominals_out = self.utils.nominals_encoder(temp_df, nominals)
            
            print(temp_df.columns)

            # 4) train test validate
            data = self.utils.train_validate_test_splitter(temp_df, target=column)

            X_train = data[0]
            y_train = data[3]

            X_val = data[1]
            y_val = data[4]

            X_test = data[2]
            y_test = data[5]

            train = self.utils.base_forest_regressor(X_train, y_train)

            model = train[0]
            params = train[1]

            # train_metrics = self.utils.base_forest_regressor(model, X_train, y_train)
            # validation_metrics = self.utils.base_classification_metrics(model, X_val, y_val)
            # test_metrics = self.utils.base_classification_metrics(model, X_test, y_test)
            

            

            # with open(f"models/{column}.pkl", 'wb') as path:
            #     pickle.dump(model, path)


            return params

    def sample_rating(self):
        pass

    def run(self, body:list):




        # 6) model fitting
        needs_best_params = {}
        # needs_based_models = {}
        # for need in financial_needs:

            
        
        # # 8) model evaluation
        # metrics = {}
        # for need in financial_needs:
        #     model = needs_based_models[need]
            
            

        #     metrics[need] = [train_metrics, validation_metrics, test_metrics]
    
        # # 9) model details saving
        # model_details_df = pd.DataFrame()
        # idx = 0
        # for need in financial_needs:
            
        #     train_metrics = metrics[need][0]
        #     evaluation_metrics = metrics[need][1]
        #     test_metrics = metrics[need][2]

        #     model_details_df.loc[idx, "target"] = need
        #     model_details_df.loc[idx, "model_name"] = "DenseNeuralNetwork"
        #     model_details_df.loc[idx, "best_params"] = str(needs_best_params[need])
        #     model_details_df.loc[idx, "best_params_validator"] = "GridSearchCV"
        #     model_details_df.loc[idx, "train_recall"] = train_metrics["recall"]
        #     model_details_df.loc[idx, "train_precision"] = train_metrics["precision"]
        #     model_details_df.loc[idx, "train_f1_score"] = train_metrics["f1_score"]
        #     model_details_df.loc[idx, "train_accuracy"] = train_metrics["accuracy"]
        #     model_details_df.loc[idx, "validation_recall"] = evaluation_metrics["recall"]
        #     model_details_df.loc[idx, "validation_precision"] = evaluation_metrics["precision"]
        #     model_details_df.loc[idx, "validation_f1_score"] = evaluation_metrics["f1_score"]
        #     model_details_df.loc[idx, "validation_accuracy"] = evaluation_metrics["accuracy"]
        #     model_details_df.loc[idx, "test_recall"] = test_metrics["recall"]
        #     model_details_df.loc[idx, "test_precision"] = test_metrics["precision"]
        #     model_details_df.loc[idx, "test_f1_score"] = test_metrics["f1_score"]
        #     model_details_df.loc[idx, "test_accuracy"] = test_metrics["accuracy"]

        #     idx += 1

        return model_details_df

