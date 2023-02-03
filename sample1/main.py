from import_bdi_families_debts import Debts
from import_bdi_families_others import Others
from import_bdi_families_savings import Savings
from import_bdi_families_wealth import Wealths
from import_bdi_individuals_income import Iincome
from sample2.insert_samples import Insert
import pandas as pd
import sqlite3

class Main:

    def __init__(self) -> None:

        self.obj_debts = Debts()
        self.obj_others = Others()
        self.obj_savings = Savings()
        self.obj_wealths = Wealths()
        self.obj_ind = Iincome()


    def ingest(self):

        df_incomes = self.obj_ind.run()
        df_others = self.obj_others.run()

        families = {}

        for idx, row in df_incomes.iterrows():

            family_id = df_incomes.loc[idx, "nquest"]
            
            if family_id in families.keys():
                families[family_id] += 1
            else:
                families[family_id] = 1
        
        df_wealth = self.obj_wealths.run()
        df_wealth.columns
        df_wealth["real_assets"] = 0
        df_wealth["financial_assets"] = 0

        for column in df_wealth.columns:

            if column.startswith("real"):
                df_wealth["real_assets"] += df_wealth[column]
            
            elif column.startswith("financial"):
                df_wealth["financial_assets"] += df_wealth[column]

        df_wealth = df_wealth[["nquest", "real_assets", "financial_assets"]]

        df_liabilities = self.obj_debts.run()

        df_liabilities["liabilities"] = 0
        
        for column in df_liabilities.columns:

            if column.startswith("debt"):
                df_liabilities["liabilities"] += df_liabilities[column].fillna(0)

        df_liabilities = df_liabilities[["nquest", "liabilities"]]

        df_flows = self.obj_savings.run()

        # merge all family attributes in one df
        df_family = pd.merge(
            left=df_flows,
            right=df_liabilities,
            on="nquest",
            how="left"
        ).merge(
            df_wealth,
            on="nquest",
            how="left"
        ).merge(
            df_others,
            on="nquest",
            how="left"
        )
        df_family["family_members"] = df_family["nquest"].map(families)

        # divide attributes to family count
        for column in df_family.columns:

            if not column in ["nquest", "family_members"]:

                df_family[column] = df_family[column] / df_family["family_members"]

        # build the final df
        df_final = pd.merge(
            df_incomes,
            df_family,
            on="nquest"
        )

        # find the indexes

        min = df_final["net_income"].min()
        max = df_final["net_income"].max()
        df_final["net_income_index"] = df_final["net_income"].apply(lambda x: (x-min)/max)

        min = df_final["spending_net"].min()
        max = df_final["spending_net"].max()
        df_final["net_expences_index"] = df_final["spending_net"].apply(lambda x: (x-min)/max)


        min = df_final["saving_net"].min()
        max = df_final["saving_net"].max()
        df_final["net_savings_index"] = df_final["saving_net"].apply(lambda x: (x-min)/max)


        min = df_final["liabilities"].min()
        max = df_final["liabilities"].max()
        df_final["net_liabilities_index"] = df_final["liabilities"].apply(lambda x: (x-min)/max)


        min = df_final["real_assets"].min()
        max = df_final["real_assets"].max()
        df_final["real_assets_index"] = df_final["real_assets"].apply(lambda x: (x-min)/max)


        min = df_final["financial_assets"].min()
        max = df_final["financial_assets"].max()
        df_final["financial_assets_index"] = df_final["financial_assets"].apply(lambda x: (x-min)/max)


        df_final["net_wealth"] = df_final["real_assets"] + df_final["financial_assets"] + df_final["liabilities"]
        min = df_final["net_wealth"].min()
        max = df_final["net_wealth"].max()
        df_final["net_wealth_index"] = df_final["net_wealth"].apply(lambda x: (x-min)/max)

        # get the maps: genders

        genders_map = {
            1 : "m",
            2 : "f",
            3 : "o"
        }

        # get the maps: locations

        locations_map = {
            1: 'veneto', 
            2: 'lombardia', 
            3: 'toscana', 
            4: 'sardegna', 
            5: 'basilicata', 
            6: 'abruzzo', 
            7: 'sicilia', 
            8: 'puglia', 
            9: 'piemonte', 
            10: 'lazio', 
            11: 'trentino alto adige', 
            12: 'campania', 
            13: 'calabria', 
            14: 'marche', 
            15: 'umbria', 
            16: 'molise', 
            17: 'emilia romagna', 
            18: "valle daosta", 
            19: 'friuli venezia giulia', 
            20: 'liguria'
        }

        # get the maps: educations

        educations_map = {
            1 : "scuola primaria",
            2 : "scuola primaria",
            3 : "scuola secondaria di I grado",
            4 : "scuola secondaria di II grado",
            5 : "scuola secondaria di II grado",
            6 : "istruzione superiore università",
            7 : "master di II livello e PHD",
            8 : "master di II livello e PHD"
        }

        # get the maps: professions
        professions_map = {
            1 : "lavoratore dipendente",
            2 : "lavoratore dipendente",
            3 : "lavoratore dipendente",
            4 : "lavoratore indipendente",
            5 : "lavoratore indipendente",
            6 : "pensionato",
            7 : "non occupato"
        }

        # map the domain
        df_final["age"] = df_final["age_id"]
        df_final["gender"] = df_final["gender_id"].map(genders_map)
        df_final["location"] = df_final["region_id"].map(locations_map)
        df_final["education"] = df_final["education_id"].map(educations_map)
        df_final["profession"] = df_final["profession_id"].map(professions_map)

        # select the desired ones

        sociodemographics = [
            "age", "gender", "location", "education", "profession"
        ]

        status = [
            "net_income_index", "net_expences_index", "net_savings_index",
            "net_wealth_index", "real_assets_index", "financial_assets_index",
            "net_liabilities_index"
        ]
        cultures = [
            'financial_litteracy_index', 'subjective_risk_propensity_index'
        ]
        attitudes = ['digital_activity_index', 'bank_activity_index']
        needs = [
        'mortgage_financing_need', 'payment_financing_need',
            'loan_financing_need'
        ]
        df_final = df_final[sociodemographics + status + cultures + attitudes + needs]

        return df_final

    def run(self):

        df_final = self.ingest()
        # prepare for inserting to enrichment db

        body = {
            "sample_source" : "Italian central bank survey",
            "sample_date" : "01/01/2020",
            "sample" : []
        }
        for idx, row in df_final.iterrows():
            
            person = {
                "status" : {
                    "net_income_index" : round(df_final.loc[idx, "net_income_index"], 2),
                    "net_expences_index" : round(df_final.loc[idx, "net_expences_index"], 2),
                    "net_savings_index" : round(df_final.loc[idx, "net_savings_index"], 2),
                    "net_wealth_index" : round(df_final.loc[idx, "net_wealth_index"], 2),
                    "real_assets_index" : round(df_final.loc[idx, "real_assets_index"], 2),
                    "financial_assets_index" : round(df_final.loc[idx, "financial_assets_index"], 2),
                    "net_liabilities_index" : round(df_final.loc[idx, "net_liabilities_index"], 2)
                },
                "cultures" : {
                    "financial_litteracy_index" : round(df_final.loc[idx, "financial_litteracy_index"], 2),
                    "subjective_risk_propensity_index" : round(df_final.loc[idx, "subjective_risk_propensity_index"], 2)
                },
                "attitudes" : {
                    "bank_activity_index" : round(df_final.loc[idx, "bank_activity_index"], 2),
                    "digital_activity_index" : round(df_final.loc[idx, "digital_activity_index"], 2)
                },
                "needs" : {
                    
                    "payment_financing_need" : round(df_final.loc[idx, "payment_financing_need"], 2),
                    "loan_financing_need" : round(df_final.loc[idx, "loan_financing_need"], 2),
                    "mortgage_financing_need" : round(df_final.loc[idx, "mortgage_financing_need"], 2)
                    
                    },
                "sociodemographics" : {
                    "age" : df_final.loc[idx, "age"],
                    "gender" : df_final.loc[idx, "gender"],
                    "location" : df_final.loc[idx, "location"],
                    "education" : df_final.loc[idx, "education"],
                    "profession" : df_final.loc[idx, "profession"]
                }
            }

            body["sample"].append(person)

        body = [body]

        # call the insert sample api

        insert = Insert()

        insert.run(body=body)

        return True
        