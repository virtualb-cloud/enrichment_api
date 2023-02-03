import sqlite3
import pandas as pd
from sample2.insert_samples import Insert


class Nyrtia:

    def __init__(self) -> None:
        
        conn = sqlite3.connect("Fideuram.sqlite3")

        query = '''
        SELECT ue.age_id, ue.gender_id, ue.region_id, ue.profession_vb_id, ue.esg_propensity_index
        FROM users_esg AS ue
        '''
        self.df = pd.read_sql(sql=query, con=conn)

    def run(self):

        df = self.df.copy()

        mappa = {

            1: "m",
            2: "f"
        }

        df["gender"] = df["gender_id"].map(mappa)

        mappa = {

            1: "lavoratore dipendente",
            2: "lavoratore indipendente",
            3: "pensionato",
            4: "non occupato"
        }

        df["profession"] = df["profession_vb_id"].map(mappa)

        mapn = {
            1: 'piemonte',
            2: 'valle daosta',
            3: 'lombardia',
            4: 'trentino alto adige',
            5: 'veneto',
            6: 'friuli venezia giulia',
            7: 'liguria',
            8: 'emilia romagna',
            9: 'toscana',
            10: 'umbria',
            11: 'marche',
            12: 'lazio',
            13: 'abruzzo',
            14: 'molise',
            15: 'campania',
            16: 'puglia',
            17: 'basilicata',
            18: 'calabria',
            19: 'sicilia',
            20: 'sardegna'
        }
        df["location"] = df["region_id"].map(mapn)
        df.dropna(subset=["age_id", "gender", "location", "profession"], inplace=True)

        body = [{
            "sample_source" : "Fideuram esg exploration",
            "sample_date" : "01/01/2022",
            "sample" : []
        }]

        educations = [
            "scuola primaria", "scuola secondaria di I grado", "scuola secondaria di II grado", 
            "istruzione superiore università", "master di II livello e PHD"
        ]

        for idx, row in df.iterrows():

            for education in educations:
                person = {
                    "cultures" : {
                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"]/10, 2)
                    },
                    "sociodemographics" : {
                        "age" : df.loc[idx, "age_id"],
                        "gender" : df.loc[idx, "gender"],
                        "location" : df.loc[idx, "location"],
                        "education" : education,
                        "profession" : df.loc[idx, "profession"]
                    }
                }
            body[0]["sample"].append(person)    
        obj = Insert()

        obj.run(body=body)

        return True