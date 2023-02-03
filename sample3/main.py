import sqlite3
import pandas as pd
from sample2.insert_samples import Insert


class Nyrtia:

    def __init__(self) -> None:
        
        conn = sqlite3.connect("Nyrtia.sqlite3")

        query = '''
        SELECT cd.age_custom_id, cd.gender_id, cd.location_macro_id, cd.profession_vb_id, ce.esg_propensity_index
        FROM clusters_esg AS ce

            JOIN clusters_definition AS cd
            ON ce.cluster_id = cd.cluster_id
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

        body = [{
            "sample_source" : "Nyrtia esg exploration",
            "sample_date" : "01/01/2022",
            "sample" : []
        }]

        educations = [
            "scuola primaria", "scuola secondaria di I grado", "scuola secondaria di II grado", 
            "istruzione superiore università", "master di II livello e PHD"
        ]

        for idx, row in df.iterrows():

            x = df.loc[idx, "age_custom_id"]
            y = df.loc[idx, "location_macro_id"]

            if x == 1: continue

            elif x == 2:
                for age in [18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34]:
                    if y == 1:
                        for location in ["piemonte", "lombardia", "valle daosta", "liguria"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    elif y == 2:
                        for location in ["veneto", "trentino alto adige", "friuli venezia giulia"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    elif y == 3:
                        for location in ["toscana", "abruzzo", "lazio", "marche", "emilia romagna", "molise"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    
                    elif y == 4:
                        for location in ["basilicata", "puglia", "campania", "calabria"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)

                    elif y == 5:
                        for location in ["sardegna", "sicilia"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    
            elif x == 3:
                for age in [35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64]:
                    if y == 1:
                        for location in ["piemonte", "lombardia", "valle daosta", "liguria"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    elif y == 2:
                        for location in ["veneto", "trentino alto adige", "friuli venezia giulia"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    elif y == 3:
                        for location in ["toscana", "abruzzo", "lazio", "marche", "emilia romagna", "molise"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    
                    elif y == 4:
                        for location in ["basilicata", "puglia", "campania", "calabria"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)

                    elif y == 5:
                        for location in ["sardegna", "sicilia"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)

            elif x == 4:
                for age in [65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100]:
                    if y == 1:
                        for location in ["piemonte", "lombardia", "valle daosta", "liguria"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    elif y == 2:
                        for location in ["veneto", "trentino alto adige", "friuli venezia giulia"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    elif y == 3:
                        for location in ["toscana", "abruzzo", "lazio", "marche", "emilia romagna", "molise"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
                    
                    elif y == 4:
                        for location in ["basilicata", "puglia", "campania", "calabria"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)

                    elif y == 5:
                        for location in ["sardegna", "sicilia"]:

                            for education in educations:
                                person = {
                                    "cultures" : {
                                        "esg_propensity_index" : round(df.loc[idx, "esg_propensity_index"], 2)
                                    },
                                    "sociodemographics" : {
                                        "age" : age,
                                        "gender" : df.loc[idx, "gender"],
                                        "location" : location,
                                        "education" : education,
                                        "profession" : df.loc[idx, "profession"]
                                    }
                                }
                                body[0]["sample"].append(person)
              
        obj = Insert()

        obj.run(body=body)

        return True