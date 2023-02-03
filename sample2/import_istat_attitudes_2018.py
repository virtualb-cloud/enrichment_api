from remotezip import RemoteZip
from dataclasses import dataclass
from typing import Any, Callable, AnyStr, List
import pandas as pd
import numpy as np


@dataclass
class Fields:
    col: Any
    cleaner: Callable = None
    func: Callable = None
    work_on_dataset: bool = False
    merged: bool = False

class Istat:

    def __init__(self) -> None:
        
        self.file_path = 'AVQ_Microdati_2018.txt'
        

    def get_sex(self, df):
        mappa = {
            1: "m",
            2: "f"
        }
        return df["SESSO"].map(mappa)

    def get_location(self, df):
        
        mappa = {
            10 : "piemonte",
            20 : "valle daosta",
            30 : "lombardia",
            40 : "trentino alto adige",
            50 : "veneto",
            60 : "friuli venezia giulia",
            70 : "liguria",
            80 : "emilia romagna",
            90 : "toscana",
            100 : "umbria",
            110 : "marche",
            120 : "lazio",
            130 : "abruzzo",
            140 : "molise",
            150 : "campania",
            160 : "puglia",
            170 : "basilicata",
            180 : "calabria",
            190 : "sicilia",
            200 : "sardegna" 
        } 
        
        return df["REGMF"].map(mappa)

    def get_age(self, df):
        
        df_out = pd.DataFrame()
        df_out["ages"] = 0
        
        for idx, row in df.iterrows():
            
            if df.loc[idx, "ETAMi"] == 1: df_out.loc[idx, "ages"] = '[1, 2]'

            elif df.loc[idx, "ETAMi"] == 2: df_out.loc[idx, "ages"] = '[3, 4, 5]'

            elif df.loc[idx, "ETAMi"] == 3: df_out.loc[idx, "ages"] = '[6, 7, 8, 9, 10]'

            elif df.loc[idx, "ETAMi"] == 4: df_out.loc[idx, "ages"] = '[11, 12, 13]'

            elif df.loc[idx, "ETAMi"] == 5: df_out.loc[idx, "ages"] = '[14, 15]'

            elif df.loc[idx, "ETAMi"] == 6: df_out.loc[idx, "ages"] = '[16, 17]'
            
            elif df.loc[idx, "ETAMi"] == 7: df_out.loc[idx, "ages"] = '[18, 19]'

            elif df.loc[idx, "ETAMi"] == 8: df_out.loc[idx, "ages"] = '[20, 21, 22, 23, 24]'

            elif df.loc[idx, "ETAMi"] == 9: df_out.loc[idx, "ages"] = '[25, 26, 27, 28, 29, 30, 31, 32, 33, 34]'

            elif df.loc[idx, "ETAMi"] == 10: df_out.loc[idx, "ages"] = '[35, 36, 37, 38, 39, 40, 41, 42, 43, 44]'

            elif df.loc[idx, "ETAMi"] == 11: df_out.loc[idx, "ages"] = '[45, 46, 47, 48, 49, 50, 51, 52, 53, 54]'

            elif df.loc[idx, "ETAMi"] == 12: df_out.loc[idx, "ages"] = '[55, 56, 57, 58, 59]'

            elif df.loc[idx, "ETAMi"] == 13: df_out.loc[idx, "ages"] = '[60, 61, 62, 63, 64]'

            elif df.loc[idx, "ETAMi"] == 14: df_out.loc[idx, "ages"] = '[65, 66, 67, 68, 69, 70, 71, 72, 73, 74]'

            elif df.loc[idx, "ETAMi"] == 15: df_out.loc[idx, "ages"] = '[75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100]'

        return df_out["ages"]

    def get_work_position(self, df):

        df_out = pd.DataFrame()
        df_out["profession"] = ""

        for idx, row in df.iterrows():

            x = df.loc[idx, "CONDMi"]
            y = df.loc[idx, "POSIZMi"]

            if x == 1:
                if y in ['01', '02']: df_out.loc[idx, "profession"] =  "lavoratore dipendente"
                elif y in ['03', '04']: df_out.loc[idx, "profession"] =  "lavoratore indipendente"
        
            elif x == 2: df_out.loc[idx, "profession"] =  "non occupato"
            elif x == 3: df_out.loc[idx, "profession"] =  "non occupato"
            elif x == 4: df_out.loc[idx, "profession"] =  "pensionato"
            
        return df_out["profession"]

    def get_education(self, df):
        
        df_out = pd.DataFrame()
        df_out["education"] = ""

        for idx, row in df.iterrows():

            x = df.loc[idx, "ISTRMi"]

            if x == '01': df_out.loc[idx, "education"] = "istruzione superiore università"
            elif x == '07': df_out.loc[idx, "education"] = "scuola secondaria di II grado"
            elif x == '09': df_out.loc[idx, "education"] = "scuola secondaria di II grado"
            elif x == '10': df_out.loc[idx, "education"] = "scuola secondaria di I grado"
            elif x == '99': df_out.loc[idx, "education"] = "scuola primaria"

        return df_out["education"]



    def get_cultural_event_activity(self, df):
        df = df.fillna(0).astype('int64')
        return self.min_max_scaler(df.sum(axis=1))

    def get_charity_activity_index(self, df):
        min_max = {1: 0, 2: 1, 3: 0, 4: 1, 5: 0, 6: 1, 7: 0, 8: 1}
        df = self.dfmap(df, min_max, 0)
        return self.min_max_scaler(df.sum(axis=1))

    def get_internet_channel_for_consumption(self, df):
        min_max = {1: 0, 2: 1, 3: 0, 4: 1, 5: 0, 6: 1, 7: 0, 8: 1}
        df_freqin = (6 - df['FREQIN12'].fillna(0).astype('int64')) * 2
        df = df.drop(columns=['FREQIN12', ])
        df = self.dfmap(df, min_max, 0)
        return self.min_max_scaler(df.sum(axis=1) + df_freqin)

    def get_solitude_index(self, df):
        df = df.fillna(0).astype('int64')
        df.loc[:, 'PARENT'] = self.dfmap(df['PARENT'], {1:3, 2:0})
        df.loc[:, 'AMICI2'] = self.dfmap(df['AMICI2'], {1:3, 2:0, 3:3})
        df.loc[:, 'VICINI'] = self.dfmap(df['VICINI'], {1:3, 2:1, 3:0})
        return self.min_max_scaler(df.sum(axis=1))

    def get_alcohol_consumption_index(self, df):
        df = 6 - df.fillna(0).astype('int64')
        df.loc[:, 'BFPAS'] = (4 - df['BFPAS']) * 2
        return self.min_max_scaler(df.sum(axis=1))

    def get_media_index(self, df):
        df.loc[:, 'HHTEL'] = self.dfmap(df['HHTEL'], {99: df['HHTEL'].median()}, 0)
        df.loc[:, ['RADIO', 'TELE']] = self.dfmap(df[['RADIO', 'TELE']], {1:0, 3:1}, 0)
        df.loc[:, ['PCTEMPO', 'INTTEMPO']] = self.dfmap(df[['PCTEMPO', 'INTTEMPO']], {4:0, 1:3, 3:1}, 0)
        return self.min_max_scaler(df.sum(axis=1))

    def get_smoke_intensity_index(self, df):
        df = self.dfmap(df.fillna(0), {3:0, 2:1, 1:2}, 0)
        return self.min_max_scaler(df.sum(axis=1))

    def get_general_health_index(self, df):
        min_max = {1: 0, 2: 1, 3: 0, 4: 1, 5: 0, 6: 1, 7: 0, 8: 1}
        df = df.fillna(0).astype('int64')
        df_farm = self.dfmap(df['FARM'], {0:0, 1:1, 2:0, 3:1}, 0)
        df_limita = 4 - df['LIMITA']
        df = df.drop(columns=['FARM', 'LIMITA'])
        df = self.dfmap(df, min_max)
        return self.min_max_scaler(df.sum(axis=1) + df_farm + df_limita)

    def get_heart_diseases_index(self, df):
        df = self.dfmap(df, {3: 0, 4: 1, 5: 0, 6: 1, 7: 0, 8: 1}, 0)
        return self.min_max_scaler(df.sum(axis=1))

    def get_lung_diseases_index(self, df):
        df = self.dfmap(df, {1: 0, 2: 1, 3: 0, 4: 1}, 0)
        return self.min_max_scaler(df.sum(axis=1))

    def get_health_checkup_index(self, df):
        df = self.dfmap(df, {1: 0, 2: 1}, 0)
        return self.min_max_scaler(df.sum(axis=1))

    def get_alcohol_consumption_index(self, df):
        df = self.dfmap(df, {1: 0, 2: 1}, 0)
        return self.min_max_scaler(df.sum(axis=1))

    def get_social_degradation_residence_index(self, df):
        return self.min_max_scaler(df.sum(axis=1))

    

    def dfmap(self, df, d, default=0, dtype='int64'):
        # Funziona con una colonna per volta 
        f = np.vectorize(d.get, otypes=[object])
        return f(df.fillna(0).astype(dtype), default)

    def min_max_scaler(self, x, fillna=True):
        y = (x - x.min()) / (x.max() - x.min())
        if fillna:
            if isinstance(y, np.ndarray):
                return np.nan_to_num(y, y.mean())
            else:
                return y.fillna(y.mean())
        else:
            return y
    
    def clean_data(self, obj):
        bool_fields = [
            'home_robbery_family_protection',
            'domestic_worker_presence',
            'nanny_presence',
            'elderly_care_person_presence',
            'cancer_presence',
        ]

        integer_fields = []

        float_fields = [
            'solitude_index', 'alcohol_consumption_index', 'classic_media_consumption_index',
            'cultural_event_activity', 'charity_activity_index', 'social_degradation_residence_index', 
            'internet_channel_for_consumption', 'health_checkup_index', 'heart_diseases_index',
            'lung_diseases_index', 'general_health_index', 'smoke_intensity_index',
        ]

        for f in bool_fields:
            obj[f] = True if obj[f] == '1' else False

        for f in integer_fields:
            obj[f] = int(float(obj[f])) if obj[f] else 0

        for f in float_fields:
            obj[f] = float(obj[f]) if obj[f] else 0.0

        return obj

    def get_record(self, row):
        rec = dict()
        for k, v in self.fields.items():
            if v.func:
                rec[k] = v.func(*row[v.col]) if not v.merged else row[k]
            else:
                rec[k] = row[v.col]
        return self.clean_data(rec)

    def prepare_dataset(self, df):
        drop_fields = []
        for k, v in self.fields.items():
            if v.func and v.work_on_dataset == True:
                df.loc[:, k] = v.func(df.loc[:, v.col])
                setattr(self.fields[k], 'merged', True)
                drop_fields +=  v.col
        df = df.drop(columns=list(set(drop_fields)))
        return df
    
    def run(self):

        columns = [
            'ETAMi', 'SESSO', 'CONDMi', 'POSIZMi', 'REGMF', 'ISTRMi', 'VISMED12', 'ANSANG12', 'ACCER12', 
            'AMICI', 'PARENT', 'AMICI2', 'VICINI', 'BIRRA', 'VINO', 'BFPAS', 'ALCOL', 'AMAR', 
            'LIQUOR', 'ESIG', 'FUMO', 'FARM', 'LIMITA', 'DIAB', 'IPAR', 'INFAR', 'CUORE', 'BRON', 
            'ASMA', 'ALLER', 'ULCER', 'FEGATO', 'CIRRO', 'CALRE', 'ARTRO', 'OSTEO', 'NEURO', 
            'RADIO', 'TELE', 'HHTEL', 'PCTEMPO', 'INTTEMPO', 'FREQIN12', 'DISP_SMA', 'DISP_LAP', 
            'DISP_TAB', 'DISPPDA', 'INCOMU5', 'INCOMU1b', 'INCOMU34', 'INCOMU6', 'INCOMU7', 
            'INTATT20', 'INTATT21', 'INTATT26', 'INTATT5', 'INTATT4', 'INTSAL3', 'INTATT14', 
            'INTATT11', 'INTATT33', 'INTATT13', 'INTATT17', 'INTATT16', 'INTATT10', 'INTATT8', 
            'INTATT31', 'INTATT32', 'INTATT7BN', 'INTATT30A', 'INTATT30B', 'INTATT28A', 'INTATT28B', 
            'CLOUDSAL', 'SKL_FRE', 'SKL_AUTO', 'SKL_PUB', 'SKL_DAT', 'P2P_ACC', 'P2P_ACCALT', 
            'P2P_TRA', 'P2P_TRALT', 'P2P_FOOD', 'P2P_WRK', 'PR_UP', 'PR_SM', 'PR_ST', 'PR_CARD', 
            'PR_COD', 'PR_PIN', 'PR_ALT', 'TEATRO', 'MUSEO', 'MUSIC', 'MONUM', 'LQUOT', 'RIVSET', 
            'PGRVO', 'FINAS', 'VOLON', 'ATGRA', 'VOLPA', 'VOLSI', 'PAESAGGIO', 'SPORCO', 'PARCH', 
            'COLMP', 'TRAF', 'INQAR', 'RUMORE', 'CRIM', 'ODSGR', 'ILLSTR', 'CONPAV', 'ASSCA', 
            'COLFAGG', 'BABYSAGG', 'ASSANZAGG', 'TUMOR'
        ]

        self.fields = {
            'gender': Fields(
                col=['SESSO'],
                func=self.get_sex, merged=False, work_on_dataset=True),
            'ages': Fields(
                col=['ETAMi'],
                func=self.get_age, merged=False, work_on_dataset=True),
            'profession': Fields(
                col=['CONDMi', 'POSIZMi'],
                func=self.get_work_position, merged=False, work_on_dataset=True),
            'education': Fields(
                col=['ISTRMi'],
                func=self.get_education, merged=False, work_on_dataset=True),
            'location': Fields(
                col=['REGMF'],
                func=self.get_location, merged=False, work_on_dataset=True),

            'user_health_checkup': Fields(
                col=['VISMED12', 'ANSANG12', 'ACCER12'],
                func=self.get_health_checkup_index, merged=False, work_on_dataset=True),
            'solitude_index': Fields(
                col=['AMICI', 'PARENT', 'AMICI2', 'VICINI'],
                func=self.get_solitude_index, merged=False, work_on_dataset=True),
            'alcohol_consumption_index': Fields(
                col=['BIRRA', 'VINO', 'BFPAS', 'ALCOL', 'AMAR', 'LIQUOR'],
                func=self.get_alcohol_consumption_index, merged=False, work_on_dataset=True),
            'smoke_intensity_index': Fields(
                col=['ESIG', 'FUMO'],
                func=self.get_smoke_intensity_index, merged=False, work_on_dataset=True),
            'heart_diseases_index': Fields(
                col=['IPAR', 'INFAR', 'CUORE'],
                func=self.get_heart_diseases_index, merged=False, work_on_dataset=True),
            'lung_diseases_index': Fields(
                col=['BRON', 'ASMA'],
                func=self.get_lung_diseases_index, merged=False, work_on_dataset=True),
            'general_health_index': Fields(
                col=['FARM', 'LIMITA', 'DIAB', 'IPAR', 'INFAR', 'CUORE', 'BRON', 'ASMA', 
                'ALLER', 'ULCER', 'FEGATO', 'CIRRO', 'CALRE', 'ARTRO', 'OSTEO', 'NEURO'],
                func=self.get_general_health_index, merged=False, work_on_dataset=True),
            'classic_media_consumption_index': Fields(
                col=['RADIO', 'TELE', 'HHTEL', 'PCTEMPO', 'INTTEMPO'],
                func=self.get_media_index, merged=False, work_on_dataset=True),
            'digital_activity_index': Fields(
                col=['FREQIN12', 'DISP_SMA', 'DISP_LAP', 'DISP_TAB', 'DISPPDA', 'INCOMU5', 'INCOMU1b', 
                'INCOMU34', 'INCOMU6', 'INCOMU7', 'INTATT20', 'INTATT21', 'INTATT26', 'INTATT5',
                'INTATT4', 'INTSAL3', 'INTATT14', 'INTATT11', 'INTATT33', 'INTATT13', 'INTATT17',
                'INTATT16', 'INTATT10', 'INTATT8', 'INTATT31', 'INTATT32', 'INTATT7BN', 'INTATT30A',
                'INTATT30B', 'INTATT28A', 'INTATT28B', 'CLOUDSAL', 'SKL_FRE', 'SKL_AUTO', 'SKL_PUB',
                'SKL_DAT', 'P2P_ACC', 'P2P_ACCALT', 'P2P_TRA', 'P2P_TRALT', 'P2P_FOOD', 'P2P_WRK',
                'PR_UP', 'PR_SM', 'PR_ST', 'PR_CARD', 'PR_COD', 'PR_PIN', 'PR_ALT'],
                func=self.get_internet_channel_for_consumption, merged=False, work_on_dataset=True),
            'cultural_activity_index': Fields(
                col=['TEATRO', 'MUSEO', 'MUSIC', 'MONUM', 'LQUOT', 'RIVSET'],
                func=self.get_cultural_event_activity, merged=False, work_on_dataset=True),
            'charity_activity_index': Fields(
                col=['PGRVO', 'FINAS', 'VOLON', 'ATGRA', 'VOLPA', 'VOLSI'],
                func=self.get_charity_activity_index, merged=False, work_on_dataset=True),
            'social_degradation_residence_index': Fields(
                col=['PAESAGGIO', 'SPORCO', 'PARCH', 'COLMP', 'TRAF', 'INQAR', 'RUMORE',
                'CRIM', 'ODSGR', 'ILLSTR', 'CONPAV'],
                func=self.get_social_degradation_residence_index, merged=False, work_on_dataset=True),
            'home_robbery_family_protection': Fields(col='ASSCA', merged=False, work_on_dataset=True),
            'domestic_worker_presence': Fields(col='COLFAGG', merged=False, work_on_dataset=True),
            'nanny_presence': Fields(col='BABYSAGG', merged=False, work_on_dataset=True),
            'elderly_care_person_presence': Fields(col='ASSANZAGG', merged=False, work_on_dataset=True),
            'cancer_presence': Fields(col='TUMOR', merged=False, work_on_dataset=True),
            
        }

        df = pd.read_csv(
            self.file_path, 
            usecols=columns,
            sep='\t', decimal='.', 
            na_values=[' .', '.', ' '], 
            low_memory=False)
        
        # Sostituisco i campi vuoti con NaN
        df.replace(r'^\s+$', np.nan, regex=True, inplace=True)

        # Preparazione del dataset
        df = self.prepare_dataset(df)

        
        # drop na and prepare for enrichment insert api
        domain = ["ages", "location", "gender", "profession", "education"]
        df.dropna(subset=domain, axis=0, inplace=True)

        sample = []
        for idx, row in df.iterrows():
            
            for age in df.loc[idx, "ages"].lstrip("[").rstrip("]").split(","):
                
                output = {
                    "sociodemographics" : {
                        "age" : int(age),
                        "gender" : df.loc[idx, "gender"],
                        "location" : df.loc[idx, "location"],
                        "profession" : df.loc[idx, "profession"],
                        "education" : df.loc[idx, "education"]
                        
                    },
                    "attitudes" : {
                        "digital_activity_index" : round(df.loc[idx, "digital_activity_index"], 2),
                        "cultural_activity_index" : round(df.loc[idx, "cultural_activity_index"], 2),
                        "charity_activity_index" : round(df.loc[idx, "charity_activity_index"], 2)
                    }
                }

                sample.append(output)


        return sample
        
