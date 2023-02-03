from remotezip import RemoteZip
from dataclasses import dataclass
from typing import Any, Callable, AnyStr, List
import pandas as pd
import numpy as np
from functools import partial

@dataclass
class FileStruct:
    keys: List
    fields: List
    drop: bool = False

@dataclass
class Fields:
    cols: Any
    dtype: AnyStr
    func: Callable = None
    merged: bool = False

    def get_value(self, df):
        dtype, func = self.dtype, self.func
        df = df.astype(dtype)
        return func(df).astype(dtype) if func else df

@dataclass
class FieldsInt(Fields):
    dtype: AnyStr = 'int'
    def get_value(self, df):
        df = df.fillna(0)
        return super().get_value(df)

@dataclass
class FieldsFloat(Fields):
    dtype: AnyStr = 'float'

@dataclass
class FieldsBool(Fields):
    dtype: AnyStr = 'bool'

# [Main Class]
class Others:

    def __init__(self) -> None:
        
        self.file_path = 'https://www.bancaditalia.it/statistiche/tematiche/' + \
            'indagini-famiglie-imprese/bilanci-famiglie/distribuzione-microdati/' + \
            'documenti/ind20_ascii.zip'


        self.families_files = {

            'CSV/q20c1.csv': FileStruct(
            keys = ['nquest'],
            fields = [ 'banche', 'ndeposit', 'utscoper', 'carte', 'usocart', 'nbancoma', 'ncartapre',
                'spesecon', 'coldis', 'trading']),

            'CSV/q20c2.csv': FileStruct(
            keys = ['nquest'],
            fields = ['risfin', 
                 'qint', 'qrisk1', 'qtasso']),

            'CSV/q20d.csv': FileStruct(
            keys = ['nquest'],
            fields = ['debitc', 'debitd', 'debite', 'debitf', 'debitg', 'mutuor', 
                'mutuor3', 'mutuoric', 'mutuoric3']),

            'CSV/q20f.csv': FileStruct(
            keys = ['nquest'],
            fields = ['ass4'])
            }

        FieldsIntAgg = partial(FieldsInt, func=self.sum_columns)
        FieldsFloatAgg = partial(FieldsFloat, func=self.sum_columns)
        FieldsBoolAgg = partial(FieldsBool, func=self.sum_columns)

        self.families_attributes = {
            'risk_aversion': FieldsInt('risfin'),

            'financial_litteracy': FieldsFloat(['qint', 'qrisk1', 'qtasso'], 
                func=self.financial_literacy),
            'mortgage_financing_need': FieldsFloatAgg(['mutuor', 'mutuor3', 'mutuoric', 'mutuoric3']),
            'digital_propension': FieldsIntAgg(['coldis', 'spesecon', 'trading']),
            'current_accounts_count': FieldsIntAgg(['banche', 'ndeposit']),#'nctit', 
            'payment_financing_need': FieldsIntAgg(['carte', 'nbancoma', 'ncartapre']),
            'payment_financing_need_index': FieldsFloat('payment_financing_need', func=self.min_max_scaler),
            'loan_financing_need': FieldsBoolAgg(
                ['debitc', 'debitd', 'debite', 'debitf', 'debitg']),

            'health_insurance_need': FieldsBool('ass4'),
        }

    def min_max_scaler(self, x, fillna=True):
        
        y = (x - x.min()) / (x.max() - x.min())
        if fillna:
            if isinstance(y, np.ndarray):
                return np.nan_to_num(y, y.mean())
            else:
                return y.fillna(y.mean())
        else:
            return y

    def sum_columns(self, df, columns=None):
        if columns:
            df = df.loc[:, columns]
        return df.sum(axis=1)

    def financial_literacy(self, df):
        df['know1'] = self.dfmap(df['qtasso'], {1: -1, 2: -1, 3: 1, 4: 0, 5: 0})
        df['know2'] = self.dfmap(df['qint'], {1: -1, 2: 1, 3: -1, 4: 0, 5: 0})
        df['know3'] = self.dfmap(df['qrisk1'], {1: -1, 2: 1, 4: 0, 5: 0})
        df = df[['know1', 'know2', 'know3']].dot([0.25, 0.25, 0.5])
        
        return self.min_max_scaler(df)

    def dfmap(self, df, d, default=0, dtype='int64'):
        f = np.vectorize(d.get, otypes=[object])
        return f(df.fillna(0).astype(dtype), default)

    def get_df(self, f, struct):
        columns = struct.keys + struct.fields
        columns_func = lambda x : x.lower() in columns
        if struct.drop:
            df = pd.read_csv(f, index_col=struct.keys)
            df = df.drop(columns=columns_func, errors='ignore')
        else:
            df = pd.read_csv(f)
            df.columns = df.columns.str.lower()
            
        return df.rename(columns=str.lower)

    def prepare_dataset(self, df, fields):
        model_fields = []
        for k, v in fields.items():    
            df.loc[:, k] = v.get_value(df.loc[:, v.cols])
            model_fields.append(k)
        model_fields.append("nquest")
        return df[model_fields]

    def merge_df(self, df_list, indexes):
        df_merged = df_list.pop()
        for df in df_list:
            df_merged = df_merged.merge(df, on=indexes, how='left')
        return df_merged


    def run(self):
        
        families = []

        with RemoteZip(self.file_path) as zip:
            for fname in zip.namelist():
                
                struct = self.families_files.get(fname, None)
                
                if struct:
                    df = self.get_df(zip.open(fname), struct)
                    families.append(df)
                else:
                    pass
        
        families_df = self.merge_df(families, ['nquest'])

        families_df = self.prepare_dataset(families_df, self.families_attributes)
        
        families_df["payment_financing_need"] = families_df["payment_financing_need_index"]
        families_df.drop("payment_financing_need_index", axis=1, inplace=True)

        bins = [0.125, 0.375, 0.625, 0.875]
        mappa = {
            1 : bins[0],
            2 : bins[1],
            3 : bins[2],
            4 : bins[3]
        }
        families_df["subjective_risk_propensity_index"] = families_df["risk_aversion"].map(mappa)
        families_df.drop("risk_aversion", axis=1, inplace=True)

        columns = ["loan_financing_need", "health_insurance_need"]
        for column in columns:
            bins = [0.25, 0.75]
            mappa = {
                False : bins[0],
                True : bins[1]
            }
            families_df[column] = families_df[column].map(mappa)

        families_df["mortgage_financing_need"] = families_df["mortgage_financing_need"] / 10


        max = families_df["digital_propension"].max()
        min = families_df["digital_propension"].min()
        families_df["digital_activity_index"] = families_df["digital_propension"].apply(lambda x: (x-min)/(max-min))
        families_df.drop("digital_propension", axis=1, inplace=True)


        max = families_df["current_accounts_count"].max()
        min = families_df["current_accounts_count"].min()
        families_df["bank_activity_index"] = families_df["current_accounts_count"].apply(lambda x: (x-min)/(max-min))
        families_df.drop("current_accounts_count", axis=1, inplace=True)
        families_df.rename(columns={"financial_litteracy":"financial_litteracy_index"}, inplace=True)
        families_df = families_df.reset_index()
        
        return families_df


    