from functools import partial
from remotezip import RemoteZip
from dataclasses import dataclass
from typing import Any, Callable, AnyStr, List
import pandas as pd
import numpy as np


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
class Wealths:

    def __init__(self) -> None:

        self.file_path = 'https://www.bancaditalia.it/statistiche/tematiche/' + \
            'indagini-famiglie-imprese/bilanci-famiglie/distribuzione-microdati/' + \
            'documenti/ind20_ascii.zip'

        self.families_files = {
            'CSV/ricfam20.csv': FileStruct(
                keys = ['nquest', ],
                fields = ['ar1', 'ar2', 'ar3', 'af1', 'af2', 'af3', 'af4', 'pf1', 'pf2', 'pf3']
            )
        }

        self.families_attributes = {
            'real_activity_houses': FieldsFloat('ar1'),
            'real_activity_corporates': FieldsFloat('ar2'),
            'real_activity_valuable_objects': FieldsFloat('ar3'),
            'financial_activity_deposits': FieldsFloat('af1'),
            'financial_activity_state_titles': FieldsFloat('af2'),
            'financial_activity_other_titles': FieldsFloat('af3'),
            'financial_activity_commercial_credits': FieldsFloat('af4'),
            'financial_passivity_banks_financial_societies': FieldsFloat('pf1'),
            'financial_passivity_commercial': FieldsFloat('pf2'),
            'financial_passivity_other': FieldsFloat('pf3')
        }

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

    def run(self):
    
        with RemoteZip(self.file_path) as zip:
            for fname in zip.namelist():
                
                struct = self.families_files.get(fname, None)
                if struct:
                    df = self.get_df(zip.open(fname), struct)
                    families_df = df
                else:
                    pass

        
        families_df = self.prepare_dataset(families_df, self.families_attributes)
        families_df = families_df.reset_index()
        return families_df

    