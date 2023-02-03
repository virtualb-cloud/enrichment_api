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
class Debts:

    def __init__(self) -> None:
        
        self.file_path = 'https://www.bancaditalia.it/statistiche/tematiche/' + \
            'indagini-famiglie-imprese/bilanci-famiglie/distribuzione-microdati/' + \
            'documenti/ind20_ascii.zip'

        self.families_files = {
            'CSV/debiti20.csv': FileStruct(
                keys = ['nquest'],
                fields = ['ratadeb_res', 'ratadeb_aimm', 'ratadeb_fam', 'ratadeb_prof',
                'pfimm', 'pfcons', 'pfaz']
            )    
        }

        self.families_attributes = {
            'debt_residence_year': FieldsFloat('ratadeb_res'),
            'debt_houses_year': FieldsFloat('ratadeb_aimm'),
            'debt_family_necessity_year': FieldsFloat('ratadeb_fam'),
            'debt_professional_activity_year': FieldsFloat('ratadeb_prof'),
            'debt_residence_houses_residual': FieldsFloat('pfimm'),
            'debt_family_necessity_residual': FieldsFloat('pfcons'),
            'debt_professional_activity_residual': FieldsFloat('pfaz')
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
                    
                else:
                    pass

        families_df = self.prepare_dataset(df, self.families_attributes)
        families_df = families_df.reset_index()
        
        return families_df


    