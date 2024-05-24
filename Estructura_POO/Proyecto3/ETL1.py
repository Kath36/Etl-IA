import pandas as pd
import boto3
from io import StringIO

class Extractor:
    def __init__(self, bucket_name, file_names):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.file_names = file_names
        self.dfs = []

    def extract(self):
        for file in self.file_names:
            csv_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
            body = csv_obj['Body']
            csv_string = body.read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_string))
            self.dfs.append(df)

class Transformer:
    def __init__(self, dfs):
        self.dfs = dfs

    def transform(self):
        combined_df = pd.concat(self.dfs, ignore_index=True)
        combined_df.replace('?', pd.NA, inplace=True)
        combined_df.dropna(inplace=True)
        columns_order = [
            "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang",
            "oldpeak", "slope", "ca", "thal", "target"
        ]
        self.clean_df = combined_df[columns_order]
        self.clean_df = self.clean_df[self.clean_df['target'].isin([1, 2])]
