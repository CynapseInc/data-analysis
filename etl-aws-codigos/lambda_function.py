import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client('s3')

BUCKET_RAW = 'raw-encanto'
BUCKET_TRUSTED = 'trusted-encanto'

FILE_NAME = 'tv_shows.csv'


def lambda_handler(event, context):

    try:
        # 1. Leitura
        obj = s3.get_object(Bucket=BUCKET_RAW, Key=FILE_NAME)
        df = pd.read_csv(obj['Body'])

        # 2. Tratamento

        # drop total (do jeito que você quer)
        df = df.dropna()

        # conversão notas
        df['imdb_100'] = pd.to_numeric(
            df['IMDb'].astype(str).str.replace('/10', '', regex=False),
            errors='coerce'
        ) * 10

        df['rottenTomatoes_100'] = pd.to_numeric(
            df['Rotten Tomatoes'].astype(str).str.replace('/100', '', regex=False),
            errors='coerce'
        )

        # remove possíveis NaN pós conversão
        df = df.dropna()

        # filtros
        df = df[df['Year'] >= 2000]
        df = df[df['imdb_100'] >= 70]
        df = df[df['rottenTomatoes_100'] >= 70]

        # plataformas
        plataformas = ['Netflix', 'Hulu', 'Prime Video', 'Disney+']
        for col in plataformas:
            if col in df.columns:
                df[col] = df[col].map({1: 'SIM', 0: 'NAO'}).fillna(df[col])

        # remove colunas
        cols_to_drop = ['IMDb', 'Rotten Tomatoes', 'Type', 'Unnamed: 0']
        cols_existentes = [col for col in cols_to_drop if col in df.columns]
        df = df.drop(columns=cols_existentes)

        # 3. salvar
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

        s3.put_object(
            Bucket=BUCKET_TRUSTED,
            Key=FILE_NAME,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

        return {
            "statusCode": 200,
            "linhas_processadas": len(df)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }