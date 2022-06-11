import json
import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    #### PARAMETERS #####
    s3_path_film_category = 's3://<<PRESSURE_LAYER_BUCKET_NAME>>/sakila/film_category/'
    
    #### LOADING DATAFRAMES #####
    df_film = wr.athena.read_sql_table(table="film", database="sakila_coal")
    df_film_category = wr.athena.read_sql_table(table="film_category", database="sakila_coal")
    df_category = wr.athena.read_sql_table(table="category", database="sakila_coal")
    
    #### MERGING DATAFRAMES #####
    df_film = pd.merge(df_film, df_film_category, on='film_id')
    df_film = pd.merge(df_film, df_category, on='category_id')

    wr.s3.to_parquet(
        df=df_film,
        path=s3_path_film_category,
        dataset=True,
        database='sakila_pressure', 
        table='film'  
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Loaded Pressure Layer Successfully')
    }