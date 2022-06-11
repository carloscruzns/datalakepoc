import json
import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    #### PARAMETERS #####
    s3_path_film_sales = 's3://<<DIAMOND_LAYER_BUCKET_NAME>>/film_sales/'
    
    #### LOADING DATAFRAMES #####
    df_film = wr.athena.read_sql_table(table="film", database="sakila_melting")
    df_inventory = wr.athena.read_sql_table(table="inventory", database="sakila_coal")
    df_rental = wr.athena.read_sql_table(table="rental", database="sakila_coal")
    df_payment = wr.athena.read_sql_table(table="payment", database="sakila_coal")
    
    #### DROPING COLUMNS #####
    df_film.drop(['last_update_x', 'last_update_y'], axis=1, inplace=True)
    df_inventory.drop(['last_update'], axis=1, inplace=True)
    df_rental.drop(['last_update'], axis=1, inplace=True)
    df_payment.drop(['last_update'], axis=1, inplace=True)
    
    #### MERGING DATAFRAMES #####
    df_film = pd.merge(df_film, df_inventory, on='film_id')
    df_film = pd.merge(df_film, df_rental, on='inventory_id')
    df_film = pd.merge(df_film, df_payment, on='rental_id')

    wr.s3.to_parquet(
        df=df_film,
        path=s3_path_film_sales,
        dataset=True,
        database='sakila_diamond', 
        table='film_sales'  
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Loaded Diamond Layer Successfully')
    }