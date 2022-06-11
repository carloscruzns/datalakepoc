import json
import awswrangler as wr

con = wr.mysql.connect(secret_id='<<SECRET ID NAME>>')

def lambda_handler(event, context):
    #### PARAMETERS #####
    s3_path_payment = 's3://<<COAL_LAYER_BUCKET_NAME>>/sakila/payment/'
    s3_path_rental = 's3://<<COAL_LAYER_BUCKET_NAME>>/sakila/rental/'
    s3_path_inventory = 's3://<<COAL_LAYER_BUCKET_NAME>>/sakila/inventory/'
    s3_path_film = 's3://<<COAL_LAYER_BUCKET_NAME>>/sakila/film/'
    s3_path_film_category = 's3://<<COAL_LAYER_BUCKET_NAME>>/sakila/film_category/'
    s3_path_category = 's3://<<COAL_LAYER_BUCKET_NAME>>/sakila/category/'
    
    #### EXTRACTING PAYMENT #####
    df_payment  = wr.mysql.read_sql_table(
        table="payment",
        schema="sakila",
        con=con
    )
    
    wr.s3.to_parquet(
        df=df_payment,
        path=s3_path_payment,
        dataset=True,
        database='sakila_coal', 
        table='payment'  
    )
    
    #### EXTRACTING RENTAL #####
    df_rental  = wr.mysql.read_sql_table(
        table="rental",
        schema="sakila",
        con=con
    )
    
    wr.s3.to_parquet(
        df=df_rental,
        path=s3_path_rental,
        dataset=True,
        database='sakila_coal', 
        table='rental'  
    )
    
    #### EXTRACTING INVENTORY #####
    df_inventory  = wr.mysql.read_sql_table(
        table="inventory",
        schema="sakila",
        con=con
    )
    
    wr.s3.to_parquet(
        df=df_inventory,
        path=s3_path_inventory,
        dataset=True,

        database='sakila_coal', 
        table='inventory'  
    )
    
    #### EXTRACTING FILM #####
    df_film  = wr.mysql.read_sql_table(
        table="film",
        schema="sakila",
        con=con
    )
    df_film['original_language_id'] = df_film['original_language_id'].astype('string')
    
    wr.s3.to_parquet(
        df=df_film,
        path=s3_path_film,
        dataset=True,
        database='sakila_coal', 
        table='film'  
    )
    
    #### EXTRACTING FILM_CATEGORY #####
    df_film_category  = wr.mysql.read_sql_table(
        table="film_category",
        schema="sakila",
        con=con
    )
    
    wr.s3.to_parquet(
        df=df_film_category,
        path=s3_path_film_category,
        dataset=True,
        database='sakila_coal', 
        table='film_category'  
    )
    
    #### EXTRACTING CATEGORY #####
    df_category  = wr.mysql.read_sql_table(
        table="category",
        schema="sakila",
        con=con
    )
    
    wr.s3.to_parquet(
        df=df_category,
        path=s3_path_category,
        dataset=True,
        database='sakila_coal', 
        table='category'  
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Loaded Coal Layer Successfully')
    }