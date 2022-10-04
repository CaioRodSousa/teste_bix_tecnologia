# -*- coding:utf-8 -*-
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
import datetime as DT
from datetime import date, timedelta
import pytz
import requests
import base64
import json
from google.cloud import bigquery

def instantiate_workflow_template(event, context):

    try:
        #Parâmetros recebidos pelo pub/sub
        pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        project_id = pubsub_message['project_id']
        region = pubsub_message['region']
        workflow_template = pubsub_message['workflow_template']
        parameters = pubsub_message['parameters']

        #VARIAVEIS
        #Gera a data e hora de São Paulo Brasil
        tz_BR = pytz.timezone('America/Sao_Paulo')
        #Gera a data atual em formato Timestamp
        start = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print("Processo iniciado em: "+start)
        #CONSTANTES
        PROJECT_ID = project_id
        print(PROJECT_ID)
        DATASET = "core_bix"
        bucket = "datamigration_caiorod"
        path_raw = "/raw/bix/"
        path_core = "/core/bix/"
        categoria_filename = "categoria/categoria.parquet"
        table_id = PROJECT_ID+"."+DATASET+".tb_info_pedidos"

        #Inicia a configuração para conexão com as tabelas do postgressql
        connection = pg.connect("host=34.173.103.16 dbname=postgres user=junior password=|?7LXmg+FWL&,2(")
        #Executa Query para retornar o resultado e armazenar um Dataframe Pandas
        #Query com solução para os dados solicitados
        QUERY = """SELECT * FROM public.venda"""
        #print(QUERY)
        df_postgressql_pd = psql.read_sql_query(QUERY, connection)
        end_consulta = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print("Consulta realizada com Sucesso: "+end_consulta)
        #comando para chamar a api
        df_funcionarios = pd.DataFrame()
        for number in range(1, 10):
            url = f'https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={number}'
            resp = requests.get(url)
            funcionario = resp.text
            df_funcionario_api = pd.DataFrame({'funcionario': [funcionario],
                                            'id_funcionario': [number]})
            df_funcionarios = pd.concat([df_funcionarios,df_funcionario_api])
        end_consulta = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print("Consulta API realizada com Sucesso: "+end_consulta)
        
        #Comando para leitura do parquet em pandas na camada raw do datalake dentro do bucket do Cloud Storage
        path_parquet_file = "gs://"+bucket+path_raw+categoria_filename
        df_parquet_pd = pd.read_parquet(path_parquet_file)
        df_tabela_final = df_postgressql_pd.merge(df_funcionarios)\
                                        .merge(df_parquet_pd, left_on='id_categoria', right_on='id',\
                                                how='left').drop('id', axis=1)
        
        #Salva os dados na camada Core do datalake do Cloud Storage
        path_core = 'gs://'+bucket+path_core+'pedidos/tabela_final.csv'
        df_tabela_final.to_csv(path_core , sep = ',', index = False)
        
        #Inicia conexão com o BigQuery
        client = bigquery.Client()
        #Deleta os dados das tabelas para não gerar duplicidade a cada rodada da Cloud Function
        QUERY = """DELETE FROM `"""+table_id+"""` WHERE 1=1"""
        query_job = client.query(QUERY)
        #Gravando dados Core no BigQuery 
        print("==== Criando e Carregando Tabela "+table_id+" no bigquery ====")
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("id_venda", "INTEGER"),
                bigquery.SchemaField("id_funcionario", "INTEGER"),
                bigquery.SchemaField("id_categoria", "INTEGER"),
                bigquery.SchemaField("data_venda", "STRING"),
                bigquery.SchemaField("venda", "INTEGER"),
                bigquery.SchemaField("funcionario", "STRING"),
                bigquery.SchemaField("nome_categoria", "STRING"	)
            ],
            skip_leading_rows=1,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
        )
        uri = str(path_core)
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
        #Gera o tempo que o processo finalizou
        end = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print("Processo finalizado com sucesso: "+end)
    except Exception as e:
        print(e)