# teste_bix_tecnologia
## Desafio criar pipeline
### Tabela de conteúdos
=================
<!--ts-->
  * [Tecnologias utilizadas](#Tecnologias)
  * [Arquitetura GCP](#Arquitetura-GCP)
  * [Análise dos dados(Ambiente de desenvolvimento)](#Análise-dos-dados)
    * [Item 1](#Item-1)
    * [Item 2](#Item-2)
    * [Item 3](#Item-3)
    * [Item 4](#Item-4)
  * GCP(Ambiente de produção)
    * [Cloud Function](#Cloud-Function)
    * [Cloud Storage](#Cloud-Storage)
    * [Cloud Scheduler](#Cloud-Scheduler)
    * [BigQuery](#BigQuery) 
<!--te-->
### Tecnologias

As seguintes ferramentas foram usadas na resolução dos questionamentos:

- Jupyter
- Anaconda
- Pandas 
- Python
- Drawio
- Google Cloud Platform

### Análise dos dados(ambiente de desenvolvimento)
### jupyter notebook
```python
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
import datetime as DT
from datetime import date, timedelta
import pytz
import requests
```
# Item 1
### 1. Dataset como fonte de dados tabela do postgressql
```python
#Gera a data e hora de São Paulo Brasil
tz_BR = pytz.timezone('America/Sao_Paulo')
#Gera a data atual em formato Timestamp
start = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print("Processo iniciado em: "+start)

#PostgreSQL:
#•	Host: 34.173.103.16
#•	User: junior
#•	Password: |?7LXmg+FWL&,2(
#•	Port: 5432
#•	Database: postgres
#•	Tabela: public.venda

#Inicia a configuração para conexão com as tabelas do postgressql
connection = pg.connect("host=34.173.103.16 dbname=postgres user=junior password=|?7LXmg+FWL&,2(")

#Executa Query para retornar o resultado e armazenar um Dataframe Pandas
#Query com solução para os dados solicitados
QUERY = """SELECT * FROM public.venda"""
print(QUERY)
df_postgressql_pd = psql.read_sql_query(QUERY, connection)
end_consulta = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print("Consulta realizada com Sucesso: "+end_consulta)
df_postgressql_pd
```
# Item 2 
### 2. Dataset como fonte de dados a API fornecida
```python
#comando para chamar a api e armazenar em um dataframe pandas
start = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print("Processo iniciado em: "+start)
df_funcionarios = pd.DataFrame()
for number in range(1, 10):
    url = f'https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={number}'
    resp = requests.get(url)
    funcionario = resp.text
    df_funcionario_api = pd.DataFrame({'funcionario': [funcionario],
                                       'id_funcionario': [number]})
    df_funcionarios = pd.concat([df_funcionarios,df_funcionario_api])
    
end_consulta = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print("Consulta realizada com Sucesso: "+end_consulta)
df_funcionarios
```
# Item 3 
### 3. Dataset como fonte de dados Parquet File
```python
#Comando para leitura do parquet e armazenar em dataframe pandas
path_parquet_file = "C:\\Users\\caior_op46gft\\Desktop\\arquivo_parquet\\"
df_parquet_pd = pd.read_parquet(path_parquet_file+'categoria.parquet')
df_parquet_pd
```
# Item 4 
### 4.	Dataset final com todos os requisitos solicitados
```python
##comando para juntar
df_tabela_final= df_postgressql_pd.merge(df_funcionarios)\
                                  .merge(df_parquet_pd, left_on='id_categoria', right_on='id',\
                                         how='left').drop('id', axis=1)

df_tabela_final
```
![image](https://user-images.githubusercontent.com/73916591/193870896-cd4833b9-6d11-4c49-94e0-78a7268ae7ba.png)


# Cloud Function
```python
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
```
Variaveis que irão servir como logs ao longo do processo
```python
#VARIAVEIS
        #Gera a data e hora de São Paulo Brasil
        tz_BR = pytz.timezone('America/Sao_Paulo')
        #Gera a data atual em formato Timestamp
        start = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print("Processo iniciado em: "+start)
```
Constantes que armazenam paths para serem utilizados ao longo do processo
```python
PROJECT_ID = project_id
        print(PROJECT_ID)
        DATASET = "core_bix"
        bucket = "datamigration_caiorod"
        path_raw = "/raw/bix/"
        path_core = "/core/bix/"
        categoria_filename = "categoria/categoria.parquet"
        table_id = PROJECT_ID+"."+DATASET+".tb_info_pedidos"
        path_core_tabela_final = 'gs://'+bucket+path_core+'pedidos/tabela_final.csv'
```
Cria um dataframe pandas tendo como fonte de dados uma tabela do postgressql e após isso armazena na camada raw do datalake dentro do bucket do Cloud Storage em um arquivo csv
```python
#Inicia a configuração para conexão com as tabelas do postgressql
        connection = pg.connect("host=34.173.103.16 dbname=postgres user=junior password=|?7LXmg+FWL&,2(")
        #Executa Query para retornar o resultado e armazenar um Dataframe Pandas
        #Query com solução para os dados solicitados
        QUERY = """SELECT * FROM public.venda"""
        #print(QUERY)
        df_postgressql_pd = psql.read_sql_query(QUERY, connection)
        #Armazena df_postgressql_pd na camada raw do bucket no formato csv
        path_raw_postgressql = 'gs://'+bucket+path_raw+'vendas/vendas.csv'
        df_postgressql_pd.to_csv(path_raw_postgressql , sep = ',', index = False)
        end_consulta = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print("Consulta realizada com Sucesso: "+end_consulta)
```
Cria um dataframe pandas tendo como fonte de dados a API fornecida e após isso armazena o dataframe na camada raw do datalake dentro do bucket do Cloud Storage em um arquivo csv
```python
 #comando para chamar a api
        df_funcionarios = pd.DataFrame()
        for number in range(1, 10):
            url = f'https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={number}'
            resp = requests.get(url)
            funcionario = resp.text
            df_funcionario_api = pd.DataFrame({'funcionario': [funcionario],
                                            'id_funcionario': [number]})
            df_funcionarios = pd.concat([df_funcionarios,df_funcionario_api])
        #Armazena df_funcionarios na camada raw do bucket no formato csv
        path_raw_api = 'gs://'+bucket+path_raw+'funcionarios/funcionarios.csv'
        df_funcionarios.to_csv(path_raw_api , sep = ',', index = False)
        end_consulta = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print("Consulta API realizada com Sucesso: "+end_consulta)
```
Leitura do parquet em pandas na camada raw do datalake dentro do bucket do Cloud Storage e armazena em um dataframe pandas
```python
path_parquet_file = "gs://"+bucket+path_raw+categoria_filename
df_parquet_pd = pd.read_parquet(path_parquet_file)
```
Junção dos dataframes contendo vendas, funcionarios e categorias em após isso armazena o dataframe final na camada core do datalake dentro do bucket do Cloud Storage em um arquivo csv
```python
df_tabela_final = df_postgressql_pd.merge(df_funcionarios)\
                                   .merge(df_parquet_pd, left_on='id_categoria', right_on='id',\
                                          how='left').drop('id', axis=1)
        
#Salva os dados na camada Core do datalake do Cloud Storage
df_tabela_final.to_csv(path_core_tabela_final , sep = ',', index = False)
```
Cria tabela no Big Query a partir da tabela final criada no datalake utilizado
```python
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
        uri = str(path_core_tabela_final)
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
```
![image](https://user-images.githubusercontent.com/73916591/193907495-b1fe9dfb-91f1-454e-a120-0336559a294b.png)

# Cloud Storage

![image](https://user-images.githubusercontent.com/73916591/193907708-d00a5298-ca10-4af5-9b5a-7ca69d810f6a.png)
![image](https://user-images.githubusercontent.com/73916591/193907865-3887436a-3550-4edf-b4b0-c4ef3fd79b6b.png)
![image](https://user-images.githubusercontent.com/73916591/193907947-b773e01a-7fc9-4d53-a795-76b0101d9a0f.png)

# Cloud Scheduler

![image](https://user-images.githubusercontent.com/73916591/193908937-406b72bc-2408-4c05-832e-3ef32d052192.png)

# BigQuery

![image](https://user-images.githubusercontent.com/73916591/193909485-27a00655-0eaf-477e-acf1-f23a046857ed.png)
