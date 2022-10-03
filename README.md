# teste_bix_tecnologia
## Desafio criar pipeline
### Tabela de conteúdos
=================
<!--ts-->
  * [Tecnologias utilizadas](#Tecnologias)
  * [Arquitetura GCP](#Arquitetura-GCP)
  * [Análise dos dados](#Análise-dos-dados)
    * [Item 1](#Item-1)
    * [Item 2](#Item-2)
    * [Item 3](#Item-3)
    * [Item 4](#Item-4)
    * [Item 5](#Item-5)
  * GCP
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

### Análise dos dados
```python
from datetime import datetime, timedelta, date
from pyspark import SparkContext, SparkConf
import datetime as dt
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import concat, lit, col
from pyspark.sql.functions import *
```
