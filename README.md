![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
# FIAP Solution Sprint Brazilian E-commerce

## Índice
* [Introdução](#introdução)
* [Descrição do Projeto](#descrição-do-projeto)
* [Notebooks](#notebooks)
* [Ecossistema Hadoop com Docker](#ecossistema-hadoop-com-docker)

## Introdução
O ecossistema hadoop é composto por diversas ferramentas, com o objetivo de utilizar os frameworks iremos analisar um dataset disponibilizado no Kaggle.

Um conjunto de dados públicos de comércio eletrônico brasileiro foi fornecido pela Olist(https://olist.com/) no Kaggle (https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), este dataset contém dados comerciais reais de 100 mil pedidos de 2016 a 2018 realizados em diversos mercados no Brasil. Os dados foram anonimizados e as referências às empresas e parceiros no texto da revisão foram substituídas pelos nomes das grandes casas de Game of Thrones. As tabelas do dataset se relacionam da seguinte forma:
![image](https://user-images.githubusercontent.com/49615846/165148902-58dcff90-dcaa-4637-85c8-b76a7f880ab0.png)

Algumas perguntas devem ser respondidas sobre este conjunto de dados:
* Segmentar os clientes por geolocalização.
* Total de pedidos por período e categorias.
* Total de pagamentos por método de pagamento.
* Notas das avaliações.
* Vendedores x vendas.
* Produtos mais vendidos.

## Descrição do Projeto
Desenhamos a seguinte arquitetura para manipularmos os dados deste dataset e responder as perguntas.

![image](https://user-images.githubusercontent.com/49615846/165752994-d7ed13db-1e58-4c2f-acf3-4cf0be87e293.png)
| Item 	| Ferramenta       	| Descrição                                                                                                                                                                                                                                                                                                                                                                                                  	|
|:----:	|------------------	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|   1  	| Fonte - Kaggle   	| O dataset Olist está disponível para download no site Kaggle.                                                                                                                                                                                                                                                                                                                                              	|
|   2  	| Fonte - Correios 	| Site para consultar endereços por CEP, aceita pesquisa usando apenas o prefixo (5 dígitos).                                                                                                                                                                                                                                                                                                                	|
|   3  	| Fonte - Geopy    	| Biblioteca Python para consultar coordenadas por endereço, inserindo a cidade e endereço, latitude e longitude serão retornados.                                                                                                                                                                                                                                                                           	|
|   4  	| Apache PySpark   	| Nesta etapa será utilizada a API do Kaggle para fazer o download do dataset no formato CSV.                                                                                                                                                                                                                                                                                                                	|
|   5  	| Jupyter Notebook 	| Aplicação web onde é possível editar e executar scripts de programação, os scripts PySpark serão criados e executados neste ambiente.                                                                                                                                                                                                                                                                      	|
|   6  	| HDFS             	| O Hadoop Distributed File System é um sistema de armazenamento distribuído, um namenode (nó mestre) gerencia e armazena metadados dos arquivos, que são armazenados em 1 ou mais datanodes (nós trabalhadores), dando escalabilidade ao armazenamento. Na landing zone os arquivos serão armazenados em sua forma bruta, sem tratamento.                                                                   	|
|   7  	| Apache PySpark   	| Nesta etapa os arquivos CSV do dataset armazenados na landing zone serão transformados para o formato ORC.                                                                                                                                                                                                                                                                                                 	|
|   8  	| Apache PySpark   	| A tabela de geolocalização possui como chave primaria os primeiros 5 digitos do CEP, e traz qual a cidade, estado e coordenadas, porém iremos atualizar as informações de cidade e estado com base nos dados da empresa Correios, esses dados serão consultados com uso da biblioteca python requests no site de busca de CEP do Correios ( https://buscacepinter.correios.com.br/app/endereco/index.php). 	|
|   9  	| Apache PySpark   	| Após os dados de CEP serem atualizados com cidade e estado iremos atualizar as coordenadas com o uso da biblioteca python geopy, é informado o endereço e a biblioteca retorna a latitude e longitude.                                                                                                                                                                                                     	|
|  10  	| HDFS             	| Os dados tratados serão armazenados em outra pasta do HDFS.                                                                                                                                                                                                                                                                                                                                                	|
|  11  	| Apache HUE       	| O Apache HUE é um editor SQL open-source, será utilizado como uma User Interface (UI) para auxiliar nas consultas SQL no Hive.                                                                                                                                                                                                                                                                             	|
|  12  	| Apache Hive      	| Software de data warehouse que facilita a leitura, escrita e manipulação de grandes datasets armazenados em armazenamento distribuído (HDFS) usando SQL.                                                                                                                                                                                                                                                   	|
|  13  	| Metabase         	| Dataviz das tabelas criadas no Hive, respondendo as questões levantadas.                                                                                                                                                                                                                                                                                                                                   	|

## Notebooks
### data/notebooks/Desafio1_FIAP/1_Kaggle Dataset Ingestion to HDFS.ipynb
<details>
<summary>clique para ver explicação</summary>
Este Notebook faz o download do dataset do Kaggle no formato CSV, utilizando a biblioteca python kaggle basta um token de autenticação, que foi gerado no site Kaggle em “Your Profile”, “Account”, em API botão “Generate New API Token”, um arquivo json será gerado e deve ser armazenado na pasta informada na variável de ambiente $KAGGLE_CONFIG_DIR, caso necessário permissões podem ser dadas com chmod

```python
import os
base_path = "/mnt/notebooks/Desafio1_FIAP"
os.environ["KAGGLE_CONFIG_DIR"] = f'{base_path}/kaggle_config_dir/'
!chmod 600 /mnt/notebooks/Desafio1_FIAP/kaggle_config_dir/kaggle.json
```

O método kaggle.api_dataset_download_files faz o download do dataset no path dos parâmetros, caso desejar os arquivos descompactados é necessário usar unzip=True.
```python
import kaggle
kaggle.api.authenticate()

kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce', 
                                  path='/mnt/notebooks/Individual_Desafio1_FIAP/olist_dataset', 
                                  unzip=True)
```

Os arquivos csv do dataset foram armazenados no HDFS com auxilio da biblioteca hdfs para fazer a conexão, pandas para formatar o arquivo csv mantendo o cabeçalho e removendo o index. Para se conectar ao HDFS foi necessário informar alguns parâmetros de conexão, incluindo o endereço URL do namenode e a porta 50070, também foi definida uma estratégia de retry. Os parâmetros foram definidos conforme código abaixo:
```python
import requests
import os
import pandas as pd 
import hdfs
import urllib3

from hdfs import InsecureClient
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry

max_threads = 50
session = requests.Session()

retry_strategy = Retry(
    total=10,
    connect=10,
    read=10,
    redirect=10,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"],
)

adapter = HTTPAdapter(
    max_retries=retry_strategy, pool_connections=max_threads, pool_maxsize=max_threads,
)

session.mount("https://", adapter)
session.mount("http://", adapter)

# client usando IP do host docker
client = 'http://192.168.56.1:50070'

# Client HDFS
hdfs_client = InsecureClient(client, session=session)
```

Os arquivos csv foram gravados numa pasta que definimos como landing_zone, que tem como objetivo armazenar os dados em sua forma original, mantendo o formato csv.
```python
# Gravar o arquivo csv no HDFS
for filename in os.listdir(f'{base_path}/olist_dataset'):
    df = pd.read_csv(f'{base_path}/olist_dataset/{filename}', sep =',')
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
    try:
        with hdfs_client.write(f'/datalake/landing_zone/{filename}', overwrite = True, encoding='utf-8') as writer:
            df.to_csv(writer, header=True, index=False)
        print(f"{filename} Gravado com sucesso")
    except hdfs.util.HdfsError as e:
        print(f"{filename} falhou")
        print(f"[ERRO] {e}")
    except urllib3.exceptions.NewConnectionError as e:
        print(f"{filename} falhou")
        print(f"[ERRO] {e}")
    except Exception as e:
        print(e)
```
A imagem abaixo mostra os arquivos armazenados no HDFS, esta é a interface do Apache HUE:
![image](https://user-images.githubusercontent.com/49615846/165817162-337b08dc-0c44-4237-8209-2ab7a6e41007.png)
  
Por fim os arquivos são convertidos para ORC com auxilio da biblioteca pyspark, o formato ORC permite particionamento, compressão e schema, funcionalidades que podem ser utilizadas futuramente.

A função spark.read.csv precisa do parâmetro inferSchema=True para o spark definir um data type para cada campo, é importante para que o Metabase funcione corretamente, na criação da tabela os datatypes precisam ser iguais aos do arquivo ORC.
Um dos campos do dataset é o zip_code_prefix, CEP, ele contém 5 digitos e pode conter zeros a esquerda, por isso este campo precisou ser definido com tipo string em alguns arquivos.
  
É importante manter o cabeçalho com o nome das colunas no arquivo, pois quando a tabela hive for criada com location no caminho desse arquivo o schema da tabela deve bater com o schema do arquivo, as colunas devem possuir o mesmo nome, incluindo a distinção dos caracteres minúsculos e maiúsculos.
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad

spark = SparkSession \
    .builder \
    .appName("Ingest Olist Dataset") \
    .getOrCreate()

landing_zone = '/datalake/landing_zone/'
files = hdfs_client.list(landing_zone)

for filename in files:
    csv = spark.read.csv(f'{landing_zone}/{filename}', header = True, inferSchema=True, sep = ',')
    if filename == 'olist_customers_dataset.csv':
        csv = csv.withColumn('customer_zip_code_prefix', lpad(csv.customer_zip_code_prefix, 5, '0'))
    elif filename == 'olist_sellers_dataset.csv':
        csv = csv.withColumn('seller_zip_code_prefix', lpad(csv.seller_zip_code_prefix, 5, '0'))
    elif filename == 'olist_geolocation_dataset.csv':
        csv = csv.withColumn('geolocation_zip_code_prefix', lpad(csv.geolocation_zip_code_prefix, 5, '0'))
    csv.printSchema()
    orc_name = filename.replace('csv', 'orc')
    csv.write.orc(f'/datalake/dadosbrutos/{orc_name}', 'overwrite')
```
Arquivos armazenados no HDFS
![image](https://user-images.githubusercontent.com/49615846/165946296-bd582fe7-7ee7-4cb8-90bc-47784c1382ca.png)
</details>

### data/notebooks/Desafio1_FIAP/2_Consulta Endereço por CEP com 5 digitos no Site do correio.ipynb
<details>
<summary>clique para ver explicação</summary>
Este notebook tem como objetivo consultar todos os CEPs armazenados no arquivo olist_geolocation_dataset.orc para encontrar a cidade e estado, apenas o prefixo dos CEPs é fornecido, caso o CEP completo estivesse disponível seria possível utilizar a biblioteca pycep, neste caso será necessário usar o site da empresa Correios que aceita CEPs parciais. 
  
Primeiro criamos um dataframe pyspark para o arquivo olist_geolocation_dataset.orc
```python
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

hive_context = HiveContext(sc)

spark = SparkSession \
    .builder \
    .appName("API Correios") \
    .enableHiveSupport() \
    .getOrCreate()

geo = spark.read.orc('/datalake/dadosbrutos/olist_geolocation_dataset.orc')
```
Separamos os CEPs distintos, foram encontrados 19015 CEPs distintos neste arquivo.
```python
cep_array = [str(row.geolocation_zip_code_prefix) for row in geo.select('geolocation_zip_code_prefix').distinct().collect()]
```
Como iremos consultar um grande volume de CEPs uma consulta síncrona levaria muito tempo, pois o programa iria aguardar a resposta do site para seguir para o próximo passo, usando consultas assíncronas a execução é muito mais rápida, pois o programa recebe as respostas do site em momentos futuros e o resto do programa continua executando.

Para atingir esse objetivo usamos a biblioteca asyncio e requests, a URL é 'https://buscacepinter.correios.com.br/app/endereco/carrega-cep-endereco.php' e consultamos com um método POST informando qual o CEP e qual o tipo de endereço, no nosso caso retornamos todos os tipos, o site retorna com uma lista de CEPs que correspondem com os 5 digitos informados, nós selecionamos o primeiro resultado encontrado com o campo cep correto, para um prefixo de CEP a cidade e estado será sempre a mesma, o que muda são os logradouros que nesse caso não serão usados.

O código a seguir cria as funções get_address, get_all_addresses e consulta_lote. Dado o grande volume de CEPs iremos separar a consulta em lotes de 1000 consultas, caso aconteça alguma falha no meio do programa não iremos perder todo o progresso, a função consulta_lote inicia o loop assíncrono para cada lote. A função get_all_addresses gera as tasks assíncronas dentro de uma sessão, cada task corresponde a consulta de um CEP. A função get_address faz a consulta no site da empresa Correios e aguarda a resposta para inserir na lista de endereços que irá gerar um dataframe do lote.
  
Por fim utilizamos numpy.array_split para criar os lotes de CEPs.
```python

import asyncio
import time
import aiohttp
import nest_asyncio
import pandas as pd
import json
from pyspark.sql import Row

global URL
# URL do site do correios
URL = 'https://buscacepinter.correios.com.br/app/endereco/carrega-cep-endereco.php'
global ceps_com_erro
ceps_com_erro = []
# Função para pegar o primeiro resultado da pesquisa de CEP com apenas 5 digitos
async def get_address(session, cep):
    async with session.post(url=URL, data={'endereco': cep, 'tipoCEP': 'ALL'}) as response:
        response = await response.text()
        try:
            for i in range(len(json.loads(response)["dados"])):
                data = json.loads(response)["dados"][i]
                if data["cep"] != '' and data["cep"][0:5] == cep: 
                    data_selected = {
                        "cep": data["cep"],
                        "uf": data["uf"],
                        "cidade": data["localidade"]
                    }
                    results.append(data_selected)
                    print(f"{str(len(results)).zfill(6)} CEPs consultados", end="\r")
                    break
        except Exception as e:
            #print(f"ERRO: {e}", end="\r")
            ceps_com_erro.append(cep)
            pass

# Função para criar as tasks assíncronas, uma task para cada cep
async def get_all_addresses(ceps):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for cep in ceps:
            task = asyncio.ensure_future(get_address(session, cep))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=False)
        

# Função prncipal para iniciar o loop assíncrono e criar o Dataframe com os resultados
def consulta_lote(ceps_array):
    global results
    results = []
    nest_asyncio.apply()
    start_time = time.time()
    asyncio.get_event_loop().run_until_complete(get_all_addresses(ceps_array))
    
    df = spark.createDataFrame((Row(**x) for x in results))
    
    duration = time.time() - start_time
    print(f"Downloaded {len(ceps_array)} ceps in {duration/60} minutes")
    return df
```
```python
import math
import numpy as np

start_time = time.time()

tamanho_lote = 1000
qtd_lotes = math.floor(len(cep_array)/tamanho_lote)

print(f"Iniciando a consulta de {qtd_lotes} lotes com aprox. {tamanho_lote} ceps cada.")
cep_lotes = np.array_split(cep_array, qtd_lotes)

dataframes = {}
counter = 0
for lote in cep_lotes:
    counter += 1
    print(f"Consultando lote {counter}")
    dataframes[f"df_part{counter}"] = consulta_lote(lote)

duration = time.time() - start_time
print(f"Tempo total da carga: {duration/60} minutos")
print(f"Total de CEPs não encontrados: {len(ceps_com_erro)}")
```
 
Resultado da execução: 19015 CEPs consultados em aproximadamente 3 minutos
![image](https://user-images.githubusercontent.com/49615846/165949926-b7496d4a-c028-4797-83db-4db6af0686db.png)

Após a união dos dataframes dos lotes em um datarame final o resultado é esse:
![image](https://user-images.githubusercontent.com/49615846/165950104-3ceeaf05-1eb0-43ec-9364-d16a74e997cf.png)

O resultado foi gravado no HDFS.
</details>

## Ecossistema Hadoop Com Docker
<br> Esse setup vai criar dockers com os frameworks HDFS, Hive, Presto, Spark, Jupyter, Hue,  Metabase, Mysql.
<br>  

### SOFTWARES NECESSÁRIOS
#### Para a criação e uso do ambiente vamos utilizar o git e o Docker 
   * Instalação do Docker Desktop no Windows [Docker Desktop](https://hub.docker.com/editions/community/docker-ce-desktop-windows) ou o docker no [Linux](https://docs.docker.com/install/linux/docker-ce/ubuntu/)
   *  [Instalação do git](https://git-scm.com/book/pt-br/v2/Come%C3%A7ando-Instalando-o-Git)
   
### SETUP
*OBS: Esse passo deve ser realizado apena uma vez. Após o ambiente criado, utilizar o docker-compose para iniciar os containers como mostrado no tópico INICIANDO O AMBIENTE*

#### Criação do diretório docker:
*OBS: Criar um diretório chamado docker*

   *  Sugestão no Windows:
      *  Criar na raiz do seu drive o diretório docker
         ex: C:\docker
          
   * Sugestão no Linux:
      * Criar o diretório na home do usuário
        ex: /home/user/docker

#### Em um terminal/DOS, dentro diretório docker, realizar o clone do projeto no github
          git clone https://github.com/Gabrielvinicius27/desafio_fase1_fiap_engdados

#### No diretório bigdata_docker vai existir os seguintes objetos
![ls](ls.JPG)
   
### INICIANDO O AMBIENTE
   
  *No Windows abrir PowerShell, do Linux um terminal*

### No terminal, no diretorio bigdata_docker, executar o docker-compose
          docker-compose up -d        

### Verificar imagens e containers
 
         docker image ls

![image](https://user-images.githubusercontent.com/49615846/165780971-03474480-c1c1-46ea-b8b8-c3214b183d35.png)

         docker container ls
         
![image](https://user-images.githubusercontent.com/49615846/165781325-c2f867da-2124-42b4-ad6f-c283db2c0b57.png)


### SOLUCIONANDO PROBLEMAS 
   
  *No Windows abrir o Docker Quickstart Terminal*

#### Parar um containers
         docker stop [nome do container]      

#### Parar todos containers
         docker stop $(docker ps -a -q)
  
#### Remover um container
         docker rm [nome do container]

#### Remover todos containers
         docker rm $(docker ps -a -q)         

#### Dados do containers
         docker container inspect [nome do container]

#### Iniciar um container
         docker-compose up -d [nome do container]

#### Iniciar todos os containers
         docker-compose up -d 

#### Acessar log do container
         docker container logs [nome do container] 

#### Acesso WebUI dos Frameworks
 
* HDFS *http://localhost:50070*
* Presto *http://localhost:8080*
* Metabase *http://localhost:3000*
* Jupyter Spark *http://localhost:8889*
* Hue *http://localhost:8888*
* Spark *http://localhost:4040*

### Acesso por shell

   ##### HDFS

          docker exec -it datanode bash

### Acesso JDBC

   ##### MySQL
          jdbc:mysql://database/employees

   ##### Hive

          jdbc:hive2://hive-server:10000/default

   ##### Presto

          jdbc:presto://presto:8080/hive/default

### Usuários e senhas

   ##### Hue
    Usuário: admin
    Senha: admin

   ##### Metabase
    Usuário: bigdata@class.com
    Senha: bigdata123 

   ##### MySQL
    Usuário: root
    Senha: secret

### Imagens   

[Docker Hub](https://hub.docker.com/u/fjardim)

### Documentação Oficial

* https://prestodb.io/
* https://spark.apache.org/
* https://www.metabase.com/
* https://jupyter.org/
* https://hadoop.apache.org/
* https://hive.apache.org/
* https://gethue.com/
* https://github.com/yahoo/CMAK
* https://www.docker.com/
