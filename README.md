# FIAP Solution Sprint Brazilian E-commerce

## Índice
* [Introdução](#introducao)
* [Descrição do Projeto](#descricao-do-projeto)
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

![Captura de tela 2022-04-27 152804](https://user-images.githubusercontent.com/49615846/165594869-28d8c1f2-116a-4424-9697-9867f9d37994.png)

| Item 	| Ferramenta       	| Descrição                                                                                                                        	|
|:----:	|------------------	|----------------------------------------------------------------------------------------------------------------------------------	|
|   1  	| Fonte - Kaggle   	| O dataset Olist está disponível para download no site Kaggle.                                                                    	|
|   2  	| Fonte - Correios 	| Site para consultar endereços por CEP, aceita pesquisa usando apenas o prefixo (5 dígitos)                                       	|
|   3  	| Fonte - Geopy    	| Biblioteca Python para consultar coordenadas por endereço, inserindo a cidade e endereço, latitude e longitude serão retornados. 	|
|   4  	| Apache PySpark   	| Nesta etapa será utilizada a API do Kaggle para fazer o download do dataset no formato CSV.                                      	|

**Fontes de dados**: 
 * Kaggle: O dataset está armazenado no site Kaggle, no formato CSV. 
 * Site Correios e Geopy: Na tabela de geolocalização temos CEP, cidade, estado, latitude e longitude, porém cada registro de cidade e estado tem coordenas diferentes, mesmo que dentro da mesma cidade, pois foi informado a coordenada do CEP com sufixo, como a coluna geolocation_zip_code_prefix apresenta apenas o CEP sem sufixo (5 digitos) desejamos que cada registro distindo de cidade e estado tenham as mesmas coordenadas. Para atingirmos este objetivo decidimos atualizar as cidades e estados de acordo com os dados dos Correios, fonte onde iremos buscar endereço usando o CEP, e as coordenadas com a API Geopy, fonte onde iremos buscar as coordenadas usando o endereço.

**HDFS, Hadoop Distributed File System**:
  O HDFS é um sistema de armazenamento de dados distribuídos, com ele é possível armazenar um grande volume de dados, pois esse framework armazena os dados em diversas máquinas, dessa forma o armazenamento se torna escalável horizontalmente, quando for necessário mais armaenamento um novo datanode é criado, quando o armazenamento diminui um datanode pode ser removido, o namenode (nó principal) faz o gerenciamento.
  * Landing Zone: Este é o local onde iremos armazenar os dados brutos, no formato em que são extraídos, sem tratamento algum.

**PySpark**:
  Aqui é onde acontece o processamento distribuído, mais de um worker pode trabalhar tornando a ferramenta escalável, antes da execução do código é feito um planejamnento de execução

## Ecossistema Hadoop Com Docker

Ambiente para estudo dos principais frameworks big data em docker.
<br> Esse setup vai criar dockers com os frameworks HDFS, HBase, Hive, Presto, Spark, Jupyter, Hue, Mongodb, Metabase, Nifi, kafka, Mysql e Zookeeper com a seguinte arquitetura:
<br>  

![Ecossistema](ecosystem.jpeg)

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
          git clone https://github.com/fabiogjardim/bigdata_docker.git

#### No diretório bigdata_docker vai existir os seguintes objetos
![ls](ls.JPG)

   
### INICIANDO O AMBIENTE
   
  *No Windows abrir PowerShell, do Linux um terminal*

### No terminal, no diretorio bigdata_docker, executar o docker-compose
          docker-compose up -d        

### Verificar imagens e containers
 
         docker image ls

![docker image ls](docker_image_ls.JPG)

         docker container ls

![docker container](docker_container_ls.JPG)

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
* Hbase *http://localhost:16010/master-status*
* Mongo Express *http://localhost:8081*
* Kafka Manager *http://localhost:9000*
* Metabase *http://localhost:3000*
* Nifi *http://localhost:9090*
* Jupyter Spark *http://localhost:8889*
* Hue *http://localhost:8888*
* Spark *http://localhost:4040*

### Acesso por shell

   ##### HDFS

          docker exec -it datanode bash

   ##### HBase

          docker exec -it hbase-master bash

   ##### Sqoop

          docker exec -it datanode bash
        
   ##### Kafka

          docker exec -it kafka bash

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
   
   ##### MongoDB
    Usuário: root
    Senha: root
    Authentication Database: admin

### Imagens   

[Docker Hub](https://hub.docker.com/u/fjardim)

### Documentação Oficial

* https://zookeeper.apache.org/
* https://kafka.apache.org/
* https://nifi.apache.org/
* https://prestodb.io/
* https://spark.apache.org/
* https://www.mongodb.com/
* https://www.metabase.com/
* https://jupyter.org/
* https://hbase.apache.org/
* https://sqoop.apache.org/
* https://hadoop.apache.org/
* https://hive.apache.org/
* https://gethue.com/
* https://github.com/yahoo/CMAK
* https://www.docker.com/
