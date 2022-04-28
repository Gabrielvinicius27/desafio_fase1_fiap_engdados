# FIAP Solution Sprint Brazilian E-commerce

## Índice
* [Introdução](#introdução)
* [Descrição do Projeto](#descrição-do-projeto)
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
