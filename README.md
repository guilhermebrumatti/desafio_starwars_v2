# DESAFIO STARWARS V2

Este é o desafio que já havia feito a algum tempo atrás.<br>
Resolvi fazer novamente esse desafio, porém, implementando meus novos conhecimentos.

Como nesse intervalo eu aprendi novas tecnologias, fiz algumas alterações como<br>
deixar de fora o docker e airflow.<br>
<br>
-Python - pandas<br>
-Requests<br>
-Banco de dados - sqlalchemy<br><br>
Dessa vez utilizei serviços AWS.<br><br>
-S3 - Bucket<br>
-RDS - Banco de dados<br>
-LAMBDA<br>
-EVENTBRIDGE<br><br>

Bom, com as configurações feitas na AWS e do "configure" do CLI, crei:<br><br>
# Bucket S3<br><br>
1- Acessei o S3 e cliquei em "Criar bucket"<br>
2- Dei um nome ao bucket no campo "Nome do bucket"<br>
3- Desmarquei a caixa "Bloquear todo o acesso público"<br>
4- Depois cliquei em "Criar bucket"<br>
5- De posse do nome do bucket, já será possível upar os arquivos para o bucket.

# Banco de dados RDS<br><br>
1- Acessei o RDS e cliquei em "Criar banco de dados"<br>
2- Criação padrão<br>
3- MySQL<br>
4- Nível gratuito<br>
5- Em "Identificador da instância de banco de dados" dei um nome ao Banco de dados<br>
6- Criei o login e senha do usuário que vai acessar o banco, "Nome do usuário principal" e "Senha principal" respectivamente.<br>
7- Em "Acesso público" selecionei "Sim"<br>
8- Fiz as devidas configurações em "Grupos de segurança da VPC" do banco de dados<br>
9- Depois que o banco de dados foi devidamente criado, já será possível pegar o endpoint e a porta para fazer a conexão com o banco<br>

Iniciamos importando as bibliotecas necessárias:<br>

```
import pandas as pd<br>
import requests<br>
import subprocess<br>
from sqlalchemy import create_engine<br>
```
No bloco abaixo, fiz uma requisição ao site e obtive os dados em JSON.<br>
Depois transferi os dados para um Dataframe e fazer as devidas manipulações:
<br><br>
####################################################################<br>
########## BLOCO QUE PEGA O JSON, TRABALHA E EXPORTA LOCALMENTE #########<br>
####################################################################<br>

```
print('ETL Iniciado!')

# Realiza a requisição HTTP para obter o JSON
url = "https://swapi.dev/api/films/?format=json"
response = requests.get(url)
data = response.json()

print('Requisição feita ao site!')

# Cria o DataFrame com base no JSON
df = pd.DataFrame(data['results'])

print('Dataframe criado!')
```

No bloco abaixo removi algumas colunas indesejadas.<br><br>
As colunas characters, planets, starships, vehicles e species vieram com links que ao<br>
abrir, eram novos JSONs com os dados dos planetas, veículos e etc, usados nos filmes.<br><br>
Então foi necessário trabalha-los para obter os dados desses links e substituir os links<br>
pelos respectivos dados.<br><br>
Por fim, exportei os dados para .XLSX, .PARQUET e .JSON<br>
```
# Remove a coluna 'url'
df.drop('url', axis=1, inplace=True)

print('Coluna url deletada!')

# Função para obter os dados de um link e retornar o nome
def get_data(link):
    response = requests.get(link)
    data = response.json()
    return data.get('name', '')

# Acessa e acrescenta os conteúdos das colunas 'characters', 'planets', 'starships', 'vehicles' e 'species'
df['characters'] = df['characters'].apply(lambda x: ", ".join([get_data(link) for link in x]))
df['planets'] = df['planets'].apply(lambda x: ", ".join([get_data(link) for link in x]))
df['starships'] = df['starships'].apply(lambda x: ", ".join([get_data(link) for link in x]))
df['vehicles'] = df['vehicles'].apply(lambda x: ", ".join([get_data(link) for link in x]))
df['species'] = df['species'].apply(lambda x: ", ".join([get_data(link) for link in x]))

# Remove as colunas 'created' e 'edited'
df.drop('created', axis=1, inplace=True)
df.drop('edited', axis=1, inplace=True)

print('Colunas created e edited deletadas!')

# Exporta o DataFrame para Excel, Parquet e JSON
df.to_excel("files/filmes.xlsx", index=False)
df.to_parquet('files/filmes.parquet')
df.to_json('files/filmes.json', orient='records')

print('Dados exportados para .xlsx, .parquet e .json')
```
<br>
No bloco de código abaixo, faço o upload dos arquivos exportados localmente<br>
para o Bucket S3<br><br>

####################################################################<br>
######## BLOCO QUE SALVA ARQUIVOS COM OS RESULTADOS EM UM BUCKET ########<br>
####################################################################<br>

```
print('Inicio do upload dos arquivos para o Bucket AWS!')

# Configurações da AWS
def upload_files_to_s3(file_paths, bucket_name):
    for file_path in file_paths:
        command = f"aws s3 cp {file_path} s3://{bucket_name}/"
        subprocess.run(command, shell=True)

# Lista dos caminhos dos arquivos locais a serem enviados para o S3
file_paths = ['files/filmes.xlsx', 'files/filmes.parquet', 'files/filmes.json']

# Nome do bucket S3
bucket_name = 'bucket-desafio-startwars'

# Faz o upload dos arquivos para o S3
upload_files_to_s3(file_paths, bucket_name)

print('Fim do upload dos arquivos para o Bucket AWS!')
```
<br>
E no bloco de código abaixo, faço o insert dos dados do Dataframe no banco de dados<br>
RDS.
<br><br>

####################################################################<br>
######## BLOCO QUE FAZ O INSERT DOS DADOS NO BANCO DE DADOS RDS #########<br>
####################################################################<br>

```
print('Inicio do upload dos arquivos para o MySql no RDS AWS')

# Dados do banco de dados AWS
db_username = 'db_user'
db_password = 'PW'
db_endpoint = 'endpoint_db'
db_name = 'db_name'

# URL de conexão do banco de dados
db_url = f"mysql+mysqlconnector://{db_username}:{db_password}@{db_endpoint}/{db_name}"

# Crie uma conexão com o banco de dados usando sqlalchemy
engine = create_engine(db_url)

# Nome da tabela no banco de dados
table_name = 'table_starwars'

# Executa o INSERT dos dados do Dataframe no banco de dados
try:
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print("Dados inseridos com sucesso na tabela table_starwars!")
except Exception as e:
    print(f"Erro ao inserir dados na tabela table_starwars: {e}")

print('ETL Finalizado!')
```

# PLUS*
Como plus:<br><br>

# LAMBDA<br><br>
1- Criei um Lambda<br>
2- Criei uma camada para atender os requisitos(bibliotecas) que o código exige.<br>

# EVENTBRIDGE<br><br>
1- Em "Ônibus" -> "Regras", criei uma nova regra<br>
2- Dei um nome, selecionei "Programação" e cliquei em Continuar no EventBridge Scheduler<br>
3- Em "Ocorrência" selecionei a opção "Cronograma recorrente"<br>
4- Em "Tipo de cronograma" selecionei a opção "Cronograma baseado em intervalor"<br>
5- E em "Expressão rate" configurei para que o lambda seja executado 1 vez ao diade 12 em 12 horas<br>
6- Em seguinda selecionei a opção "Destinos modelados" e escolhi "AWS Lambda Invoke"<br>
7- No campo "Função do Lambda" selecionei o Lambda previamente criado.<br>
8- Mantive as demais configurações em Default e finalizei a criação do evento.

Fim!
