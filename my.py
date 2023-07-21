import pandas as pd<br>
import requests<br>
import subprocess<br>
from sqlalchemy import create_engine<br>

print('ETL Iniciado!')

# Realiza a requisição HTTP para obter o JSON
url = "https://swapi.dev/api/films/?format=json"
response = requests.get(url)
data = response.json()

print('Requisição feita ao site!')

# Cria o DataFrame com base no JSON
df = pd.DataFrame(data['results'])

print('Dataframe criado!')

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

print('Inicio do upload dos arquivos para o MySql no RDS AWS')

# Dados do banco de dados AWS
db_username = 'db_user'
db_password = 'pw'
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
