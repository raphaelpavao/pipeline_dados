import json
import csv

#Funções

def leitura_json(path_json):
    dados_json=[]
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json

def leitura_csv(path_csv):
    dados_csv=[]
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file,delimiter=',')
        for row in spamreader:
            dados_csv.append(row)
    return dados_csv

def leitura_dados(path,tipo_arquivo):
    dados=[]
    if tipo_arquivo == 'csv':
        dados = leitura_csv(path)

    elif tipo_arquivo == 'json':
        dados = leitura_json(path)
    
    return dados

def get_columns(dados):
    return list(dados[-1].keys())

def rename_coluns(dados,key_mapping):
    new_dados_csv=[]
    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    return new_dados_csv

def size_data(dados):
    return len(dados)

def join_data(dadosA,dadosB):
    combined_list=[]
    combined_list.extend(dadosA)
    combined_list.extend(dadosB)
    return (combined_list)

def dados_para_tabela(dados,nome_colunas):
    dados_combinados_tabela = [nome_colunas]
    for row in dados:
        linha=[]
        for coluna in nome_colunas:
            linha.append(row.get(coluna,'Indisponível'))
        dados_combinados_tabela.append(linha)
    return (dados_combinados_tabela)

def salvando_dados(dados,path):
    with open(path,'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#Iniciando a leitura dos dados

dados_json = leitura_dados(path_json,'json')
nome_colunas_json = get_columns(dados_json)
tamanho_dados_json = size_data(dados_json)
print(f"Nome das colunas do json: {nome_colunas_json}")
print(f"Tamanho dos dados Json:{tamanho_dados_json}")

dados_csv = leitura_dados(path_csv,'csv')
nome_colunas_csv = get_columns(dados_csv)
tamanho_dados_csv = size_data(dados_csv)
print(f"Nome das colunas do csv:{nome_colunas_csv}")
print(f"Tamanho dos dados csv:{tamanho_dados_csv}")

#Iniciando a transformação dos dados

key_mapping = {
    'Nome do Item':'Nome do Produto',
    'Classificação do Produto':'Categoria do Produto',
    'Valor em Reais (R$)':'Preço do Produto (R$)',
    'Quantidade em Estoque':'Quantidade em Estoque',
    'Nome da Loja':'Filial',
    'Data da Venda':'Data da Venda'}

dados_csv = rename_coluns(dados_csv,key_mapping)
nome_colunas_csv = get_columns(dados_csv)
print(f"Nome das colunas do csv depois da transformação:{nome_colunas_csv}")

dados_fusao = join_data(dados_json,dados_csv)
nome_colunas_fusao = get_columns(dados_fusao)
tamanho_dados_fusao = size_data(dados_fusao)
print(f"Nome das colunas depois da fusão:{nome_colunas_fusao}")
print(f"Tamanho dos dados fusao:{tamanho_dados_fusao}")

#Salvando os dados

dados_fusao_tabela = dados_para_tabela(dados_fusao,nome_colunas_fusao)
tamanho_dados_tabela = size_data(dados_fusao_tabela)
print(f"Tamanho dos dados para gerar o arquivo:{tamanho_dados_tabela}")
path_dados_combinados = 'data_processed/dados_combinados.csv'
salvando_dados(dados_fusao_tabela,path_dados_combinados);
print(f"Arquivo salvo em:{path_dados_combinados}")
