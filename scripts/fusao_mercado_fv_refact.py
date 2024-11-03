from processamento_dados import Dados

#Iniciando a extração dos dados

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

dadosEmpresaA = Dados(path_json,'json')
print (dadosEmpresaA.path)
print (dadosEmpresaA.dados[-1])
print (dadosEmpresaA.size)

dadosEmpresaB = Dados(path_csv,'csv')
print (dadosEmpresaB.path)
print (dadosEmpresaB.dados[-1])
print (dadosEmpresaB.size)

#Iniciando a transformação dos dados
#O key_mapping faz parte da regra de negócio, então ele fica no código
key_mapping = {
    'Nome do Item':'Nome do Produto',
    'Classificação do Produto':'Categoria do Produto',
    'Valor em Reais (R$)':'Preço do Produto (R$)',
    'Quantidade em Estoque':'Quantidade em Estoque',
    'Nome da Loja':'Filial',
    'Data da Venda':'Data da Venda'}

dadosEmpresaB.rename_coluns(key_mapping)
print (dadosEmpresaB.dados[-1])

dadosFusao = Dados.join_data(dadosEmpresaA,dadosEmpresaB)
print (dadosFusao.dados[0])
print (dadosFusao.dados[-1])
print (dadosFusao.size)

#Iniciando o salvamento dos dados
path_dados_combinados = 'data_processed/dados_combinados.csv'
dadosFusao.salvando_dados(path_dados_combinados)