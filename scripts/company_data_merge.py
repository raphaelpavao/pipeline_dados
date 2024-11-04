from data_processing import Data

# Starting data extraction

json_path = 'data_raw/companyA_data.json'
csv_path = 'data_raw/companyB_data.csv'

companyA_data = Data(json_path, 'json')
print(companyA_data.path)
print(companyA_data.data[-1])
print(companyA_data.size)

companyB_data = Data(csv_path, 'csv')
print(companyB_data.path)
print(companyB_data.data[-1])
print(companyB_data.size)

# Starting data transformation
# Key mapping is a business rule, so it's in the code
key_mapping = {
    'Nome do Item':'Nome do Produto',
    'Classificação do Produto':'Categoria do Produto',
    'Valor em Reais (R$)':'Preço do Produto (R$)',
    'Quantidade em Estoque':'Quantidade em Estoque',
    'Nome da Loja':'Filial',
    'Data da Venda':'Data da Venda'}

companyB_data.rename_columns(key_mapping)
print(companyB_data.data[-1])

merged_data = Data.join_data(companyA_data, companyB_data)
print(merged_data.data[0])
print(merged_data.data[-1])
print(merged_data.size)

# Starting data save
path_merged_data = 'data_processed/merged_data.csv'
merged_data.save_data(path_merged_data)