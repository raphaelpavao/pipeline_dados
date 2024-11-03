import json
import csv

class Dados:
    def __init__(self,path, tipo):
        self.path = path
        self.tipo = tipo
        self.dados = self.leitura_dados()
        self.nome_colunas = self.get_columns()
        self.size = self.size_data()

    def leitura_json(self):
        dados_json=[]
        with open(self.path, 'r') as file:
            dados_json = json.load(file)
        return dados_json

    def leitura_csv(self):
        dados_csv=[]
        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file,delimiter=',')
            for row in spamreader:
                dados_csv.append(row)
        return dados_csv

    def leitura_dados(self):
        dados=[]
        if self.tipo == 'csv':
            dados = self.leitura_csv()

        elif self.tipo == 'json':
            dados = self.leitura_json()
        
        elif self.tipo == 'list':
            dados = self.path
            self.path = 'lista em memória'
        
        return dados
    
    def get_columns(self):
        return list(self.dados[-1].keys())

    def rename_coluns(self,key_mapping):
        new_dados=[]
        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)

        self.dados = new_dados
        self.nome_colunas = self.get_columns()
    
    def size_data(self):
        return len(self.dados)

    def join_data(dadosA, dadosB):
        combined_list=[]
        combined_list.extend(dadosA.dados)
        combined_list.extend(dadosB.dados)
        return Dados(combined_list,'list')
    
    def dados_para_tabela(self):
        dados_combinados_tabela = [self.nome_colunas]
        for row in self.dados:
            linha=[]
            for coluna in self.nome_colunas:
                linha.append(row.get(coluna,'Indisponível'))
            dados_combinados_tabela.append(linha)
        return (dados_combinados_tabela)
        
    def salvando_dados(self,path):
        dados_combinados_tabela = self.dados_para_tabela()
        with open(path,'w') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinados_tabela)