import pandas as pd
import asyncio
from tools.api.api_cnpj import ReceitaWSClient

class DataApiManager:
    """
    Classe responsável por transformar e adequar os dados retornados da Classe ReceitaWSClient
    """
    def __init__(self):
        pass

    def valida_cnpj(self, cnpj_list):
        cnpj_validos, cnpj_invalidos = [], []
        for cnpj in cnpj_list:
            if len(cnpj) != 14 or not cnpj.isdigit():
                cnpj_invalidos.append(cnpj)
                continue
            if self._validar_cnpj(cnpj):
                cnpj_validos.append(cnpj)
            else:
                cnpj_invalidos.append(cnpj)
        return cnpj_validos, cnpj_invalidos

    def _validar_cnpj(self, cnpj):
        def calcular_digito_verificador(cnpj, pesos):
            soma = sum(int(cnpj[i]) * peso for i, peso in enumerate(pesos))
            resto = soma % 11
            return 11 - resto if resto > 1 else 0

        peso1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        peso2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

        digito1 = calcular_digito_verificador(cnpj[:12], peso1)
        digito2 = calcular_digito_verificador(cnpj[:13], peso2)
        return cnpj[12:] == f"{digito1}{digito2}"

    def get_cnpj_data(self, results):
        print("Tentando extrair as informações de CNPJ")
        cnpj_data ={
            'cnpj': [],
            'cnae': [],
            'code': [],
            'data_situação' : [],
            'complemento' : [],
            'tipo': [],
            'nome': [],
            'uf': [],
            'telefone': [],
            'email':[],
            'situação': [],
            'bairro': [],
            'logradouro': [],
            'numero': [],
            'cep': [],
            'municipio': [],
            'porte': [],
            'abertura': [],
            'fantasia': [],
            'capital_social': [],
        }
        cnpj_rejeitados = []
        
        for data in results:
            if data is not None and data[0].get('status') == "OK":
                cnpj_data['cnpj'].append(data[0].get('cnpj'))
                
                atividade_principal = data[0].get('atividade_principal', [])
                if atividade_principal:
                    cnpj_data['cnae'].append(atividade_principal[0].get('text', ""))
                    cnpj_data['code'].append(atividade_principal[0].get("code", ""))
                
                cnpj_data['data_situação'].append(data[0].get("data_situacao", ""))
                cnpj_data['complemento'].append(data[0].get("complemento", ""))
                cnpj_data['tipo'].append(data[0].get('tipo'))
                cnpj_data['nome'].append(data[0].get('nome'))
                cnpj_data['uf'].append(data[0].get("uf", ""))
                cnpj_data['telefone'].append(data[0].get("telefone", ""))
                cnpj_data['email'].append(data[0].get("email", ""))
                cnpj_data['situação'].append(data[0].get("situacao", ""))
                cnpj_data['bairro'].append(data[0].get("bairro", ""))
                cnpj_data['logradouro'].append(data[0].get("logradouro", ""))
                cnpj_data['numero'].append(data[0].get("numero", ""))
                cnpj_data['cep'].append(data[0].get('cep'))
                cnpj_data['municipio'].append(data[0].get('municipio'))
                cnpj_data['porte'].append(data[0].get('porte'))
                cnpj_data['abertura'].append(data[0].get('abertura'))
                cnpj_data['fantasia'].append(data[0].get('fantasia'))
                cnpj_data['capital_social'].append(data[0].get('capital_social'))
                    
            elif data is not None: 
                cnpj_rejeitados.append(data[0].get('cnpj')) 
        
        return cnpj_data, cnpj_rejeitados

    def get_cnae_data(self, api_data):
        cnaes = {
            'cnpj': [], 'cnae_principal': [], 'code_cnae_principal': [], 'code_cnae_sec': [], 'text': []
        }
        print("Tentando extrair as informações de CNAEs")
        for data in api_data:
            if data and isinstance(data, list) and len(data) > 0:
                dados = data[0]
                cnpj = dados.get('cnpj', '')
                atividade_principal = dados.get('atividade_principal', [])
                cnae_principal = atividade_principal[0].get('text', '') if atividade_principal else ''
                code_principal = atividade_principal[0].get('code', '') if atividade_principal else ''

                for cnae_secundario in dados.get('atividades_secundarias', []):
                    cnaes['cnpj'].append(cnpj)
                    cnaes['cnae_principal'].append(cnae_principal)
                    cnaes['code_cnae_principal'].append(code_principal)
                    cnaes['code_cnae_sec'].append(cnae_secundario.get('code', ''))
                    cnaes['text'].append(cnae_secundario.get('text', ''))
            else:
                print("Dados retornados inválidos ou vazios para CNAE")

        return cnaes
    
    def remove_chars(self, df, list_column_name, list_chars):
        """
        Remove caracteres específicos de colunas de um DataFrame.
        
        :param df: DataFrame do pandas.
        :param list_column_name: Lista contendo os nomes das colunas a serem limpas.
        :param list_chars: Lista contendo os caracteres a serem removidos.
        :return: DataFrame com os caracteres removidos.
        """
        print('Removendo caracteres')
        try:
            for column_name in list_column_name:
                if column_name in df.columns:
                    if df[column_name].dtype == 'object':  # Verifica se a coluna é do tipo string.
                        for char in list_chars:
                            df[column_name] = df[column_name].astype(str).str.replace(char, '', regex=False)
                            df[column_name] = df[column_name].astype(str)
                    else:
                        print(f"Coluna {column_name} não é do tipo string e será ignorada.")
                else:
                    print(f"Coluna {column_name} não encontrada no DataFrame.")
            return df
        except Exception as e:
            print(f"Erro ao remover caracteres do DataFrame: {e}")
            return df
        

    def api_launcher(self, cnpj_list):

        cnpj_validos, cnpj_invalidos = self.valida_cnpj(cnpj_list)
        print(f"QTD válidos: {len(cnpj_validos)}, QTD inválidos {len(cnpj_invalidos)}")
        api_client = ReceitaWSClient()
        api_data = asyncio.run(api_client.create_request(cnpj_validos))
        cnpj_data, cnpj_rejeitados = self.get_cnpj_data(api_data)
        cnae_data = self.get_cnae_data(api_data)
  
        transformed_data = {
            'cnpj_data': cnpj_data,
            'cnae_data': cnae_data,
            'cnpj_rejeitados': cnpj_rejeitados,
            'cnpj_invalidos': cnpj_invalidos
        }
        return transformed_data



    