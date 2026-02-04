import hashlib
import pandas as pd

def gerar_id(df, colunas, nome_coluna='id_validacao', prefixo=None, tamanho=10):
    """
    Gera um ID determinístico baseado em colunas de um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame com os dados.
        colunas (list): Lista de colunas que serão combinadas para gerar o ID.
        nome_coluna (str): Nome da nova coluna com o ID.
        prefixo (str, opcional): Prefixo opcional para o ID.
        tamanho (int): Tamanho do hash gerado.

    Returns:
        pd.DataFrame: DataFrame com a nova coluna de ID.
    """
    def gerar_hash(row):
        texto = '|'.join(str(row[col]).strip().lower() if pd.notna(row[col]) else '' for col in colunas)
        hash_valor = hashlib.sha256(texto.encode()).hexdigest()[:tamanho]
        return f"{prefixo}_{hash_valor}" if prefixo else hash_valor

    df[nome_coluna] = df.apply(gerar_hash, axis=1)
    return df

def tratamento_cnpj(cnpj: str) -> str:
    return ''.join(filter(str.isdigit, cnpj))