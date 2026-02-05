import hashlib
import pandas as pd

def gerar_id(text,  prefixo=None, tamanho=10):
    """
    Gera um ID determinístico baseado em uma lista de valores.

    Args:
        text: texto enviado que será passando pelo hash.
        prefixo (str, opcional): Prefixo opcional para o ID.
        tamanho (int): Tamanho do hash gerado.

    Returns:
        hash gerado
        
    """
    
    texto = '|'.join(text).strip().lower() if text is not None else ''
    hash_valor = hashlib.sha256(texto.encode()).hexdigest()[:tamanho]

    return f"{prefixo}_{hash_valor}" if prefixo else hash_valor

def tratamento_cnpj(cnpj: str) -> str:
    return ''.join(filter(str.isdigit, cnpj))