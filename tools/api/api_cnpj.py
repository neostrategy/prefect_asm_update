import aiohttp
import asyncio
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class ReceitaWSClient:
    
    def __init__(self):
        self.api_key = os.getenv("API_CNPJ_TOKEN")
        self.headers = {'Authorization': f'Bearer {self.api_key}'}

    async def get_data_from_receita(self, batch_cnpj):
        print(f"Extraindo dados da Receita QTD {len(batch_cnpj)}")
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [self.fetch_data(session, cnpj) for cnpj in batch_cnpj]
            results = await asyncio.gather(*tasks)
        return results

    async def fetch_data(self, session, cnpj, retries=1):
        url = f'https://www.receitaws.com.br/v1/cnpj/{cnpj}/days/720'
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=50) as response:
                    response.raise_for_status()
                    return [await response.json()]
            except (aiohttp.ClientOSError, asyncio.TimeoutError, aiohttp.ClientResponseError, aiohttp.ClientConnectorError) as e:
                print(f"Erro ao processar CNPJ {cnpj} (tentativa {attempt + 1}/{retries}): {e}")
                if attempt + 1 < retries:
                    await asyncio.sleep(5)
        return []
    
    
    async def create_request(self, cnpj_list):
        results = []
        if cnpj_list:
            chunk_size = 100
            total_cnpjs_processados = 0
            
            for i in range(0, len(cnpj_list), chunk_size):
                chunk = cnpj_list[i:i + chunk_size]
                if total_cnpjs_processados >= 1000:
                    print("Limite de 1 mil CNPJs processados atingido. Aguardando 10 segundos antes de continuar...")
                    await asyncio.sleep(10)
                    total_cnpjs_processados = 0  # Reinicia o contador
                    print("Continuando a pesquisa...")
                
                chunk_results = await self.get_data_from_receita(chunk)
                results.extend(chunk_results)
                total_cnpjs_processados += len(chunk)  # Incrementa o contador
            
            print("Processamento concluído.")
        else:
            print("Nenhum CNPJ válido encontrado.")

        return results
