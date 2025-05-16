# openf1_importer_base.py
import os
import json
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# URL base da API OpenF1
OPENF1_API_URL = "https://api.openf1.org/v1"

class OpenF1ImporterBase:
    """
    Classe base para importadores de dados da API OpenF1 para o Supabase.
    """
    
    def __init__(self):
        """Inicializa o importador e conecta ao Supabase."""
        self.supabase = self._init_supabase()
        self.session_cache = {}  # Cache para evitar consultas repetidas
    
    def _init_supabase(self):
        """Inicializa o cliente Supabase."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("ERRO: SUPABASE_URL e SUPABASE_KEY devem ser configurados como variáveis de ambiente")
            return None
        
        try:
            print(f"Conectando ao Supabase: {SUPABASE_URL}")
            client = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            # Teste de conexão
            test_result = client.table("races").select("id").limit(1).execute()
            print(f"Conexão com Supabase estabelecida com sucesso!")
            return client
        except Exception as e:
            print(f"Erro ao inicializar o cliente Supabase: {str(e)}")
            return None
    
    def _get_flag_url(self, country_code):
        """
        Gera URL da bandeira com base no código do país.
        
        Args:
            country_code: Código do país (3 letras)
            
        Returns:
            str: URL da bandeira
        """
        if not country_code or len(country_code) != 3:
            return None
        
        # Converter para minúsculo
        code = country_code.lower()
        
        # Mapeamento de códigos específicos
        code_mapping = {
            "brn": "bh",  # Bahrain
            "uae": "ae",  # Emirados Árabes Unidos
            "sgp": "sg",  # Singapura
            "ned": "nl",  # Holanda
            "ksa": "sa",  # Arábia Saudita
        }
        
        # Aplicar mapeamento se o código estiver nele
        if code in code_mapping:
            code = code_mapping[code]
        
        # Retornar URL da bandeira
        return f"https://flagcdn.com/w320/{code}.png"
    
    def _extract_meeting_name(self, session_data):
        """
        Extrai o nome do meeting a partir dos dados da sessão.
        
        Args:
            session_data: Dados da sessão
            
        Returns:
            str: Nome do meeting formatado para uso em caminhos
        """
        # Tentar extrair do nome do país primeiro
        country_name = session_data.get("country_name", "")
        if country_name:
            return f"{country_name.replace(' ', '_')}_Grand_Prix"
        
        # Ou usar a localização como fallback
        location = session_data.get("location", "Unknown")
        return f"{location.replace(' ', '_')}_Grand_Prix"
    
    def fetch_data(self, endpoint, params=None):
        """
        Busca dados da API OpenF1.
        
        Args:
            endpoint: Endpoint da API (meetings, sessions, etc)
            params: Parâmetros da consulta (dict)
            
        Returns:
            list: Lista de dados ou lista vazia em caso de erro
        """
        url = f"{OPENF1_API_URL}/{endpoint}"
        
        try:
            print(f"Buscando dados de {url} com parâmetros: {params}")
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                print(f"Recebidos {len(data)} registros de {endpoint}")
                return data
            else:
                print(f"Erro ao acessar {url}: Status {response.status_code}")
                return []
        except Exception as e:
            print(f"Erro ao buscar dados de {endpoint}: {str(e)}")
            return []
    
    def get_table_columns(self, table_name):
        """
        Obtém a lista de colunas disponíveis em uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            list: Lista de nomes das colunas ou None se não for possível obter
        """
        if not self.supabase:
            return None
        
        try:
            # Realizar uma consulta para obter informações sobre a tabela
            # Usamos SELECT com LIMIT 0 para não pegar registros, apenas estrutura
            result = self.supabase.table(table_name).select("*").limit(0).execute()
            
            # A partir do resultado, podemos extrair os nomes das colunas
            if hasattr(result, 'columns') and result.columns:
                return result.columns
            
            # Se result.columns não existir, tentar obter de outra forma
            if result.data is not None:
                # Obtém nomes das colunas a partir da primeira linha retornada
                if result.data and len(result.data) > 0:
                    return list(result.data[0].keys())
            
            print(f"Não foi possível obter informações de colunas para a tabela {table_name}")
            return None
        except Exception as e:
            print(f"Erro ao obter colunas da tabela {table_name}: {str(e)}")
            return None
    
    def filter_record_columns(self, record, table_name):
        """
        Filtra um registro para conter apenas colunas que existem na tabela.
        
        Args:
            record: Dicionário com dados a inserir/atualizar
            table_name: Nome da tabela
            
        Returns:
            dict: Registro filtrado apenas com colunas válidas
        """
        columns = self.get_table_columns(table_name)
        
        if not columns:
            # Se não conseguir obter as colunas, retorna o registro original
            print(f"Aviso: Não foi possível verificar colunas para tabela {table_name}. Usando registro completo.")
            return record
        
        # Filtrar o registro para conter apenas colunas válidas
        filtered_record = {}
        for key, value in record.items():
            if key in columns:
                filtered_record[key] = value
            else:
                print(f"Ignorando coluna '{key}' que não existe na tabela {table_name}")
        
    def get_session_id(self, session_key):
        """
        Obtém o ID da sessão no Supabase a partir da session_key.
        Utiliza cache para evitar consultas repetidas.
        
        Args:
            session_key: Chave da sessão na API OpenF1
            
        Returns:
            int: ID da sessão no Supabase ou None se não encontrada
        """
        if not self.supabase:
            return None
        
        # Verificar cache
        if session_key in self.session_cache:
            return self.session_cache[session_key]
        
        try:
            # Tentar buscar diretamente pela chave da sessão
            session_query = self.supabase.table("sessions").select("id").eq("key", session_key).execute()
            
            if session_query.data:
                session_id = session_query.data[0]["id"]
                # Armazenar no cache
                self.session_cache[session_key] = session_id
                return session_id
            else:
                print(f"Sessão não encontrada com session_key: {session_key}")
                return None
        except Exception as e:
            print(f"Erro ao buscar ID da sessão: {str(e)}")
            return None