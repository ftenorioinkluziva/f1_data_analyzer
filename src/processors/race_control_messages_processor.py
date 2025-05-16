"""
Processor for RaceControlMessages streams from F1 races.
Simplified version that only generates CSV and stores data in the database.
"""
import pandas as pd
import json
import os
import time
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

class RaceControlMessagesProcessor(BaseProcessor):
    """
    Process RaceControlMessages streams to extract and store official race control communications.
    Simplified version focused on CSV generation and database storage only.
    """
    
    def __init__(self):
        """Initialize the RaceControlMessages processor."""
        super().__init__()
        self.topic_name = "RaceControlMessages"
        self.supabase = self._init_supabase()
    
    def _init_supabase(self):
        """Initialize Supabase client with improved error handling."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("AVISO: SUPABASE_URL e SUPABASE_KEY não estão configurados. Os dados não serão salvos no banco.")
            return None
        
        try:
            print(f"Conectando ao Supabase: {SUPABASE_URL}")
            client = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            # Fazer uma consulta de teste simples para verificar a conexão
            test_result = client.table("races").select("id").limit(1).execute()
            print(f"Conexão com Supabase estabelecida com sucesso! Dados de teste: {test_result.data}")
            return client
        except Exception as e:
            print(f"Erro ao inicializar o cliente Supabase: {str(e)}")
            return None
    
    def get_session_id_by_keys(self, meeting_key, session_key):
        """
        Get the session ID from the database based on meeting_key and session_key.
        
        Args:
            meeting_key: Key of the meeting (race)
            session_key: Key of the session
            
        Returns:
            int: Session ID or None if not found
        """
        if not self.supabase:
            return None
            
        try:
            # Primeiro, tente buscar diretamente pela chave da sessão
            session_query = self.supabase.table("sessions").select("id").eq("key", session_key).execute()
            
            if session_query.data:
                print(f"Sessão encontrada diretamente pela chave {session_key}")
                return session_query.data[0]["id"]
                
            # Se não encontrou pela chave, verificar se essa chave é um inteiro
            try:
                session_key_int = int(session_key)
                session_query = self.supabase.table("sessions").select("id").eq("key", session_key_int).execute()
                
                if session_query.data:
                    print(f"Sessão encontrada pela chave convertida para inteiro {session_key_int}")
                    return session_query.data[0]["id"]
            except (ValueError, TypeError):
                pass
                
            # Se ainda não encontrou, tente buscar pela relação com a corrida
            race_query = self.supabase.table("races").select("id").eq("key", meeting_key).execute()
            
            if not race_query.data:
                # Tentar converter meeting_key para inteiro também
                try:
                    meeting_key_int = int(meeting_key)
                    race_query = self.supabase.table("races").select("id").eq("key", meeting_key_int).execute()
                except (ValueError, TypeError):
                    pass
                    
            if not race_query.data:
                print(f"Corrida não encontrada com meeting_key: {meeting_key}")
                return None
                
            race_id = race_query.data[0]["id"]
            print(f"Corrida encontrada com ID: {race_id}")
            
            # Agora, buscar sessões para esta corrida
            session_query = self.supabase.table("sessions").select("id").eq("race_id", race_id).execute()
            
            if not session_query.data:
                print(f"Nenhuma sessão encontrada para corrida {race_id} (meeting_key {meeting_key})")
                return None
            
            # Se há várias sessões, tentar encontrar a correspondente ao session_key
            for session in session_query.data:
                # Retornar a primeira sessão disponível
                session_id = session["id"]
                print(f"Usando sessão com ID: {session_id} (primeira disponível para race_id {race_id})")
                return session_id
                
            return None
                
        except Exception as e:
            print(f"Erro ao buscar ID da sessão: {str(e)}")
            if isinstance(e, dict) and 'message' in e:
                print(f"Detalhe do erro: {e['message']}")
            return None
    
    def extract_messages(self, timestamped_data):
        """
        Extract race control messages from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            list: List of dictionaries containing race control messages
        """
        messages = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                session_timestamp = timestamp
                
                # Check if "Messages" is an array
                if "Messages" in data and isinstance(data["Messages"], list):
                    for msg in data["Messages"]:
                        message_entry = {
                            "timestamp": session_timestamp,
                            "utc_time": msg.get("Utc"),
                            "category": msg.get("Category"),
                            "message": msg.get("Message"),
                            "flag": msg.get("Flag"),
                            "scope": msg.get("Scope"),
                            "sector": msg.get("Sector")
                        }
                        messages.append(message_entry)
                
                # Check if "Messages" is a dictionary (with numbered keys)
                elif "Messages" in data and isinstance(data["Messages"], dict):
                    for msg_id, msg in data["Messages"].items():
                        message_entry = {
                            "timestamp": session_timestamp,
                            "utc_time": msg.get("Utc"),
                            "category": msg.get("Category"),
                            "message": msg.get("Message"),
                            "flag": msg.get("Flag"),
                            "scope": msg.get("Scope"),
                            "sector": msg.get("Sector")
                        }
                        messages.append(message_entry)
                
                # Progress reporting
                if (i + 1) % 50 == 0:
                    print(f"Processados {i+1} registros de mensagens...")
                    
            except json.JSONDecodeError:
                print(f"Erro ao analisar JSON no timestamp {timestamp}")
                continue
            except Exception as e:
                print(f"Erro inesperado ao processar mensagem no timestamp {timestamp}: {str(e)}")
                continue
        
        return messages
    
    def save_to_database(self, messages_df, session_id):
        """
        Save the race control messages to the database.
        
        Args:
            messages_df: DataFrame containing race control messages
            session_id: ID of the session in the database
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or session_id is None:
            return False
            
        try:
            # Verificar registros existentes para esta sessão
            print(f"Verificando registros existentes para a sessão ID: {session_id}")
            existing_query = self.supabase.table("race_control_messages").select("id").eq("session_id", session_id)
            existing_records = existing_query.execute()
            
            # Se já existem registros, remover automaticamente para evitar duplicações
            if existing_records.data:
                existing_count = len(existing_records.data)
                print(f"ATENÇÃO: Encontrados {existing_count} registros existentes para esta sessão.")
                print(f"Removendo registros existentes para evitar duplicação...")
                self.supabase.table("race_control_messages").delete().eq("session_id", session_id).execute()
                print(f"Removidos {existing_count} registros existentes.")
            
            # Criar uma coluna de timestamp em formato ISO
            session_date = datetime.now().strftime("%Y-%m-%d")
            
            # Verificar se há dados para inserir
            if messages_df.empty:
                print("Nenhuma mensagem de controle de corrida para inserir no banco.")
                return False
                
            # Preparar dados para inserção
            message_records = []
            
            for _, row in messages_df.iterrows():
                # Converter o timestamp para formato ISO
                time_part = row.get("timestamp", "00:00:00.000")  # formato: "00:00:17.964"
                if isinstance(time_part, str) and ':' in time_part:
                    iso_timestamp = f"{session_date} {time_part}"
                else:
                    # Se o timestamp não estiver no formato esperado, usar o horário atual
                    iso_timestamp = datetime.now().isoformat()
                
                # Tratar valores NaN
                message = None if pd.isna(row.get("message")) else row.get("message", "")
                category = None if pd.isna(row.get("category")) else row.get("category", "")
                flag = None if pd.isna(row.get("flag")) else row.get("flag", "")
                scope = None if pd.isna(row.get("scope")) else row.get("scope", "")
                sector = None if pd.isna(row.get("sector")) else row.get("sector", "")
                utc_time = None if pd.isna(row.get("utc_time")) else row.get("utc_time", "")
                
                # Converter sector para número se possível
                if sector is not None:
                    try:
                        sector = int(sector)
                    except (ValueError, TypeError):
                        pass
                
                # Criar registro para o banco
                message_record = {
                    "session_id": session_id,
                    "timestamp": iso_timestamp,
                    "utc_time": utc_time,
                    "category": category,
                    "message": message,
                    "flag": flag,
                    "scope": scope,
                    "sector": sector
                }
                
                message_records.append(message_record)
            
            # Inserir em lotes para evitar problemas com tamanho da requisição
            batch_size = 100
            total_records = len(message_records)
            
            # Mostrar exemplo do primeiro registro para verificação
            if message_records:
                print(f"Exemplo de registro a ser inserido: {message_records[0]}")
            
            for i in range(0, total_records, batch_size):
                batch = message_records[i:i + batch_size]
                self.supabase.table("race_control_messages").insert(batch).execute()
                print(f"Inseridos registros {i+1} a {min(i + batch_size, total_records)} de {total_records}")
                # Pequena pausa para evitar sobrecarga da API
                time.sleep(0.1)
            
            print(f"Todos os {total_records} registros de mensagens de controle foram salvos no banco de dados.")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar dados no banco: {str(e)}")
            if isinstance(e, dict) and 'message' in e:
                print(f"Detalhe do erro: {e['message']}")
            return False
    
    def process(self, meeting_key, session_key):
        """
        Process RaceControlMessages for a specific meeting and session.
        Simplified to only generate CSV and save to database.
        
        Args:
            meeting_key: Key of the meeting (race)
            session_key: Key of the session
            
        Returns:
            dict: Processing results with file paths
        """
        results = {}
        start_time = datetime.now()
        
        print(f"Processando RaceControlMessages para meeting_key={meeting_key}, session_key={session_key}")
        
        # Get the path to the raw data file using key-based structure
        raw_file_path = self.get_raw_file_path(meeting_key, session_key, self.topic_name)
        
        if not raw_file_path.exists():
            print(f"Arquivo de dados brutos não encontrado: {raw_file_path}")
            return results
        
        # Extract timestamped data
        timestamped_data = self.extract_timestamped_data(raw_file_path)
        
        if not timestamped_data:
            print("Nenhum dado encontrado no arquivo bruto")
            return results
        
        # Extract race control messages
        messages = self.extract_messages(timestamped_data)
        
        if not messages:
            print("Nenhuma mensagem de controle de corrida encontrada")
            return results
        
        # Create DataFrame for messages
        messages_df = pd.DataFrame(messages)
        
        # Save the messages to CSV using key-based structure
        csv_path = self.save_to_csv(
            messages_df,
            meeting_key,
            session_key,
            self.topic_name,
            "race_control_messages.csv"
        )
        results["messages_file"] = csv_path
        print(f"Mensagens de controle de corrida salvas em {csv_path}")
        
        # Salvar no banco de dados
        if self.supabase:
            print("Preparando para salvar mensagens de controle no banco...")
            session_id = self.get_session_id_by_keys(meeting_key, session_key)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                
                # Salvar mensagens no banco de dados
                db_success = self.save_to_database(messages_df, session_id)
                results["database_save"] = db_success
            else:
                print("Não foi possível obter o ID da sessão. Os dados não serão salvos no banco.")
                results["database_save"] = False
        else:
            print("Cliente Supabase não inicializado. Os dados não serão salvos no banco.")
            results["database_save"] = False
        
        # Calcular e exibir o tempo de processamento
        end_time = datetime.now()
        process_time = (end_time - start_time).total_seconds()
        print(f"Tempo total de processamento: {process_time:.2f} segundos")
        results["processing_time"] = process_time
        
        return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process RaceControlMessages data from F1 sessions")
    parser.add_argument("--meeting", type=int, required=True, help="Meeting key (corrida)")
    parser.add_argument("--session", type=int, required=True, help="Session key (sessão)")
    
    args = parser.parse_args()
    
    processor = RaceControlMessagesProcessor()
    results = processor.process(args.meeting, args.session)
    
    print("\nProcessamento concluído!")
    print(f"Resultados: {results}")