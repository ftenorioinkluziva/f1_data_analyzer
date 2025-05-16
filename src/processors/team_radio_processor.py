"""
Processor for TeamRadio streams from F1 races.
Updated version with key-based structure and Supabase integration.
"""
import pandas as pd
import json
import re
from pathlib import Path
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

class TeamRadioProcessor(BaseProcessor):
    """
    Process TeamRadio streams to extract and analyze team radio communications during F1 sessions.
    Updated with key-based folder structure and Supabase integration.
    """
    
    def __init__(self):
        """Initialize the TeamRadio processor."""
        super().__init__()
        self.topic_name = "TeamRadio"
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
    
    def custom_extract_timestamped_data(self, file_path):
        """
        Custom extraction method that reads the file line by line.
        
        Args:
            file_path: Path to the raw data file
            
        Returns:
            list: List of tuples containing (timestamp, json_data)
        """
        print(f"Usando extração personalizada para: {file_path}")
        
        try:
            # Ler o arquivo linha a linha
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                lines = [line.strip() for line in f if line.strip()]
            
            print(f"Encontradas {len(lines)} linhas no arquivo")
            
            # Extrair timestamp e JSON de cada linha
            results = []
            for i, line in enumerate(lines):
                # Extrair o timestamp (padrão: 00:00:00.000) e o resto da linha como JSON
                match = re.match(r'^(\d{2}:\d{2}:\d{2}\.\d{3})(.*)$', line)
                if match:
                    timestamp = match.group(1)
                    json_data = match.group(2)
                    results.append((timestamp, json_data))
                else:
                    print(f"Linha {i+1} não corresponde ao padrão esperado: {line[:50]}...")
            
            print(f"Extraídos com sucesso {len(results)} registros")
            return results
            
        except Exception as e:
            print(f"Erro durante extração personalizada: {str(e)}")
            return []
    
    def extract_team_radio_data(self, timestamped_data):
        """
        Extract team radio data from timestamped entries, handling different JSON formats.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            list: List of dictionaries containing team radio data
        """
        radio_messages = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                
                # Check if "Captures" exists in the data
                if "Captures" in data:
                    captures = data["Captures"]
                    
                    # Format 1: Captures is a list/array
                    if isinstance(captures, list):
                        for capture in captures:
                            if "RacingNumber" in capture and "Path" in capture:
                                radio_message = {
                                    "timestamp": timestamp,
                                    "utc_time": capture.get("Utc", ""),
                                    "driver_number": capture["RacingNumber"],
                                    "audio_path": capture["Path"]
                                }
                                radio_messages.append(radio_message)
                    
                    # Format 2: Captures is a dictionary/object
                    elif isinstance(captures, dict):
                        for capture_key, capture_value in captures.items():
                            if isinstance(capture_value, dict) and "RacingNumber" in capture_value and "Path" in capture_value:
                                radio_message = {
                                    "timestamp": timestamp,
                                    "utc_time": capture_value.get("Utc", ""),
                                    "driver_number": capture_value["RacingNumber"],
                                    "audio_path": capture_value["Path"]
                                    
                                }
                                radio_messages.append(radio_message)
                
            except json.JSONDecodeError as e:
                print(f"Erro ao analisar JSON no timestamp {timestamp}: {str(e)}")
                print(f"JSON bruto: {json_str[:100]}...")
                continue
            except Exception as e:
                print(f"Erro inesperado ao processar rádio da session no timestamp {timestamp}: {str(e)}")
                continue
        
        return radio_messages
    
    def save_to_database(self, radio_messages_df, session_id):
            """
            Save the team radio messages to the database.
            
            Args:
                radio_messages_df: DataFrame containing team radio messages
                session_id: ID of the session in the database
                
            Returns:
                bool: True if successful, False otherwise
            """
            if not self.supabase or session_id is None:
                return False
                
            try:
                # Verificar registros existentes para esta sessão
                print(f"Verificando registros existentes para a sessão ID: {session_id}")
                existing_query = self.supabase.table("team_radio").select("id").eq("session_id", session_id)
                existing_records = existing_query.execute()
                
                # Se já existem registros, remover automaticamente para evitar duplicações
                if existing_records.data:
                    existing_count = len(existing_records.data)
                    print(f"ATENÇÃO: Encontrados {existing_count} registros existentes para esta sessão.")
                    print(f"Removendo registros existentes para evitar duplicação...")
                    self.supabase.table("team_radio").delete().eq("session_id", session_id).execute()
                    print(f"Removidos {existing_count} registros existentes.")
                
                # Criar uma coluna de timestamp em formato ISO
                session_date = datetime.now().strftime("%Y-%m-%d")
                
                # Verificar se há dados para inserir
                if radio_messages_df.empty:
                    print("Nenhum dado de rádio para inserir no banco.")
                    return False
                    
                # Preparar dados para inserção
                radio_records = []
                
                for _, row in radio_messages_df.iterrows():
                    # Converter o timestamp para formato ISO
                    time_part = row["timestamp"]  # formato: "00:00:17.964"
                    iso_timestamp = f"{session_date} {time_part}"
                    

                    # Criar registro para o banco
                    radio_record = {
                        "session_id": session_id,
                        "timestamp": iso_timestamp,
                        "utc_time": row.get("utc_time", ""),
                        "driver_number": row.get("driver_number", ""),
                        "audio_path": row.get("audio_path", "")
                    }
                    
                    radio_records.append(radio_record)
                
                # Inserir em lotes para evitar problemas com tamanho da requisição
                batch_size = 100
                total_records = len(radio_records)
                
                # Mostrar exemplo do primeiro registro para verificação
                if radio_records:
                    print(f"Exemplo de registro a ser inserido: {radio_records[0]}")
                
                for i in range(0, total_records, batch_size):
                    batch = radio_records[i:i + batch_size]
                    self.supabase.table("team_radio").insert(batch).execute()
                    print(f"Inseridos registros {i+1} a {min(i + batch_size, total_records)} de {total_records}")
                    # Pequena pausa para evitar sobrecarga da API
                    time.sleep(0.1)
                
                print(f"Todos os {total_records} registros de rádio da session foram salvos no banco de dados.")
                return True
                
            except Exception as e:
                print(f"Erro ao salvar dados no banco: {str(e)}")
                if isinstance(e, dict) and 'message' in e:
                    print(f"Detalhe do erro: {e['message']}")
                return False

    def process(self, meeting_key, session_key, race_name=None, session_name=None):
        """
        Process TeamRadio data for a specific race and session using key-based structure.
        
        Args:
            meeting_key: Key of the meeting (race)
            session_key: Key of the session
            race_name: Optional name of the race for display purposes
            session_name: Optional name of the session for display purposes
            
        Returns:
            dict: Processing results with file paths
        """
        results = {}
        start_time = datetime.now()
        
        # Use race_name and session_name if provided, otherwise use keys for display
        display_race = race_name or f"Meeting {meeting_key}"
        display_session = session_name or f"Session {session_key}"
        
        print(f"Processando TeamRadio para {display_race}/{display_session} (Keys: {meeting_key}/{session_key})")
        
        # Get the path to the raw data file using key-based structure
        raw_file_path = self.get_raw_file_path(meeting_key, session_key, self.topic_name)
        
        if not raw_file_path.exists():
            print(f"Arquivo de dados brutos não encontrado: {raw_file_path}")
            return results
        
        # Mostrar algumas informações sobre o arquivo
        file_size = os.path.getsize(raw_file_path)
        print(f"Processando arquivo TeamRadio: {raw_file_path} (Tamanho: {file_size} bytes)")
        
        # Mostrar primeiras linhas do arquivo para depuração
        try:
            with open(raw_file_path, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                print(f"Primeira linha: {first_line}")
        except Exception as e:
            print(f"Erro ao ler a primeira linha: {str(e)}")
        
        # Use custom extraction method
        timestamped_data = self.custom_extract_timestamped_data(raw_file_path)
        
        if not timestamped_data:
            print("Nenhum dado encontrado no arquivo bruto")
            return results
        
        print(f"Encontrados {len(timestamped_data)} registros brutos em {raw_file_path}")
        
        # Mostrar informações detalhadas sobre o primeiro registro
        if timestamped_data:
            first_timestamp, first_json = timestamped_data[0]
            print(f"Primeiro timestamp: {first_timestamp}")
            print(f"Primeiros dados JSON: {first_json}")
        
        # Extract team radio data
        radio_messages = self.extract_team_radio_data(timestamped_data)
        
        if not radio_messages:
            print("Nenhuma mensagem de rádio da session encontrada")
            return results
        
        print(f"Extraídas {len(radio_messages)} mensagens de rádio da session")
        
        # Count unique driver numbers
        driver_numbers = set(msg.get('driver_number', "") for msg in radio_messages)
        print(f"Encontradas mensagens de {len(driver_numbers)} pilotos únicos: {', '.join(sorted(driver_numbers))}")
        
        # Create DataFrame for radio messages
        radio_messages_df = pd.DataFrame(radio_messages)
        
        # Save the radio messages to CSV using key-based structure
        csv_path = self.save_to_csv(
            radio_messages_df,
            meeting_key,
            session_key,
            self.topic_name,
            "team_radio_messages.csv",
            race_name,
            session_name
        )
        results["team_radio_file"] = csv_path
        
        # Organize messages by driver
        driver_files = {}
        for driver, group in radio_messages_df.groupby('driver_number'):
            driver_csv_path = self.save_to_csv(
                group,
                meeting_key,
                session_key,
                self.topic_name,
                f"driver_{driver}_radio.csv",
                race_name,
                session_name
            )
            driver_files[driver] = driver_csv_path
        
        if driver_files:
            results["driver_files"] = driver_files
        
        # Salvar no banco de dados
        if self.supabase:
            print("Preparando para salvar mensagens de rádio no banco...")
            session_id = self.get_session_id_by_keys(meeting_key, session_key)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                
                # Salvar todas as mensagens de rádio no banco de dados
                db_success = self.save_to_database(radio_messages_df, session_id)
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
    
    def process_by_name(self, race_name, session_name):
        """
        Legacy method to process data using race and session names.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Processing results
        """
        print(f"Usando método legado process_by_name para {race_name}/{session_name}")
        print("Nota: Este método será descontinuado no futuro. Use o método process com meeting_key e session_key.")
        
        # Try to convert names to keys
        mappings = {
            "Miami_Grand_Prix": {
                "key": 1264,
                "sessions": {
                    "Race": 1297,
                    "Qualifying": 1296,
                    "Practice_1": 1295
                }
            }
            # Add other mappings as needed
        }
        
        if race_name in mappings:
            meeting_key = mappings[race_name]["key"]
            session_mappings = mappings[race_name]["sessions"]
            
            if session_name in session_mappings:
                session_key = session_mappings[session_name]
                print(f"Convertendo {race_name}/{session_name} para keys {meeting_key}/{session_key}")
                return self.process(meeting_key, session_key, race_name, session_name)
        
        # Fall back to legacy path-based processing if needed
        legacy_path = self.get_raw_file_path_by_name(race_name, session_name, self.topic_name)
        
        if legacy_path.exists():
            # Simplified legacy processing (similar to new process method)
            start_time = datetime.now()
            results = {}
            
            # Extract timestamped data
            timestamped_data = self.custom_extract_timestamped_data(legacy_path)
            
            if not timestamped_data:
                print("Nenhum dado encontrado no arquivo bruto")
                return results
            
            # Extract team radio data
            radio_messages = self.extract_team_radio_data(timestamped_data)
            
            if not radio_messages:
                print("Nenhuma mensagem de rádio da session encontrada")
                return results
            
            # Create DataFrame for radio messages
            radio_messages_df = pd.DataFrame(radio_messages)
            
            # Save using legacy method
            output_dir = self.processed_dir / race_name / session_name / self.topic_name
            ensure_directory(output_dir)
            
            csv_path = output_dir / "team_radio_messages.csv"
            radio_messages_df.to_csv(csv_path, index=False)
            results["team_radio_file"] = csv_path
            
            # Organize messages by driver
            driver_files = {}
            for driver, group in radio_messages_df.groupby('driver_number'):
                driver_csv_path = output_dir / f"driver_{driver}_radio.csv"
                group.to_csv(driver_csv_path, index=False)
                driver_files[driver] = driver_csv_path
            
            if driver_files:
                results["driver_files"] = driver_files
            
            # Save to database
            if self.supabase:
                # Attempt to get session ID by name
                session_id = None
                try:
                    race_query = self.supabase.table("races").select("id").ilike("name", f"%{race_name}%").execute()
                    if race_query.data:
                        race_id = race_query.data[0]["id"]
                        session_query = self.supabase.table("sessions").select("id").eq("race_id", race_id).ilike("name", f"%{session_name}%").execute()
                        if session_query.data:
                            session_id = session_query.data[0]["id"]
                except Exception as e:
                    print(f"Erro ao buscar sessão: {e}")
                
                if session_id:
                    print(f"ID da sessão: {session_id}")
                    db_success = self.save_to_database(radio_messages_df, session_id)
                    results["database_save"] = db_success
                else:
                    print("Não foi possível obter o ID da sessão. Os dados não serão salvos no banco.")
                    results["database_save"] = False
            
            # Calcular e exibir o tempo de processamento
            end_time = datetime.now()
            process_time = (end_time - start_time).total_seconds()
            print(f"Tempo total de processamento: {process_time:.2f} segundos")
            results["processing_time"] = process_time
            
            return results
        
        print(f"Não foi possível processar {race_name}/{session_name} - arquivo não encontrado e não foi possível converter para chaves")
        return {}

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process TeamRadio data from F1 sessions")
    parser.add_argument("--meeting", type=int, help="Meeting key (corrida)")
    parser.add_argument("--session", type=int, help="Session key (sessão)")
    # Manter opções legadas para compatibilidade
    parser.add_argument("--race", help="Race name (legado)")
    parser.add_argument("--session-name", dest="session_name", help="Session name (legado)")
    
    args = parser.parse_args()
    
    processor = TeamRadioProcessor()
    
    # Verificar se estamos usando a interface baseada em chaves ou a legada
    if args.meeting is not None and args.session is not None:
        # Nova interface baseada em chaves
        results = processor.process(args.meeting, args.session)
    elif args.race and args.session_name:
        # Interface legada baseada em nomes
        results = processor.process_by_name(args.race, args.session_name)
    else:
        print("Erro: Você deve fornecer --meeting e --session (novo formato) ou --race e --session-name (formato legado)")
        exit(1)
    
    print("\nProcessamento concluído!")
    print(f"Resultados: {results}")