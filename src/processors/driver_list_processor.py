"""
Processor for DriverList streams from F1 races.
Simplified version that only generates CSV and stores data in a session-specific model.
"""
import pandas as pd
import json
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path
from datetime import datetime

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

class DriverListProcessor(BaseProcessor):
    """
    Process DriverList streams to extract driver information and position changes during F1 sessions.
    Simplified version that focuses on CSV generation and database storage.
    Uses a session-specific model for driver data to account for team changes between sessions.
    """
    
    def __init__(self):
        """Initialize the DriverList processor."""
        super().__init__()
        self.topic_name = "DriverList"
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

    def extract_driver_info(self, timestamped_data):
        """
        Extract driver information from the first entry in DriverList data.
        Process only the first line which contains full driver details.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            dict: Dict with two items: 'driver_info' (DataFrame with driver details) and 
                  'position_updates' (DataFrame with position changes)
        """
        if not timestamped_data:
            return None
        
        drivers_info = []
        position_updates = []
        
        # Get the first entry which contains full driver information
        first_timestamp, first_json_str = timestamped_data[0]
        
        try:
            # Parse the driver information from the first entry
            driver_data = json.loads(first_json_str)
            
            # Process each driver
            for driver_number, info in driver_data.items():
                # Extract driver details
                driver_info = {
                    "timestamp": first_timestamp,
                    "driver_number": driver_number,
                    "full_name": info.get("FullName"),
                    "broadcast_name": info.get("BroadcastName"),
                    "tla": info.get("Tla"),
                    "team_name": info.get("TeamName"),
                    "team_color": info.get("TeamColour"),
                    "first_name": info.get("FirstName"),
                    "last_name": info.get("LastName"),
                    "reference": info.get("Reference"),
                    "headshot_url": info.get("HeadshotUrl"),
                    "initial_position": info.get("Line")
                }
                drivers_info.append(driver_info)
            
            print(f"Extracted information for {len(drivers_info)} drivers")
            
            # Now process all entries for position updates
            for timestamp, json_str in timestamped_data:
                driver_data = json.loads(json_str)
                
                for driver_number, update in driver_data.items():
                    # If this is just a position update (contains only Line)
                    if "Line" in update and len(update) == 1:
                        position_update = {
                            "timestamp": timestamp,
                            "driver_number": driver_number,
                            "position": update["Line"]
                        }
                        position_updates.append(position_update)
            
            print(f"Extracted {len(position_updates)} position updates")
            
            # Create DataFrames
            drivers_df = pd.DataFrame(drivers_info)
            positions_df = pd.DataFrame(position_updates) if position_updates else None
            
            return {
                "driver_info": drivers_df,
                "position_updates": positions_df
            }
            
        except json.JSONDecodeError:
            print(f"Error parsing driver data JSON")
            return None
        except Exception as e:
            print(f"Error processing driver data: {str(e)}")
            return None
    
    def save_drivers_to_database(self, drivers_df, session_id):
        """
        Save the driver information directly to session_drivers table.
        No longer maintains a separate drivers table.
        
        Args:
            drivers_df: DataFrame containing driver information
            session_id: ID of the session in the database
            
        Returns:
            set: Set of driver_numbers that were successfully saved
        """
        if not self.supabase or session_id is None:
            return set()
            
        try:
            # Conjunto para armazenar driver_numbers salvos com sucesso
            saved_drivers = set()
            
            # Verificar registros existentes para esta sessão
            print(f"Verificando pilotos existentes para a sessão ID: {session_id}")
            existing_query = self.supabase.table("session_drivers").select("id", "driver_number").eq("session_id", session_id)
            existing_records = existing_query.execute()
            
            # Mapear driver_numbers existentes para seus IDs
            existing_drivers = {record['driver_number']: record['id'] for record in existing_records.data} if existing_records.data else {}
            
            if existing_drivers:
                print(f"Encontrados {len(existing_drivers)} pilotos existentes para esta sessão.")
            
            # Processar cada piloto
            for _, driver in drivers_df.iterrows():
                driver_number = driver['driver_number']
                
                # Criar o registro do piloto para a sessão
                driver_record = {
                    "session_id": session_id,
                    "driver_number": driver_number,
                    "full_name": driver.get('full_name', ''),
                    "broadcast_name": driver.get('broadcast_name'),
                    "tla": driver.get('tla'),
                    "team_name": driver.get('team_name'),
                    "team_color": driver.get('team_color'),
                    "first_name": driver.get('first_name'),
                    "last_name": driver.get('last_name'),
                    "reference": driver.get('reference'),
                    "headshot_url": driver.get('headshot_url'),
                    "initial_position": driver.get('initial_position')
                }
                
                # Verificar se o piloto já existe para esta sessão
                if driver_number in existing_drivers:
                    # Atualizar piloto existente
                    driver_id = existing_drivers[driver_number]
                    self.supabase.table("session_drivers").update(driver_record).eq("id", driver_id).execute()
                    print(f"Piloto atualizado: {driver.get('full_name')} (#{driver_number}) para sessão {session_id}")
                else:
                    # Inserir novo piloto para esta sessão
                    insert_result = self.supabase.table("session_drivers").insert(driver_record).execute()
                    print(f"Piloto inserido: {driver.get('full_name')} (#{driver_number}) para sessão {session_id}")
                
                # Marcar este driver_number como processado com sucesso
                saved_drivers.add(driver_number)
            
            print(f"Informações de {len(saved_drivers)} pilotos salvas/atualizadas no banco de dados para a sessão {session_id}.")
            return saved_drivers
            
        except Exception as e:
            print(f"Erro ao salvar pilotos no banco: {str(e)}")
            if isinstance(e, dict) and 'message' in e:
                print(f"Detalhe do erro: {e['message']}")
            return set()
    
    def save_positions_to_database(self, positions_df, session_id, valid_drivers):
        """
        Save the driver positions to the database.
        Now using valid_drivers set instead of driver_id_map.
        
        Args:
            positions_df: DataFrame containing position updates
            session_id: ID of the session in the database
            valid_drivers: Set of driver_numbers that are valid for this session
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or session_id is None or not valid_drivers or positions_df is None or positions_df.empty:
            return False
            
        try:
            # Verificar registros existentes para esta sessão
            print(f"Verificando registros existentes para a sessão ID: {session_id}")
            existing_query = self.supabase.table("driver_positions").select("id").eq("session_id", session_id)
            existing_records = existing_query.execute()
            
            # Se já existem registros, remover automaticamente para evitar duplicações
            if existing_records.data:
                existing_count = len(existing_records.data)
                print(f"ATENÇÃO: Encontrados {existing_count} registros de posição existentes para esta sessão.")
                print(f"Removendo registros existentes para evitar duplicação...")
                self.supabase.table("driver_positions").delete().eq("session_id", session_id).execute()
                print(f"Removidos {existing_count} registros existentes.")
            
            # Preparar dados para inserção
            position_records = []
            session_date = datetime.now().strftime("%Y-%m-%d")
            
            skipped_records = 0
            
            for _, row in positions_df.iterrows():
                driver_number = row['driver_number']
                
                # Verificar se este driver_number é válido para esta sessão
                if driver_number not in valid_drivers:
                    skipped_records += 1
                    continue
                
                # Converter o timestamp para formato ISO
                time_part = row["timestamp"]
                iso_timestamp = f"{session_date} {time_part}"
                
                # Criar o registro
                position_record = {
                    "session_id": session_id,
                    "timestamp": iso_timestamp,
                    "driver_number": driver_number,
                    "position": int(row['position'])
                }
                
                position_records.append(position_record)
            
            if skipped_records > 0:
                print(f"Aviso: {skipped_records} registros de posição foram ignorados por não terem piloto correspondente.")
            
            # Inserir em lotes para evitar problemas com tamanho da requisição
            batch_size = 100
            total_records = len(position_records)
            
            if total_records == 0:
                print("Nenhum registro de posição válido para inserir.")
                return False
                
            print(f"Inserindo {total_records} registros de posição no banco de dados...")
            
            for i in range(0, total_records, batch_size):
                batch = position_records[i:i + batch_size]
                self.supabase.table("driver_positions").insert(batch).execute()
                print(f"Inseridos registros de posição {i+1} a {min(i + batch_size, total_records)} de {total_records}")
            
            print(f"Todos os {total_records} registros de posição foram salvos no banco de dados.")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar posições no banco: {str(e)}")
            if isinstance(e, dict) and 'message' in e:
                print(f"Detalhe do erro: {e['message']}")
            return False
    
    def process(self, meeting_key, session_key, race_name=None, session_name=None):
        """
        Process DriverList data for a specific race and session.
        Simplified to only generate CSV and save data to database.
        
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
        
        print(f"Processando DriverList para {display_race}/{display_session} (Keys: {meeting_key}/{session_key})")
        
        # Get the path to the raw data file using key-based structure
        raw_file_path = self.get_raw_file_path(meeting_key, session_key, self.topic_name)
        
        if not raw_file_path.exists():
            print(f"Raw data file not found: {raw_file_path}")
            return results
        
        # Extract timestamped data
        timestamped_data = self.extract_timestamped_data(raw_file_path)
        
        if not timestamped_data:
            print("No data found in the raw file")
            return results
        
        # Extract driver information
        data = self.extract_driver_info(timestamped_data)
        
        if not data:
            print("No driver data found")
            return results
        
        # Get driver_info and position_updates DataFrames
        driver_info_df = data["driver_info"]
        positions_df = data["position_updates"]
        
        # Save the processed data to CSV using key-based structure
        driver_csv_path = self.save_to_csv(
            driver_info_df,
            meeting_key,
            session_key,
            self.topic_name,
            "driver_info.csv",
            race_name,
            session_name
        )
        results["driver_info_file"] = driver_csv_path
        
        # Save position updates if available
        if positions_df is not None and not positions_df.empty:
            positions_csv_path = self.save_to_csv(
                positions_df,
                meeting_key,
                session_key,
                self.topic_name,
                "position_updates.csv",
                race_name,
                session_name
            )
            results["position_updates_file"] = positions_csv_path
        
        # Salvar no banco de dados
        if self.supabase:
            print("Preparando para salvar dados de pilotos no banco...")
            session_id = self.get_session_id_by_keys(meeting_key, session_key)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                
                # 1. Salvar informações dos pilotos e obter conjunto de driver_numbers válidos
                valid_drivers = self.save_drivers_to_database(driver_info_df, session_id)
                results["database_save_drivers"] = bool(valid_drivers)
                
                # 2. Salvar atualizações de posição se disponíveis
                if positions_df is not None and not positions_df.empty and valid_drivers:
                    print("Preparando para salvar atualizações de posição no banco...")
                    
                    # Amostrar dados para não sobrecarregar o banco
                    if len(positions_df) > 1000:
                        # Amostrar aproximadamente 300-500 pontos por sessão
                        sample_size = max(len(positions_df) // 500, 1)
                        positions_sample = positions_df.iloc[::sample_size].copy()
                        print(f"Reduzindo de {len(positions_df)} para {len(positions_sample)} registros de posição para banco de dados")
                    else:
                        positions_sample = positions_df
                    
                    db_success_positions = self.save_positions_to_database(positions_sample, session_id, valid_drivers)
                    results["database_save_positions"] = db_success_positions
                else:
                    print("Não foi possível salvar as posições no banco.")
                    results["database_save_positions"] = False
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
            timestamped_data = self.extract_timestamped_data(legacy_path)
            
            if not timestamped_data:
                print("No data found in the raw file")
                return results
            
            # Extract driver information
            data = self.extract_driver_info(timestamped_data)
            
            if not data:
                print("No driver data found")
                return results
            
            # Get driver_info and position_updates DataFrames
            driver_info_df = data["driver_info"]
            positions_df = data["position_updates"]
            
            # Save using legacy method
            output_dir = self.processed_dir / race_name / session_name / self.topic_name
            ensure_directory(output_dir)
            
            # Save driver info
            driver_csv_path = output_dir / "driver_info.csv"
            driver_info_df.to_csv(driver_csv_path, index=False)
            results["driver_info_file"] = driver_csv_path
            
            # Save position updates if available
            if positions_df is not None and not positions_df.empty:
                positions_csv_path = output_dir / "position_updates.csv"
                positions_df.to_csv(positions_csv_path, index=False)
                results["position_updates_file"] = positions_csv_path
            
            # Save to database
            if self.supabase:
                # Get session_id by name
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
                    
                    # 1. Salvar drivers e obter conjunto de driver_numbers válidos
                    valid_drivers = self.save_drivers_to_database(driver_info_df, session_id)
                    results["database_save_drivers"] = bool(valid_drivers)
                    
                    # 2. Salvar posições se disponíveis
                    if positions_df is not None and not positions_df.empty and valid_drivers:
                        if len(positions_df) > 1000:
                            sample_size = max(len(positions_df) // 500, 1)
                            positions_sample = positions_df.iloc[::sample_size].copy()
                            print(f"Reduzindo de {len(positions_df)} para {len(positions_sample)} registros de posição")
                        else:
                            positions_sample = positions_df
                        
                        db_success = self.save_positions_to_database(positions_sample, session_id, valid_drivers)
                        results["database_save_positions"] = db_success
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
    
    parser = argparse.ArgumentParser(description="Process DriverList data from F1 sessions")
    parser.add_argument("--meeting", type=int, help="Meeting key (corrida)")
    parser.add_argument("--session", type=int, help="Session key (sessão)")
    # Manter opções legadas para compatibilidade
    parser.add_argument("--race", help="Race name (legado)")
    parser.add_argument("--session-name", dest="session_name", help="Session name (legado)")
    
    args = parser.parse_args()
    
    processor = DriverListProcessor()
    
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