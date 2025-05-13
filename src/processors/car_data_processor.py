"""
Processor for CarData.z compressed streams from F1 races.
Simplified version that only generates CSV and stores data in the database.
"""
import pandas as pd
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path
from datetime import datetime
import time
import zlib
import base64
import json

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory
from src.utils.data_decoders import decode_compressed_data

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

class CarDataProcessor(BaseProcessor):
    """
    Process CarData.z compressed streams to extract telemetry data and store in database.
    Simplified version that focuses only on CSV generation and database storage.
    """
    
    def __init__(self):
        """Initialize the CarData processor."""
        super().__init__()
        self.topic_name = "CarData.z"
        self.supabase = self._init_supabase()
    
    def _init_supabase(self):
        """Initialize Supabase client."""
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
    
    def extract_car_telemetry(self, timestamped_data):
        """
        Extract car telemetry data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, compressed_data)
            
        Returns:
            list: List of dictionaries containing car telemetry data
        """
        car_data = []
        
        for i, (timestamp, encoded_data) in enumerate(timestamped_data):
            try:
                # Remove surrounding quotes if present
                if isinstance(encoded_data, str) and encoded_data.startswith('"') and encoded_data.endswith('"'):
                    encoded_data = encoded_data[1:-1]
                
                # Decode the compressed data
                try:
                    # Primeiro, tentar com a função auxiliar
                    data = decode_compressed_data(encoded_data)
                except Exception:
                    # Se falhar, tentar manualmente
                    try:
                        # Decodificar base64 e descomprimir zlib
                        decoded_data = zlib.decompress(base64.b64decode(encoded_data), -zlib.MAX_WBITS)
                        # Converter para JSON
                        data = json.loads(decoded_data)
                    except Exception as e:
                        print(f"Erro ao decodificar dados: {e}")
                        continue
                
                if data and "Entries" in data:
                    for entry in data["Entries"]:
                        if "Utc" in entry and "Cars" in entry:
                            entry_time = entry["Utc"]
                            
                            for driver_number, car_info in entry["Cars"].items():
                                if "Channels" in car_info:
                                    channels = car_info["Channels"]
                                    
                                    car_entry = {
                                        "timestamp": timestamp,
                                        "utc": entry_time,
                                        "driver_number": driver_number,
                                        "rpm": channels.get("0"),
                                        "speed": channels.get("2"),
                                        "gear": channels.get("3"),
                                        "throttle": channels.get("4"),
                                        "brake": channels.get("5"),
                                        "drs": channels.get("45")
                                    }
                                    
                                    car_data.append(car_entry)
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f"Processados {i+1} registros de telemetria...")
            
            except Exception as e:
                print(f"Erro ao processar entrada {i}: {e}")
                continue
        
        return car_data
    
    def save_to_database(self, car_data_df, session_id):
        """
        Save the processed car telemetry data to the database.
        Automatically checks for and removes duplicate records.
        
        Args:
            car_data_df: DataFrame containing car telemetry data
            session_id: ID of the session in the database
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or session_id is None:
            return False
            
        try:
            # Verificar registros existentes para esta sessão
            print(f"Verificando registros existentes para a sessão ID: {session_id}")
            existing_query = self.supabase.table("car_telemetry").select("id").eq("session_id", session_id)
            existing_records = existing_query.execute()
            
            # Se já existem registros, remover automaticamente para evitar duplicações
            if existing_records.data:
                existing_count = len(existing_records.data)
                print(f"ATENÇÃO: Encontrados {existing_count} registros existentes para esta sessão.")
                print(f"Removendo registros existentes para evitar duplicação...")
                self.supabase.table("car_telemetry").delete().eq("session_id", session_id).execute()
                print(f"Removidos {existing_count} registros existentes.")
            
            # Preparar dados para inserção
            db_records = []
            
            # Criar uma coluna de timestamp em formato ISO
            session_date = datetime.now().strftime("%Y-%m-%d")
            
            print(f"Preparando dados para inserção...")
            
            for _, row in car_data_df.iterrows():
                # Converter o timestamp para formato ISO
                time_part = row["timestamp"]  # formato: "00:00:17.964"
                iso_timestamp = f"{session_date} {time_part}"
                
                # Converter valores numéricos para tipos corretos
                try:
                    rpm = int(row.get("rpm")) if row.get("rpm") is not None else None
                except (ValueError, TypeError):
                    rpm = None
                    
                try:
                    speed = int(row.get("speed")) if row.get("speed") is not None else None
                except (ValueError, TypeError):
                    speed = None
                
                try:
                    gear = int(row.get("gear")) if row.get("gear") is not None else None
                except (ValueError, TypeError):
                    gear = None
                
                try:
                    throttle = float(row.get("throttle")) if row.get("throttle") is not None else None
                except (ValueError, TypeError):
                    throttle = None
                
                try:
                    brake = float(row.get("brake")) if row.get("brake") is not None else None
                except (ValueError, TypeError):
                    brake = None
                
                try:
                    drs = int(row.get("drs")) if row.get("drs") is not None else None
                except (ValueError, TypeError):
                    drs = None
                
                # Criar registro para o banco
                db_record = {
                    "session_id": session_id,
                    "timestamp": iso_timestamp,
                    "utc_timestamp": row.get("utc"),
                    "driver_number": row.get("driver_number"),
                    "rpm": rpm,
                    "speed": speed,
                    "gear": gear,
                    "throttle": throttle,
                    "brake": brake,
                    "drs": drs
                }
                
                db_records.append(db_record)
            
            # Inserir em lotes para evitar problemas com tamanho da requisição
            batch_size = 100
            total_records = len(db_records)
            
            # Mostrar exemplo do primeiro registro para verificação
            if db_records:
                print(f"Exemplo de registro a ser inserido: {db_records[0]}")
            
            for i in range(0, total_records, batch_size):
                batch = db_records[i:i + batch_size]
                self.supabase.table("car_telemetry").insert(batch).execute()
                print(f"Inseridos registros {i+1} a {min(i + batch_size, total_records)} de {total_records}")
                # Pequena pausa para evitar sobrecarga da API
                time.sleep(0.1)
            
            print(f"Todos os {total_records} registros de telemetria foram salvos no banco de dados.")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar dados no banco: {str(e)}")
            if isinstance(e, dict) and 'message' in e:
                print(f"Detalhe do erro: {e['message']}")
            return False
    
    def process(self, meeting_key, session_key, race_name=None, session_name=None):
        """
        Process CarData.z for a specific race and session.
        Simplified to only generate CSV and save to database.
        
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
        
        print(f"Processando CarData.z para {display_race}/{display_session} (Keys: {meeting_key}/{session_key})")
        
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
        
        # Extract car telemetry
        car_data = self.extract_car_telemetry(timestamped_data)
        
        if not car_data:
            print("No car telemetry data found")
            return results
        
        # Create a DataFrame with the car data
        df_car_data = pd.DataFrame(car_data)
        
        # Convert numeric columns
        numeric_columns = ['rpm', 'speed', 'gear', 'throttle', 'brake', 'drs']
        for col in numeric_columns:
            df_car_data[col] = pd.to_numeric(df_car_data[col], errors='coerce')
        
        # Save the processed data to CSV using key-based structure
        csv_path = self.save_to_csv(
            df_car_data,
            meeting_key,
            session_key,
            self.topic_name,
            "car_data.csv",
            race_name,
            session_name
        )
        results["car_data_file"] = csv_path
        
        # Split data by driver for easier analysis
        driver_dir = self.get_processed_dir(meeting_key, session_key, self.topic_name) / "drivers"
        driver_dir.mkdir(exist_ok=True, parents=True)
        
        drivers = df_car_data['driver_number'].unique()
        
        for driver in drivers:
            driver_data = df_car_data[df_car_data['driver_number'] == driver]
            driver_file = driver_dir / f"telemetry_driver_{driver}.csv"
            driver_data.to_csv(driver_file, index=False)
            
            if "driver_files" not in results:
                results["driver_files"] = {}
            
            results["driver_files"][driver] = driver_file
        
        # Salvar no banco de dados
        if self.supabase:
            print("Preparando para salvar dados no banco...")
            session_id = self.get_session_id_by_keys(meeting_key, session_key)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                
                # Aqui vamos limitar o número de registros para o banco para evitar sobrecarga
                # Vamos amostrar para ter aproximadamente 1 ponto a cada segundo
                if len(df_car_data) > 3600:  # Se tivermos mais de 1 hora de dados
                    sample_size = max(len(df_car_data) // 3600, 1)
                    car_data_sample = df_car_data.iloc[::sample_size].copy()
                    print(f"Reduzindo de {len(df_car_data)} para {len(car_data_sample)} registros para o banco de dados")
                else:
                    car_data_sample = df_car_data
                
                db_success = self.save_to_database(car_data_sample, session_id)
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
        
        print(f"Processed car telemetry data for {display_race}/{display_session}")
        return results
    
    def process_by_name(self, race_name, session_name):
        """
        Legacy method to process data using race and session names.
        Simplified to match the optimized version.
        
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
            
            # Extract car telemetry
            car_data = self.extract_car_telemetry(timestamped_data)
            
            if not car_data:
                print("No car telemetry data found")
                return results
            
            # Create a DataFrame with the car data
            df_car_data = pd.DataFrame(car_data)
            
            # Convert numeric columns
            numeric_columns = ['rpm', 'speed', 'gear', 'throttle', 'brake', 'drs']
            for col in numeric_columns:
                df_car_data[col] = pd.to_numeric(df_car_data[col], errors='coerce')
            
            # Save using legacy method
            output_dir = self.processed_dir / race_name / session_name / self.topic_name
            ensure_directory(output_dir)
            
            csv_path = output_dir / "car_data.csv"
            df_car_data.to_csv(csv_path, index=False)
            results["car_data_file"] = csv_path
            
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
                    
                    # Limitar registros para o banco
                    if len(df_car_data) > 3600:
                        sample_size = max(len(df_car_data) // 3600, 1)
                        car_data_sample = df_car_data.iloc[::sample_size].copy()
                        print(f"Reduzindo de {len(df_car_data)} para {len(car_data_sample)} registros para o banco de dados")
                    else:
                        car_data_sample = df_car_data
                    
                    db_success = self.save_to_database(car_data_sample, session_id)
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
    
    parser = argparse.ArgumentParser(description="Process CarData.z telemetry data")
    parser.add_argument("--meeting", type=int, help="Meeting key (corrida)")
    parser.add_argument("--session", type=int, help="Session key (sessão)")
    # Manter opções legadas para compatibilidade
    parser.add_argument("--race", help="Race name (legado)")
    parser.add_argument("--session-name", dest="session_name", help="Session name (legado)")
    
    args = parser.parse_args()
    
    processor = CarDataProcessor()
    
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