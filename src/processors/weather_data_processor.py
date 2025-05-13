"""
Processor for WeatherData streams from F1 races.
Simplified version that only generates CSV and stores data in the database.
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

class WeatherDataProcessor(BaseProcessor):
    """
    Simplified Weather Data Processor that focuses on:
    1. Generating CSV files
    2. Storing data in the database
    
    Now with protection against duplicating records.
    """
    
    def __init__(self):
        """Initialize the WeatherData processor."""
        super().__init__()
        self.topic_name = "WeatherData"
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

    def extract_weather_data(self, timestamped_data):
        """
        Extract weather data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            list: List of dictionaries containing weather data
        """
        weather_data = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                
                # Create a weather entry with the timestamp
                weather_entry = data.copy()
                weather_entry["timestamp"] = timestamp
                
                # Convert numeric values
                for key in ["AirTemp", "Humidity", "Pressure", "Rainfall", "TrackTemp", "WindDirection", "WindSpeed"]:
                    if key in weather_entry:
                        try:
                            weather_entry[key] = float(weather_entry[key])
                        except (ValueError, TypeError):
                            # Keep as string if conversion fails
                            pass
                
                weather_data.append(weather_entry)
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1} weather records...")
                    
            except json.JSONDecodeError:
                print(f"Error parsing JSON at timestamp {timestamp}")
                continue
        
        return weather_data
    
    def save_to_database(self, weather_data_df, session_id):
        """
        Save the processed weather data to the database.
        Automatically checks for and removes duplicate records.
        
        Args:
            weather_data_df: DataFrame containing weather data
            session_id: ID of the session in the database
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or session_id is None:
            return False
            
        try:
            # Verificar registros existentes para esta sessão
            print(f"Verificando registros existentes para a sessão ID: {session_id}")
            existing_query = self.supabase.table("weather_data").select("id").eq("session_id", session_id)
            existing_records = existing_query.execute()
            
            # Se já existem registros, remover automaticamente para evitar duplicações
            if existing_records.data:
                existing_count = len(existing_records.data)
                print(f"ATENÇÃO: Encontrados {existing_count} registros existentes para esta sessão.")
                print(f"Removendo registros existentes para evitar duplicação...")
                self.supabase.table("weather_data").delete().eq("session_id", session_id).execute()
                print(f"Removidos {existing_count} registros existentes.")
            
            # Preparar dados para inserção
            db_records = []
            
            # Criar uma coluna de timestamp em formato ISO
            session_date = datetime.now().strftime("%Y-%m-%d")
            
            print(f"Convertendo timestamps com data base: {session_date}")
            
            for _, row in weather_data_df.iterrows():
                # Converter o timestamp para formato ISO
                time_part = row["timestamp"]  # formato: "00:00:17.964"
                iso_timestamp = f"{session_date} {time_part}"
                
                # Converter valores numéricos para tipos corretos
                air_temp = float(row.get("AirTemp")) if row.get("AirTemp") is not None else None
                track_temp = float(row.get("TrackTemp")) if row.get("TrackTemp") is not None else None
                humidity = float(row.get("Humidity")) if row.get("Humidity") is not None else None
                pressure = float(row.get("Pressure")) if row.get("Pressure") is not None else None
                rainfall = float(row.get("Rainfall")) if row.get("Rainfall") is not None else None
                
                try:
                    wind_direction = int(float(row.get("WindDirection"))) if row.get("WindDirection") is not None else None
                except (ValueError, TypeError):
                    wind_direction = None
                    
                try:
                    wind_speed = float(row.get("WindSpeed")) if row.get("WindSpeed") is not None else None
                except (ValueError, TypeError):
                    wind_speed = None
                
                # Criar registro para o banco SEM a coluna 'conditions'
                db_record = {
                    "session_id": session_id,
                    "timestamp": iso_timestamp,
                    "air_temp": air_temp,
                    "track_temp": track_temp,
                    "humidity": humidity,
                    "pressure": pressure,
                    "rainfall": rainfall,
                    "wind_direction": wind_direction,
                    "wind_speed": wind_speed
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
                self.supabase.table("weather_data").insert(batch).execute()
                print(f"Inseridos registros {i+1} a {min(i + batch_size, total_records)} de {total_records}")
            
            print(f"Todos os {total_records} registros meteorológicos foram salvos no banco de dados.")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar dados no banco: {str(e)}")
            if isinstance(e, dict) and 'message' in e:
                print(f"Detalhe do erro: {e['message']}")
            return False
    
    def process(self, meeting_key, session_key, race_name=None, session_name=None):
        """
        Process WeatherData for a specific race and session.
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
        
        print(f"Processando WeatherData para {display_race}/{display_session} (Keys: {meeting_key}/{session_key})")
        
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
        
        # Extract weather data
        weather_data = self.extract_weather_data(timestamped_data)
        
        if not weather_data:
            print("No weather data found")
            return results
        
        # Create a DataFrame with the weather data
        df_weather = pd.DataFrame(weather_data)
        
        # Reorder columns to make timestamp the first column
        if 'timestamp' in df_weather.columns:
            columns = ['timestamp'] + [col for col in df_weather.columns if col != 'timestamp']
            df_weather = df_weather[columns]
        
        # Save the processed data to CSV using key-based structure
        csv_path = self.save_to_csv(
            df_weather,
            meeting_key,
            session_key,
            self.topic_name,
            "weather_data.csv",
            race_name,
            session_name
        )
        results["weather_data_file"] = csv_path
        
        # Salvar no banco de dados
        if self.supabase:
            print("Preparando para salvar dados meteorológicos no banco...")
            session_id = self.get_session_id_by_keys(meeting_key, session_key)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                
                # Salvar todos os registros no banco de dados
                total_records = len(df_weather)
                print(f"Salvando {total_records} registros no banco de dados")
                
                # Usar o DataFrame completo para salvar no banco
                db_success = self.save_to_database(df_weather, session_id)
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
            
            # Extract weather data
            weather_data = self.extract_weather_data(timestamped_data)
            
            if not weather_data:
                print("No weather data found")
                return results
            
            # Create a DataFrame with the weather data
            df_weather = pd.DataFrame(weather_data)
            
            # Reorder columns to make timestamp the first column
            if 'timestamp' in df_weather.columns:
                columns = ['timestamp'] + [col for col in df_weather.columns if col != 'timestamp']
                df_weather = df_weather[columns]
            
            # Save using legacy method
            output_dir = self.processed_dir / race_name / session_name / self.topic_name
            ensure_directory(output_dir)
            
            csv_path = output_dir / "weather_data.csv"
            df_weather.to_csv(csv_path, index=False)
            results["weather_data_file"] = csv_path
            print(f"Dados meteorológicos salvos em: {csv_path}")
            
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
                    print(f"Salvando {len(df_weather)} registros no banco de dados")
                    db_success = self.save_to_database(df_weather, session_id)
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
    
    parser = argparse.ArgumentParser(description="Process WeatherData from F1 sessions")
    parser.add_argument("--meeting", type=int, help="Meeting key (corrida)")
    parser.add_argument("--session", type=int, help="Session key (sessão)")
    # Manter opções legadas para compatibilidade
    parser.add_argument("--race", help="Race name (legado)")
    parser.add_argument("--session-name", dest="session_name", help="Session name (legado)")
    
    args = parser.parse_args()
    
    processor = WeatherDataProcessor()
    
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