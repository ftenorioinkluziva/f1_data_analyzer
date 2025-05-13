"""
Processor for CarData.z compressed streams from F1 races.
Processes and stores telemetry data in the database.
"""
import pandas as pd
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path
import time

from src.processors.base_processor import BaseProcessor
from src.utils.data_decoders import decode_compressed_data

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

class CarDataProcessor(BaseProcessor):
    """
    Process CarData.z compressed streams to extract telemetry data and store in database.
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
            return create_client(SUPABASE_URL, SUPABASE_KEY)
        except Exception as e:
            print(f"Erro ao inicializar o cliente Supabase: {str(e)}")
            return None
    
    def get_session_id(self, race_name, session_name):
        """
        Get the session ID from the database based on race name and session name.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            int: Session ID or None if not found
        """
        if not self.supabase:
            return None
            
        try:
            # Primeiro, encontrar a corrida pelo nome
            race_query = self.supabase.table("races").select("id").ilike("name", f"%{race_name}%")
            race_result = race_query.execute()
            
            if not race_result.data:
                print(f"Corrida não encontrada: {race_name}")
                return None
                
            race_id = race_result.data[0]["id"]
            
            # Agora, encontrar a sessão para esta corrida
            session_query = self.supabase.table("sessions").select("id").eq("race_id", race_id).ilike("name", f"%{session_name}%")
            session_result = session_query.execute()
            
            if not session_result.data:
                print(f"Sessão não encontrada: {session_name} para corrida {race_name}")
                return None
                
            return session_result.data[0]["id"]
            
        except Exception as e:
            print(f"Erro ao buscar ID da sessão: {str(e)}")
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
            # Decode the compressed data
            data = decode_compressed_data(encoded_data)
            
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
            if (i + 1) % 500 == 0:
                print(f"Processados {i+1} registros de telemetria...")
        
        return car_data
    
    def save_to_database(self, car_data, session_id):
        """
        Save the processed car telemetry data to the database.
        
        Args:
            car_data: List of car telemetry data dictionaries
            session_id: ID of the session in the database
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or not session_id:
            return False
            
        try:
            # Preparar dados para inserção
            db_records = []
            
            for entry in car_data:
                # Converter RPM, speed e gear para inteiros se possível
                rpm = int(entry["rpm"]) if entry["rpm"] is not None else None
                speed = int(entry["speed"]) if entry["speed"] is not None else None
                gear = int(entry["gear"]) if entry["gear"] is not None else None
                
                # Converter throttle e brake para decimais
                throttle = float(entry["throttle"]) if entry["throttle"] is not None else None
                brake = float(entry["brake"]) if entry["brake"] is not None else None
                drs = int(entry["drs"]) if entry["drs"] is not None else None
                
                db_record = {
                    "timestamp": entry["timestamp"],
                    "utc_timestamp": entry["utc"],
                    "session_id": session_id,
                    "driver_number": entry["driver_number"],
                    "rpm": rpm,
                    "speed": speed,
                    "gear": gear,
                    "throttle": throttle,
                    "brake": brake,
                    "drs": drs
                }
                
                db_records.append(db_record)
            
            # Inserir em lotes de 1000 para evitar problemas com tamanho da requisição
            batch_size = 1000
            total_records = len(db_records)
            
            for i in range(0, total_records, batch_size):
                batch = db_records[i:i + batch_size]
                self.supabase.table("car_telemetry").insert(batch).execute()
                print(f"Inseridos registros {i} a {min(i + batch_size, total_records)} de {total_records}")
                # Pequena pausa para evitar sobrecarga do Supabase
                time.sleep(0.5)
            
            print(f"Todos os {total_records} registros de telemetria foram salvos no banco de dados.")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar dados no banco: {str(e)}")
            return False
    
    def process(self, race_name, session_name):
        """
        Process CarData.z for a specific race and session.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Processing results with file paths
        """
        results = {}
        
        # Get the path to the raw data file
        raw_file_path = self.get_raw_file_path(race_name, session_name, self.topic_name)
        
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
        
        # Save the processed data to CSV
        file_path = self.save_to_csv(
            df_car_data, 
            race_name, 
            session_name, 
            self.topic_name, 
            "car_data.csv"
        )
        results["car_data_file"] = file_path
        
        # Split data by driver for easier analysis
        driver_dir = self.processed_dir / race_name / session_name / self.topic_name / "drivers"
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
            session_id = self.get_session_id(race_name, session_name)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                # Usar uma amostra reduzida para evitar excesso de dados
                # Em produção, talvez queira limitar ou agregar os dados
                sample_size = min(len(car_data), 5000)  # Limitar a 5000 registros
                sample_data = car_data[:sample_size]
                
                db_success = self.save_to_database(sample_data, session_id)
                results["database_save"] = db_success
            else:
                print("Não foi possível obter o ID da sessão. Os dados não serão salvos no banco.")
                results["database_save"] = False
        else:
            print("Cliente Supabase não inicializado. Os dados não serão salvos no banco.")
            results["database_save"] = False
        
        print(f"Processed car telemetry data for {race_name}/{session_name}")
        return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process CarData.z telemetry data")
    parser.add_argument("--race", required=True, help="Race name")
    parser.add_argument("--session", required=True, help="Session name")
    
    args = parser.parse_args()
    
    processor = CarDataProcessor()
    results = processor.process(args.race, args.session)
    
    print("\nProcessamento concluído!")
    print(f"Resultados: {results}")