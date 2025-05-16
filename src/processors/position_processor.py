"""
Processor for Position.z compressed streams from F1 races.
Simplified version that only generates CSV and stores data in the database.
"""
import pandas as pd
import json
import zlib
import base64
import os
import time
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path
from datetime import datetime

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory
from src.utils.data_decoders import decode_compressed_data

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

class PositionProcessor(BaseProcessor):
    """
    Process Position.z compressed streams to extract track positions.
    Simplified version focused on CSV generation and database storage only.
    """
    
    def __init__(self):
        """Initialize the Position processor."""
        super().__init__()
        self.topic_name = "Position.z"
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
    
    def extract_position_data(self, timestamped_data):
        """
        Extract position data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, compressed_data)
            
        Returns:
            list: List of dictionaries containing position data
        """
        position_data = []
        
        for i, (timestamp, encoded_data) in enumerate(timestamped_data):
            try:
                # Remove surrounding quotes if present
                if encoded_data.startswith('"') and encoded_data.endswith('"'):
                    encoded_data = encoded_data[1:-1]
                
                # Decode base64 and decompress zlib
                try:
                    # First, try with helper function
                    data = decode_compressed_data(encoded_data)
                except Exception:
                    # If it fails, try manually
                    try:
                        # Decode base64 and decompress zlib
                        decoded_data = zlib.decompress(base64.b64decode(encoded_data), -zlib.MAX_WBITS)
                        # Convert to JSON
                        data = json.loads(decoded_data)
                    except Exception as e:
                        print(f"Error decoding data: {e}")
                        continue
                
                # Extract position entries
                if "Position" in data:
                    for entry in data["Position"]:
                        if "Timestamp" in entry and "Entries" in entry:
                            entry_time = entry["Timestamp"]
                            
                            for driver_number, coords in entry["Entries"].items():
                                position_entry = {
                                    "timestamp": timestamp,
                                    "utc": entry_time,
                                    "driver_number": driver_number,
                                    "x": coords.get("X"),
                                    "y": coords.get("Y"),
                                    "z": coords.get("Z")
                                }
                                
                                position_data.append(position_entry)
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1} position records...")
                    
            except Exception as e:
                print(f"Error processing position data at timestamp {timestamp}: {str(e)}")
                continue
        
        return position_data
    
    def save_to_database(self, position_data_df, session_id):
        """
        Save the position data to the database.
        
        Args:
            position_data_df: DataFrame containing position data
            session_id: ID of the session in the database
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or session_id is None:
            return False
            
        try:
            # Verificar registros existentes para esta sessão
            print(f"Verificando registros existentes para a sessão ID: {session_id}")
            existing_query = self.supabase.table("car_positions").select("id").eq("session_id", session_id)
            existing_records = existing_query.execute()
            
            # Se já existem registros, remover automaticamente para evitar duplicações
            if existing_records.data:
                existing_count = len(existing_records.data)
                print(f"ATENÇÃO: Encontrados {existing_count} registros existentes para esta sessão.")
                print(f"Removendo registros existentes para evitar duplicação...")
                self.supabase.table("car_positions").delete().eq("session_id", session_id).execute()
                print(f"Removidos {existing_count} registros existentes.")
            
            # Verificar se há dados para inserir
            if position_data_df.empty:
                print("Nenhum dado de posição para inserir no banco.")
                return False
                
            # Aqui vamos reduzir o volume de dados para não sobrecarregar o banco
            # Para posições, uma abordagem comum é amostrar os dados
            if len(position_data_df) > 5000:
                # Reduzir para aproximadamente 5000 pontos
                sample_size = max(len(position_data_df) // 5000, 1)
                sampled_df = position_data_df.iloc[::sample_size].copy()
                print(f"Reduzindo de {len(position_data_df)} para {len(sampled_df)} registros para o banco")
                position_data_df = sampled_df
            
            # Criar uma coluna de timestamp em formato ISO
            session_date = datetime.now().strftime("%Y-%m-%d")
            
            # Preparar dados para inserção
            position_records = []
            
            for _, row in position_data_df.iterrows():
                # Converter o timestamp para formato ISO
                time_part = row.get("timestamp", "00:00:00.000")  # formato: "00:00:17.964"
                if isinstance(time_part, str) and ':' in time_part:
                    iso_timestamp = f"{session_date} {time_part}"
                else:
                    # Se o timestamp não estiver no formato esperado, usar o horário atual
                    iso_timestamp = datetime.now().isoformat()
                
                # Tratar valores NaN
                x = None if pd.isna(row.get("x")) else row.get("x")
                y = None if pd.isna(row.get("y")) else row.get("y")
                z = None if pd.isna(row.get("z")) else row.get("z")
                utc = None if pd.isna(row.get("utc")) else row.get("utc")
                
                # Converter coordenadas para números float se possível
                try:
                    x = float(x) if x is not None else None
                    y = float(y) if y is not None else None
                    z = float(z) if z is not None else None
                except (ValueError, TypeError):
                    pass
                
                # Criar registro para o banco
                position_record = {
                    "session_id": session_id,
                    "timestamp": iso_timestamp,
                    "utc_time": utc,
                    "driver_number": row.get("driver_number", ""),
                    "x_coord": x,
                    "y_coord": y,
                    "z_coord": z
                }
                
                position_records.append(position_record)
            
            # Inserir em lotes para evitar problemas com tamanho da requisição
            batch_size = 100
            total_records = len(position_records)
            
            # Mostrar exemplo do primeiro registro para verificação
            if position_records:
                print(f"Exemplo de registro a ser inserido: {position_records[0]}")
            
            for i in range(0, total_records, batch_size):
                batch = position_records[i:i + batch_size]
                self.supabase.table("car_positions").insert(batch).execute()
                print(f"Inseridos registros {i+1} a {min(i + batch_size, total_records)} de {total_records}")
                # Pequena pausa para evitar sobrecarga da API
                time.sleep(0.1)
            
            print(f"Todos os {total_records} registros de posição foram salvos no banco de dados.")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar dados no banco: {str(e)}")
            if isinstance(e, dict) and 'message' in e:
                print(f"Detalhe do erro: {e['message']}")
            return False
    
    def process(self, meeting_key, session_key):
        """
        Process Position.z data for a specific meeting and session.
        Simplified to only generate CSV and save to database.
        
        Args:
            meeting_key: Key of the meeting (race)
            session_key: Key of the session
            
        Returns:
            dict: Processing results with file paths
        """
        results = {}
        start_time = datetime.now()
        
        print(f"Processando Position.z para meeting_key={meeting_key}, session_key={session_key}")
        
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
        
        # Extract position data
        position_data = self.extract_position_data(timestamped_data)
        
        if not position_data:
            print("Nenhum dado de posição encontrado")
            return results
        
        # Create DataFrame with the position data
        position_data_df = pd.DataFrame(position_data)
        
        # Save the processed data to CSV
        csv_path = self.save_to_csv(
            position_data_df,
            meeting_key,
            session_key,
            self.topic_name,
            "position_data.csv"
        )
        results["position_data_file"] = csv_path
        
        # Split data by driver for easier analysis
        driver_dir = self.get_processed_dir(meeting_key, session_key, self.topic_name) / "drivers"
        driver_dir.mkdir(exist_ok=True, parents=True)
        
        drivers = position_data_df['driver_number'].unique()
        
        for driver in drivers:
            driver_data = position_data_df[position_data_df['driver_number'] == driver]
            driver_file = driver_dir / f"position_driver_{driver}.csv"
            driver_data.to_csv(driver_file, index=False)
            
            if "driver_files" not in results:
                results["driver_files"] = {}
            
            results["driver_files"][driver] = driver_file
        
        # Salvar no banco de dados
        if self.supabase:
            print("Preparando para salvar dados de posição no banco...")
            session_id = self.get_session_id_by_keys(meeting_key, session_key)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                
                # Salvar posições no banco de dados
                db_success = self.save_to_database(position_data_df, session_id)
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
    
    parser = argparse.ArgumentParser(description="Process Position.z data from F1 sessions")
    parser.add_argument("--meeting", type=int, required=True, help="Meeting key (corrida)")
    parser.add_argument("--session", type=int, required=True, help="Session key (sessão)")
    
    args = parser.parse_args()
    
    processor = PositionProcessor()
    results = processor.process(args.meeting, args.session)
    
    print("\nProcessamento concluído!")
    print(f"Resultados: {results}")