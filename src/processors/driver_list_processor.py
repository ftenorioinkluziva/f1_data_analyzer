"""
Processador de DriverList modificado para relacionar corretamente com Sessions
"""
import pandas as pd
import json
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path
import re
from datetime import datetime
import numpy as np
import time

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
    Also stores data in the database for future analysis.
    """
    
    def __init__(self):
        """Initialize the DriverList processor."""
        super().__init__()
        self.topic_name = "DriverList"
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
    
    def save_drivers_to_database(self, drivers_df, session_id):
        """
        Save the driver information to the database and create session-driver relationships.
        
        Args:
            drivers_df: DataFrame containing driver information
            session_id: ID of the session in the database
            
        Returns:
            dict: Dictionary with driver_number: driver_id mapping
        """
        if not self.supabase or session_id is None:
            return {}
            
        try:
            # Dicionário para mapear driver_number para driver_id para uso posterior
            driver_id_map = {}
            
            # Processar cada piloto
            for _, driver in drivers_df.iterrows():
                # Verificar se o piloto já existe
                driver_query = self.supabase.table("drivers").select("id").eq("driver_number", driver['driver_number']).execute()
                
                driver_record = {
                    "driver_number": driver['driver_number'],
                    "racing_number": driver.get('racing_number'),
                    "full_name": driver.get('full_name', ''),
                    "broadcast_name": driver.get('broadcast_name'),
                    "tla": driver.get('tla'),
                    "team_name": driver.get('team_name'),
                    "team_color": driver.get('team_color'),
                    "first_name": driver.get('first_name'),
                    "last_name": driver.get('last_name'),
                    "reference": driver.get('reference'),
                    "headshot_url": driver.get('headshot_url')
                }
                
                driver_id = None
                
                if not driver_query.data:
                    # Inserir novo piloto
                    insert_result = self.supabase.table("drivers").insert(driver_record).execute()
                    driver_id = insert_result.data[0]["id"]
                    print(f"Piloto inserido: {driver.get('full_name')} (#{driver['driver_number']}), ID: {driver_id}")
                else:
                    # Atualizar piloto existente
                    driver_id = driver_query.data[0]["id"]
                    self.supabase.table("drivers").update(driver_record).eq("id", driver_id).execute()
                    print(f"Piloto atualizado: {driver.get('full_name')} (#{driver['driver_number']}), ID: {driver_id}")
                
                # Armazenar o ID do piloto no mapeamento
                driver_id_map[driver['driver_number']] = driver_id
                
                # Criar/atualizar relação entre piloto e sessão
                session_driver_query = self.supabase.table("session_drivers").select("id").eq("session_id", session_id).eq("driver_id", driver_id).execute()
                
                session_driver_record = {
                    "session_id": session_id,
                    "driver_id": driver_id,
                    "initial_position": driver.get('initial_position')
                }
                
                if not session_driver_query.data:
                    # Inserir nova relação
                    self.supabase.table("session_drivers").insert(session_driver_record).execute()
                    print(f"Relação sessão-piloto inserida para piloto ID: {driver_id}")
                else:
                    # Atualizar relação existente
                    session_driver_id = session_driver_query.data[0]["id"]
                    self.supabase.table("session_drivers").update(session_driver_record).eq("id", session_driver_id).execute()
                    print(f"Relação sessão-piloto atualizada para piloto ID: {driver_id}")
            
            print(f"Informações de {len(driver_id_map)} pilotos salvas/atualizadas no banco de dados.")
            return driver_id_map
            
        except Exception as e:
            print(f"Erro ao salvar pilotos no banco: {str(e)}")
            return {}
    
    def save_positions_to_database(self, positions_df, session_id, driver_id_map):
        """
        Save the driver positions to the database.
        
        Args:
            positions_df: DataFrame containing position updates
            session_id: ID of the session in the database
            driver_id_map: Dictionary mapping driver_number to driver_id
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or session_id is None or not driver_id_map:
            return False
            
        try:
            # Preparar dados para inserção
            position_records = []
            
            for _, row in positions_df.iterrows():
                driver_number = row['driver_number']
                
                # Verificar se temos o driver_id para este driver_number
                if driver_number not in driver_id_map:
                    print(f"Aviso: driver_id não encontrado para driver_number {driver_number}. Pulando registro de posição.")
                    continue
                
                driver_id = driver_id_map[driver_number]
                
                position_record = {
                    "session_id": session_id,
                    "driver_id": driver_id,
                    "timestamp": row['timestamp'],
                    "position": row['position']
                }
                
                position_records.append(position_record)
            
            # Inserir em lotes para evitar problemas com tamanho da requisição
            batch_size = 100
            total_records = len(position_records)
            
            for i in range(0, total_records, batch_size):
                batch = position_records[i:i + batch_size]
                self.supabase.table("driver_positions").insert(batch).execute()
                print(f"Inseridos registros de posição {i} a {min(i + batch_size, total_records)} de {total_records}")
                # Pausa pequena para evitar sobrecarga da API
                time.sleep(0.5)
            
            print(f"Todos os {total_records} registros de posição foram salvos no banco de dados.")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar posições no banco: {str(e)}")
            return False
    
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
                    "racing_number": info.get("RacingNumber"),
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
    
    def create_driver_positions_chart(self, positions_df, driver_info_df, race_name, session_name):
        """
        Create a chart showing driver position changes over time.
        
        Args:
            positions_df: DataFrame containing position updates
            driver_info_df: DataFrame containing driver information
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if positions_df is None or positions_df.empty:
            print("No position data available for visualization")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Convert timestamp to datetime for better chronological sorting
        positions_df['datetime'] = pd.to_datetime(
            positions_df['timestamp'], 
            format='%H:%M:%S.%f', 
            errors='coerce'
        )
        
        # Create a mapping of driver numbers to colors
        driver_colors = {}
        for _, row in driver_info_df.iterrows():
            driver_num = row['driver_number']
            team_color = row['team_color']
            # Convert team_color to proper matplotlib color format
            if team_color and team_color.strip():
                if not team_color.startswith('#'):
                    team_color = f"#{team_color}"
                driver_colors[driver_num] = team_color
            else:
                # Default color if team color not available
                driver_colors[driver_num] = "#CCCCCC"
        
        # Select top drivers (limit to avoid overcrowding)
        top_drivers = positions_df['driver_number'].unique()
        if len(top_drivers) > 10:
            # If more than 10 drivers, select a subset
            # Try to get driver names
            if 'full_name' in driver_info_df.columns:
                # Sort by initial position
                sorted_drivers = driver_info_df.sort_values('initial_position')
                top_drivers = sorted_drivers['driver_number'].head(10).tolist()
            else:
                # Just take the first 10
                top_drivers = top_drivers[:10]
        
        # Create a position chart
        plt.figure(figsize=(14, 10))
        
        # Get start time for X-axis
        start_time = positions_df['datetime'].min()
        
        # Create a mapping of driver numbers to names if available
        driver_names = {}
        if 'full_name' in driver_info_df.columns:
            for _, row in driver_info_df.iterrows():
                driver_names[row['driver_number']] = row['full_name']
        
        # Plot positions for each driver
        for driver in top_drivers:
            driver_data = positions_df[positions_df['driver_number'] == driver]
            if not driver_data.empty:
                # Convert minutes from start
                driver_data['minutes'] = (driver_data['datetime'] - start_time).dt.total_seconds() / 60
                
                # Sort by time
                driver_data = driver_data.sort_values('datetime')
                
                # Get display name
                if driver in driver_names:
                    display_name = driver_names[driver]
                    # Extract just the last name
                    if " " in display_name:
                        display_name = display_name.split(" ")[-1]
                else:
                    display_name = f"Driver #{driver}"
                
                # Plot position
                plt.plot(
                    driver_data['minutes'],
                    driver_data['position'],
                    'o-',
                    label=display_name,
                    color=driver_colors.get(driver, "#CCCCCC"),
                    linewidth=2,
                    markersize=5
                )
        
        # Invert Y-axis so position 1 is at the top
        plt.gca().invert_yaxis()
        
        # Set labels and title
        plt.xlabel('Minutes from Session Start')
        plt.ylabel('Position')
        plt.title(f'Driver Positions - {race_name} - {session_name}')
        plt.grid(True, linestyle='--', alpha=0.7)
        
        # Add legend
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
        
        plt.tight_layout()
        
        # Save the figure
        viz_path = viz_dir / "driver_positions.png"
        plt.savefig(viz_path)
        plt.close()
        
        print(f"Driver positions chart saved to {viz_path}")
        return viz_path
    
    def create_team_distribution_chart(self, driver_info_df, race_name, session_name):
        """
        Create a chart showing the distribution of teams in the session.
        
        Args:
            driver_info_df: DataFrame containing driver information
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if driver_info_df is None or driver_info_df.empty or 'team_name' not in driver_info_df.columns:
            print("No team data available for visualization")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Count drivers per team
        team_counts = driver_info_df['team_name'].value_counts()
        
        # Get team colors
        team_colors = {}
        for team_name in team_counts.index:
            # Get the first color for each team
            team_data = driver_info_df[driver_info_df['team_name'] == team_name]
            if not team_data.empty and 'team_color' in team_data.columns:
                color = team_data.iloc[0]['team_color']
                if color and color.strip():
                    if not color.startswith('#'):
                        color = f"#{color}"
                    team_colors[team_name] = color
                else:
                    team_colors[team_name] = "#CCCCCC"
            else:
                team_colors[team_name] = "#CCCCCC"
        
        # Create a bar chart
        plt.figure(figsize=(12, 8))
        
        # Create bars with team colors
        bars = plt.bar(
            team_counts.index,
            team_counts.values,
            color=[team_colors.get(team, "#CCCCCC") for team in team_counts.index]
        )
        
        # Add count labels on top of bars
        for bar in bars:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2,
                height,
                f'{int(height)}',
                ha='center',
                va='bottom',
                fontweight='bold'
            )
        
        plt.title(f'Team Distribution - {race_name} - {session_name}')
        plt.xlabel('Team')
        plt.ylabel('Number of Drivers')
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        plt.tight_layout()
        
        # Save the figure
        viz_path = viz_dir / "team_distribution.png"
        plt.savefig(viz_path)
        plt.close()
        
        print(f"Team distribution chart saved to {viz_path}")
        return viz_path
    
    def process(self, race_name, session_name):
        """
        Process DriverList data for a specific race and session.
        
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
        
        # Extract driver information
        data = self.extract_driver_info(timestamped_data)
        
        if not data:
            print("No driver data found")
            return results
        
        # Get driver_info and position_updates DataFrames
        driver_info_df = data["driver_info"]
        positions_df = data["position_updates"]
        
        # Save the driver information to CSV
        output_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(output_dir)
        
        # Save driver info
        driver_csv_path = self.save_to_csv(
            driver_info_df,
            race_name,
            session_name,
            self.topic_name,
            "driver_info.csv"
        )
        results["driver_info_file"] = driver_csv_path
        
        # Save position updates if available
        if positions_df is not None and not positions_df.empty:
            positions_csv_path = self.save_to_csv(
                positions_df,
                race_name,
                session_name,
                self.topic_name,
                "position_updates.csv"
            )
            results["position_updates_file"] = positions_csv_path
            
            # Create visualizations
            viz_paths = {}
            
            # Driver positions chart
            positions_viz_path = self.create_driver_positions_chart(
                positions_df,
                driver_info_df,
                race_name,
                session_name
            )
            if positions_viz_path:
                viz_paths["positions_chart"] = positions_viz_path
            
            # Team distribution chart
            team_viz_path = self.create_team_distribution_chart(
                driver_info_df,
                race_name,
                session_name
            )
            if team_viz_path:
                viz_paths["team_distribution"] = team_viz_path
            
            results["visualizations"] = viz_paths
        
        # Salvar no banco de dados
        if self.supabase:
            print("Preparando para salvar dados de pilotos no banco...")
            session_id = self.get_session_id(race_name, session_name)
            
            if session_id:
                print(f"ID da sessão: {session_id}")
                
                # 1. Salvar informações dos pilotos e obter o mapeamento driver_number -> driver_id
                driver_id_map = self.save_drivers_to_database(driver_info_df, session_id)
                results["database_save_drivers"] = bool(driver_id_map)
                
                # 2. Salvar atualizações de posição se disponíveis
                if positions_df is not None and not positions_df.empty and driver_id_map:
                    print("Preparando para salvar atualizações de posição no banco...")
                    
                    # Reduzir a quantidade de dados se necessário
                    # Para análise de posição, geralmente é suficiente ter pontos a cada 10-15 segundos
                    if len(positions_df) > 500:
                        # Amostrar aproximadamente 1 ponto a cada 10-15 segundos
                        sample_size = max(len(positions_df) // 500, 1)
                        positions_sample = positions_df.iloc[::sample_size].copy()
                        print(f"Reduzindo de {len(positions_df)} para {len(positions_sample)} registros de posição para banco de dados")
                    else:
                        positions_sample = positions_df
                    
                    db_success_positions = self.save_positions_to_database(positions_sample, session_id, driver_id_map)
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
        
        return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process DriverList data from F1 sessions")
    parser.add_argument("--race", required=True, help="Race name")
    parser.add_argument("--session", required=True, help="Session name")
    
    args = parser.parse_args()
    
    processor = DriverListProcessor()
    results = processor.process(args.race, args.session)
    
    print("\nProcessamento concluído!")
    print(f"Resultados: {results}")