"""
Main entry point for the F1 Data Analyzer application.
Provides a command-line interface to explore, collect, process and visualize F1 race data.
"""
import argparse
import sys
import os
import subprocess
import importlib
from pathlib import Path
import asyncio

# Adicionar o diretório raiz ao sys.path para importar módulos corretamente
sys.path.insert(0, str(Path(__file__).parent.absolute()))

# Importar classes dos processadores para uso direto
from src.processors.timing_data_processor import TimingDataProcessor
from src.processors.car_data_processor import CarDataProcessor
from src.processors.timing_app_processor import TimingAppProcessor
from src.processors.weather_data_processor import WeatherDataProcessor
from src.processors.current_tyres_processor import CurrentTyresProcessor
from src.processors.driver_list_processor import DriverListProcessor
from src.processors.pit_lane_processor import PitLaneProcessor
from src.processors.position_processor import PositionProcessor
from src.processors.race_control_messages_processor import RaceControlMessagesProcessor
from src.processors.team_radio_processor import TeamRadioProcessor
from src.processors.stint_analyzer import StintAnalyzer

import config

# Mapeamento de nomes de tópicos para suas classes de processador
PROCESSOR_MAP = {
    "TimingData": TimingDataProcessor,
    "CarData.z": CarDataProcessor,
    "TimingAppData": TimingAppProcessor,
    "WeatherData": WeatherDataProcessor,
    "CurrentTyres": CurrentTyresProcessor,
    "DriverList": DriverListProcessor,
    "PitLaneTimeCollection": PitLaneProcessor,
    "Position.z": PositionProcessor,
    "RaceControlMessages": RaceControlMessagesProcessor,
    "TeamRadio": TeamRadioProcessor,
    "StintAnalysis": StintAnalyzer
}

def parse_arguments():
    parser = argparse.ArgumentParser(description="F1 Data Analyzer - Collect and analyze Formula 1 race data")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Explore command
    explore_parser = subparsers.add_parser("explore", help="Explore available F1 data")

    # Collect command
    collect_parser = subparsers.add_parser("collect", help="Collect F1 data using f1_collector.py")
    collect_parser.add_argument("--list", action="store_true", help="List all available races and sessions")
    collect_parser.add_argument("--meeting", type=int, help="Meeting key (race)")
    collect_parser.add_argument("--session", type=int, help="Session key (session)")
    
    # Process command
    process_parser = subparsers.add_parser("process", help="Process collected raw data")
    process_parser.add_argument("--race", type=str, required=True, help="Race name to process")
    process_parser.add_argument("--session", type=str, required=True, help="Session name to process")
    process_parser.add_argument("--topics", nargs="+", help="Specific topics to process (default: all available)")
    
    # Import command
    import_parser = subparsers.add_parser("import", help="Import race data to Supabase")
    
    return parser.parse_args()

def run_f1_explorer():
    """Execute f1_explorer.py to explore available F1 data."""
    print("Explorando dados disponíveis da F1...")
    
    try:
        # Executar o script f1_explorer.py como um processo separado
        result = subprocess.run(["python", "f1_explorer.py"], check=True)
        
        if result.returncode == 0:
            print("Exploração concluída com sucesso.")
            print("Os arquivos index_YEAR.json foram gerados na pasta f1_data_explorer/")
        else:
            print("Exploração falhou.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar f1_explorer.py: {str(e)}")
    except FileNotFoundError:
        print("Arquivo f1_explorer.py não encontrado. Certifique-se de que está no diretório correto.")

def run_f1_collector(args):
    """Execute f1_collector.py to collect F1 data."""
    command = ["python", "f1_collector.py"]
    
    if args.list:
        command.append("--list")
    elif args.meeting and args.session:
        command.extend(["--meeting", str(args.meeting), "--session", str(args.session)])
    else:
        print("Especifique --list para listar as corridas/sessões disponíveis ou forneça --meeting e --session para coletar dados.")
        return
    
    try:
        print(f"Executando: {' '.join(command)}")
        subprocess.run(command, check=True)
        
        if args.list:
            print("\nUse o comando a seguir para coletar dados:")
            print("python main.py collect --meeting [MEETING_KEY] --session [SESSION_KEY]")
        else:
            print("\nColeta de dados concluída.")
            print("\nPara processar os dados coletados, use:")
            print("python main.py process --race [RACE_NAME] --session [SESSION_NAME]")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar f1_collector.py: {str(e)}")
    except FileNotFoundError:
        print("Arquivo f1_collector.py não encontrado. Certifique-se de que está no diretório correto.")

def run_processor(processor_class, meeting_key, session_key):
    """
    Executa um processador específico para processar os dados.
    
    Args:
        processor_class: Classe do processador
        race_name: Nome da corrida
        session_name: Nome da sessão
        
    Returns:
        dict: Resultados do processamento
    """
    try:
        processor = processor_class()
        result = processor.process(meeting_key, session_key)
        return result
    except Exception as e:
        print(f"Erro ao executar o processador {processor_class.__name__}: {str(e)}")
        return None

def detect_available_topics(meeting_key, session_key):
    """
    Detecta quais tópicos estão disponíveis para uma corrida e sessão específicas.
    
    Args:
        race_name: Nome da corrida
        session_name: Nome da sessão
        
    Returns:
        list: Lista de tópicos disponíveis
    """
    raw_dir = config.RAW_DATA_DIR / meeting_key / session_key
    
    if not raw_dir.exists():
        print(f"Diretório de dados brutos não encontrado: {raw_dir}")
        return []
    
    available_topics = []
    
    for file_path in raw_dir.glob("*.jsonStream"):
        topic = file_path.stem
        available_topics.append(topic)
    
    return available_topics

def run_import_to_supabase():
    """Execute import_races_to_supabase.py to import race data to Supabase."""
    print("Importando dados de corridas para o Supabase...")
    
    try:
        # Executar o script como um processo separado
        result = subprocess.run(["python", "import_races_to_supabase.py"], check=True)
        
        if result.returncode == 0:
            print("Importação concluída com sucesso.")
        else:
            print("Importação falhou.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar import_races_to_supabase.py: {str(e)}")
    except FileNotFoundError:
        print("Arquivo import_races_to_supabase.py não encontrado. Certifique-se de que está no diretório correto.")

def process_data(args):
    """Process collected raw data using individual processors."""
    meeting_key = args.race
    session_key = args.session
    specified_topics = args.topics
    
    print(f"Processando dados para {meeting_key}/{session_key}...")
    
    # Detectar tópicos disponíveis
    available_topics = detect_available_topics(meeting_key, session_key)
    
    if not available_topics:
        print("Nenhum dado bruto encontrado para esta corrida/sessão.")
        print("Execute primeiro a coleta de dados:")
        print("python main.py collect --meeting [MEETING_KEY] --session [SESSION_KEY]")
        return
    
    print(f"Tópicos disponíveis: {', '.join(available_topics)}")
    
    # Determinar quais tópicos processar
    topics_to_process = specified_topics if specified_topics else available_topics
    
    # Filtrar apenas tópicos que estão disponíveis
    topics_to_process = [topic for topic in topics_to_process if topic in available_topics]
    
    if not topics_to_process:
        print("Nenhum dos tópicos especificados está disponível.")
        return
    
    # Processar cada tópico
    for topic in topics_to_process:
        print(f"\nProcessando tópico: {topic}")
        
        if topic in PROCESSOR_MAP:
            processor_class = PROCESSOR_MAP[topic]
            print(f"Usando processador: {processor_class.__name__}")
            
            result = run_processor(processor_class, meeting_key, session_key)
            
            if result:
                print(f"Processamento de {topic} concluído com sucesso.")
            else:
                print(f"Processamento de {topic} falhou.")
        else:
            print(f"Não há processador disponível para o tópico: {topic}")
    
    print("\nProcessamento de dados concluído.")
    print(f"Os dados processados estão em: {config.PROCESSED_DATA_DIR / meeting_key / session_key}")
def main():
    """Main entry point for the application"""
    args = parse_arguments()
    
    if args.command == "explore":
        run_f1_explorer()
    elif args.command == "collect":
        run_f1_collector(args)
    elif args.command == "process":
        process_data(args)
    elif args.command == "import":
        run_import_to_supabase()
    else:
        print("Por favor, especifique um comando: explore, collect, process ou import")
        sys.exit(1)

if __name__ == "__main__":
    main()