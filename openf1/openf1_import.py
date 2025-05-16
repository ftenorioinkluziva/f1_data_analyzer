# openf1_import.py - Script principal para importação de dados da API OpenF1
import os
import sys
import argparse
from datetime import datetime

# Importar todos os importadores
from openf1_meetings_importer import MeetingsImporter
from openf1_sessions_importer import SessionsImporter
from openf1_drivers_importer import DriversImporter
from openf1_weather_importer import WeatherImporter
from openf1_car_data_importer import CarDataImporter
from openf1_race_control_importer import RaceControlImporter

def print_header(text):
    """Imprime um cabeçalho formatado."""
    print("\n" + "=" * 80)
    print(f" {text} ".center(80, "="))
    print("=" * 80)

def print_section(text):
    """Imprime um cabeçalho de seção formatado."""
    print("\n" + "-" * 80)
    print(f" {text} ".center(80, "-"))
    print("-" * 80)

def print_summary(title, stats):
    """Imprime um resumo das estatísticas de importação."""
    print(f"\n{title}:")
    for key, value in stats.items():
        if key != "driver_numbers" and key != "error" and key != "success":
            print(f"  {key.replace('_', ' ').title()}: {value}")

def import_year_data(year, options):
    """
    Importa todos os dados de um ano específico.
    
    Args:
        year: Ano para importar os dados
        options: Opções de importação
    
    Returns:
        dict: Estatísticas consolidadas da importação
    """
    print_header(f"Importando dados para o ano {year}")
    
    start_time = datetime.now()
    
    # Inicializar importadores
    meetings_importer = MeetingsImporter()
    sessions_importer = SessionsImporter()
    
    # Importar meetings (eventos)
    print_section("Importando eventos (meetings)")
    meetings_stats = meetings_importer.import_meetings(year, update_existing=options["update"])
    
    # Importar sessões para todos os meetings do ano
    print_section("Importando sessões")
    sessions_stats = sessions_importer.import_sessions(year=year, update_existing=options["update"])
    
    # Se solicitado, importar dados detalhados de cada sessão
    if options["import_details"]:
        print_section("Buscando sessões para importar dados detalhados")
        # Buscar todas as sessões do ano
        sessions_data = sessions_importer.fetch_data("sessions", {"year": year})
        
        for session in sessions_data:
            session_key = session.get("session_key")
            session_name = session.get("session_name", "")
            session_type = session.get("session_type", "")
            
            if session_key:
                print_section(f"Importando dados detalhados para {session_type} - {session_name} (Key: {session_key})")
                
                # Importar pilotos
                if options["import_drivers"]:
                    print_section(f"Importando pilotos para sessão {session_key}")
                    drivers_importer = DriversImporter()
                    drivers_stats = drivers_importer.import_drivers(session_key, update_existing=options["update"])
                    print_summary("Resumo da importação de pilotos", drivers_stats)
                
                # Importar dados meteorológicos
                if options["import_weather"]:
                    print_section(f"Importando dados meteorológicos para sessão {session_key}")
                    weather_importer = WeatherImporter()
                    weather_stats = weather_importer.import_weather(
                        session_key, 
                        limit=options["limit_weather"] if "limit_weather" in options else None
                    )
                    print_summary("Resumo da importação de dados meteorológicos", weather_stats)
                
                # Importar mensagens do controle de corrida
                if options["import_race_control"]:
                    print_section(f"Importando mensagens do controle de corrida para sessão {session_key}")
                    race_control_importer = RaceControlImporter()
                    race_control_stats = race_control_importer.import_race_control(session_key)
                    print_summary("Resumo da importação de mensagens do controle de corrida", race_control_stats)
                
                # Importar telemetria dos carros
                if options["import_car_data"]:
                    print_section(f"Importando telemetria dos carros para sessão {session_key}")
                    car_data_importer = CarDataImporter()
                    car_data_stats = car_data_importer.import_car_data(
                        session_key,
                        driver_numbers=options.get("drivers"),  # Pode ser None para importar todos
                        limit=options["limit_telemetry"] if "limit_telemetry" in options else None
                    )
                    print_summary("Resumo da importação de telemetria dos carros", car_data_stats)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Estatísticas consolidadas
    stats = {
        "year": year,
        "meetings_imported": meetings_stats.get("inserted", 0) + meetings_stats.get("updated", 0),
        "sessions_imported": sessions_stats.get("inserted", 0) + sessions_stats.get("updated", 0),
        "start_time": start_time.strftime("%H:%M:%S"),
        "end_time": end_time.strftime("%H:%M:%S"),
        "duration_seconds": duration.total_seconds(),
        "duration_formatted": str(duration)
    }
    
    print_header(f"Importação do ano {year} concluída em {duration}")
    print_summary("Resumo geral", stats)
    
    return stats

def import_session_data(session_key, options):
    """
    Importa todos os dados de uma sessão específica.
    
    Args:
        session_key: Chave da sessão
        options: Opções de importação
    
    Returns:
        dict: Estatísticas consolidadas da importação
    """
    print_header(f"Importando dados para a sessão {session_key}")
    
    start_time = datetime.now()
    
    # Inicializar importadores
    drivers_importer = DriversImporter()
    weather_importer = WeatherImporter()
    car_data_importer = CarDataImporter()
    race_control_importer = RaceControlImporter()
    
    stats = {
        "drivers_imported": 0,
        "weather_records": 0,
        "telemetry_records": 0,
        "race_control_messages": 0
    }
    
    # Importar pilotos
    if options["import_drivers"]:
        print_section(f"Importando pilotos para sessão {session_key}")
        drivers_stats = drivers_importer.import_drivers(session_key, update_existing=options["update"])
        print_summary("Resumo da importação de pilotos", drivers_stats)
        stats["drivers_imported"] = drivers_stats.get("inserted", 0) + drivers_stats.get("updated", 0)
    
    # Importar dados meteorológicos
    if options["import_weather"]:
        print_section(f"Importando dados meteorológicos para sessão {session_key}")
        weather_stats = weather_importer.import_weather(
            session_key, 
            limit=options["limit_weather"] if "limit_weather" in options else None
        )
        print_summary("Resumo da importação de dados meteorológicos", weather_stats)
        stats["weather_records"] = weather_stats.get("inserted", 0)
    
    # Importar mensagens do controle de corrida
    if options["import_race_control"]:
        print_section(f"Importando mensagens do controle de corrida para sessão {session_key}")
        race_control_stats = race_control_importer.import_race_control(session_key)
        print_summary("Resumo da importação de mensagens do controle de corrida", race_control_stats)
        stats["race_control_messages"] = race_control_stats.get("inserted", 0)
    
    # Importar telemetria dos carros
    if options["import_car_data"]:
        print_section(f"Importando telemetria dos carros para sessão {session_key}")
        car_data_stats = car_data_importer.import_car_data(
            session_key,
            driver_numbers=options.get("drivers"),  # Pode ser None para importar todos
            limit=options["limit_telemetry"] if "limit_telemetry" in options else None
        )
        print_summary("Resumo da importação de telemetria dos carros", car_data_stats)
        stats["telemetry_records"] = car_data_stats.get("inserted", 0)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Adicionar tempo às estatísticas
    stats["session_key"] = session_key
    stats["start_time"] = start_time.strftime("%H:%M:%S")
    stats["end_time"] = end_time.strftime("%H:%M:%S")
    stats["duration_seconds"] = duration.total_seconds()
    stats["duration_formatted"] = str(duration)
    
    print_header(f"Importação da sessão {session_key} concluída em {duration}")
    print_summary("Resumo geral", stats)
    
    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Importar dados da API OpenF1 para o Supabase")
    
    # Grupo de seleção (ano ou sessão)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Ano para importar dados (ex: 2023, 2024, 2025)")
    group.add_argument("--session", type=int, help="Chave da sessão específica para importar dados")
    
    # Opções de importação
    parser.add_argument("--no-update", action="store_true", help="Não atualizar registros existentes")
    parser.add_argument("--no-details", action="store_true", help="Importar apenas meetings e sessões sem dados detalhados")
    
    # Opções para filtrar o que importar
    parser.add_argument("--no-drivers", action="store_true", help="Não importar dados dos pilotos")
    parser.add_argument("--no-weather", action="store_true", help="Não importar dados meteorológicos")
    parser.add_argument("--no-race-control", action="store_true", help="Não importar mensagens do controle de corrida")
    parser.add_argument("--no-car-data", action="store_true", help="Não importar telemetria dos carros")
    
    # Opções de limitação para reduzir o tamanho da importação
    parser.add_argument("--limit-weather", type=int, help="Limitar número de registros meteorológicos")
    parser.add_argument("--limit-telemetry", type=int, help="Limitar número de registros de telemetria por piloto")
    parser.add_argument("--driver", type=int, action="append", dest="drivers", help="Importar apenas pilotos específicos (pode ser usado múltiplas vezes)")
    
    args = parser.parse_args()
    
    # Configurar opções
    options = {
        "update": not args.no_update,
        "import_details": not args.no_details,
        "import_drivers": not args.no_drivers,
        "import_weather": not args.no_weather,
        "import_race_control": not args.no_race_control,
        "import_car_data": not args.no_car_data,
        "limit_weather": args.limit_weather,
        "limit_telemetry": args.limit_telemetry,
        "drivers": args.drivers
    }
    
    # Executar a importação
    try:
        if args.year:
            stats = import_year_data(args.year, options)
        elif args.session:
            stats = import_session_data(args.session, options)
        
        print("\nImportação concluída com sucesso!")
        sys.exit(0)
    except Exception as e:
        print(f"\nERRO: Falha na importação: {str(e)}")
        sys.exit(1)