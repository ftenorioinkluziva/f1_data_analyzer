# f1_data_manager.py - Interface unificada para exploração e importação de dados F1
import os
import sys
import argparse
import subprocess
from datetime import datetime

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

def run_explorer(years=None):
    """
    Executa o f1_explorer.py para explorar dados disponíveis.
    
    Args:
        years: Lista de anos para explorar
        
    Returns:
        bool: True se executado com sucesso, False caso contrário
    """
    print_header("Explorando dados da F1")
    
    try:
        command = ["python", "f1_explorer.py"]
        
        if years:
            command.extend(["--years"] + [str(year) for year in years])
        
        print(f"Executando: {' '.join(command)}")
        result = subprocess.run(command, check=True)
        
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar f1_explorer.py: {str(e)}")
        return False
    except FileNotFoundError:
        print("Erro: Arquivo f1_explorer.py não encontrado. Verifique o diretório atual.")
        return False

def run_importer(args_dict):
    """
    Executa o openf1_import.py com os argumentos fornecidos.
    
    Args:
        args_dict: Dicionário com os argumentos para o importador
        
    Returns:
        bool: True se executado com sucesso, False caso contrário
    """
    print_header("Importando dados da F1 para o Supabase")
    
    try:
        command = ["python", "openf1_import.py"]
        
        # Converter o dicionário em argumentos de linha de comando
        for key, value in args_dict.items():
            if isinstance(value, bool):
                if value:
                    command.append(f"--{key.replace('_', '-')}")
            elif isinstance(value, list):
                for item in value:
                    command.append(f"--{key.replace('_', '-')}")
                    command.append(str(item))
            elif value is not None:
                command.append(f"--{key.replace('_', '-')}")
                command.append(str(value))
        
        print(f"Executando: {' '.join(command)}")
        result = subprocess.run(command, check=True)
        
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar openf1_import.py: {str(e)}")
        return False
    except FileNotFoundError:
        print("Erro: Arquivo openf1_import.py não encontrado. Verifique o diretório atual.")
        return False

def full_workflow(args):
    """
    Executa o fluxo completo: exploração e importação.
    
    Args:
        args: Argumentos da linha de comando
        
    Returns:
        bool: True se executado com sucesso, False caso contrário
    """
    success = True
    start_time = datetime.now()
    
    # Explorar dados disponíveis
    if args.explore:
        years = [args.year] if args.year else None
        success = success and run_explorer(years)
    
    # Importar dados
    if args.import_data and success:
        import_args = {}
        
        # Adicionar ano ou sessão
        if args.session:
            import_args["session"] = args.session
        elif args.year:
            import_args["year"] = args.year
        
        # Adicionar outras opções
        if args.no_update:
            import_args["no_update"] = True
        if args.no_details:
            import_args["no_details"] = True
        if args.no_drivers:
            import_args["no_drivers"] = True
        if args.no_weather:
            import_args["no_weather"] = True
        if args.no_race_control:
            import_args["no_race_control"] = True
        if args.no_car_data:
            import_args["no_car_data"] = True
        if args.limit_weather:
            import_args["limit_weather"] = args.limit_weather
        if args.limit_telemetry:
            import_args["limit_telemetry"] = args.limit_telemetry
        if args.drivers:
            import_args["driver"] = args.drivers
        
        success = success and run_importer(import_args)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print_header(f"Operação concluída em {duration}")
    print(f"Status: {'Sucesso' if success else 'Falha'}")
    
    return success

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gerenciador de dados F1 - Explorar e importar dados da F1")
    
    # Modos de operação
    parser.add_argument("--explore", action="store_true", help="Explorar dados disponíveis")
    parser.add_argument("--import", dest="import_data", action="store_true", help="Importar dados para o Supabase")
    
    # Seleção de dados
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--year", type=int, help="Ano para explorar/importar dados (ex: 2023, 2024, 2025)")
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
    
    # Verificar se pelo menos um modo de operação foi especificado
    if not args.explore and not args.import_data:
        print("Erro: Pelo menos um modo de operação (--explore ou --import) deve ser especificado.")
        parser.print_help()
        sys.exit(1)
    
    # Executar o fluxo completo
    success = full_workflow(args)
    
    # Definir código de saída
    sys.exit(0 if success else 1)