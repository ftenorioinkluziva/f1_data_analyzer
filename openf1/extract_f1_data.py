# extract_f1_data.py - Extrai dados da API OpenF1 para arquivos JSON sem modificar o banco de dados
import os
import sys
import json
import argparse
import requests
from pathlib import Path
from datetime import datetime

# URL base da API OpenF1
OPENF1_API_URL = "https://api.openf1.org/v1"

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

def fetch_data(endpoint, params=None):
    """
    Busca dados da API OpenF1.
    
    Args:
        endpoint: Endpoint da API (meetings, sessions, etc)
        params: Parâmetros da consulta (dict)
        
    Returns:
        list: Lista de dados ou lista vazia em caso de erro
    """
    url = f"{OPENF1_API_URL}/{endpoint}"
    
    try:
        print(f"Buscando dados de {url} com parâmetros: {params}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Recebidos {len(data)} registros de {endpoint}")
            return data
        else:
            print(f"Erro ao acessar {url}: Status {response.status_code}")
            return []
    except Exception as e:
        print(f"Erro ao buscar dados de {endpoint}: {str(e)}")
        return []

def extract_meetings(year, output_file=None):
    """
    Extrai dados de meetings (eventos) de um ano específico e salva em um arquivo JSON.
    
    Args:
        year: Ano para extrair os eventos
        output_file: Caminho para o arquivo de saída (opcional)
        
    Returns:
        list: Lista de meetings extraídos
    """
    print_header(f"Extraindo meetings para o ano {year}")
    
    # Buscar dados dos meetings
    meetings_data = fetch_data("meetings", {"year": year})
    
    if not meetings_data:
        print("Nenhum meeting encontrado para o ano especificado.")
        return []
    
    # Definir nome do arquivo de saída se não foi fornecido
    if not output_file:
        output_dir = Path("f1_data_json")
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"meetings_{year}.json"
    
    # Salvar dados em arquivo JSON
    try:
        with open(output_file, 'w') as f:
            json.dump(meetings_data, f, indent=2)
        print(f"Dados de {len(meetings_data)} meetings salvos em {output_file}")
    except Exception as e:
        print(f"Erro ao salvar arquivo JSON: {str(e)}")
    
    return meetings_data

def extract_sessions(year=None, meeting_key=None, output_file=None):
    """
    Extrai dados de sessões para um ano ou meeting específico e salva em um arquivo JSON.
    
    Args:
        year: Ano para extrair as sessões (opcional)
        meeting_key: Chave do meeting para extrair sessões (opcional)
        output_file: Caminho para o arquivo de saída (opcional)
        
    Returns:
        list: Lista de sessões extraídas
    """
    if not year and not meeting_key:
        print("Erro: É necessário fornecer year ou meeting_key")
        return []
    
    # Definir parâmetros da consulta
    params = {}
    if year:
        params["year"] = year
        print_header(f"Extraindo sessões para o ano {year}")
    elif meeting_key:
        params["meeting_key"] = meeting_key
        print_header(f"Extraindo sessões para o meeting {meeting_key}")
    
    # Buscar dados das sessões
    sessions_data = fetch_data("sessions", params)
    
    if not sessions_data:
        print("Nenhuma sessão encontrada para os parâmetros especificados.")
        return []
    
    # Definir nome do arquivo de saída se não foi fornecido
    if not output_file:
        output_dir = Path("f1_data_json")
        output_dir.mkdir(exist_ok=True)
        
        if year:
            output_file = output_dir / f"sessions_{year}.json"
        else:
            output_file = output_dir / f"sessions_meeting_{meeting_key}.json"
    
    # Salvar dados em arquivo JSON
    try:
        with open(output_file, 'w') as f:
            json.dump(sessions_data, f, indent=2)
        print(f"Dados de {len(sessions_data)} sessões salvos em {output_file}")
    except Exception as e:
        print(f"Erro ao salvar arquivo JSON: {str(e)}")
    
    return sessions_data

def extract_all_sessions_for_meetings(meetings, output_dir=None):
    """
    Extrai dados de sessões para cada meeting na lista fornecida.
    
    Args:
        meetings: Lista de meetings para extrair sessões
        output_dir: Diretório para salvar os arquivos JSON (opcional)
        
    Returns:
        dict: Dicionário de sessões por meeting_key
    """
    if not meetings:
        print("Nenhum meeting fornecido para extrair sessões.")
        return {}
    
    # Definir diretório de saída se não foi fornecido
    if not output_dir:
        output_dir = Path("f1_data_json/sessions")
    
    # Criar diretório se não existir
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print_header(f"Extraindo sessões para {len(meetings)} meetings")
    
    # Dicionário para armazenar sessões por meeting_key
    all_sessions = {}
    
    # Extrair sessões para cada meeting
    for meeting in meetings:
        meeting_key = meeting.get("meeting_key")
        meeting_name = meeting.get("meeting_name", "Unknown")
        
        if not meeting_key:
            continue
        
        print_section(f"Extraindo sessões para {meeting_name} (Key: {meeting_key})")
        
        # Definir arquivo de saída para as sessões deste meeting
        output_file = output_dir / f"sessions_meeting_{meeting_key}.json"
        
        # Extrair sessões
        sessions = extract_sessions(meeting_key=meeting_key, output_file=output_file)
        
        if sessions:
            all_sessions[meeting_key] = sessions
    
    return all_sessions

def extract_session_details(session_key, output_dir=None, endpoints=None):
    """
    Extrai detalhes específicos de uma sessão (pilotos, tempos, etc).
    
    Args:
        session_key: Chave da sessão
        output_dir: Diretório para salvar os arquivos JSON (opcional)
        endpoints: Lista de endpoints específicos para extrair (opcional)
        
    Returns:
        dict: Dicionário com dados extraídos por endpoint
    """
    # Lista padrão de endpoints a extrair se não especificado
    if not endpoints:
        endpoints = ["drivers", "weather", "race_control", "car_data"]
    
    # Definir diretório de saída se não foi fornecido
    if not output_dir:
        output_dir = Path(f"f1_data_json/session_{session_key}")
    
    # Criar diretório se não existir
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print_header(f"Extraindo detalhes para a sessão {session_key}")
    
    # Dicionário para armazenar dados por endpoint
    session_data = {}
    
    # Extrair dados de cada endpoint solicitado
    for endpoint in endpoints:
        print_section(f"Extraindo dados de {endpoint}")
        
        # Alguns endpoints podem precisar de parâmetros adicionais
        params = {"session_key": session_key}
        
        # Buscar dados
        data = fetch_data(endpoint, params)
        
        if data:
            # Salvar em arquivo JSON
            output_file = output_dir / f"{endpoint}.json"
            try:
                with open(output_file, 'w') as f:
                    json.dump(data, f, indent=2)
                print(f"Dados de {endpoint} salvos em {output_file}")
            except Exception as e:
                print(f"Erro ao salvar arquivo JSON para {endpoint}: {str(e)}")
            
            session_data[endpoint] = data
        else:
            print(f"Nenhum dado de {endpoint} encontrado para a sessão {session_key}")
    
    return session_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrair dados da API OpenF1 para arquivos JSON")
    
    # Grupo de comandos
    subparsers = parser.add_subparsers(dest="command", help="Comando a executar")
    
    # Comando meetings
    meetings_parser = subparsers.add_parser("meetings", help="Extrair dados de meetings/eventos")
    meetings_parser.add_argument("--year", type=int, required=True, help="Ano para extrair os meetings")
    meetings_parser.add_argument("--output", help="Caminho para o arquivo de saída")
    
    # Comando sessions
    sessions_parser = subparsers.add_parser("sessions", help="Extrair dados de sessões")
    sessions_group = sessions_parser.add_mutually_exclusive_group(required=True)
    sessions_group.add_argument("--year", type=int, help="Ano para extrair as sessões")
    sessions_group.add_argument("--meeting", type=int, dest="meeting_key", help="Chave do meeting para extrair sessões")
    sessions_parser.add_argument("--output", help="Caminho para o arquivo de saída")
    
    # Comando all_sessions
    all_sessions_parser = subparsers.add_parser("all_sessions", help="Extrair sessões para todos os meetings de um ano")
    all_sessions_parser.add_argument("--year", type=int, required=True, help="Ano para extrair todas as sessões")
    all_sessions_parser.add_argument("--output-dir", help="Diretório para salvar os arquivos JSON")
    
    # Comando session_details
    details_parser = subparsers.add_parser("session_details", help="Extrair detalhes de uma sessão específica")
    details_parser.add_argument("--session", type=int, dest="session_key", required=True, help="Chave da sessão")
    details_parser.add_argument("--output-dir", help="Diretório para salvar os arquivos JSON")
    details_parser.add_argument("--endpoints", nargs="+", help="Lista de endpoints específicos para extrair")
    
    args = parser.parse_args()
    
    # Executar o comando apropriado
    if args.command == "meetings":
        extract_meetings(args.year, args.output)
    elif args.command == "sessions":
        extract_sessions(args.year, args.meeting_key, args.output)
    elif args.command == "all_sessions":
        # Primeiro extrair os meetings
        meetings = extract_meetings(args.year)
        if meetings:
            # Depois extrair as sessões para cada meeting
            extract_all_sessions_for_meetings(meetings, args.output_dir)
    elif args.command == "session_details":
        extract_session_details(args.session_key, args.output_dir, args.endpoints)
    else:
        parser.print_help()