# f1_explorer.py (versão atualizada para usar a API OpenF1)
import requests
import json
import time
from pathlib import Path
import argparse

def check_url_exists(url):
    """
    Verifica se uma URL existe e retorna o conteúdo se for acessível.
    
    Args:
        url: URL a ser verificada
    
    Returns:
        tuple: (status_boolean, content ou None)
    """
    try:
        print(f"Verificando URL: {url}")
        response = requests.get(url)
        status = response.status_code
        print(f"Status: {status}")
        return status == 200, response.content
    except Exception as e:
        print(f"Erro ao acessar URL: {str(e)}")
        return False, None

def fetch_meetings(year):
    """
    Busca dados dos eventos (meetings) de F1 para um determinado ano na API OpenF1.
    
    Args:
        year: Ano para o qual buscar os eventos
    
    Returns:
        list: Lista de eventos ou lista vazia em caso de erro
    """
    url = f"https://api.openf1.org/v1/meetings?year={year}"
    print(f"Buscando meetings para o ano {year}...")
    
    success, content = check_url_exists(url)
    if not success or not content:
        print(f"Falha ao buscar meetings para o ano {year}")
        return []
    
    try:
        # Decodificar o conteúdo JSON
        meetings_data = json.loads(content)
        print(f"Encontrados {len(meetings_data)} eventos para o ano {year}")
        return meetings_data
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar dados JSON de meetings: {str(e)}")
        return []

def fetch_sessions(meeting_key):
    """
    Busca dados das sessões para um determinado evento (meeting) na API OpenF1.
    
    Args:
        meeting_key: Chave do evento para o qual buscar as sessões
    
    Returns:
        list: Lista de sessões ou lista vazia em caso de erro
    """
    url = f"https://api.openf1.org/v1/sessions?meeting_key={meeting_key}"
    print(f"Buscando sessões para o meeting_key {meeting_key}...")
    
    success, content = check_url_exists(url)
    if not success or not content:
        print(f"Falha ao buscar sessões para o meeting_key {meeting_key}")
        return []
    
    try:
        # Decodificar o conteúdo JSON
        sessions_data = json.loads(content)
        print(f"Encontradas {len(sessions_data)} sessões para o meeting_key {meeting_key}")
        return sessions_data
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar dados JSON de sessões: {str(e)}")
        return []

def explore_f1_data(years=None, output_format="json"):
    """
    Explora dados de F1 para os anos especificados e salva em arquivos.
    
    Args:
        years: Lista de anos para explorar. Se None, usa o ano atual.
        output_format: Formato de saída dos dados ("json" ou "csv")
    
    Returns:
        dict: Resumo dos dados coletados
    """
    output_dir = Path("f1_data_explorer")
    output_dir.mkdir(exist_ok=True)
    
    # Definir anos para explorar
    if not years:
        import datetime
        current_year = datetime.datetime.now().year
        years = [current_year]
    
    # Armazenar resultados
    results = {
        "years_processed": [],
        "meetings_found": 0,
        "sessions_found": 0
    }
    
    # Processar cada ano
    for year in years:
        print(f"\n{'='*50}")
        print(f"Processando ano: {year}")
        print(f"{'='*50}")
        
        # Buscar meetings para o ano
        meetings_data = fetch_meetings(year)
        
        if not meetings_data:
            print(f"Nenhum meeting encontrado para o ano {year}")
            continue
        
        # Salvar dados dos meetings
        meetings_file = output_dir / f"meetings_{year}.json"
        with open(meetings_file, "w") as f:
            json.dump(meetings_data, f, indent=2)
        
        print(f"Dados de meetings para o ano {year} salvos em {meetings_file}")
        
        # Criar dicionário para armazenar todos os dados (meetings + sessions)
        all_data = {
            "Year": year,
            "Meetings": []
        }
        
        # Processar cada meeting e buscar suas sessões
        for meeting in meetings_data:
            meeting_key = meeting.get("meeting_key")
            meeting_name = meeting.get("meeting_name", "Unknown")
            country_name = meeting.get("country_name", "Unknown")
            
            print(f"\nProcessando meeting: {meeting_name} ({country_name}) - Key: {meeting_key}")
            
            # Converter meeting para o formato esperado pelos scripts existentes
            formatted_meeting = {
                "Key": meeting.get("meeting_key"),
                "Code": meeting.get("meeting_code"),
                "Number": meeting.get("meeting_number", 0),
                "Name": meeting.get("meeting_name", ""),
                "OfficialName": meeting.get("meeting_official_name", ""),
                "Location": meeting.get("location", ""),
                "Country": {
                    "Key": meeting.get("country_key"),
                    "Code": meeting.get("country_code"),
                    "Name": meeting.get("country_name", "")
                },
                "Circuit": {
                    "Key": meeting.get("circuit_key"),
                    "ShortName": meeting.get("circuit_short_name", "")
                },
                "Sessions": []
            }
            
            # Buscar sessões para o meeting
            if meeting_key:
                sessions_data = fetch_sessions(meeting_key)
                
                # Processar cada sessão
                for session in sessions_data:
                    session_key = session.get("session_key")
                    session_name = session.get("session_name", "Unknown")
                    session_type = session.get("session_type", "Unknown")
                    
                    print(f"  - Sessão: {session_name} ({session_type}) - Key: {session_key}")
                    
                    # Criar nome de caminho para a sessão usando o formato esperado pelos scripts existentes
                    session_path_name = session.get("session_name", "").replace(" ", "_")
                    date_start = session.get("date_start", "")
                    
                    # Extrair data do campo date_start (formato: "2025-05-16T11:30:00+00:00")
                    session_date = ""
                    if date_start and "T" in date_start:
                        session_date = date_start.split("T")[0]
                    
                    # Criar caminho no formato esperado pelos scripts existentes
                    path = f"{year}/{year}-{session_date}_{meeting.get('location', '').replace(' ', '_')}_{meeting.get('meeting_name', '').replace(' ', '_')}/{session_date}_{session_name.replace(' ', '_')}"
                    
                    # Converter sessão para o formato esperado pelos scripts existentes
                    formatted_session = {
                        "Key": session.get("session_key"),
                        "Type": session.get("session_type", ""),
                        "Name": session.get("session_name", ""),
                        "StartDate": session.get("date_start", ""),
                        "EndDate": session.get("date_end", ""),
                        "GmtOffset": session.get("gmt_offset", ""),
                        "Path": path
                    }
                    
                    # Adicionar sessão ao meeting
                    formatted_meeting["Sessions"].append(formatted_session)
            
            # Adicionar meeting aos dados do ano
            all_data["Meetings"].append(formatted_meeting)
        
        # Salvar arquivo index completo no formato esperado pelos scripts existentes
        index_file = output_dir / f"index_{year}.json"
        with open(index_file, "w") as f:
            json.dump(all_data, f, indent=2)
        
        print(f"\nArquivo de índice para o ano {year} salvo em {index_file}")
        
        # Atualizar estatísticas
        results["years_processed"].append(year)
        results["meetings_found"] += len(meetings_data)
        sessions_count = sum(len(meeting["Sessions"]) for meeting in all_data["Meetings"])
        results["sessions_found"] += sessions_count
    
    # Mostrar resumo
    print("\n" + "="*50)
    print("Resumo da exploração de dados F1:")
    print(f"Anos processados: {', '.join(map(str, results['years_processed']))}")
    print(f"Total de meetings encontrados: {results['meetings_found']}")
    print(f"Total de sessões encontradas: {results['sessions_found']}")
    print("="*50)
    
    return results

def main():
    """Função principal com processamento de argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description="Explorador de dados F1 usando API OpenF1")
    parser.add_argument("--years", nargs="+", type=int, help="Anos específicos para explorar (ex: 2023 2024 2025)")
    parser.add_argument("--format", choices=["json", "csv"], default="json", help="Formato de saída dos dados (padrão: json)")
    
    args = parser.parse_args()
    
    # Usar os anos especificados ou deixar a função usar o ano atual como padrão
    years = args.years if args.years else None
    
    # Executar a exploração de dados
    explore_f1_data(years, args.format)
    
    print("\nPróximos passos:")
    print("1. Os arquivos index_YEAR.json contêm informações sobre todas as corridas e sessões do ano.")
    print("2. Você pode usá-los para extrair os caminhos das sessões e as chaves necessárias para o seu projeto.")
    print("3. Para coletar dados brutos, use o f1_collector.py com as chaves de meeting e session:")
    print("   python f1_collector.py --meeting [MEETING_KEY] --session [SESSION_KEY]")
    print("4. Para listar todas as corridas e sessões disponíveis:")
    print("   python f1_collector.py --list")

if __name__ == "__main__":
    main()