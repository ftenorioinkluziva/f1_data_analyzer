# f1_collector.py - versão modificada para coletar dados brutos usando as chaves como nomes de pastas
import asyncio
import json
import aiohttp
from datetime import datetime
import os
from pathlib import Path
import argparse
import glob

# Configuração
OUTPUT_DIR = Path("f1_data")
RAW_DIR = OUTPUT_DIR / "raw"

# Criar diretórios
OUTPUT_DIR.mkdir(exist_ok=True)
RAW_DIR.mkdir(exist_ok=True)

def fix_utf8_bom(content):
    """Corrige o problema de UTF-8 BOM nos arquivos JSON."""
    try:
        # Decodifica o conteúdo considerando o BOM
        text = content.decode('utf-8-sig')
        # Converte para JSON
        data = json.loads(text)
        return data
    except Exception as e:
        print(f"Erro ao processar JSON: {str(e)}")
        return None

# Função para verificar se uma URL existe
async def check_url_exists_async(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return True, await response.read()
            return False, None
    except Exception as e:
        print(f"Erro ao acessar {url}: {str(e)}")
        return False, None

# Função para extrair tópicos disponíveis de uma sessão
async def get_session_topics(session, session_url):
    exists, content = await check_url_exists_async(session, f"{session_url}/Index.json")
    
    if not exists or not content:
        print(f"Não foi possível acessar {session_url}/Index.json")
        return []
    
    session_index = fix_utf8_bom(content)
    
    if not session_index or "Feeds" not in session_index:
        return []
    
    topics = []
    for feed_key, feed_info in session_index["Feeds"].items():
        if "StreamPath" in feed_info:
            stream_path = feed_info["StreamPath"]
            if stream_path.endswith(".jsonStream"):
                topic = stream_path[:-11]  # Remove '.jsonStream'
                topics.append(topic)
    
    return topics

# Função para baixar dados brutos de um tópico usando as chaves como nomes de pastas
async def download_raw_data(http_session, session_url, topic, meeting_key, session_key):
    print(f"Baixando tópico: {topic}")
    
    # Criar diretório para os dados brutos usando as chaves como nomes
    raw_topic_dir = RAW_DIR / str(meeting_key) / str(session_key)
    raw_topic_dir.mkdir(exist_ok=True, parents=True)
    
    # Arquivo para os dados brutos
    raw_file = raw_topic_dir / f"{topic}.jsonStream"
    
    # Verificar se o arquivo já existe
    if raw_file.exists():
        file_size = raw_file.stat().st_size
        print(f"  Arquivo já existe: {raw_file} ({file_size/1024:.1f} KB)")
        return raw_file
    
    # Buscar dados do tópico
    topic_url = f"{session_url}/{topic}.jsonStream"
    exists, content = await check_url_exists_async(http_session, topic_url)
    
    if not exists or not content:
        print(f"  Não foi possível acessar {topic_url}")
        return None
    
    # Salvar dados brutos
    with open(raw_file, "wb") as f:
        f.write(content)
    
    file_size = len(content)
    print(f"  Dados brutos salvos em {raw_file} ({file_size/1024:.1f} KB)")
    
    # Criar um arquivo de metadados para facilitar a referência futura
    metadata = {
        "url": topic_url,
        "meeting_key": meeting_key,
        "session_key": session_key,
        "topic": topic,
        "collected_at": datetime.now().isoformat()
    }
    
    # Salvar metadados
    metadata_file = raw_topic_dir / f"{topic}_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)
    
    return raw_file

# Função para processar uma sessão específica
async def process_session(year, race_path, session_name, meeting_key, session_key):
    base_url = "https://livetiming.formula1.com/static"
    session_url = f"{base_url}/{year}/{race_path}/{session_name}"
    
    race_name = race_path.split('_', 1)[1] if '_' in race_path else race_path
    print(f"\nProcessando sessão: {race_name}/{session_name} (Meeting Key: {meeting_key}, Session Key: {session_key})")
    
    # Criar uma sessão HTTP para reutilizar conexões
    async with aiohttp.ClientSession() as session:
        # Obter tópicos disponíveis
        topics = await get_session_topics(session, session_url)
        
        if not topics:
            print("  Nenhum tópico encontrado.")
            return
        
        print(f"  Encontrados {len(topics)} tópicos.")
        
        # Listar todos os tópicos encontrados
        print("  Lista de todos os tópicos encontrados:")
        for i, topic in enumerate(sorted(topics)):
            print(f"    {i+1}. {topic}")
        
        # Baixar todos os tópicos disponíveis com limite de concorrência
        semaphore = asyncio.Semaphore(5)  # Limitar a 5 downloads concorrentes
        
        async def download_with_limit(topic):
            async with semaphore:
                return await download_raw_data(session, session_url, topic, meeting_key, session_key)
                
        # Criar tarefas para todos os tópicos
        tasks = [download_with_limit(topic) for topic in topics]
        results = await asyncio.gather(*tasks)
        
        # Contar quantos tópicos foram baixados com sucesso
        successful_downloads = [r for r in results if r is not None]
        print(f"\n  Baixados com sucesso: {len(successful_downloads)}/{len(topics)} tópicos")

# Função para encontrar informações a partir das chaves de meeting e session
def find_meeting_session_by_keys(meeting_key, session_key):
    """
    Encontra informações da corrida e sessão a partir das chaves.
    
    Args:
        meeting_key: Chave do meeting (corrida)
        session_key: Chave da session (sessão)
        
    Returns:
        tuple: (year, race_path, session_name, meeting_key, session_key)
    """
    # Procurar em todos os arquivos index_year.json
    index_files = glob.glob("f1_data_explorer/index_*.json")
    
    if not index_files:
        print("Nenhum arquivo index_year.json encontrado. Execute primeiro f1_explorer.py para gerar os arquivos.")
        return None, None, None, None, None
    
    for index_file in index_files:
        try:
            # Ler o arquivo index_year.json
            with open(index_file, 'r') as f:
                index_data = json.load(f)
            
            # Extrair o ano do arquivo
            year = str(index_data.get("Year", ""))
            
            # Procurar pelo meeting com a chave especificada
            for meeting in index_data.get("Meetings", []):
                if meeting.get("Key") == meeting_key:
                    meeting_name = meeting.get("Name", "Unknown Meeting")
                    
                    # Encontramos o meeting, agora procurar a session
                    for session in meeting.get("Sessions", []):
                        if session.get("Key") == session_key:
                            session_path = session.get("Path", "")
                            
                            # Extrair nome da sessão e race_path do caminho
                            if session_path:
                                path_parts = session_path.split("/")
                                if len(path_parts) >= 3:
                                    race_path = path_parts[1]
                                    session_name = path_parts[2]
                                    
                                    print(f"Encontrado: {meeting_name} ({race_path}) - {session.get('Name')} ({session_name})")
                                    return year, race_path, session_name, meeting_key, session_key
            
        except Exception as e:
            print(f"Erro ao processar o arquivo {index_file}: {str(e)}")
    
    print(f"Não foi possível encontrar meeting_key={meeting_key} e session_key={session_key} nos arquivos index.")
    return None, None, None, None, None

# Função para listar todas as corridas e sessões disponíveis
def list_all_meetings_sessions():
    """
    Lista todas as corridas e sessões encontradas nos arquivos index_year.json
    """
    # Procurar em todos os arquivos index_year.json
    index_files = glob.glob("f1_data_explorer/index_*.json")
    
    if not index_files:
        print("Nenhum arquivo index_year.json encontrado. Execute primeiro f1_explorer.py para gerar os arquivos.")
        return
    
    print("\n=== Corridas e Sessões Disponíveis ===\n")
    
    for index_file in index_files:
        try:
            # Ler o arquivo index_year.json
            with open(index_file, 'r') as f:
                index_data = json.load(f)
            
            # Extrair o ano do arquivo
            year = str(index_data.get("Year", ""))
            print(f"\n## Ano: {year}")
            
            # Listar todas as corridas e sessões
            for meeting in index_data.get("Meetings", []):
                meeting_name = meeting.get("Name", "Unknown Meeting")
                meeting_key = meeting.get("Key")
                
                print(f"\n* {meeting_name} (meeting_key: {meeting_key})")
                
                for session in meeting.get("Sessions", []):
                    session_name = session.get("Name", "Unknown Session")
                    session_key = session.get("Key")
                    session_type = session.get("Type", "Unknown Type")
                    session_path = session.get("Path", "")
                    
                    print(f"  - {session_name} ({session_type}) - session_key: {session_key}")
                    if session_path:
                        print(f"    Path: {session_path}")
            
        except Exception as e:
            print(f"Erro ao processar o arquivo {index_file}: {str(e)}")

# Função principal para processar corridas
async def main(meeting_key=None, session_key=None, list_all=False):
    # Verificar se devemos listar todas as corridas e sessões
    if list_all:
        list_all_meetings_sessions()
        return
    
    # Determinar os valores para processamento
    if meeting_key is not None and session_key is not None:
        # Converter para inteiros
        meeting_key = int(meeting_key)
        session_key = int(session_key)
        
        print(f"Buscando informações para meeting_key={meeting_key}, session_key={session_key}...")
        
        # Encontrar as informações a partir das chaves
        year, race_path, session_name, meeting_key, session_key = find_meeting_session_by_keys(meeting_key, session_key)
        
        if not race_path or not session_name:
            print("Não foi possível encontrar as informações necessárias. Use --list para ver todas as opções disponíveis.")
            return
    else:
        # Valores default para demonstração (Miami GP 2024)
        print("Usando valores padrão para Miami GP 2024")
        year = "2024"
        race_path = "2024-05-05_Miami_Grand_Prix"
        session_names = ["2024-05-03_Practice_1", "2024-05-04_Qualifying", "2024-05-05_Race"]
        meeting_key = 1264  # Exemplo - usando valor real para Miami GP 2024
        session_keys = [1295, 1296, 1297]  # Exemplo - usando valores reais para as sessões
        
        # Processar todas as sessões padrão
        for session_name, session_key in zip(session_names, session_keys):
            await process_session(year, race_path, session_name, meeting_key, session_key)
        
        print("\nColeta concluída!")
        print(f"Dados brutos salvos em: {RAW_DIR.absolute()}")
        print(f"Os dados estão organizados por: {RAW_DIR.absolute()}/[meeting_key]/[session_key]/")
        print("\nTodos os dados brutos foram salvos com sucesso. Você pode processá-los usando suas ferramentas preferidas.")
        return
    
    # Processar uma única sessão usando as chaves fornecidas
    print(f"Processando corrida: {race_path}, Sessão: {session_name}")
    await process_session(year, race_path, session_name, meeting_key, session_key)
    
    print("\nColeta concluída!")
    print(f"Dados brutos salvos em: {RAW_DIR.absolute()}")
    print(f"Os dados estão organizados por: {RAW_DIR.absolute()}/{meeting_key}/{session_key}/")
    print("\nTodos os dados brutos foram salvos com sucesso. Você pode processá-los usando suas ferramentas preferidas.")

if __name__ == "__main__":
    # Configurar argumentos da linha de comando
    parser = argparse.ArgumentParser(description="Coletar dados brutos de corridas da F1")
    parser.add_argument("--meeting", type=int, help="Chave do meeting (corrida)")
    parser.add_argument("--session", type=int, help="Chave da session (sessão)")
    parser.add_argument("--list", action="store_true", help="Listar todas as corridas e sessões disponíveis")
    
    args = parser.parse_args()
    
    # Executar o coletor com os argumentos fornecidos
    asyncio.run(main(args.meeting, args.session, args.list))