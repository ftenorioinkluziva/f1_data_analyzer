# import_races_to_supabase.py
import os
import json
import glob
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime

load_dotenv()  # Carrega variáveis de ambiente do arquivo .env

# Configuração do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Diretório onde estão os arquivos index_year.json
DATA_DIR = Path("f1_data_explorer")

# Base URL para as bandeiras (usando uma API pública)
FLAG_API_BASE = "https://flagcdn.com/w320"

def get_flag_url(country_code):
    """
    Gera URL da bandeira com base no código do país.
    Converte o código para minúsculo, já que a API usa códigos em minúsculo.
    
    Args:
        country_code: Código do país (3 letras)
        
    Returns:
        str: URL da bandeira
    """
    # Verificar se o código do país está no formato esperado
    if not country_code or len(country_code) != 3:
        return None
    
    # Converter para minúsculo
    code = country_code.lower()
    
    # Para alguns países, o código F1 é diferente do padrão ISO
    # Mapeamento de códigos específicos (adicione conforme necessário)
    code_mapping = {
        "brn": "bh",  # Bahrain
        "uae": "ae",  # Emirados Árabes Unidos
        "sgp": "sg",  # Singapura
        "ned": "nl",  # Holanda
        "ksa": "sa",  # Arábia Saudita
    }
    
    # Aplicar mapeamento se o código estiver nele
    if code in code_mapping:
        code = code_mapping[code]
    
    # Retornar URL da bandeira
    return f"{FLAG_API_BASE}/{code}.png"

def import_races_to_supabase():
    """
    Importa dados de corridas dos arquivos index_year.json para o Supabase.
    """
    # Verificar configuração do Supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL e SUPABASE_KEY devem ser configurados como variáveis de ambiente")

    # Inicializar cliente do Supabase
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Encontrar todos os arquivos index_year.json
    index_files = glob.glob(str(DATA_DIR / "index_*.json"))
    
    if not index_files:
        print("Nenhum arquivo index_year.json encontrado. Execute primeiro python f1_explorer.py")
        return
    
    # Contadores para estatísticas
    countries_added = 0
    circuits_added = 0
    races_added = 0
    sessions_added = 0
    
    # Processar cada arquivo index_year.json
    for index_file in index_files:
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                index_data = json.load(f)
            
            # Extrair o ano do arquivo (formato: index_YEAR.json)
            year_str = Path(index_file).stem.split('_')[1]
            year = int(year_str)
            print(f"Processando arquivos para o ano {year}...")
            
            # Processar cada corrida no arquivo
            for meeting in index_data.get("Meetings", []):
                # Extrair dados do país
                country_data = meeting.get("Country")
                country_id = None
                if country_data and "Key" in country_data:
                    is_new_country, country_id = import_country(supabase, country_data)
                    if is_new_country:
                        countries_added += 1
                
                # Extrair dados do circuito
                circuit_data = meeting.get("Circuit")
                circuit_id = None
                if circuit_data and "Key" in circuit_data:
                    is_new_circuit, circuit_id = import_circuit(supabase, circuit_data)
                    if is_new_circuit:
                        circuits_added += 1
                
                # Extrair dados da corrida
                race_key = meeting.get("Key")
                if race_key:
                    # Verificar se a corrida já existe
                    race_exists = supabase.table("races").select("id").eq("key", race_key).execute()
                    
                    race_data = {
                        "key": race_key,
                        "code": meeting.get("Code"),
                        "number": meeting.get("Number"),
                        "location": meeting.get("Location"),
                        "official_name": meeting.get("OfficialName"),
                        "name": meeting.get("Name"),
                        "year": year,
                        "country_id": country_id,
                        "circuit_id": circuit_id
                    }
                    
                    race_id = None
                    
                    if not race_exists.data:
                        # Inserir nova corrida
                        result = supabase.table("races").insert(race_data).execute()
                        race_id = result.data[0]["id"]
                        print(f"Adicionada corrida: {meeting.get('Name')} {year}")
                        races_added += 1
                    else:
                        # Atualizar corrida existente
                        race_id = race_exists.data[0]["id"]
                        race_data["updated_at"] = "NOW()"
                        supabase.table("races").update(race_data).eq("id", race_id).execute()
                        print(f"Atualizada corrida: {meeting.get('Name')} {year}")
                    
                    # Importar sessões da corrida
                    if race_id and "Sessions" in meeting:
                        session_count = import_sessions(supabase, race_id, meeting.get("Sessions", []))
                        sessions_added += session_count
                
        except Exception as e:
            print(f"Erro ao processar o arquivo {index_file}: {str(e)}")
    
    print(f"\nImportação concluída!")
    print(f"Países adicionados: {countries_added}")
    print(f"Circuitos adicionados: {circuits_added}")
    print(f"Corridas adicionadas: {races_added}")
    print(f"Sessões adicionadas: {sessions_added}")

def import_country(supabase, country_data):
    """
    Importa ou atualiza dados de um país e retorna o ID.
    
    Returns:
        tuple: (is_new, country_id) - is_new indica se é um novo país, country_id é o ID do país
    """
    country_key = country_data.get("Key")
    country_code = country_data.get("Code")
    
    # Verificar se o país já existe
    country_exists = supabase.table("countries").select("id").eq("key", country_key).execute()
    
    # Obter URL da bandeira
    flag_url = get_flag_url(country_code)
    
    country_record = {
        "key": country_key,
        "code": country_code,
        "name": country_data.get("Name"),
        "flag_url": flag_url
    }
    
    if not country_exists.data:
        # Inserir novo país
        result = supabase.table("countries").insert(country_record).execute()
        country_id = result.data[0]["id"]
        flag_info = f" (Bandeira: {flag_url})" if flag_url else ""
        print(f"Adicionado país: {country_data.get('Name')}{flag_info}")
        return True, country_id  # Novo país, retorna o ID real
    else:
        # Atualizar país existente
        country_id = country_exists.data[0]["id"]
        country_record["updated_at"] = "NOW()"
        supabase.table("countries").update(country_record).eq("id", country_id).execute()
        return False, country_id  # País existente, retorna o ID

def import_circuit(supabase, circuit_data):
    """
    Importa ou atualiza dados de um circuito e retorna o ID.
    
    Returns:
        tuple: (is_new, circuit_id) - is_new indica se é um novo circuito, circuit_id é o ID do circuito
    """
    circuit_key = circuit_data.get("Key")
    
    # Verificar se o circuito já existe
    circuit_exists = supabase.table("circuits").select("id").eq("key", circuit_key).execute()
    
    circuit_record = {
        "key": circuit_key,
        "short_name": circuit_data.get("ShortName"),
        # track_image_path ficará como NULL até que as imagens sejam carregadas pelo frontend
    }
    
    if not circuit_exists.data:
        # Inserir novo circuito
        result = supabase.table("circuits").insert(circuit_record).execute()
        circuit_id = result.data[0]["id"]
        print(f"Adicionado circuito: {circuit_data.get('ShortName')}")
        return True, circuit_id  # Novo circuito, retorna o ID real
    else:
        # Atualizar circuito existente
        circuit_id = circuit_exists.data[0]["id"]
        circuit_record["updated_at"] = "NOW()"
        supabase.table("circuits").update(circuit_record).eq("id", circuit_id).execute()
        return False, circuit_id  # Circuito existente, retorna o ID

def import_sessions(supabase, race_id, sessions_data):
    """
    Importa sessões de uma corrida no Supabase.
    
    Args:
        supabase: Cliente do Supabase
        race_id: ID da corrida
        sessions_data: Lista de dados de sessões
        
    Returns:
        int: Número de sessões adicionadas
    """
    sessions_added = 0
    
    for session in sessions_data:
        session_key = session.get("Key")
        
        if not session_key:
            continue
        
        # Verificar se a sessão já existe
        session_exists = supabase.table("sessions").select("id").eq("key", session_key).execute()
        
        # Preparar dados da sessão
        session_record = {
            "race_id": race_id,
            "key": session_key,
            "type": session.get("Type"),
            "name": session.get("Name"),
            "start_date": session.get("StartDate"),
            "end_date": session.get("EndDate"),
            "gmt_offset": session.get("GmtOffset"),
            "path": session.get("Path")
        }
        
        if not session_exists.data:
            # Inserir nova sessão
            try:
                result = supabase.table("sessions").insert(session_record).execute()
                print(f"Adicionada sessão: {session.get('Name')} (Tipo: {session.get('Type')})")
                sessions_added += 1
            except Exception as e:
                print(f"Erro ao adicionar sessão {session.get('Name')}: {str(e)}")
        else:
            # Atualizar sessão existente
            session_id = session_exists.data[0]["id"]
            session_record["updated_at"] = "NOW()"
            try:
                supabase.table("sessions").update(session_record).eq("id", session_id).execute()
                print(f"Atualizada sessão: {session.get('Name')} (Tipo: {session.get('Type')})")
            except Exception as e:
                print(f"Erro ao atualizar sessão {session.get('Name')}: {str(e)}")
    
    return sessions_added

if __name__ == "__main__":
    import_races_to_supabase()