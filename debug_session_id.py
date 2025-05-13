# debug_session_id.py
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def check_all_sessions():
    """Lista todas as sessões com seus campos completos para depuração."""
    sessions = supabase.table("sessions").select("id,path,name,type,race_id").execute()
    print(f"Total de sessões no banco: {len(sessions.data)}")
    
    for session in sessions.data:
        # Obter o nome da corrida para referência
        race_id = session['race_id']
        race = supabase.table("races").select("name").eq("id", race_id).execute()
        race_name = race.data[0]['name'] if race.data else "Unknown"
        
        print(f"\nSession ID: {session['id']}")
        print(f"  Race: {race_name} (ID: {race_id})")
        print(f"  Path: {session['path']}")
        print(f"  Name: {session['name']}")
        print(f"  Type: {session['type']}")

def test_get_session_id(race_name, session_name):
    """Testa a busca do session_id com depuração detalhada."""
    print(f"\nTestando busca com: race_name='{race_name}', session_name='{session_name}'")
    
    try:
        # Buscar a corrida
        race_query = supabase.table("races").select("id,name").ilike("name", f"%{race_name}%")
        race_result = race_query.execute()
        
        if not race_result.data:
            print(f"❌ Corrida não encontrada com nome contendo '{race_name}'")
            return None
        
        race = race_result.data[0]
        print(f"✅ Corrida encontrada: ID={race['id']}, Nome={race['name']}")
        
        # Buscar a sessão pelo path
        session_query = supabase.table("sessions").select("id,path,name,type").eq("race_id", race['id']).ilike("path", f"%{session_name}%")
        session_result = session_query.execute()
        
        if not session_result.data:
            print(f"❌ Sessão não encontrada pelo path contendo '{session_name}'")
            
            # Tentar pelo tipo
            session_type = session_name.split('_')[-1] if '_' in session_name else session_name
            session_query = supabase.table("sessions").select("id,path,name,type").eq("race_id", race['id']).ilike("type", f"%{session_type}%")
            session_result = session_query.execute()
            
            if not session_result.data:
                print(f"❌ Sessão não encontrada pelo tipo contendo '{session_type}'")
                
                # Listar todas as sessões desta corrida
                all_sessions = supabase.table("sessions").select("id,path,name,type").eq("race_id", race['id']).execute()
                print(f"📋 Sessões disponíveis para esta corrida:")
                for s in all_sessions.data:
                    print(f"  ID: {s['id']}, Path: {s['path']}, Nome: {s['name']}, Tipo: {s['type']}")
                
                return None
            else:
                print(f"✅ Sessão encontrada pelo tipo: ID={session_result.data[0]['id']}")
                return session_result.data[0]['id']
        else:
            print(f"✅ Sessão encontrada pelo path: ID={session_result.data[0]['id']}")
            return session_result.data[0]['id']
        
    except Exception as e:
        print(f"❌ Erro durante a busca: {str(e)}")
        return None

# Listar todas as sessões para referência
print("==== LISTAGEM COMPLETA DE SESSÕES ====")
check_all_sessions()

# Testar com casos específicos
print("\n==== TESTES DE BUSCA DE SESSION_ID ====")
test_cases = [
    ("Miami_Grand_Prix", "2025-05-04_Race"),
    ("Miami", "Race"),
    ("Miami", "2025-05-04"),
    # Adicione o formato exato que você está usando nos processadores
]

for race_name, session_name in test_cases:
    session_id = test_get_session_id(race_name, session_name)
    print(f"Resultado: {'✅ SUCESSO' if session_id else '❌ FALHA'} - Session ID: {session_id}")