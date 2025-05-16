# openf1_sessions_importer.py
from openf1_importer_base import OpenF1ImporterBase
from openf1_meetings_importer import MeetingsImporter

class SessionsImporter(OpenF1ImporterBase):
    """
    Importador de dados de sessões da API OpenF1 para o Supabase.
    """
    
    def __init__(self):
        """Inicializa o importador de sessões."""
        super().__init__()
        self.meetings_importer = MeetingsImporter()
    
    def import_sessions(self, meeting_key=None, year=None, update_existing=True):
        """
        Importa sessões para um meeting específico ou para um ano inteiro.
        
        Args:
            meeting_key: Chave do meeting para importar sessões (opcional)
            year: Ano para importar todas as sessões (opcional, se meeting_key não for fornecido)
            update_existing: Se True, atualiza registros existentes
        
        Returns:
            dict: Estatísticas da importação
        """
        if not self.supabase:
            return {"success": False, "error": "Supabase não conectado"}
        
        stats = {
            "fetched": 0,
            "inserted": 0,
            "updated": 0,
            "errors": 0
        }
        
        # Parâmetros da consulta
        params = {}
        if meeting_key:
            params["meeting_key"] = meeting_key
        elif year:
            params["year"] = year
        else:
            return {"success": False, "error": "É necessário fornecer meeting_key ou year"}
        
        # Buscar dados das sessões
        sessions_data = self.fetch_data("sessions", params)
        stats["fetched"] = len(sessions_data)
        
        if not sessions_data:
            print(f"Nenhuma sessão encontrada para os parâmetros fornecidos.")
            return stats
        
        # Processar cada sessão
        for session in sessions_data:
            try:
                session_key = session.get("session_key")
                meeting_key = session.get("meeting_key")
                
                if not session_key or not meeting_key:
                    print("Sessão sem keys necessárias, ignorando")
                    stats["errors"] += 1
                    continue
                
                # Verificar se a sessão já existe
                existing_query = self.supabase.table("sessions").select("id").eq("key", session_key).execute()
                
                # Buscar o ID da corrida no Supabase
                race_query = self.supabase.table("races").select("id").eq("key", meeting_key).execute()
                
                if not race_query.data:
                    print(f"Corrida não encontrada para meeting_key {meeting_key}. Importando meeting primeiro...")
                    # Tentar importar o meeting primeiro
                    meeting_data = self.fetch_data("meetings", {"meeting_key": meeting_key})
                    if meeting_data:
                        year = meeting_data[0].get("year")
                        self.meetings_importer.import_meetings(year)
                        # Tentar novamente
                        race_query = self.supabase.table("races").select("id").eq("key", meeting_key).execute()
                
                if not race_query.data:
                    print(f"Corrida não encontrada para meeting_key {meeting_key}, ignorando sessão {session_key}")
                    stats["errors"] += 1
                    continue
                
                race_id = race_query.data[0]["id"]
                
                # Criar caminho no formato esperado pelos scripts existentes
                session_name = session.get("session_name", "").replace(" ", "_")
                date_start = session.get("date_start", "")
                year = session.get("year")
                location = session.get("location", "").replace(" ", "_")
                meeting_name = self._extract_meeting_name(session)
                
                # Extrair data do campo date_start (formato: "2025-05-16T11:30:00+00:00")
                session_date = ""
                if date_start and "T" in date_start:
                    session_date = date_start.split("T")[0]
                
                # Criar caminho no formato esperado pelos scripts existentes
                path = f"{year}/{year}-{session_date}_{location}_{meeting_name}/{session_date}_{session_name}"
                
                # Dados para inserção/atualização
                session_record = {
                    "race_id": race_id,
                    "key": session_key,
                    "type": session.get("session_type"),
                    "name": session.get("session_name"),
                    "start_date": session.get("date_start"),
                    "end_date": session.get("date_end"),
                    "gmt_offset": session.get("gmt_offset"),
                    "path": path
                }
                
                # Filtrar o registro para ter apenas colunas que existem na tabela
                session_record = self.filter_record_columns(session_record, "sessions")
                
                # Inserir ou atualizar sessão
                if not existing_query.data:
                    # Inserir nova sessão
                    session_result = self.supabase.table("sessions").insert(session_record).execute()
                    print(f"Inserida sessão: {session.get('session_name')} (Tipo: {session.get('session_type')})")
                    stats["inserted"] += 1
                elif update_existing:
                    # Atualizar sessão existente
                    session_id = existing_query.data[0]["id"]
                    session_record["updated_at"] = "NOW()"
                    self.supabase.table("sessions").update(session_record).eq("id", session_id).execute()
                    print(f"Atualizada sessão: {session.get('session_name')} (Tipo: {session.get('session_type')})")
                    stats["updated"] += 1
            
            except Exception as e:
                print(f"Erro ao processar sessão: {str(e)}")
                stats["errors"] += 1
        
        print(f"\nImportação de sessões concluída: {stats['inserted']} inseridas, {stats['updated']} atualizadas")
        return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Importar dados de sessões F1 da API OpenF1 para o Supabase")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--meeting", type=int, help="Chave do meeting para importar sessões")
    group.add_argument("--year", type=int, help="Ano para importar todas as sessões")
    parser.add_argument("--no-update", action="store_true", help="Não atualizar registros existentes")
    
    args = parser.parse_args()
    
    importer = SessionsImporter()
    
    print(f"{'='*80}")
    if args.meeting:
        print(f" Importando sessões para o meeting {args.meeting} ".center(80, "="))
        result = importer.import_sessions(meeting_key=args.meeting, update_existing=not args.no_update)
    else:
        print(f" Importando sessões para o ano {args.year} ".center(80, "="))
        result = importer.import_sessions(year=args.year, update_existing=not args.no_update)
    print(f"{'='*80}")
    
    print("\nResultado da importação:")
    print(f"Total buscado: {result['fetched']}")
    print(f"Inseridos: {result['inserted']}")
    print(f"Atualizados: {result['updated']}")
    print(f"Erros: {result['errors']}")