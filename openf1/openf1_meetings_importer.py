# openf1_meetings_importer.py
from openf1_importer_base import OpenF1ImporterBase

class MeetingsImporter(OpenF1ImporterBase):
    """
    Importador de dados de meetings (eventos) da API OpenF1 para o Supabase.
    """
    
    def import_meeting_by_key(self, meeting_key, update_existing=True):
        """
        Importa um meeting específico pelo seu meeting_key.
        
        Args:
            meeting_key: Chave do meeting a ser importado
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
        
        # Buscar dados do meeting específico
        meetings_data = self.fetch_data("meetings", {"meeting_key": meeting_key})
        stats["fetched"] = len(meetings_data)
        
        if not meetings_data:
            print(f"Meeting com key {meeting_key} não encontrado.")
            return stats
        
        # Deve haver apenas um meeting com esta key
        meeting = meetings_data[0]
        
        try:
            # Verificar se o meeting já existe
            existing_query = self.supabase.table("races").select("id").eq("key", meeting_key).execute()
            
            # Dados para inserção/atualização
            meeting_record = {
                "key": meeting_key,
                "code": meeting.get("meeting_code"),
                "number": meeting.get("meeting_number", 0),
                "location": meeting.get("location"),
                "official_name": meeting.get("meeting_official_name"),
                "name": meeting.get("meeting_name"),
                "year": meeting.get("year")
                # Removido "date_start" porque a coluna não existe na tabela races
            }
            
            # Buscar ou criar país
            country_key = meeting.get("country_key")
            country_id = None
            if country_key:
                # Verificar se o país já existe
                country_query = self.supabase.table("countries").select("id").eq("key", country_key).execute()
                
                if country_query.data:
                    country_id = country_query.data[0]["id"]
                else:
                    # Inserir novo país
                    country_record = {
                        "key": country_key,
                        "code": meeting.get("country_code"),
                        "name": meeting.get("country_name"),
                        "flag_url": self._get_flag_url(meeting.get("country_code"))
                    }
                    
                    country_result = self.supabase.table("countries").insert(country_record).execute()
                    if country_result.data:
                        country_id = country_result.data[0]["id"]
            
            # Adicionar country_id ao registro do meeting se disponível
            if country_id:
                meeting_record["country_id"] = country_id
            
            # Buscar ou criar circuito
            circuit_key = meeting.get("circuit_key")
            circuit_id = None
            if circuit_key:
                # Verificar se o circuito já existe
                circuit_query = self.supabase.table("circuits").select("id").eq("key", circuit_key).execute()
                
                if circuit_query.data:
                    circuit_id = circuit_query.data[0]["id"]
                else:
                    # Inserir novo circuito
                    circuit_record = {
                        "key": circuit_key,
                        "short_name": meeting.get("circuit_short_name")
                    }
                    
                    circuit_result = self.supabase.table("circuits").insert(circuit_record).execute()
                    if circuit_result.data:
                        circuit_id = circuit_result.data[0]["id"]
            
            # Adicionar circuit_id ao registro do meeting se disponível
            if circuit_id:
                meeting_record["circuit_id"] = circuit_id
            
            # Filtrar o registro para ter apenas colunas que existem na tabela
            meeting_record = self.filter_record_columns(meeting_record, "races")
            
            # Inserir ou atualizar meeting
            if not existing_query.data:
                # Inserir novo meeting
                meeting_result = self.supabase.table("races").insert(meeting_record).execute()
                print(f"Inserido evento: {meeting.get('meeting_name')} (Key: {meeting_key})")
                stats["inserted"] += 1
            elif update_existing:
                # Atualizar meeting existente
                race_id = existing_query.data[0]["id"]
                meeting_record["updated_at"] = "NOW()"
                self.supabase.table("races").update(meeting_record).eq("id", race_id).execute()
                print(f"Atualizado evento: {meeting.get('meeting_name')} (Key: {meeting_key})")
                stats["updated"] += 1
        
        except Exception as e:
            print(f"Erro ao processar meeting {meeting_key}: {str(e)}")
            stats["errors"] += 1
        
        print(f"\nImportação do meeting {meeting_key} concluída: {stats['inserted']} inseridos, {stats['updated']} atualizados")
        return stats
    
    def import_meetings(self, year, update_existing=True):
        """
        Importa dados de meetings (eventos) de um ano específico.
        
        Args:
            year: Ano para importar os eventos
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
        
        # Buscar dados dos meetings
        meetings_data = self.fetch_data("meetings", {"year": year})
        stats["fetched"] = len(meetings_data)
        
        if not meetings_data:
            return stats
        
        # Processar cada meeting
        for meeting in meetings_data:
            try:
                meeting_key = meeting.get("meeting_key")
                
                if not meeting_key:
                    print("Meeting sem key, ignorando")
                    stats["errors"] += 1
                    continue
                
                # Verificar se o meeting já existe
                existing_query = self.supabase.table("races").select("id").eq("key", meeting_key).execute()
                
                # Dados para inserção/atualização
                meeting_record = {
                    "key": meeting_key,
                    "code": meeting.get("meeting_code"),
                    "number": meeting.get("meeting_number", 0),
                    "location": meeting.get("location"),
                    "official_name": meeting.get("meeting_official_name"),
                    "name": meeting.get("meeting_name"),
                    "year": meeting.get("year")
                    # Removido "date_start" porque a coluna não existe na tabela races
                }
                
                # Buscar ou criar país
                country_key = meeting.get("country_key")
                country_id = None
                if country_key:
                    # Verificar se o país já existe
                    country_query = self.supabase.table("countries").select("id").eq("key", country_key).execute()
                    
                    if country_query.data:
                        country_id = country_query.data[0]["id"]
                    else:
                        # Inserir novo país
                        country_record = {
                            "key": country_key,
                            "code": meeting.get("country_code"),
                            "name": meeting.get("country_name"),
                            "flag_url": self._get_flag_url(meeting.get("country_code"))
                        }
                        
                        country_result = self.supabase.table("countries").insert(country_record).execute()
                        if country_result.data:
                            country_id = country_result.data[0]["id"]
                
                # Adicionar country_id ao registro do meeting se disponível
                if country_id:
                    meeting_record["country_id"] = country_id
                
                # Buscar ou criar circuito
                circuit_key = meeting.get("circuit_key")
                circuit_id = None
                if circuit_key:
                    # Verificar se o circuito já existe
                    circuit_query = self.supabase.table("circuits").select("id").eq("key", circuit_key).execute()
                    
                    if circuit_query.data:
                        circuit_id = circuit_query.data[0]["id"]
                    else:
                        # Inserir novo circuito
                        circuit_record = {
                            "key": circuit_key,
                            "short_name": meeting.get("circuit_short_name")
                        }
                        
                        circuit_result = self.supabase.table("circuits").insert(circuit_record).execute()
                        if circuit_result.data:
                            circuit_id = circuit_result.data[0]["id"]
                
                # Adicionar circuit_id ao registro do meeting se disponível
                if circuit_id:
                    meeting_record["circuit_id"] = circuit_id
                
                # Filtrar o registro para ter apenas colunas que existem na tabela
                meeting_record = self.filter_record_columns(meeting_record, "races")
                
                # Inserir ou atualizar meeting
                if not existing_query.data:
                    # Inserir novo meeting
                    meeting_result = self.supabase.table("races").insert(meeting_record).execute()
                    print(f"Inserido evento: {meeting.get('meeting_name')} {year}")
                    stats["inserted"] += 1
                elif update_existing:
                    # Atualizar meeting existente
                    race_id = existing_query.data[0]["id"]
                    meeting_record["updated_at"] = "NOW()"
                    self.supabase.table("races").update(meeting_record).eq("id", race_id).execute()
                    print(f"Atualizado evento: {meeting.get('meeting_name')} {year}")
                    stats["updated"] += 1
            
            except Exception as e:
                print(f"Erro ao processar meeting: {str(e)}")
                stats["errors"] += 1
        
        print(f"\nImportação de meetings concluída: {stats['inserted']} inseridos, {stats['updated']} atualizados")
        return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Importar dados de eventos F1 da API OpenF1 para o Supabase")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Ano para importar os eventos")
    group.add_argument("--key", type=int, help="Chave (meeting_key) do evento específico para importar")
    parser.add_argument("--no-update", action="store_true", help="Não atualizar registros existentes")
    
    args = parser.parse_args()
    
    importer = MeetingsImporter()
    
    if args.year:
        result = importer.import_meetings(args.year, update_existing=not args.no_update)
    elif args.key:
        result = importer.import_meeting_by_key(args.key, update_existing=not args.no_update)
    
    print("\nResultado da importação:")
    print(f"Total buscado: {result['fetched']}")
    print(f"Inseridos: {result['inserted']}")
    print(f"Atualizados: {result['updated']}")
    print(f"Erros: {result['errors']}")