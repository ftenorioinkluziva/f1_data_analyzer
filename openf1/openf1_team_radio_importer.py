# openf1_team_radio_importer.py
from openf1_importer_base import OpenF1ImporterBase
from datetime import datetime

class TeamRadioImporter(OpenF1ImporterBase):
    """
    Importador de dados de rádio da equipe da API OpenF1 para o Supabase.
    """
    
    def import_team_radio(self, session_key, incremental=False, limit=None):
        """
        Importa dados de rádio da equipe para uma sessão específica.
        
        Args:
            session_key: Chave da sessão
            incremental: Se True, apenas insere novos registros sem excluir os existentes
            limit: Limite de registros a importar (None para todos)
        
        Returns:
            dict: Estatísticas da importação
        """
        if not self.supabase:
            return {"success": False, "error": "Supabase não conectado"}
        
        stats = {
            "fetched": 0,
            "inserted": 0,
            "deleted": 0,
            "skipped": 0,
            "errors": 0
        }
        
        # Obter o ID da sessão
        session_id = self.get_session_id(session_key)
        if not session_id:
            return {"success": False, "error": f"Sessão {session_key} não encontrada"}
        
        # Buscar dados de rádio da equipe da API
        team_radio_data = self.fetch_data("team_radio", {"session_key": session_key})
        stats["fetched"] = len(team_radio_data)
        
        if not team_radio_data:
            return stats
        
        # Limitar registros se necessário
        if limit and limit < len(team_radio_data):
            print(f"Limitando de {len(team_radio_data)} para {limit} registros de rádio da equipe")
            team_radio_data = team_radio_data[:limit]
        
        # Se for incremental, vamos verificar quais URLs de áudio já existem no banco
        if incremental:
            try:
                # Buscar os URLs de áudio que já existem no banco
                existing_audio_paths_query = self.supabase.table("team_radio")\
                    .select("audio_path")\
                    .eq("session_id", session_id)\
                    .execute()
                
                existing_audio_paths = set()
                if existing_audio_paths_query.data:
                    for record in existing_audio_paths_query.data:
                        if "audio_path" in record and record["audio_path"]:
                            existing_audio_paths.add(record["audio_path"])
                
                print(f"Encontrados {len(existing_audio_paths)} registros de áudio existentes no banco")
            except Exception as e:
                print(f"Erro ao verificar registros existentes: {str(e)}")
                existing_audio_paths = set()
        else:
            # No modo não-incremental, excluímos todos os registros existentes
            try:
                existing_query = self.supabase.table("team_radio").select("id").eq("session_id", session_id).execute()
                
                # Remover registros existentes para evitar duplicação
                if existing_query.data:
                    existing_count = len(existing_query.data)
                    print(f"Encontrados {existing_count} registros de rádio existentes, removendo...")
                    self.supabase.table("team_radio").delete().eq("session_id", session_id).execute()
                    stats["deleted"] = existing_count
            except Exception as e:
                print(f"Erro ao verificar/remover registros existentes: {str(e)}")
                stats["errors"] += 1
        
        # Criar registros para inserção em lote
        team_radio_records = []
        
        for radio in team_radio_data:
            try:
                # Obter o URL de gravação
                recording_url = radio.get("recording_url")
                
                # Se estamos no modo incremental, verificar se já temos este áudio
                if incremental and recording_url in existing_audio_paths:
                    stats["skipped"] += 1
                    continue
                
                # Obter o timestamp da data
                timestamp = radio.get("date")
                
                # Criar registro para o banco
                radio_record = {
                    "session_id": session_id,
                    "timestamp": timestamp,
                    "utc_time": timestamp,  # Usar o mesmo valor para utc_time e timestamp
                    "driver_number": str(radio.get("driver_number")),  # Converter para string pois o banco espera texto
                    "audio_path": recording_url
                }
                
                # Filtrar para apenas colunas válidas
                radio_record = self.filter_record_columns(radio_record, "team_radio")
                
                team_radio_records.append(radio_record)
            except Exception as e:
                print(f"Erro ao processar registro de rádio: {str(e)}")
                stats["errors"] += 1
        
        # Inserir em lotes
        if team_radio_records:
            batch_size = 100
            total_records = len(team_radio_records)
            
            for i in range(0, total_records, batch_size):
                batch = team_radio_records[i:i + batch_size]
                try:
                    self.supabase.table("team_radio").insert(batch).execute()
                    stats["inserted"] += len(batch)
                    print(f"Inseridos {i+1} a {min(i + batch_size, total_records)} de {total_records} registros de rádio")
                except Exception as e:
                    print(f"Erro ao inserir lote de registros de rádio: {str(e)}")
                    stats["errors"] += len(batch)
        
        if incremental:
            print(f"\nImportação incremental de dados de rádio da equipe concluída: {stats['inserted']} inseridos, {stats['skipped']} ignorados (já existentes)")
        else:
            print(f"\nImportação completa de dados de rádio da equipe concluída: {stats['inserted']} inseridos")
        return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Importar dados de rádio da equipe F1 da API OpenF1 para o Supabase")
    parser.add_argument("--session", type=int, required=True, help="Chave da sessão para importar dados de rádio da equipe")
    parser.add_argument("--limit", type=int, help="Limite de registros a importar (None para todos)")
    parser.add_argument("--incremental", action="store_true", help="Modo incremental: adicionar apenas novos registros sem excluir existentes")
    
    args = parser.parse_args()
    
    importer = TeamRadioImporter()
    result = importer.import_team_radio(args.session, incremental=args.incremental, limit=args.limit)
    
    print("\nResultado da importação:")
    print(f"Total buscado: {result['fetched']}")
    if not args.incremental:
        print(f"Registros excluídos: {result['deleted']}")
    else:
        print(f"Registros ignorados (já existentes): {result['skipped']}")
    print(f"Inseridos: {result['inserted']}")
    print(f"Erros: {result['errors']}")