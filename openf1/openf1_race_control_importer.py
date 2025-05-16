# openf1_race_control_importer.py
from openf1_importer_base import OpenF1ImporterBase

class RaceControlImporter(OpenF1ImporterBase):
    """
    Importador de mensagens do controle de corrida da API OpenF1 para o Supabase.
    """
    
    def import_race_control(self, session_key):
        """
        Importa mensagens do controle de corrida para uma sessão específica.
        
        Args:
            session_key: Chave da sessão
        
        Returns:
            dict: Estatísticas da importação
        """
        if not self.supabase:
            return {"success": False, "error": "Supabase não conectado"}
        
        stats = {
            "fetched": 0,
            "inserted": 0,
            "deleted": 0,
            "errors": 0
        }
        
        # Obter o ID da sessão
        session_id = self.get_session_id(session_key)
        if not session_id:
            return {"success": False, "error": f"Sessão {session_key} não encontrada"}
        
        # Buscar mensagens do controle de corrida
        race_control_data = self.fetch_data("race_control", {"session_key": session_key})
        stats["fetched"] = len(race_control_data)
        
        if not race_control_data:
            return stats
        
        # Verificar registros existentes para esta sessão
        try:
            existing_query = self.supabase.table("race_control_messages").select("id").eq("session_id", session_id).execute()
            
            # Remover registros existentes para evitar duplicação
            if existing_query.data:
                existing_count = len(existing_query.data)
                print(f"Encontrados {existing_count} mensagens existentes, removendo...")
                self.supabase.table("race_control_messages").delete().eq("session_id", session_id).execute()
                stats["deleted"] = existing_count
        except Exception as e:
            print(f"Erro ao verificar/remover registros existentes: {str(e)}")
            stats["errors"] += 1
        
        # Criar registros para inserção em lote
        message_records = []
        
        for message in race_control_data:
            try:
                # Criar registro para o banco
                message_record = {
                    "session_id": session_id,
                    "timestamp": message.get("date"),
                    "category": message.get("category"),
                    "message": message.get("message"),
                    "flag": message.get("flag"),
                    "scope": message.get("scope"),
                    "sector": message.get("sector"),
                    "driver_number": message.get("driver_number"),
                    "lap_number": message.get("lap_number")
                }
                
                message_records.append(message_record)
            except Exception as e:
                print(f"Erro ao processar mensagem: {str(e)}")
                stats["errors"] += 1
        
        # Inserir em lotes
        if message_records:
            batch_size = 100
            total_records = len(message_records)
            
            for i in range(0, total_records, batch_size):
                batch = message_records[i:i + batch_size]
                try:
                    self.supabase.table("race_control_messages").insert(batch).execute()
                    stats["inserted"] += len(batch)
                    print(f"Inseridas {i+1} a {min(i + batch_size, total_records)} de {total_records} mensagens")
                except Exception as e:
                    print(f"Erro ao inserir lote de mensagens: {str(e)}")
                    stats["errors"] += len(batch)
        
        print(f"\nImportação de mensagens do controle de corrida concluída: {stats['inserted']} inseridas")
        return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Importar mensagens do controle de corrida F1 da API OpenF1 para o Supabase")
    parser.add_argument("--session", type=int, required=True, help="Chave da sessão para importar mensagens")
    
    args = parser.parse_args()
    
    importer = RaceControlImporter()
    result = importer.import_race_control(args.session)
    
    print("\nResultado da importação:")
    print(f"Total buscado: {result['fetched']}")
    print(f"Registros excluídos: {result['deleted']}")
    print(f"Inseridos: {result['inserted']}")
    print(f"Erros: {result['errors']}")