# openf1_drivers_importer.py
from openf1_importer_base import OpenF1ImporterBase

class DriversImporter(OpenF1ImporterBase):
    """
    Importador de dados de pilotos da API OpenF1 para o Supabase.
    """
    
    def import_drivers(self, session_key, update_existing=True):
        """
        Importa dados dos pilotos para uma sessão específica.
        
        Args:
            session_key: Chave da sessão
            update_existing: Se True, atualiza registros existentes
        
        Returns:
            dict: Estatísticas da importação e lista de driver_numbers importados
        """
        if not self.supabase:
            return {"success": False, "error": "Supabase não conectado"}
        
        stats = {
            "fetched": 0,
            "inserted": 0,
            "updated": 0,
            "errors": 0,
            "driver_numbers": []
        }
        
        # Obter o ID da sessão
        session_id = self.get_session_id(session_key)
        if not session_id:
            return {"success": False, "error": f"Sessão {session_key} não encontrada"}
        
        # Buscar dados dos pilotos
        drivers_data = self.fetch_data("drivers", {"session_key": session_key})
        stats["fetched"] = len(drivers_data)
        
        if not drivers_data:
            return stats
        
        # Processar cada piloto
        for driver in drivers_data:
            try:
                driver_number = driver.get("driver_number")
                
                if not driver_number:
                    print("Piloto sem número, ignorando")
                    stats["errors"] += 1
                    continue
                
                # Verificar se o piloto já existe para esta sessão
                existing_query = self.supabase.table("session_drivers").select("id").eq("session_id", session_id).eq("driver_number", driver_number).execute()
                
                # Dados para inserção/atualização
                driver_record = {
                    "session_id": session_id,
                    "driver_number": driver_number,
                    "full_name": driver.get("full_name", ""),
                    "broadcast_name": driver.get("broadcast_name"),
                    "tla": driver.get("name_acronym"),
                    "team_name": driver.get("team_name"),
                    "team_color": driver.get("team_colour"),
                    "first_name": driver.get("first_name"),
                    "last_name": driver.get("last_name"),
                    "headshot_url": driver.get("headshot_url")
                }
                
                # Filtrar o registro para ter apenas colunas que existem na tabela
                driver_record = self.filter_record_columns(driver_record, "session_drivers")
                
                # Inserir ou atualizar piloto
                if not existing_query.data:
                    # Inserir novo piloto
                    driver_result = self.supabase.table("session_drivers").insert(driver_record).execute()
                    print(f"Inserido piloto: {driver.get('full_name')} (#{driver_number})")
                    stats["inserted"] += 1
                elif update_existing:
                    # Atualizar piloto existente
                    driver_id = existing_query.data[0]["id"]
                    driver_record["updated_at"] = "NOW()"
                    self.supabase.table("session_drivers").update(driver_record).eq("id", driver_id).execute()
                    print(f"Atualizado piloto: {driver.get('full_name')} (#{driver_number})")
                    stats["updated"] += 1
                
                # Adicionar driver_number à lista de pilotos processados
                stats["driver_numbers"].append(driver_number)
            
            except Exception as e:
                print(f"Erro ao processar piloto: {str(e)}")
                stats["errors"] += 1
        
        print(f"\nImportação de pilotos concluída: {stats['inserted']} inseridos, {stats['updated']} atualizados")
        return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Importar dados de pilotos F1 da API OpenF1 para o Supabase")
    parser.add_argument("--session", type=int, required=True, help="Chave da sessão para importar pilotos")
    parser.add_argument("--no-update", action="store_true", help="Não atualizar registros existentes")
    
    args = parser.parse_args()
    
    importer = DriversImporter()
    result = importer.import_drivers(args.session, update_existing=not args.no_update)
    
    print("\nResultado da importação:")
    print(f"Total buscado: {result['fetched']}")
    print(f"Inseridos: {result['inserted']}")
    print(f"Atualizados: {result['updated']}")
    print(f"Erros: {result['errors']}")
    print(f"Pilotos importados: {len(result['driver_numbers'])}")