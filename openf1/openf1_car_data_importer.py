# openf1_car_data_importer.py
from openf1_importer_base import OpenF1ImporterBase
from openf1_drivers_importer import DriversImporter

class CarDataImporter(OpenF1ImporterBase):
    """
    Importador de dados telemétricos dos carros da API OpenF1 para o Supabase.
    """
    
    def __init__(self):
        """Inicializa o importador de dados telemétricos."""
        super().__init__()
        self.drivers_importer = DriversImporter()
    
    def import_car_data(self, session_key, driver_numbers=None, limit=None):
        """
        Importa dados telemétricos dos carros para uma sessão específica.
        
        Args:
            session_key: Chave da sessão
            driver_numbers: Lista de números dos pilotos a importar (None para todos)
            limit: Limite de registros por piloto (None para todos)
        
        Returns:
            dict: Estatísticas da importação
        """
        if not self.supabase:
            return {"success": False, "error": "Supabase não conectado"}
        
        stats = {
            "fetched": 0,
            "inserted": 0,
            "deleted": 0,
            "errors": 0,
            "drivers_processed": 0
        }
        
        # Obter o ID da sessão
        session_id = self.get_session_id(session_key)
        if not session_id:
            return {"success": False, "error": f"Sessão {session_key} não encontrada"}
        
        # Se não foram especificados números de pilotos, buscar todos os pilotos da sessão
        if not driver_numbers:
            print("Nenhum piloto especificado. Importando dados de pilotos primeiro...")
            drivers_result = self.drivers_importer.import_drivers(session_key)
            driver_numbers = drivers_result.get("driver_numbers", [])
        
        if not driver_numbers:
            return {"success": False, "error": "Nenhum piloto encontrado para a sessão"}
        
        print(f"Processando {len(driver_numbers)} pilotos: {driver_numbers}")
        
        # Verificar registros existentes para esta sessão
        try:
            existing_query = self.supabase.table("car_telemetry").select("id").eq("session_id", session_id).execute()
            
            # Remover registros existentes para evitar duplicação
            if existing_query.data:
                existing_count = len(existing_query.data)
                print(f"Encontrados {existing_count} registros telemétricos existentes, removendo...")
                self.supabase.table("car_telemetry").delete().eq("session_id", session_id).execute()
                stats["deleted"] = existing_count
        except Exception as e:
            print(f"Erro ao verificar/remover registros existentes: {str(e)}")
            stats["errors"] += 1
        
        # Importar dados de cada piloto
        for driver_number in driver_numbers:
            try:
                # Buscar dados telemétricos do piloto
                car_data = self.fetch_data("car_data", {"session_key": session_key, "driver_number": driver_number})
                
                if not car_data:
                    print(f"Nenhum dado telemétrico encontrado para o piloto #{driver_number}")
                    continue
                
                stats["fetched"] += len(car_data)
                
                # Limitar registros se necessário
                if limit and limit < len(car_data):
                    print(f"Limitando de {len(car_data)} para {limit} registros do piloto #{driver_number}")
                    # Selecionar registros distribuídos uniformemente
                    step = len(car_data) // limit
                    car_data = car_data[::step][:limit]
                
                # Criar registros para inserção em lote
                car_records = []
                
                for data in car_data:
                    try:
                        # Criar registro para o banco
                        car_record = {
                            "session_id": session_id,
                            "timestamp": data.get("date"),
                            "driver_number": data.get("driver_number"),
                            "rpm": data.get("rpm"),
                            "speed": data.get("speed"),
                            "gear": data.get("n_gear"),
                            "throttle": data.get("throttle"),
                            "brake": data.get("brake"),
                            "drs": data.get("drs")
                        }
                        
                        car_records.append(car_record)
                    except Exception as e:
                        print(f"Erro ao processar registro telemétrico: {str(e)}")
                        stats["errors"] += 1
                
                # Inserir em lotes
                if car_records:
                    batch_size = 100
                    total_records = len(car_records)
                    
                    for i in range(0, total_records, batch_size):
                        batch = car_records[i:i + batch_size]
                        try:
                            self.supabase.table("car_telemetry").insert(batch).execute()
                            stats["inserted"] += len(batch)
                            print(f"Inseridos {i+1} a {min(i + batch_size, total_records)} de {total_records} registros telemétricos para piloto #{driver_number}")
                        except Exception as e:
                            print(f"Erro ao inserir lote de registros telemétricos: {str(e)}")
                            stats["errors"] += len(batch)
                
                stats["drivers_processed"] += 1
                
            except Exception as e:
                print(f"Erro ao processar dados do piloto #{driver_number}: {str(e)}")
                stats["errors"] += 1
        
        print(f"\nImportação de dados telemétricos concluída: {stats['inserted']} inseridos, {stats['drivers_processed']} pilotos processados")
        return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Importar dados telemétricos dos carros F1 da API OpenF1 para o Supabase")
    parser.add_argument("--session", type=int, required=True, help="Chave da sessão para importar dados telemétricos")
    parser.add_argument("--driver", type=int, action="append", help="Número do piloto para importar (pode ser usado múltiplas vezes)")
    parser.add_argument("--limit", type=int, help="Limite de registros por piloto a importar (None para todos)")
    
    args = parser.parse_args()
    
    importer = CarDataImporter()
    result = importer.import_car_data(args.session, args.driver, limit=args.limit)
    
    print("\nResultado da importação:")
    print(f"Total buscado: {result['fetched']}")
    print(f"Registros excluídos: {result['deleted']}")
    print(f"Inseridos: {result['inserted']}")
    print(f"Pilotos processados: {result['drivers_processed']}")
    print(f"Erros: {result['errors']}")