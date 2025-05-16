# openf1_weather_importer.py
from openf1_importer_base import OpenF1ImporterBase
from datetime import datetime

class WeatherImporter(OpenF1ImporterBase):
    """
    Importador de dados meteorológicos da API OpenF1 para o Supabase.
    """
    
    def import_weather(self, session_key, incremental=False, limit=None):
        """
        Importa dados meteorológicos para uma sessão específica.
        
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
        
        # Buscar dados meteorológicos da API
        weather_data = self.fetch_data("weather", {"session_key": session_key})
        stats["fetched"] = len(weather_data)
        
        if not weather_data:
            return stats
        
        # Limitar registros se necessário
        if limit and limit < len(weather_data):
            print(f"Limitando de {len(weather_data)} para {limit} registros meteorológicos")
            # Selecionar registros distribuídos uniformemente
            step = len(weather_data) // limit
            weather_data = weather_data[::step][:limit]
        
        # Se for incremental, vamos verificar quais timestamps já existem no banco
        if incremental:
            try:
                # Buscar os timestamps que já existem no banco
                existing_timestamps_query = self.supabase.table("weather_data")\
                    .select("timestamp")\
                    .eq("session_id", session_id)\
                    .execute()
                
                existing_timestamps = set()
                if existing_timestamps_query.data:
                    for record in existing_timestamps_query.data:
                        if "timestamp" in record:
                            # Extrair apenas a parte do timestamp sem a data (hh:mm:ss.fff)
                            if "T" in record["timestamp"]:
                                time_part = record["timestamp"].split("T")[1].split("+")[0].split(".")[0]
                                existing_timestamps.add(time_part)
                            else:
                                existing_timestamps.add(record["timestamp"])
                
                print(f"Encontrados {len(existing_timestamps)} timestamps existentes no banco")
            except Exception as e:
                print(f"Erro ao verificar timestamps existentes: {str(e)}")
                existing_timestamps = set()
        else:
            # No modo não-incremental, excluímos todos os registros existentes
            try:
                existing_query = self.supabase.table("weather_data").select("id").eq("session_id", session_id).execute()
                
                # Remover registros existentes para evitar duplicação
                if existing_query.data:
                    existing_count = len(existing_query.data)
                    print(f"Encontrados {existing_count} registros meteorológicos existentes, removendo...")
                    self.supabase.table("weather_data").delete().eq("session_id", session_id).execute()
                    stats["deleted"] = existing_count
            except Exception as e:
                print(f"Erro ao verificar/remover registros existentes: {str(e)}")
                stats["errors"] += 1
        
        # Criar registros para inserção em lote
        weather_records = []
        
        # Data base para timestamps
        session_date = datetime.now().strftime("%Y-%m-%d")
        
        for weather in weather_data:
            try:
                # Obter o timestamp original
                timestamp = weather.get("date")
                
                # Se estamos no modo incremental, verificar se já temos este timestamp
                if incremental:
                    # Extrair apenas a parte do timestamp sem a data (hh:mm:ss.fff)
                    if "T" in timestamp:
                        time_part = timestamp.split("T")[1].split("+")[0].split(".")[0]
                    else:
                        time_part = timestamp
                    
                    if time_part in existing_timestamps:
                        stats["skipped"] += 1
                        continue
                
                # Formatar timestamp para ISO
                if "T" not in timestamp:
                    # Se o timestamp não incluir a data, adicionar a data base
                    timestamp = f"{session_date}T{timestamp}"
                
                # Criar registro para o banco
                weather_record = {
                    "session_id": session_id,
                    "timestamp": timestamp,
                    "air_temp": weather.get("air_temperature"),
                    "track_temp": weather.get("track_temperature"),
                    "humidity": weather.get("humidity"),
                    "pressure": weather.get("pressure"),
                    "rainfall": weather.get("rainfall"),
                    "wind_direction": weather.get("wind_direction"),
                    "wind_speed": weather.get("wind_speed")
                }
                
                # Filtrar para apenas colunas válidas
                weather_record = self.filter_record_columns(weather_record, "weather_data")
                
                weather_records.append(weather_record)
            except Exception as e:
                print(f"Erro ao processar registro meteorológico: {str(e)}")
                stats["errors"] += 1
        
        # Inserir em lotes
        if weather_records:
            batch_size = 100
            total_records = len(weather_records)
            
            for i in range(0, total_records, batch_size):
                batch = weather_records[i:i + batch_size]
                try:
                    self.supabase.table("weather_data").insert(batch).execute()
                    stats["inserted"] += len(batch)
                    print(f"Inseridos {i+1} a {min(i + batch_size, total_records)} de {total_records} registros meteorológicos")
                except Exception as e:
                    print(f"Erro ao inserir lote de registros meteorológicos: {str(e)}")
                    stats["errors"] += len(batch)
        
        if incremental:
            print(f"\nImportação incremental de dados meteorológicos concluída: {stats['inserted']} inseridos, {stats['skipped']} ignorados (já existentes)")
        else:
            print(f"\nImportação completa de dados meteorológicos concluída: {stats['inserted']} inseridos")
        return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Importar dados meteorológicos F1 da API OpenF1 para o Supabase")
    parser.add_argument("--session", type=int, required=True, help="Chave da sessão para importar dados meteorológicos")
    parser.add_argument("--limit", type=int, help="Limite de registros a importar (None para todos)")
    parser.add_argument("--incremental", action="store_true", help="Modo incremental: adicionar apenas novos registros sem excluir existentes")
    
    args = parser.parse_args()
    
    importer = WeatherImporter()
    result = importer.import_weather(args.session, incremental=args.incremental, limit=args.limit)
    
    print("\nResultado da importação:")
    print(f"Total buscado: {result['fetched']}")
    if not args.incremental:
        print(f"Registros excluídos: {result['deleted']}")
    else:
        print(f"Registros ignorados (já existentes): {result['skipped']}")
    print(f"Inseridos: {result['inserted']}")
    print(f"Erros: {result['errors']}")