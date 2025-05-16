# auto_update_weather.py
import subprocess
import time
import argparse
import os
from datetime import datetime, timedelta

def run_update(session_key, count=None, interval=60, log_file=None):
    """
    Executa atualizações incrementais de dados meteorológicos.
    
    Args:
        session_key: Chave da sessão
        count: Número de atualizações (None para infinito)
        interval: Intervalo em segundos entre atualizações
        log_file: Arquivo de log para registrar as atualizações
    """
    i = 1
    start_time = datetime.now()
    
    # Configurar log
    log_enabled = log_file is not None
    if log_enabled:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    def log_message(message):
        """Registra uma mensagem no arquivo de log e exibe no console."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}"
        print(log_line)
        
        if log_enabled:
            try:
                with open(log_file, "a") as f:
                    f.write(log_line + "\n")
            except Exception as e:
                print(f"Erro ao escrever no arquivo de log: {str(e)}")
    
    log_message(f"Iniciando atualizações incrementais para sessão {session_key}")
    log_message(f"Intervalo: {interval} segundos")
    if count:
        log_message(f"Total de atualizações: {count}")
    else:
        log_message("Executando infinitamente (pressione Ctrl+C para parar)")
    
    # Estatísticas de atualização
    total_fetched = 0
    total_inserted = 0
    total_skipped = 0
    total_errors = 0
    
    try:
        while True:
            # Formatação e exibição do cabeçalho da atualização
            update_header = "\n" + "=" * 60
            if count:
                update_header += f"\n Atualização {i} de {count} - {datetime.now().strftime('%H:%M:%S')} "
            else:
                update_header += f"\n Atualização {i} - {datetime.now().strftime('%H:%M:%S')} "
            update_header += "\n" + "=" * 60
            log_message(update_header)
            
            # Executar o comando de importação
            cmd = ["python", "openf1_weather_importer.py", "--session", str(session_key), "--incremental"]
            
            try:
                # Capturar saída do processo
                process = subprocess.run(cmd, capture_output=True, text=True)
                
                # Exibir saída do processo
                if process.stdout:
                    for line in process.stdout.splitlines():
                        log_message(line)
                
                # Exibir erros, se houver
                if process.returncode != 0:
                    log_message(f"ERRO: Processo retornou código {process.returncode}")
                    if process.stderr:
                        for line in process.stderr.splitlines():
                            log_message(f"STDERR: {line}")
                    total_errors += 1
                
                # Extrair estatísticas
                if process.stdout:
                    output = process.stdout
                    if "Total buscado:" in output:
                        try:
                            fetched = int(output.split("Total buscado:")[1].split("\n")[0].strip())
                            total_fetched += fetched
                        except:
                            pass
                    
                    if "Inseridos:" in output:
                        try:
                            inserted = int(output.split("Inseridos:")[1].split("\n")[0].strip())
                            total_inserted += inserted
                        except:
                            pass
                    
                    if "Registros ignorados" in output:
                        try:
                            skipped = int(output.split("Registros ignorados (já existentes):")[1].split("\n")[0].strip())
                            total_skipped += skipped
                        except:
                            pass
                
            except Exception as e:
                log_message(f"Erro ao executar o comando: {str(e)}")
                total_errors += 1
            
            # Verificar se atingiu o número máximo de atualizações
            if count and i >= count:
                log_message("\nNúmero máximo de atualizações atingido.")
                break
            
            # Incrementar contador
            i += 1
            
            # Calcular e mostrar estatísticas
            elapsed_time = datetime.now() - start_time
            avg_updates_per_hour = (i - 1) / elapsed_time.total_seconds() * 3600
            
            stats_msg = "\n----- Estatísticas de Atualização -----"
            stats_msg += f"\nTempo decorrido: {elapsed_time}"
            stats_msg += f"\nAtualizações realizadas: {i-1}"
            stats_msg += f"\nMédia de atualizações por hora: {avg_updates_per_hour:.2f}"
            stats_msg += f"\nTotal de registros buscados: {total_fetched}"
            stats_msg += f"\nTotal de registros inseridos: {total_inserted}"
            stats_msg += f"\nTotal de registros ignorados: {total_skipped}"
            stats_msg += f"\nTotal de erros: {total_errors}"
            stats_msg += "\n-----------------------------------------"
            
            log_message(stats_msg)
            
            # Aguardar intervalo
            next_update_time = datetime.now() + timedelta(seconds=interval)
            log_message(f"\nPróxima atualização às {next_update_time.strftime('%H:%M:%S')} (aguardando {interval} segundos)")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        log_message("\nAtualização interrompida pelo usuário.")
    except Exception as e:
        log_message(f"\nErro inesperado no processo de atualização: {str(e)}")
    
    # Estatísticas finais
    final_stats = "\n" + "=" * 60
    final_stats += "\n ESTATÍSTICAS FINAIS "
    final_stats += "\n" + "=" * 60
    final_stats += f"\nDuração total: {datetime.now() - start_time}"
    final_stats += f"\nTotal de atualizações realizadas: {i-1}"
    final_stats += f"\nTotal de registros buscados: {total_fetched}"
    final_stats += f"\nTotal de registros inseridos: {total_inserted}"
    final_stats += f"\nTotal de registros ignorados: {total_skipped}"
    final_stats += f"\nTotal de erros: {total_errors}"
    
    log_message(final_stats)
    log_message("Processo de atualização finalizado.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Atualizar dados meteorológicos F1 incrementalmente")
    parser.add_argument("--session", type=int, required=True, help="Chave da sessão")
    parser.add_argument("--count", type=int, help="Número de atualizações (omitir para infinito)")
    parser.add_argument("--interval", type=int, default=60, help="Intervalo em segundos entre atualizações (padrão: 60)")
    parser.add_argument("--log", help="Caminho para o arquivo de log")
    
    args = parser.parse_args()
    
    # Gerar nome de arquivo de log automático se não fornecido
    log_file = args.log
    if not log_file:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"weather_update_session_{args.session}_{timestamp}.log")
    
    run_update(args.session, args.count, args.interval, log_file)