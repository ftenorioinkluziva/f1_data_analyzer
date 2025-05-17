#!/usr/bin/env python3
"""
pitstop_visualizer.py - Script para visualizar dados de pit stops em corridas de F1

Este script lê os dados processados de PitLaneTimeCollection do F1 Data Analyzer e gera
visualizações detalhadas dos pit stops durante uma sessão de corrida da F1.

Uso:
    python pitstop_visualizer.py --meeting 1264 --session 1297 --output-dir visualizations
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.dates as mdates

# Constantes e configurações
FIG_SIZE = (16, 10)  # Tamanho padrão para gráficos
DPI = 300  # Resolução para salvar imagens

# Cores para diferentes equipes
TEAM_COLORS = {
    'Mercedes': '#00D2BE',
    'Red Bull': '#0600EF',
    'Ferrari': '#DC0000',
    'Alpine': '#0090FF',
    'McLaren': '#FF8700',
    'Alfa Romeo': '#900000',
    'Aston Martin': '#006F62',
    'Haas': '#FFFFFF',
    'AlphaTauri': '#2B4562',
    'Williams': '#005AFF'
}

def parse_args():
    """Processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='Visualizar dados de pit stops da F1')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Chave do evento (ex: 1264 para Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Chave da sessão (ex: 1297 para corrida principal)')
    
    parser.add_argument('--driver', type=str, default=None,
                        help='Número do piloto específico para visualizar')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Diretório para salvar visualizações')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Nome personalizado para o evento')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Nome personalizado para a sessão')
    
    parser.add_argument('--dark-mode', action='store_true', default=False,
                        help='Usar tema escuro para visualizações')
    
    return parser.parse_args()

def load_pit_data(meeting_key, session_key):
    """
    Carrega os dados de pit stops da sessão.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        pd.DataFrame: DataFrame contendo os dados de pit stops ou None se não disponível
    """
    # Verificar várias possíveis localizações de arquivos
    possible_files = [
        f"f1_data/processed/{meeting_key}/{session_key}/PitLaneTimeCollection/pit_stops.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/StintAnalysis/pit_stops.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/PitLaneTimeCollection/raw_pit_stops.csv"
    ]
    
    for file_path in possible_files:
        if os.path.exists(file_path):
            try:
                print(f"Carregando dados de pit stops de: {file_path}")
                df = pd.read_csv(file_path)
                
                # Converter números de piloto para strings para consistência
                if 'driver_number' in df.columns:
                    df['driver_number'] = df['driver_number'].astype(str)
                
                # Se o arquivo existe mas está vazio
                if df.empty:
                    print(f"Aviso: Arquivo de pit stops vazio: {file_path}")
                    continue
                
                print(f"Dados de pit stops carregados: {len(df)} registros")
                return df
            except Exception as e:
                print(f"Erro ao carregar dados de pit stops de {file_path}: {str(e)}")
    
    print("Aviso: Nenhum arquivo de dados de pit stops encontrado ou utilizável")
    return None

def load_driver_info(meeting_key, session_key):
    """
    Carrega informações dos pilotos para correlacionar com os dados de pit stops.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        pd.DataFrame: DataFrame com informações dos pilotos, ou None se não disponível
    """
    driver_file = f"f1_data/processed/{meeting_key}/{session_key}/DriverList/driver_info.csv"
    
    if not os.path.exists(driver_file):
        print(f"Aviso: Informações de pilotos não encontradas: {driver_file}")
        return None
    
    try:
        print(f"Carregando informações de pilotos de: {driver_file}")
        df = pd.read_csv(driver_file)
        
        # Converter números de piloto para strings para consistência
        if 'driver_number' in df.columns:
            df['driver_number'] = df['driver_number'].astype(str)
        
        print(f"Informações de pilotos carregadas: {len(df)} pilotos")
        return df
    except Exception as e:
        print(f"Erro ao carregar informações de pilotos: {str(e)}")
        return None

def load_timing_data(meeting_key, session_key):
    """
    Carrega dados de tempos de volta para correlacionar com pit stops.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        pd.DataFrame: DataFrame com tempos de volta, ou None se não disponível
    """
    lap_file = f"f1_data/processed/{meeting_key}/{session_key}/TimingData/lap_times.csv"
    
    if not os.path.exists(lap_file):
        print(f"Aviso: Dados de tempos de volta não encontrados: {lap_file}")
        return None
    
    try:
        print(f"Carregando dados de tempos de volta de: {lap_file}")
        df = pd.read_csv(lap_file)
        
        # Converter números de piloto para strings para consistência
        if 'driver_number' in df.columns:
            df['driver_number'] = df['driver_number'].astype(str)
        
        print(f"Dados de tempos de volta carregados: {len(df)} registros")
        return df
    except Exception as e:
        print(f"Erro ao carregar dados de tempos de volta: {str(e)}")
        return None

def process_pit_data(pit_df, driver_info=None, timing_df=None, specific_driver=None):
    """
    Processa dados de pit stops para visualização.
    
    Args:
        pit_df: DataFrame com dados de pit stops
        driver_info: DataFrame com informações dos pilotos
        timing_df: DataFrame com tempos de volta
        specific_driver: Número do piloto específico para filtrar
        
    Returns:
        dict: Dicionário com dados processados
    """
    if pit_df is None:
        print("Erro: Nenhum dado de pit stops disponível para processamento")
        return None
    
    # Verificar colunas necessárias
    required_columns = ['driver_number', 'duration']
    missing_columns = [col for col in required_columns if col not in pit_df.columns]
    
    if missing_columns:
        print(f"Aviso: Dados de pit stops não contêm colunas essenciais: {missing_columns}")
        # Tentar mapear colunas alternativas
        column_mapping = {}
        for col in missing_columns:
            if col == 'driver_number' and 'racing_number' in pit_df.columns:
                column_mapping['racing_number'] = 'driver_number'
            elif col == 'duration' and 'Duration' in pit_df.columns:
                column_mapping['Duration'] = 'duration'
        
        # Aplicar mapeamento se encontrado
        if column_mapping:
            print(f"Usando mapeamento de colunas alternativas: {column_mapping}")
            pit_df = pit_df.rename(columns=column_mapping)
        else:
            print("Não foi possível encontrar colunas alternativas compatíveis")
    
    # Verificar novamente se temos as colunas necessárias
    missing_columns = [col for col in required_columns if col not in pit_df.columns]
    if missing_columns:
        print(f"Erro: Dados de pit stops ainda não contêm colunas essenciais: {missing_columns}")
        return None
    
    # Se especificado, filtrar para um piloto específico
    if specific_driver:
        pit_df = pit_df[pit_df['driver_number'] == str(specific_driver)].copy()
        if pit_df.empty:
            print(f"Erro: Nenhum pit stop encontrado para o piloto #{specific_driver}")
            return None
    
    # Converter duração para números se necessário
    if 'duration' in pit_df.columns and not pd.api.types.is_numeric_dtype(pit_df['duration']):
        try:
            pit_df['duration'] = pd.to_numeric(pit_df['duration'], errors='coerce')
            pit_df = pit_df.dropna(subset=['duration'])
        except Exception as e:
            print(f"Erro ao converter durações para números: {str(e)}")
    
    # Se temos dados de lap, adicionar ao dataframe
    if 'lap' in pit_df.columns:
        # Converter lap para número se necessário
        if not pd.api.types.is_numeric_dtype(pit_df['lap']):
            try:
                pit_df['lap'] = pd.to_numeric(pit_df['lap'], errors='coerce')
            except Exception as e:
                print(f"Aviso: Não foi possível converter número de voltas para números: {str(e)}")
    
    # Adicionar informações de pilotos se disponíveis
    if driver_info is not None:
        # Mesclar com informações de pilotos
        pit_df = pit_df.merge(driver_info[['driver_number', 'team_name', 'full_name', 'tla', 'last_name']], 
                             on='driver_number', how='left')
    
    # Se tempos de volta estão disponíveis, calcular lap delta
    if timing_df is not None and 'lap' in pit_df.columns:
        # Implementar futuramente: calcular impacto do pit stop no tempo de volta
        pass
    
    # Estatísticas básicas por piloto
    driver_stats = {}
    
    for driver, group in pit_df.groupby('driver_number'):
        # Nome do piloto (usar informações disponíveis ou número do piloto)
        driver_name = f"Driver #{driver}"
        team_name = None
        
        if 'last_name' in group.columns and not pd.isna(group['last_name'].iloc[0]):
            driver_name = group['last_name'].iloc[0]
        elif 'full_name' in group.columns and not pd.isna(group['full_name'].iloc[0]):
            driver_name = group['full_name'].iloc[0]
        elif 'tla' in group.columns and not pd.isna(group['tla'].iloc[0]):
            driver_name = group['tla'].iloc[0]
        
        # Equipe do piloto
        if 'team_name' in group.columns and not pd.isna(group['team_name'].iloc[0]):
            team_name = group['team_name'].iloc[0]
        
        # Estatísticas
        stats = {
            'name': driver_name,
            'team': team_name,
            'pit_stops': group.to_dict('records'),
            'total_stops': len(group),
            'avg_duration': group['duration'].mean() if 'duration' in group.columns else None,
            'min_duration': group['duration'].min() if 'duration' in group.columns else None,
            'max_duration': group['duration'].max() if 'duration' in group.columns else None
        }
        
        driver_stats[driver] = stats
    
    # Estatísticas gerais
    total_pit_stops = len(pit_df)
    avg_duration = pit_df['duration'].mean() if 'duration' in pit_df.columns else None
    
    result = {
        'driver_stats': driver_stats,
        'total_pit_stops': total_pit_stops,
        'avg_duration': avg_duration,
        'raw_data': pit_df
    }
    
    return result

def create_pit_durations_chart(pit_data, race_name, session_name, output_path, dark_mode=False):
    """
    Cria visualização da duração dos pit stops por piloto.
    
    Args:
        pit_data: Dicionário com dados processados de pit stops
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if not pit_data or 'driver_stats' not in pit_data or not pit_data['driver_stats']:
        print("Aviso: Dados insuficientes para visualização de durações de pit stops")
        return
    
    # Configurar tema escuro se solicitado
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
    
    # Extrair dados para o gráfico
    drivers = []
    durations = []
    colors = []
    
    # Ordenar pilotos por duração média de pit stop (mais rápido primeiro)
    sorted_drivers = sorted(
        [(d, s) for d, s in pit_data['driver_stats'].items() if s.get('avg_duration') is not None],
        key=lambda x: x[1]['avg_duration']
    )
    
    for driver_number, stats in sorted_drivers:
        if stats.get('avg_duration') is not None:
            drivers.append(stats['name'])
            durations.append(stats['avg_duration'])
            
            # Cor baseada na equipe se disponível
            if stats.get('team') and stats['team'] in TEAM_COLORS:
                colors.append(TEAM_COLORS[stats['team']])
            else:
                colors.append('skyblue')  # Cor padrão
    
    if not drivers:
        print("Aviso: Nenhum dado válido para o gráfico de durações de pit stops")
        return
    
    # Criar figura
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Criar gráfico de barras horizontais
    bars = plt.barh(drivers, durations, color=colors, alpha=0.8, edgecolor='black')
    
    # Adicionar rótulos de duração
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                 f"{width:.2f}s", ha='left', va='center', color=text_color)
    
    # Configurar título e rótulos
    plt.title(f"Pit Stop Durations - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel("Duration (seconds)", fontsize=12, color=text_color)
    plt.ylabel("Driver", fontsize=12, color=text_color)
    
    # Adicionar linha para média geral
    if pit_data.get('avg_duration') is not None:
        plt.axvline(x=pit_data['avg_duration'], color='red', linestyle='--', alpha=0.8)
        plt.text(pit_data['avg_duration'], plt.ylim()[0] - 0.5, 
                 f"Avg: {pit_data['avg_duration']:.2f}s", 
                 ha='center', va='top', color='red', fontsize=10)
    
    # Adicionar grade para facilitar a leitura
    plt.grid(axis='x', linestyle='--', alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de durações de pit stops salva em: {output_path}")
    plt.close()

def create_pit_timing_chart(pit_data, race_name, session_name, output_path, dark_mode=False):
    """
    Cria visualização do momento dos pit stops durante a corrida.
    
    Args:
        pit_data: Dicionário com dados processados de pit stops
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if not pit_data or 'raw_data' not in pit_data or pit_data['raw_data'].empty:
        print("Aviso: Dados insuficientes para visualização de timing de pit stops")
        return
    
    # Verificar se temos dados de lap
    if 'lap' not in pit_data['raw_data'].columns:
        print("Aviso: Dados de voltas não disponíveis para visualização de timing de pit stops")
        return
    
    # Configurar tema escuro se solicitado
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
    
    # Criar figura
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Extrair dados
    df = pit_data['raw_data']
    
    # Preparar pontos para scatter plot
    x = df['lap']  # Volta do pit stop
    y = []  # Posição vertical (baseada no driver_number)
    colors = []  # Cores baseadas na equipe/piloto
    sizes = []  # Tamanhos baseados na duração
    
    # Mapear números de piloto para índices Y consistentes
    driver_numbers = sorted(pit_data['driver_stats'].keys())
    driver_to_y = {d: i for i, d in enumerate(driver_numbers)}
    
    for _, row in df.iterrows():
        driver = row['driver_number']
        y.append(driver_to_y[driver])
        
        # Cor baseada na equipe se disponível
        if 'team_name' in row and pd.notna(row['team_name']) and row['team_name'] in TEAM_COLORS:
            colors.append(TEAM_COLORS[row['team_name']])
        else:
            colors.append('skyblue')  # Cor padrão
        
        # Tamanho baseado na duração (pit stops mais rápidos = pontos menores)
        if 'duration' in row and pd.notna(row['duration']):
            # Normalizar para pontos entre 50 e 200
            min_size = 50
            max_size = 200
            if 'duration' in df.columns:
                min_duration = df['duration'].min()
                max_duration = df['duration'].max()
                range_duration = max_duration - min_duration
                
                if range_duration > 0:
                    normalized_size = min_size + (row['duration'] - min_duration) / range_duration * (max_size - min_size)
                    sizes.append(normalized_size)
                else:
                    sizes.append(100)  # Tamanho padrão se não houver variação
            else:
                sizes.append(100)  # Tamanho padrão
        else:
            sizes.append(100)  # Tamanho padrão
    
    # Criar scatter plot
    scatter = plt.scatter(x, y, c=colors, s=sizes, alpha=0.7, edgecolor='black')
    
    # Configurar eixo Y (nomes dos pilotos)
    y_labels = [pit_data['driver_stats'][d]['name'] for d in driver_numbers]
    plt.yticks(range(len(driver_numbers)), y_labels)
    
    # Configurar título e rótulos
    plt.title(f"Pit Stop Timing - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel("Lap", fontsize=12, color=text_color)
    
    # Adicionar legendas para o tamanho dos pontos
    if 'duration' in df.columns:
        min_duration = df['duration'].min()
        max_duration = df['duration'].max()
        mid_duration = (min_duration + max_duration) / 2
        
        # Adicionar legenda para os tamanhos
        plt.scatter([], [], s=50, color='gray', alpha=0.7, edgecolor='black', 
                    label=f"Fast: {min_duration:.2f}s")
        plt.scatter([], [], s=125, color='gray', alpha=0.7, edgecolor='black', 
                    label=f"Mid: {mid_duration:.2f}s")
        plt.scatter([], [], s=200, color='gray', alpha=0.7, edgecolor='black', 
                    label=f"Slow: {max_duration:.2f}s")
        plt.legend(title="Pit Stop Duration", loc='upper right')
    
    # Adicionar grade para facilitar a leitura
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de timing de pit stops salva em: {output_path}")
    plt.close()

def create_pit_counts_chart(pit_data, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de quantidade de pit stops por piloto.
    
    Args:
        pit_data: Dicionário com dados processados de pit stops
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if not pit_data or 'driver_stats' not in pit_data or not pit_data['driver_stats']:
        print("Aviso: Dados insuficientes para gráfico de quantidade de pit stops")
        return
    
    # Configurar tema escuro se solicitado
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
    
    # Extrair dados para o gráfico
    driver_names = []
    pit_counts = []
    colors = []
    
    # Ordenar pilotos por número de pit stops (mais stops primeiro)
    sorted_drivers = sorted(
        pit_data['driver_stats'].items(),
        key=lambda x: x[1]['total_stops'],
        reverse=True
    )
    
    for driver_number, stats in sorted_drivers:
        driver_names.append(stats['name'])
        pit_counts.append(stats['total_stops'])
        
        # Cor baseada na equipe se disponível
        if stats.get('team') and stats['team'] in TEAM_COLORS:
            colors.append(TEAM_COLORS[stats['team']])
        else:
            colors.append('coral')  # Cor padrão
    
    # Criar figura
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Criar gráfico de barras
    bars = plt.bar(driver_names, pit_counts, color=colors, alpha=0.8, edgecolor='black')
    
    # Adicionar rótulos com contagem
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 0.1,
                 str(int(height)), ha='center', va='bottom', fontsize=10, color=text_color)
    
    # Configurar título e rótulos
    plt.title(f"Pit Stop Counts - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.ylabel("Number of Pit Stops", fontsize=12, color=text_color)
    
    # Rotacionar rótulos do eixo X se houver muitos pilotos
    if len(driver_names) > 8:
        plt.xticks(rotation=45, ha='right')
    
    # Adicionar grade para facilitar a leitura
    plt.grid(axis='y', linestyle='--', alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de quantidade de pit stops salva em: {output_path}")
    plt.close()

def main():
    """Função principal do script."""
    # Processar argumentos de linha de comando
    args = parse_args()
    
    # Extrair argumentos
    meeting_key = args.meeting
    session_key = args.session
    specific_driver = args.driver
    dark_mode = args.dark_mode
    
    # Definir o diretório de saída
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Usar o diretório de visualizações dentro dos dados processados
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/pitstops")
    
    # Criar diretório de saída se não existir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Definir nomes para o evento e sessão
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Carregar dados de pit stops
        pit_df = load_pit_data(meeting_key, session_key)
        
        if pit_df is None:
            print("Erro: Não foi possível carregar dados de pit stops")
            return 1
        
        # Carregar informações dos pilotos
        driver_info = load_driver_info(meeting_key, session_key)
        
        # Carregar dados de tempos de volta
        timing_df = load_timing_data(meeting_key, session_key)
        
        # Processar dados de pit stops
        pit_data = process_pit_data(pit_df, driver_info, timing_df, specific_driver)
        
        if pit_data is None:
            print("Erro: Não foi possível processar dados de pit stops")
            return 1
        
        # Criar visualização das durações de pit stops
        durations_path = output_dir / f"pit_durations_{meeting_key}_{session_key}.png"
        create_pit_durations_chart(pit_data, race_name, session_name, durations_path, dark_mode)
        
        # Criar visualização do timing dos pit stops
        timing_path = output_dir / f"pit_timing_{meeting_key}_{session_key}.png"
        create_pit_timing_chart(pit_data, race_name, session_name, timing_path, dark_mode)
        
        # Criar visualização da quantidade de pit stops
        counts_path = output_dir / f"pit_counts_{meeting_key}_{session_key}.png"
        create_pit_counts_chart(pit_data, race_name, session_name, counts_path, dark_mode)
        
        print("Todas as visualizações de pit stops foram geradas com sucesso!")
        print(f"As visualizações estão disponíveis em: {output_dir}")
        
    except Exception as e:
        print(f"Erro ao criar visualizações: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())