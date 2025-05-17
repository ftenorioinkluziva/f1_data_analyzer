from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.gridspec as gridspec
import argparse
import os
from scipy.signal import savgol_filter

"""
telemetry_visualizer.py - Script para visualizar telemetria de F1 a partir dos dados processados

Este script lê os dados processados de CarData.z do F1 Data Analyzer e gera
visualizações detalhadas da telemetria do carro, incluindo:
- Gráficos de velocidade por tempo/posição
- Visualizações de acelerador/freio
- Traçados de circuito coloridos por métricas
- Análises comparativas entre pilotos

Uso:
    python telemetry_visualizer.py --meeting 1264 --session 1297 --driver 1 --laps 5 --output-dir visualizations
"""

# Constantes e configurações
FIG_SIZE = (16, 10)  # Tamanho padrão para gráficos
DPI = 300  # Resolução para salvar imagens
DEFAULT_CMAP = 'viridis'  # Mapa de cores padrão
POS_CMAP = 'plasma'  # Mapa de cores para o traçado do circuito

# Cores para diferentes métricas
COLORS = {
    'speed': 'blue',
    'rpm': 'red',
    'throttle': 'green',
    'brake': 'orange',
    'gear': 'purple',
    'drs': 'cyan'
}

def parse_args():
    """Processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='Visualizar telemetria de F1')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Chave do evento (ex: 1264 para Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Chave da sessão (ex: 1297 para corrida principal)')
    
    parser.add_argument('--driver', type=str, required=True,
                        help='Número do piloto para visualizar telemetria')
    
    parser.add_argument('--compare', type=str, default=None,
                        help='Número do piloto para comparar telemetria (opcional)')
    
    parser.add_argument('--laps', type=int, default=None,
                        help='Número específico de voltas para visualizar (usa voltas mais rápidas se não especificado)')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Diretório para salvar visualizações')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Nome personalizado para o evento')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Nome personalizado para a sessão')
    
    parser.add_argument('--smooth', action='store_true', default=False,
                       help='Aplicar suavização aos dados de telemetria')
    
    return parser.parse_args()

def load_telemetry_data(meeting_key, session_key, driver_number):
    """
    Carrega os dados de telemetria do piloto especificado.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        driver_number: Número do piloto
        
    Returns:
        pd.DataFrame: DataFrame contendo os dados de telemetria
    """
    # Tentar carregar o arquivo específico do piloto primeiro (é mais eficiente)
    driver_file = f"f1_data/processed/{meeting_key}/{session_key}/CarData.z/drivers/telemetry_driver_{driver_number}.csv"
    
    # Se não existir arquivo específico do piloto, carregar o arquivo geral e filtrar
    if not os.path.exists(driver_file):
        general_file = f"f1_data/processed/{meeting_key}/{session_key}/CarData.z/car_data.csv"
        
        if not os.path.exists(general_file):
            raise FileNotFoundError(f"Arquivo de telemetria não encontrado: {general_file}")
        
        print(f"Carregando dados de telemetria de: {general_file}")
        df = pd.read_csv(general_file)
        
        # Filtrar para o piloto especificado
        df = df[df['driver_number'].astype(str) == str(driver_number)].copy()
    else:
        print(f"Carregando dados de telemetria do piloto #{driver_number} de: {driver_file}")
        df = pd.read_csv(driver_file)
    
    if df.empty:
        raise ValueError(f"Nenhum dado de telemetria encontrado para o piloto #{driver_number}")
    
    # Converter números de piloto para string para consistência
    df['driver_number'] = df['driver_number'].astype(str)
    
    # Ordenar por timestamp
    if 'timestamp' in df.columns:
        df = df.sort_values('timestamp')
    
    # Normalizar os valores
    if 'throttle' in df.columns:
        # Normalizar throttle para 0-100%
        if df['throttle'].max() <= 1.0:
            df['throttle'] = df['throttle'] * 100
    
    if 'brake' in df.columns:
        # Normalizar brake para 0-100%
        if df['brake'].max() <= 1.0:
            df['brake'] = df['brake'] * 100
    
    # Converter DRS para valores binários (0=desativado, 1=ativado)
    if 'drs' in df.columns:
        df['drs'] = df['drs'].apply(lambda x: 1 if x > 0 else 0)
    
    # Verificar e corrigir valores inválidos
    for col in ['speed', 'rpm', 'gear']:
        if col in df.columns and df[col].isnull().any():
            print(f"Aviso: Valores nulos encontrados na coluna '{col}', serão preenchidos por interpolação")
            df[col] = df[col].interpolate(method='linear')
    
    print(f"Dados carregados: {len(df)} pontos de telemetria para o piloto #{driver_number}")
    return df

def load_position_data(meeting_key, session_key, driver_number):
    """
    Carrega os dados de posição do piloto para correlacionar com a telemetria.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        driver_number: Número do piloto
        
    Returns:
        pd.DataFrame: DataFrame contendo os dados de posição, ou None se não disponível
    """
    # Tentar carregar o arquivo específico do piloto primeiro
    driver_file = f"f1_data/processed/{meeting_key}/{session_key}/Position.z/drivers/position_driver_{driver_number}.csv"
    
    # Se não existir arquivo específico do piloto, carregar o arquivo geral e filtrar
    if not os.path.exists(driver_file):
        general_file = f"f1_data/processed/{meeting_key}/{session_key}/Position.z/position_data.csv"
        
        if not os.path.exists(general_file):
            print(f"Aviso: Dados de posição não encontrados. Visualizações baseadas em posição não serão geradas.")
            return None
        
        print(f"Carregando dados de posição de: {general_file}")
        df = pd.read_csv(general_file)
        
        # Filtrar para o piloto especificado
        df = df[df['driver_number'].astype(str) == str(driver_number)].copy()
    else:
        print(f"Carregando dados de posição do piloto #{driver_number} de: {driver_file}")
        df = pd.read_csv(driver_file)
    
    if df.empty:
        print(f"Aviso: Nenhum dado de posição encontrado para o piloto #{driver_number}")
        return None
    
    # Converter números de piloto para string para consistência
    df['driver_number'] = df['driver_number'].astype(str)
    
    # Ordenar por timestamp
    if 'timestamp' in df.columns:
        df = df.sort_values('timestamp')
    
    print(f"Dados de posição carregados: {len(df)} pontos para o piloto #{driver_number}")
    return df

def load_timing_data(meeting_key, session_key, driver_number):
    """
    Carrega os dados de tempos de volta para identificar voltas específicas.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        driver_number: Número do piloto
        
    Returns:
        pd.DataFrame: DataFrame contendo os tempos de volta, ou None se não disponível
    """
    # Caminho para o arquivo de tempos de volta
    lap_file = f"f1_data/processed/{meeting_key}/{session_key}/TimingData/lap_times.csv"
    
    if not os.path.exists(lap_file):
        print(f"Aviso: Dados de tempos de volta não encontrados: {lap_file}")
        return None
    
    try:
        df = pd.read_csv(lap_file)
        
        # Filtrar para o piloto especificado
        df = df[df['driver_number'].astype(str) == str(driver_number)].copy()
        
        if df.empty:
            print(f"Aviso: Nenhum tempo de volta encontrado para o piloto #{driver_number}")
            return None
        
        # Ordenar por timestamp
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
        
        print(f"Dados de tempos de volta carregados: {len(df)} voltas para o piloto #{driver_number}")
        return df
    except Exception as e:
        print(f"Erro ao carregar dados de tempos de volta: {str(e)}")
        return None

def identify_laps(telemetry_df, timing_df, num_laps=3):
    """
    Identifica as voltas mais rápidas ou específicas para análise.
    
    Args:
        telemetry_df: DataFrame com dados de telemetria
        timing_df: DataFrame com tempos de volta
        num_laps: Número de voltas para identificar
        
    Returns:
        dict: Dicionário com timestamps de início/fim de cada volta ou None se não for possível identificar
    """
    if timing_df is None:
        print("Aviso: Não é possível identificar voltas sem dados de tempos de volta")
        return None
    
    # Verificar se temos as colunas necessárias
    if 'lap_time' not in timing_df.columns or 'timestamp' not in timing_df.columns:
        print("Aviso: Dados de tempos de volta faltando colunas necessárias")
        return None
    
    try:
        # Converter tempos de volta para segundos se estiverem em formato "mm:ss.sss"
        if isinstance(timing_df['lap_time'].iloc[0], str) and ':' in timing_df['lap_time'].iloc[0]:
            def convert_to_seconds(time_str):
                try:
                    parts = time_str.split(':')
                    if len(parts) == 2:
                        return float(parts[0]) * 60 + float(parts[1])
                    else:
                        return float(time_str)
                except:
                    return None
            
            timing_df['lap_seconds'] = timing_df['lap_time'].apply(convert_to_seconds)
        else:
            timing_df['lap_seconds'] = timing_df['lap_time']
        
        # Ordenar voltas por tempo (mais rápido primeiro)
        fastest_laps = timing_df.sort_values('lap_seconds').head(num_laps).copy()
        
        if len(fastest_laps) == 0:
            print("Aviso: Nenhuma volta válida encontrada")
            return None
        
        print(f"Identificadas {len(fastest_laps)} voltas mais rápidas")
        
        # Correlacionar voltas com dados de telemetria
        lap_segments = {}
        
        for i, (idx, lap) in enumerate(fastest_laps.iterrows()):
            lap_time = lap['lap_time']
            lap_end = lap['timestamp']
            
            # Encontrar o timestamp correspondente na telemetria
            if 'timestamp' in telemetry_df.columns:
                # Encontrar o índice do timestamp de fim de volta na telemetria
                closest_idx = telemetry_df['timestamp'].searchsorted(lap_end)
                
                # Estimar tempo de uma volta em número de pontos de telemetria
                avg_points_per_lap = len(telemetry_df) / len(timing_df)
                
                # Estimar índice de início de volta
                start_idx = max(0, int(closest_idx - avg_points_per_lap))
                
                lap_segments[i+1] = {
                    'start_idx': start_idx,
                    'end_idx': closest_idx,
                    'lap_time': lap_time,
                    'lap_number': i+1
                }
            
        return lap_segments
    
    except Exception as e:
        print(f"Erro ao identificar voltas: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def smooth_telemetry(df, smooth_window=15, poly_order=3):
    """
    Aplica um filtro de suavização nos dados de telemetria.
    
    Args:
        df: DataFrame com dados de telemetria
        smooth_window: Tamanho da janela de suavização
        poly_order: Ordem polinomial para o filtro Savitzky-Golay
        
    Returns:
        pd.DataFrame: DataFrame com dados suavizados
    """
    smoothed_df = df.copy()
    
    # Colunas para suavizar
    columns_to_smooth = ['speed', 'rpm', 'throttle', 'brake']
    
    # Aplicar suavização às colunas disponíveis
    for col in columns_to_smooth:
        if col in df.columns and len(df) > smooth_window:
            try:
                smoothed_df[col] = savgol_filter(df[col], smooth_window, poly_order)
            except Exception as e:
                print(f"Aviso: Não foi possível suavizar a coluna {col}: {str(e)}")
    
    return smoothed_df

def create_telemetry_over_time(telemetry_df, race_name, session_name, driver_number, lap_segments, output_path, smooth=False):
    """
    Cria visualização de telemetria ao longo do tempo.
    
    Args:
        telemetry_df: DataFrame com dados de telemetria
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        driver_number: Número do piloto
        lap_segments: Dicionário com informações das voltas ou None
        output_path: Caminho para salvar a visualização
        smooth: Aplicar suavização aos dados
    """
    # Aplicar suavização se solicitado
    if smooth:
        telemetry_df = smooth_telemetry(telemetry_df)
    
    # Criar figura
    fig = plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Definir layout com 4 subplots compartilhando o eixo X
    gs = gridspec.GridSpec(4, 1, height_ratios=[3, 2, 2, 1])
    
    # Criar array de pontos para o eixo X (ponto de dados)
    x = np.arange(len(telemetry_df))
    
    # 1. Gráfico de Velocidade
    ax1 = plt.subplot(gs[0])
    ax1.plot(x, telemetry_df['speed'], color=COLORS['speed'], linewidth=1.5)
    ax1.set_ylabel('Speed (km/h)', fontsize=10)
    ax1.set_title(f"Telemetry - {race_name} - {session_name} - Driver #{driver_number}", fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # 2. Gráfico de RPM
    ax2 = plt.subplot(gs[1], sharex=ax1)
    ax2.plot(x, telemetry_df['rpm'], color=COLORS['rpm'], linewidth=1.5)
    ax2.set_ylabel('Engine RPM', fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    # 3. Gráfico de Throttle/Brake
    ax3 = plt.subplot(gs[2], sharex=ax1)
    ax3.plot(x, telemetry_df['throttle'], color=COLORS['throttle'], linewidth=1.5, label='Throttle %')
    
    if 'brake' in telemetry_df.columns:
        ax3.plot(x, telemetry_df['brake'], color=COLORS['brake'], linewidth=1.5, label='Brake %')
    
    ax3.set_ylabel('Pedal %', fontsize=10)
    ax3.set_ylim(-5, 105)  # Dar margem para visualização
    ax3.grid(True, alpha=0.3)
    ax3.legend(loc='upper right')
    
    # 4. Gráfico de Gear/DRS
    ax4 = plt.subplot(gs[3], sharex=ax1)
    
    if 'gear' in telemetry_df.columns:
        ax4.plot(x, telemetry_df['gear'], color=COLORS['gear'], linewidth=1.5, label='Gear')
        ax4.set_ylabel('Gear', fontsize=10)
        
        # Ajustar limites do eixo Y para os valores de marcha
        max_gear = telemetry_df['gear'].max()
        ax4.set_ylim(-0.5, max_gear + 0.5)
        
        # Definir ticks específicos para marchas
        ax4.set_yticks(range(int(max_gear) + 1))
    
    if 'drs' in telemetry_df.columns:
        # Criar um segundo eixo Y para DRS
        ax4_twin = ax4.twinx()
        ax4_twin.plot(x, telemetry_df['drs'] * max_gear, color=COLORS['drs'], linewidth=1, alpha=0.7, label='DRS')
        ax4_twin.set_ylabel('DRS', fontsize=10)
        ax4_twin.set_ylim(-0.5, max_gear + 0.5)
        ax4_twin.set_yticks([0, max_gear])
        ax4_twin.set_yticklabels(['OFF', 'ON'])
    
    ax4.grid(True, alpha=0.3)
    ax4.set_xlabel('Data Point', fontsize=10)
    
    # Adicionar marcadores de volta se disponíveis
    if lap_segments:
        for lap_num, lap_info in lap_segments.items():
            start_idx = lap_info['start_idx']
            end_idx = lap_info['end_idx']
            
            # Adicionar marcações verticais para início e fim de volta
            for ax in [ax1, ax2, ax3, ax4]:
                ax.axvline(x=start_idx, color='k', linestyle='--', alpha=0.5)
                ax.axvline(x=end_idx, color='k', linestyle='--', alpha=0.5)
            
            # Adicionar texto indicando o número da volta
            ax1.text(start_idx + (end_idx - start_idx) / 2, ax1.get_ylim()[1] * 0.9,
                    f"Lap {lap_num} - {lap_info['lap_time']}", ha='center', 
                    bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.3'))
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização de telemetria salva em: {output_path}")
    plt.close()

def create_speed_trace_visualization(telemetry_df, position_df, race_name, session_name, driver_number, output_path, metric='speed', smooth=False):
    """
    Cria visualização do traçado do circuito colorido por velocidade ou outra métrica.
    
    Args:
        telemetry_df: DataFrame com dados de telemetria
        position_df: DataFrame com dados de posição
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        driver_number: Número do piloto
        output_path: Caminho para salvar a visualização
        metric: Métrica para colorir o traçado ('speed', 'rpm', 'throttle', 'brake')
        smooth: Aplicar suavização aos dados
    """
    if position_df is None:
        print("Aviso: Dados de posição não disponíveis para visualização do traçado")
        return
    
    # Verificar se temos a métrica solicitada
    if metric not in telemetry_df.columns:
        print(f"Aviso: Métrica '{metric}' não disponível nos dados de telemetria")
        return
    
    # Aplicar suavização se solicitado
    if smooth:
        telemetry_df = smooth_telemetry(telemetry_df)
    
    # Sincronizar telemetria com posição
    # Este é um problema complexo, já que os timestamps podem não ser exatamente compatíveis
    # Vamos usar uma abordagem simplificada: interpolar valores com base no índice
    
    # Garantir que ambos os DataFrames estão ordenados por timestamp
    if 'timestamp' in telemetry_df.columns and 'timestamp' in position_df.columns:
        telemetry_df = telemetry_df.sort_values('timestamp')
        position_df = position_df.sort_values('timestamp')
    
    # Criar arrays de posição X, Y
    x = position_df['x'].values
    y = position_df['y'].values
    
    # Interpolar os valores da métrica para os pontos de posição
    if len(telemetry_df) > 1 and len(position_df) > 1:
        # Criar uma versão normalizada dos índices
        norm_telemetry_idx = np.linspace(0, 1, len(telemetry_df))
        norm_position_idx = np.linspace(0, 1, len(position_df))
        
        # Interpolar valores da métrica nos pontos de posição
        metric_values = np.interp(norm_position_idx, norm_telemetry_idx, telemetry_df[metric].values)
    else:
        print("Aviso: Dados insuficientes para interpolar valores")
        return
    
    # Criar figura
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Plotar o traçado do circuito colorido pela métrica
    scatter = plt.scatter(
        x, y,
        c=metric_values,
        cmap=POS_CMAP,
        s=5,
        alpha=0.8
    )
    
    # Adicionar barra de cores
    cbar = plt.colorbar(scatter)
    
    # Definir rótulo da barra de cores baseado na métrica
    metric_labels = {
        'speed': 'Speed (km/h)',
        'rpm': 'Engine RPM',
        'throttle': 'Throttle %',
        'brake': 'Brake %',
        'gear': 'Gear',
        'drs': 'DRS'
    }
    cbar.set_label(metric_labels.get(metric, metric.title()))
    
    # Configurar título e rótulos
    title = f"Track {metric.title()} Trace - {race_name} - {session_name} - Driver #{driver_number}"
    plt.title(title, fontsize=14)
    plt.xlabel("X Coordinate", fontsize=12)
    plt.ylabel("Y Coordinate", fontsize=12)
    
    # Garantir que o gráfico seja proporcional
    plt.axis('equal')
    
    # Adicionar grade
    plt.grid(True, alpha=0.3)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização do traçado por {metric} salva em: {output_path}")
    plt.close()

def create_driver_comparison(telemetry1_df, telemetry2_df, race_name, session_name, driver1, driver2, output_path, smooth=False):
    """
    Cria visualização comparativa de telemetria entre dois pilotos.
    
    Args:
        telemetry1_df: DataFrame com dados de telemetria do primeiro piloto
        telemetry2_df: DataFrame com dados de telemetria do segundo piloto
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        driver1: Número do primeiro piloto
        driver2: Número do segundo piloto
        output_path: Caminho para salvar a visualização
        smooth: Aplicar suavização aos dados
    """
    # Aplicar suavização se solicitado
    if smooth:
        telemetry1_df = smooth_telemetry(telemetry1_df)
        telemetry2_df = smooth_telemetry(telemetry2_df)
    
    # Criar arrays de pontos para o eixo X (porcentagem da volta)
    # Isso permite comparar voltas de comprimentos diferentes
    x1 = np.linspace(0, 100, len(telemetry1_df))
    x2 = np.linspace(0, 100, len(telemetry2_df))
    
    # Criar figura com subplots
    fig, axs = plt.subplots(3, 1, figsize=FIG_SIZE, dpi=100, sharex=True)
    
    # 1. Comparação de velocidade
    axs[0].plot(x1, telemetry1_df['speed'], color='blue', linewidth=1.5, label=f'Driver #{driver1}')
    axs[0].plot(x2, telemetry2_df['speed'], color='red', linewidth=1.5, label=f'Driver #{driver2}')
    axs[0].set_ylabel('Speed (km/h)', fontsize=10)
    axs[0].set_title(f"Driver Comparison - {race_name} - {session_name}", fontsize=14)
    axs[0].grid(True, alpha=0.3)
    axs[0].legend()
    
    # 2. Comparação de RPM
    axs[1].plot(x1, telemetry1_df['rpm'], color='blue', linewidth=1.5, alpha=0.7)
    axs[1].plot(x2, telemetry2_df['rpm'], color='red', linewidth=1.5, alpha=0.7)
    axs[1].set_ylabel('Engine RPM', fontsize=10)
    axs[1].grid(True, alpha=0.3)
    
    # 3. Comparação de Throttle
    axs[2].plot(x1, telemetry1_df['throttle'], color='blue', linewidth=1.5, alpha=0.7)
    axs[2].plot(x2, telemetry2_df['throttle'], color='red', linewidth=1.5, alpha=0.7)
    axs[2].set_ylabel('Throttle %', fontsize=10)
    axs[2].set_ylim(-5, 105)
    axs[2].grid(True, alpha=0.3)
    
    # Configurar eixo X
    axs[2].set_xlabel('Lap Percentage %', fontsize=10)
    axs[2].set_xlim(0, 100)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização de comparação entre pilotos salva em: {output_path}")
    plt.close()

def create_lap_comparison(telemetry_df, position_df, timing_df, race_name, session_name, driver_number, lap_segments, output_path, smooth=False):
    """
    Cria visualização comparativa entre diferentes voltas do mesmo piloto.
    
    Args:
        telemetry_df: DataFrame com dados de telemetria
        position_df: DataFrame com dados de posição
        timing_df: DataFrame com tempos de volta
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        driver_number: Número do piloto
        lap_segments: Dicionário com informações das voltas
        output_path: Caminho para salvar a visualização
        smooth: Aplicar suavização aos dados
    """
    if lap_segments is None or len(lap_segments) < 2:
        print("Aviso: Dados insuficientes para comparação de voltas")
        return
    
    # Aplicar suavização se solicitado
    if smooth:
        telemetry_df = smooth_telemetry(telemetry_df)
    
    # Criar figura com subplots
    fig, axs = plt.subplots(2, 1, figsize=FIG_SIZE, dpi=100, sharex=True)
    
    # Definir cores para diferentes voltas
    lap_colors = ['blue', 'red', 'green', 'purple', 'orange']
    
    # Para cada volta, extrair e plotar os dados
    for i, (lap_num, lap_info) in enumerate(lap_segments.items()):
        # Extrair dados desta volta
        start_idx = lap_info['start_idx']
        end_idx = lap_info['end_idx']
        lap_data = telemetry_df.iloc[start_idx:end_idx].copy()
        
        # Criar array para o eixo X (porcentagem de volta)
        x = np.linspace(0, 100, len(lap_data))
        
        # Cor para esta volta (ciclando pelas cores disponíveis)
        color = lap_colors[i % len(lap_colors)]
        
        # 1. Velocidade por porcentagem de volta
        axs[0].plot(x, lap_data['speed'], color=color, linewidth=1.5, 
                   label=f"Lap {lap_num} - {lap_info['lap_time']}")
        
        # 2. Throttle por porcentagem de volta
        axs[1].plot(x, lap_data['throttle'], color=color, linewidth=1.5)
    
    # Configurar gráficos
    axs[0].set_title(f"Lap Comparison - {race_name} - {session_name} - Driver #{driver_number}", fontsize=14)
    axs[0].set_ylabel('Speed (km/h)', fontsize=10)
    axs[0].grid(True, alpha=0.3)
    axs[0].legend()
    
    axs[1].set_ylabel('Throttle %', fontsize=10)
    axs[1].set_xlabel('Lap Percentage %', fontsize=10)
    axs[1].set_ylim(-5, 105)
    axs[1].grid(True, alpha=0.3)
    axs[1].set_xlim(0, 100)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização de comparação de voltas salva em: {output_path}")
    plt.close()

def main():
    """Função principal do script."""
    # Processar argumentos de linha de comando
    args = parse_args()
    
    # Extrair argumentos
    meeting_key = args.meeting
    session_key = args.session
    driver_number = args.driver
    compare_driver = args.compare
    smooth = args.smooth
    
    # Definir o diretório de saída
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Usar o diretório de visualizações dentro dos dados processados
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/telemetry")
    
    # Criar diretório de saída se não existir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Definir nomes para o evento e sessão
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Carregar dados de telemetria para o piloto principal
        telemetry_df = load_telemetry_data(meeting_key, session_key, driver_number)
        
        # Carregar dados de posição (para traçado do circuito)
        position_df = load_position_data(meeting_key, session_key, driver_number)
        
        # Carregar dados de tempos de volta
        timing_df = load_timing_data(meeting_key, session_key, driver_number)
        
        # Identificar voltas específicas para análise
        laps_to_analyze = args.laps or 3  # Valor padrão: 3 voltas
        lap_segments = identify_laps(telemetry_df, timing_df, laps_to_analyze)
        
        # 1. Visualização de telemetria ao longo do tempo
        telemetry_path = output_dir / f"telemetry_driver_{driver_number}_{meeting_key}_{session_key}.png"
        create_telemetry_over_time(telemetry_df, race_name, session_name, driver_number, lap_segments, telemetry_path, smooth)
        
        # 2. Visualização do traçado por velocidade
        if position_df is not None:
            for metric in ['speed', 'throttle', 'brake']:
                if metric in telemetry_df.columns:
                    trace_path = output_dir / f"track_{metric}_driver_{driver_number}_{meeting_key}_{session_key}.png"
                    create_speed_trace_visualization(telemetry_df, position_df, race_name, session_name, driver_number, trace_path, metric, smooth)
        
        # 3. Comparação de voltas
        if lap_segments and len(lap_segments) >= 2:
            lap_compare_path = output_dir / f"lap_comparison_driver_{driver_number}_{meeting_key}_{session_key}.png"
            create_lap_comparison(telemetry_df, position_df, timing_df, race_name, session_name, driver_number, lap_segments, lap_compare_path, smooth)
        
        # 4. Comparação com outro piloto (se especificado)
        if compare_driver:
            try:
                # Carregar dados do segundo piloto
                compare_telemetry_df = load_telemetry_data(meeting_key, session_key, compare_driver)
                
                # Criar visualização comparativa
                compare_path = output_dir / f"driver_compare_{driver_number}_vs_{compare_driver}_{meeting_key}_{session_key}.png"
                create_driver_comparison(telemetry_df, compare_telemetry_df, race_name, session_name, driver_number, compare_driver, compare_path, smooth)
            except Exception as e:
                print(f"Erro ao criar comparação entre pilotos: {str(e)}")
        
        print("Todas as visualizações de telemetria foram geradas com sucesso!")
        print(f"As visualizações estão disponíveis em: {output_dir}")
        
    except Exception as e:
        print(f"Erro ao criar visualizações de telemetria: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())