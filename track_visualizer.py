#!/usr/bin/env python3
"""
track_visualizer.py - Script para visualizar traçados de circuitos F1 a partir de dados de posição

Este script lê os dados processados de Position.z do F1 Data Analyzer e gera
visualizações 2D e 3D do traçado do circuito, com opções para customizar os gráficos.

Uso:
    python track_visualizer.py --meeting 1264 --session 1297 --driver 1 --all-drivers False --output-dir visualizations

Autor: [Seu Nome]
Data: [Data atual]
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path
import matplotlib.gridspec as gridspec

# Constantes e configurações
DEFAULT_CMAP = 'viridis'  # Mapa de cores padrão
SCATTER_SIZES = {
    '2d': 2,     # Tamanho dos pontos no gráfico 2D
    '3d': 0.5    # Tamanho dos pontos no gráfico 3D (menores no 3D para melhor visualização)
}
DEFAULT_ALPHA = 0.8  # Transparência padrão dos pontos
FIG_SIZE = {
    '2d': (14, 12),  # Tamanho da figura 2D
    '3d': (16, 14)   # Tamanho da figura 3D
}
DPI = 300  # Resolução das imagens salvas

def parse_args():
    """Processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='Visualizar traçados de circuitos F1')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Chave do evento (ex: 1264 para Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Chave da sessão (ex: 1297 para corrida principal)')
    
    parser.add_argument('--driver', type=str, default=None,
                        help='Número do piloto específico para visualizar (ex: 1 para o piloto #1)')
    
    parser.add_argument('--all-drivers', type=bool, default=False,
                        help='Visualizar traçados de todos os pilotos juntos')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Diretório para salvar visualizações (padrão: diretório de dados processados)')
    
    parser.add_argument('--cmap', type=str, default=DEFAULT_CMAP,
                        help=f'Mapa de cores para os traçados (padrão: {DEFAULT_CMAP})')
    
    parser.add_argument('--2d-only', dest='two_d_only', action='store_true',
                        help='Gerar apenas visualizações 2D')
    
    parser.add_argument('--3d-only', dest='three_d_only', action='store_true',
                        help='Gerar apenas visualizações 3D')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Nome personalizado para o evento (ex: "Miami Grand Prix")')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Nome personalizado para a sessão (ex: "Practice 1")')
    
    return parser.parse_args()

def load_position_data(meeting_key, session_key):
    """
    Carrega os dados de posição do arquivo processado.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        pd.DataFrame: DataFrame contendo os dados de posição
    """
    # Definir o caminho para o arquivo de dados
    position_file = f"f1_data/processed/{meeting_key}/{session_key}/Position.z/position_data.csv"
    
    # Verificar se o arquivo existe
    if not os.path.exists(position_file):
        raise FileNotFoundError(f"Arquivo de posição não encontrado: {position_file}")
    
    # Carregar o arquivo CSV
    print(f"Carregando dados de posição de: {position_file}")
    df = pd.read_csv(position_file)
    
    # Verificar colunas esperadas
    required_columns = ['driver_number', 'x', 'y', 'z']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    # Mapear nomes de colunas padrão para possíveis alternativas
    column_mapping = {
        'x': ['x', 'x_coord', 'X'],
        'y': ['y', 'y_coord', 'Y'],
        'z': ['z', 'z_coord', 'Z'],
        'driver_number': ['driver_number', 'driver', 'DriverNumber']
    }
    
    # Se colunas estiverem faltando, verificar nomes alternativos
    if missing_columns:
        print(f"Colunas ausentes: {missing_columns}")
        print("Verificando nomes de colunas alternativos...")
        
        # Criar um novo mapeamento de colunas
        new_columns = {}
        for miss_col in missing_columns:
            for col in df.columns:
                if col in column_mapping[miss_col]:
                    new_columns[col] = miss_col
                    print(f"Usando '{col}' como '{miss_col}'")
                    break
        
        # Renomear colunas se necessário
        if new_columns:
            df = df.rename(columns=new_columns)
    
    # Verificar se ainda faltam colunas essenciais
    still_missing = [col for col in required_columns if col not in df.columns]
    if still_missing:
        raise ValueError(f"Dados de posição não contêm colunas essenciais: {still_missing}")
    
    # Converter números do piloto para strings para consistência
    df['driver_number'] = df['driver_number'].astype(str)
    
    # Informações básicas sobre os dados carregados
    print(f"Dados carregados: {len(df)} pontos de posição")
    print(f"Pilotos disponíveis: {sorted(df['driver_number'].unique())}")
    
    return df

def prepare_track_data(df, driver_number=None):
    """
    Prepara os dados do traçado para visualização.
    
    Args:
        df: DataFrame com dados de posição
        driver_number: Número do piloto específico para visualizar ou None para todos
        
    Returns:
        pd.DataFrame: DataFrame filtrado com dados preparados para visualização
    """
    if driver_number:
        # Filtrar para um piloto específico
        driver_data = df[df['driver_number'] == str(driver_number)].copy()
        
        if driver_data.empty:
            raise ValueError(f"Nenhum dado encontrado para o piloto #{driver_number}")
        
        print(f"Preparando dados para o piloto #{driver_number}: {len(driver_data)} pontos")
        return driver_data
    
    # Se nenhum piloto específico, usar todos os dados
    print(f"Preparando dados para todos os pilotos: {len(df)} pontos")
    return df.copy()

def clean_track_data(df):
    """
    Limpa e prepara os dados do traçado para visualização.
    
    Args:
        df: DataFrame com dados de posição
        
    Returns:
        pd.DataFrame: DataFrame limpo e preparado
    """
    # Ordenar por timestamp para sequência temporal correta
    if 'timestamp' in df.columns:
        df = df.sort_values('timestamp')
    
    # Criar uma coluna de sequência para coloração
    df['sequence'] = np.arange(len(df))
    
    # Remover outliers nas coordenadas (pontos muito distantes que podem distorcer a visualização)
    for coord in ['x', 'y', 'z']:
        q1 = df[coord].quantile(0.05)
        q3 = df[coord].quantile(0.95)
        iqr = q3 - q1
        
        # Definir limites mais amplos para não perder detalhes do circuito
        lower_bound = q1 - 2 * iqr
        upper_bound = q3 + 2 * iqr
        
        # Marcar (mas não remover) outliers
        outliers = (df[coord] < lower_bound) | (df[coord] > upper_bound)
        n_outliers = outliers.sum()
        
        if n_outliers > 0:
            print(f"Atenção: Identificados {n_outliers} possíveis outliers em {coord}")
    
    return df

def create_2d_track_visualization(df, race_name, session_name, output_path, cmap=DEFAULT_CMAP):
    """
    Cria visualização 2D do traçado do circuito.
    
    Args:
        df: DataFrame com dados de posição preparados
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        cmap: Mapa de cores a ser usado
    """
    # Criar figura com tamanho adequado
    plt.figure(figsize=FIG_SIZE['2d'], dpi=100)
    
    # Plotar os pontos do traçado
    scatter = plt.scatter(
        df['x'], 
        df['y'],
        c=df['sequence'],  # Colorir pelo sequencial para mostrar a progressão da volta
        cmap=cmap,
        s=SCATTER_SIZES['2d'],
        alpha=DEFAULT_ALPHA
    )
    
    # Configurar título e rótulos dos eixos
    title = f"Track Layout - {race_name} - {session_name}"
    plt.title(title, fontsize=14)
    plt.xlabel("X Coordinate", fontsize=12)
    plt.ylabel("Y Coordinate", fontsize=12)
    
    # Adicionar grade para melhor visualização
    plt.grid(True)
    
    # Adicionar barra de cores
    cbar = plt.colorbar(scatter, label="Time Sequence")
    
    # Ajustar limites dos eixos para garantir que o gráfico seja proporcional
    # (importante para que o circuito não pareça distorcido)
    plt.axis('equal')
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização 2D salva em: {output_path}")
    plt.close()

def create_3d_track_visualization(df, race_name, session_name, output_path, cmap=DEFAULT_CMAP):
    """
    Cria visualização 3D do traçado do circuito.
    
    Args:
        df: DataFrame com dados de posição preparados
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        cmap: Mapa de cores a ser usado
    """
    # Criar figura 3D
    fig = plt.figure(figsize=FIG_SIZE['3d'], dpi=100)
    ax = fig.add_subplot(111, projection='3d')
    
    # Plotar o traçado 3D
    scatter = ax.scatter(
        df['x'],
        df['y'],
        df['z'],
        c=df['sequence'],  # Colorir por sequencial para mostrar a progressão
        cmap=cmap,
        s=SCATTER_SIZES['3d'],
        alpha=DEFAULT_ALPHA
    )
    
    # Configurar títulos e rótulos
    title = f"3D Track Layout - {race_name} - {session_name}"
    ax.set_title(title, fontsize=14)
    ax.set_xlabel("X Coordinate", fontsize=12)
    ax.set_ylabel("Y Coordinate", fontsize=12)
    ax.set_zlabel("Altitude (Z)", fontsize=12)
    
    # Adicionar barra de cores
    cbar = fig.colorbar(scatter, ax=ax, label="Time Sequence")
    
    # Ajustar ângulo de visualização para melhor perspectiva
    ax.view_init(elev=30, azim=-45)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização 3D salva em: {output_path}")
    plt.close()

def create_multi_driver_visualization(df, race_name, session_name, output_path):
    """
    Cria visualização de traçados de múltiplos pilotos.
    
    Args:
        df: DataFrame com dados de posição preparados
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
    """
    # Obter pilotos únicos
    drivers = sorted(df['driver_number'].unique())
    
    # Criar figura com tamanho adequado
    plt.figure(figsize=(15, 10), dpi=100)
    
    # Definir cores distintas para cada piloto
    colors = plt.cm.jet(np.linspace(0, 1, len(drivers)))
    
    # Plotar o traçado de cada piloto com cor diferente
    for i, driver in enumerate(drivers):
        driver_data = df[df['driver_number'] == driver]
        if len(driver_data) > 50:  # Verificar se há dados suficientes
            plt.scatter(
                driver_data['x'],
                driver_data['y'],
                s=1,
                alpha=0.7,
                color=colors[i],
                label=f"Driver #{driver}"
            )
    
    # Configurar título e rótulos dos eixos
    title = f"Multi-Driver Track Layout - {race_name} - {session_name}"
    plt.title(title, fontsize=14)
    plt.xlabel("X Coordinate", fontsize=12)
    plt.ylabel("Y Coordinate", fontsize=12)
    
    # Adicionar grade para melhor visualização
    plt.grid(True)
    
    # Adicionar legenda
    plt.legend(loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Ajustar limites dos eixos para garantir que o gráfico seja proporcional
    plt.axis('equal')
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização multi-piloto salva em: {output_path}")
    plt.close()

def create_elevation_profile(df, race_name, session_name, output_path):
    """
    Cria um perfil de elevação do circuito.
    
    Args:
        df: DataFrame com dados de posição preparados
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
    """
    # Calcular a distância acumulada ao longo do traçado
    # (aproximação usando distância euclidiana entre pontos consecutivos)
    x = df['x'].values
    y = df['y'].values
    z = df['z'].values
    
    # Calcular diferenças entre pontos consecutivos
    dx = np.diff(x)
    dy = np.diff(y)
    
    # Calcular distâncias incrementais (apenas no plano XY)
    distances = np.sqrt(dx**2 + dy**2)
    
    # Calcular distância acumulada
    cumulative_distance = np.concatenate(([0], np.cumsum(distances)))
    
    # Criar figura
    plt.figure(figsize=(14, 6), dpi=100)
    
    # Plotar o perfil de elevação
    plt.plot(cumulative_distance, z, 'b-', linewidth=2)
    
    # Adicionar linha horizontal na elevação mínima para referência
    plt.axhline(y=min(z), color='k', linestyle='--', alpha=0.3)
    
    # Configurar título e rótulos
    title = f"Elevation Profile - {race_name} - {session_name}"
    plt.title(title, fontsize=14)
    plt.xlabel("Distance Along Track (m)", fontsize=12)
    plt.ylabel("Elevation (m)", fontsize=12)
    
    # Adicionar grade
    plt.grid(True, alpha=0.3)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Perfil de elevação salvo em: {output_path}")
    plt.close()

def create_combined_visualization(df, race_name, session_name, output_path, cmap=DEFAULT_CMAP):
    """
    Cria uma visualização combinada com traçado 2D, 3D e perfil de elevação.
    
    Args:
        df: DataFrame com dados de posição preparados
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        cmap: Mapa de cores a ser usado
    """
    # Criar figura com três subplots
    fig = plt.figure(figsize=(20, 15), dpi=100)
    gs = gridspec.GridSpec(2, 2, height_ratios=[2, 1])
    
    # 1. Traçado 2D (superior esquerdo)
    ax1 = plt.subplot(gs[0, 0])
    scatter1 = ax1.scatter(
        df['x'], 
        df['y'],
        c=df['sequence'],
        cmap=cmap,
        s=SCATTER_SIZES['2d'],
        alpha=DEFAULT_ALPHA
    )
    ax1.set_title(f"2D Track Layout - {race_name}", fontsize=14)
    ax1.set_xlabel("X Coordinate", fontsize=12)
    ax1.set_ylabel("Y Coordinate", fontsize=12)
    ax1.grid(True)
    ax1.axis('equal')
    
    # 2. Traçado 3D (superior direito)
    ax2 = plt.subplot(gs[0, 1], projection='3d')
    scatter2 = ax2.scatter(
        df['x'],
        df['y'],
        df['z'],
        c=df['sequence'],
        cmap=cmap,
        s=SCATTER_SIZES['3d'],
        alpha=DEFAULT_ALPHA
    )
    ax2.set_title(f"3D Track Layout", fontsize=14)
    ax2.set_xlabel("X Coordinate", fontsize=12)
    ax2.set_ylabel("Y Coordinate", fontsize=12)
    ax2.set_zlabel("Altitude (Z)", fontsize=12)
    ax2.view_init(elev=30, azim=-45)
    
    # 3. Perfil de elevação (inferior)
    ax3 = plt.subplot(gs[1, :])
    
    # Calcular a distância acumulada ao longo do traçado
    x = df['x'].values
    y = df['y'].values
    z = df['z'].values
    
    # Calcular diferenças entre pontos consecutivos
    dx = np.diff(x)
    dy = np.diff(y)
    
    # Calcular distâncias incrementais (apenas no plano XY)
    distances = np.sqrt(dx**2 + dy**2)
    
    # Calcular distância acumulada
    cumulative_distance = np.concatenate(([0], np.cumsum(distances)))
    
    # Plotar o perfil de elevação
    ax3.plot(cumulative_distance, z, 'b-', linewidth=2)
    ax3.set_title(f"Elevation Profile - {session_name}", fontsize=14)
    ax3.set_xlabel("Distance Along Track (m)", fontsize=12)
    ax3.set_ylabel("Elevation (m)", fontsize=12)
    ax3.grid(True, alpha=0.3)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Adicionar barra de cores para a sequência temporal
    cbar = fig.colorbar(scatter1, ax=[ax1, ax2, ax3], label="Time Sequence", pad=0.01)
    
    # Ajustar espaçamento
    plt.subplots_adjust(wspace=0.3, hspace=0.3)
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI)
    print(f"Visualização combinada salva em: {output_path}")
    plt.close()

def main():
    """Função principal do script."""
    # Processar argumentos de linha de comando
    args = parse_args()
    
    # Extrair argumentos
    meeting_key = args.meeting
    session_key = args.session
    driver_number = args.driver
    all_drivers = args.all_drivers
    cmap = args.cmap
    
    # Definir o diretório de saída
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Usar o diretório de visualizações dentro dos dados processados
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/track")
    
    # Criar diretório de saída se não existir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Definir nomes para o evento e sessão
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Carregar dados
        df = load_position_data(meeting_key, session_key)
        
        # Se solicitado, criar visualização para todos os pilotos juntos
        if all_drivers:
            full_track_path = output_dir / f"track_all_drivers_{meeting_key}_{session_key}.png"
            create_multi_driver_visualization(df, race_name, session_name, full_track_path)
        
        # Preparar dados do traçado para o piloto específico ou todos
        track_df = prepare_track_data(df, driver_number)
        
        # Limpar e preparar dados
        track_df = clean_track_data(track_df)
        
        # Definir o sufixo para nomes de arquivo
        driver_suffix = f"_driver_{driver_number}" if driver_number else ""
        
        # Criar visualizações
        if not args.three_d_only:
            # Visualização 2D
            track_2d_path = output_dir / f"track_2d{driver_suffix}_{meeting_key}_{session_key}.png"
            create_2d_track_visualization(track_df, race_name, session_name, track_2d_path, cmap)
        
        if not args.two_d_only:
            # Visualização 3D
            track_3d_path = output_dir / f"track_3d{driver_suffix}_{meeting_key}_{session_key}.png"
            create_3d_track_visualization(track_df, race_name, session_name, track_3d_path, cmap)
        
        # Perfil de elevação
        if not args.two_d_only:
            elevation_path = output_dir / f"elevation_profile{driver_suffix}_{meeting_key}_{session_key}.png"
            create_elevation_profile(track_df, race_name, session_name, elevation_path)
        
        # Remover a visualização combinada conforme solicitado
    # A visualização combinada foi removida para manter apenas gráficos individuais
        
        print("Todas as visualizações foram geradas com sucesso!")
        print(f"As visualizações estão disponíveis em: {output_dir}")
        
    except Exception as e:
        print(f"Erro ao criar visualizações: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())