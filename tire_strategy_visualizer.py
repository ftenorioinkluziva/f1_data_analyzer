#!/usr/bin/env python3
"""
tire_strategy_visualizer.py - Script para visualizar estratégias de pneus em corridas de F1

Este script lê os dados processados de CurrentTyres do F1 Data Analyzer e gera visualizações
detalhadas das estratégias de pneus usadas durante uma sessão de corrida da F1.

Uso:
    python tire_strategy_visualizer.py --meeting 1264 --session 1297 --output-dir visualizations
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

# Cores dos compostos de pneus
COMPOUND_COLORS = {
    'SOFT': 'red',
    'MEDIUM': 'yellow',
    'HARD': 'white',
    'INTERMEDIATE': 'green',
    'WET': 'blue',
    'UNKNOWN': 'gray'
}

def parse_args():
    """Processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='Visualizar estratégias de pneus da F1')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Chave do evento (ex: 1264 para Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Chave da sessão (ex: 1297 para corrida principal)')
    
    parser.add_argument('--drivers', type=str, nargs='+', default=None,
                        help='Números dos pilotos específicos para visualizar (ex: 1 16 44 para mostrar apenas esses pilotos)')
    
    parser.add_argument('--top', type=int, default=None,
                        help='Número de pilotos do topo da classificação para mostrar (ex: 10 para os 10 primeiros)')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Diretório para salvar visualizações')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Nome personalizado para o evento')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Nome personalizado para a sessão')
    
    parser.add_argument('--dark-mode', action='store_true', default=False,
                        help='Usar tema escuro para visualizações')
    
    return parser.parse_args()

def load_tire_data(meeting_key, session_key):
    """
    Carrega os dados de pneus da sessão.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        tuple: (tire_entries_df, tire_history_df) ou (None, None) se não disponível
    """
    # Primeiro tenta carregar o arquivo de histórico de pneus
    history_file = f"f1_data/processed/{meeting_key}/{session_key}/CurrentTyres/tyre_history.csv"
    entries_file = f"f1_data/processed/{meeting_key}/{session_key}/CurrentTyres/tyre_entries.csv"
    
    history_df = None
    entries_df = None
    
    # Verificar e carregar histórico de pneus
    if os.path.exists(history_file):
        try:
            print(f"Carregando histórico de pneus de: {history_file}")
            history_df = pd.read_csv(history_file)
            
            # Converter números de piloto para strings para consistência
            if 'driver_number' in history_df.columns:
                history_df['driver_number'] = history_df['driver_number'].astype(str)
            
            print(f"Dados de histórico de pneus carregados: {len(history_df)} registros")
        except Exception as e:
            print(f"Erro ao carregar histórico de pneus: {str(e)}")
    else:
        print(f"Aviso: Arquivo de histórico de pneus não encontrado: {history_file}")
    
    # Verificar e carregar entradas de pneus
    if os.path.exists(entries_file):
        try:
            print(f"Carregando entradas de pneus de: {entries_file}")
            entries_df = pd.read_csv(entries_file)
            
            # Converter números de piloto para strings para consistência
            if 'driver_number' in entries_df.columns:
                entries_df['driver_number'] = entries_df['driver_number'].astype(str)
            
            print(f"Dados de entradas de pneus carregados: {len(entries_df)} registros")
        except Exception as e:
            print(f"Erro ao carregar entradas de pneus: {str(e)}")
    else:
        print(f"Aviso: Arquivo de entradas de pneus não encontrado: {entries_file}")
    
    return entries_df, history_df

def load_driver_info(meeting_key, session_key):
    """
    Carrega informações dos pilotos para correlacionar com os dados de pneus.
    
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

def load_position_data(meeting_key, session_key):
    """
    Carrega dados de posição/classificação para ordenar os pilotos.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        pd.DataFrame: DataFrame com posições finais, ou None se não disponível
    """
    # Tentar várias fontes possíveis de dados de posição/classificação
    possible_files = [
        f"f1_data/processed/{meeting_key}/{session_key}/TimingData/positions.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/TimingAppData/grid_positions.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/DriverList/position_updates.csv"
    ]
    
    for file_path in possible_files:
        if os.path.exists(file_path):
            try:
                print(f"Carregando dados de posição de: {file_path}")
                df = pd.read_csv(file_path)
                
                # Converter números de piloto para strings para consistência
                if 'driver_number' in df.columns:
                    df['driver_number'] = df['driver_number'].astype(str)
                
                print(f"Dados de posição carregados: {len(df)} registros")
                return df
            except Exception as e:
                print(f"Erro ao carregar dados de posição de {file_path}: {str(e)}")
    
    print("Aviso: Nenhum arquivo de dados de posição encontrado")
    return None

def process_tire_data(history_df, entries_df, driver_positions=None, driver_info=None, selected_drivers=None, top_n=None):
    """
    Processa dados de pneus para visualização da estratégia.
    
    Args:
        history_df: DataFrame com histórico de pneus
        entries_df: DataFrame com entradas de pneus
        driver_positions: DataFrame com posições/classificação
        driver_info: DataFrame com informações dos pilotos
        selected_drivers: Lista de números de pilotos específicos para mostrar
        top_n: Número de pilotos do topo da classificação para mostrar
        
    Returns:
        dict: Dicionário com dados processados por piloto
    """
    if history_df is None and entries_df is None:
        print("Erro: Nenhum dado de pneus disponível para processamento")
        return None
    
    # Priorizar história de pneus se disponível, caso contrário usar entradas
    df_to_use = history_df if history_df is not None else entries_df
    
    # Lista de pilotos no conjunto de dados
    all_drivers = sorted(df_to_use['driver_number'].unique())
    print(f"Pilotos disponíveis nos dados: {', '.join(all_drivers)}")
    
    # Determinar quais pilotos visualizar
    drivers_to_display = all_drivers
    
    # Se selected_drivers está definido, filtrar para apenas esses pilotos
    if selected_drivers:
        # Converter para strings para garantir consistência
        selected_drivers = [str(d) for d in selected_drivers]
        drivers_to_display = [d for d in selected_drivers if d in all_drivers]
        print(f"Pilotos selecionados para visualização: {', '.join(drivers_to_display)}")
    
    # Se top_n está definido e temos dados de posição, selecionar os N primeiros pilotos
    elif top_n is not None and driver_positions is not None:
        try:
            # Obter posições finais dos pilotos
            final_positions = {}
            
            # Abordagem 1: Se temos uma coluna 'position', usar a última entrada de cada piloto
            if 'position' in driver_positions.columns:
                # Ordenar por timestamp para pegar a última posição
                if 'timestamp' in driver_positions.columns:
                    driver_positions = driver_positions.sort_values('timestamp')
                
                # Pegar a última posição de cada piloto
                for driver in all_drivers:
                    driver_data = driver_positions[driver_positions['driver_number'] == driver]
                    if not driver_data.empty:
                        final_positions[driver] = driver_data['position'].iloc[-1]
            
            # Abordagem 2: Se temos uma coluna 'grid_position', usar diretamente
            elif 'grid_position' in driver_positions.columns:
                for driver in all_drivers:
                    driver_data = driver_positions[driver_positions['driver_number'] == driver]
                    if not driver_data.empty:
                        final_positions[driver] = driver_data['grid_position'].iloc[0]
            
            # Ordenar pilotos por posição
            if final_positions:
                sorted_drivers = sorted(final_positions.items(), key=lambda x: int(x[1]) if str(x[1]).isdigit() else 999)
                drivers_to_display = [d[0] for d in sorted_drivers[:top_n]]
                print(f"Top {top_n} pilotos por classificação: {', '.join(drivers_to_display)}")
        except Exception as e:
            print(f"Erro ao determinar top {top_n} pilotos: {str(e)}")
    
    # Processar dados por piloto
    driver_data = {}
    
    for driver in drivers_to_display:
        # Filtrar dados para este piloto
        driver_history = df_to_use[df_to_use['driver_number'] == driver].sort_values('timestamp')
        
        if driver_history.empty:
            print(f"Aviso: Nenhum dado de pneus para o piloto #{driver}")
            continue
        
        # Obter o nome do piloto se disponível
        driver_name = f"Driver #{driver}"
        if driver_info is not None:
            driver_info_row = driver_info[driver_info['driver_number'] == driver]
            if not driver_info_row.empty:
                if 'full_name' in driver_info_row.columns and not pd.isna(driver_info_row['full_name'].iloc[0]):
                    driver_name = driver_info_row['full_name'].iloc[0]
                elif 'last_name' in driver_info_row.columns and not pd.isna(driver_info_row['last_name'].iloc[0]):
                    driver_name = driver_info_row['last_name'].iloc[0]
                elif 'tla' in driver_info_row.columns and not pd.isna(driver_info_row['tla'].iloc[0]):
                    driver_name = driver_info_row['tla'].iloc[0]
        
        # Extrair stints de pneus
        stints = []
        
        for i, (idx, row) in enumerate(driver_history.iterrows()):
            # Pular se não temos o composto (isso não deveria acontecer com dados de qualidade)
            if pd.isna(row.get('compound')) or row.get('compound') == '':
                continue
            
            # Verificar se este é o início de um novo stint
            new_stint = False
            if i == 0:
                # Primeiro registro sempre inicia um stint
                new_stint = True
            elif 'stint_start' in row and row['stint_start']:
                # Se temos uma coluna específica indicando início de stint
                new_stint = True
            elif history_df is not None and row.get('compound') != driver_history.iloc[i-1].get('compound'):
                # Se o composto mudou em relação ao registro anterior
                new_stint = True
            
            if new_stint:
                stint = {
                    'start_time': row['timestamp'],
                    'compound': row.get('compound', 'UNKNOWN'),
                    'new_tire': row.get('new_tire', False),
                    'end_time': None  # Será definido posteriormente
                }
                stints.append(stint)
            
            # Atualizar o tempo de término do stint atual
            if stints:
                stints[-1]['end_time'] = row['timestamp']
        
        # Armazenar os dados de stints para este piloto
        driver_data[driver] = {
            'name': driver_name,
            'stints': stints
        }
    
    return driver_data

def convert_to_minute_duration(time_str):
    """
    Converte um timestamp no formato HH:MM:SS.sss para minutos desde o início da sessão.
    
    Args:
        time_str: String no formato HH:MM:SS.sss
        
    Returns:
        float: Duração em minutos
    """
    try:
        time_parts = time_str.split(':')
        hours = int(time_parts[0])
        minutes = int(time_parts[1])
        seconds = float(time_parts[2])
        
        return hours * 60 + minutes + seconds / 60
    except:
        return 0

def create_tire_strategy_chart(driver_data, race_name, session_name, output_path, dark_mode=False):
    """
    Cria visualização da estratégia de pneus por piloto.
    
    Args:
        driver_data: Dicionário com dados processados por piloto
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if not driver_data:
        print("Aviso: Nenhum dado de piloto disponível para visualização de estratégia de pneus")
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
    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=100)
    
    # Definir eixo Y para os pilotos
    drivers = list(driver_data.keys())
    
    # Converter tempos para duração em minutos
    for driver, data in driver_data.items():
        for stint in data['stints']:
            stint['start_minute'] = convert_to_minute_duration(stint['start_time'])
            stint['end_minute'] = convert_to_minute_duration(stint['end_time'])
            stint['duration'] = stint['end_minute'] - stint['start_minute']
    
    # Determinar o tempo máximo da sessão em minutos
    max_time = 0
    for data in driver_data.values():
        for stint in data['stints']:
            max_time = max(max_time, stint['end_minute'])
    
    # Plotar as barras de stint para cada piloto
    for i, (driver, data) in enumerate(driver_data.items()):
        y_pos = len(drivers) - i - 1  # Reverter ordem para pilotos do topo ficarem em cima
        
        for stint in data['stints']:
            # Determinar cor com base no composto
            compound = stint['compound']
            color = COMPOUND_COLORS.get(compound, COMPOUND_COLORS['UNKNOWN'])
            
            # Ajustar altura da barra (menor para barras mais finas)
            bar_height = 0.6
            
            # Criar retângulo para o stint
            rect = patches.Rectangle(
                (stint['start_minute'], y_pos - bar_height/2),
                stint['duration'],
                bar_height,
                linewidth=1,
                edgecolor='black',
                facecolor=color,
                alpha=0.8 if color != 'white' else 1.0  # Ajustar alfa para cores claras
            )
            ax.add_patch(rect)
            
            # Adicionar texto indicando o composto
            # Apenas adicionar se o stint for longo o suficiente para ser visível
            if stint['duration'] > 3:
                text_x = stint['start_minute'] + stint['duration'] / 2
                text_y = y_pos
                
                # Escolher cor do texto com base na cor do composto
                if color in ['yellow', 'white']:
                    text_color_stint = 'black'
                else:
                    text_color_stint = 'white'
                
                # Adicionar texto do composto
                new_marker = "N" if stint['new_tire'] else ""
                ax.text(text_x, text_y, f"{compound[0]}{new_marker}", 
                        ha='center', va='center', fontsize=8, 
                        fontweight='bold', color=text_color_stint)
    
    # Configurar rótulos do eixo Y (nomes dos pilotos)
    y_ticks = list(range(len(drivers)))
    y_labels = [driver_data[driver]['name'] for driver in drivers]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)
    
    # Configurar eixo X (tempo em minutos)
    ax.set_xlim(0, max_time + 5)  # Adicionar margem
    ax.set_xlabel('Minutes from Session Start', fontsize=12, color=text_color)
    
    # Adicionar título
    ax.set_title(f"Tire Strategy - {race_name} - {session_name}", fontsize=14, color=text_color)
    
    # Adicionar legenda para os compostos
    legend_elements = [
        patches.Patch(facecolor=color, edgecolor='black', label=compound)
        for compound, color in COMPOUND_COLORS.items()
        if compound != 'UNKNOWN'  # Não incluir compostos desconhecidos na legenda
    ]
    ax.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Adicionar grade para facilitar a leitura
    ax.grid(axis='x', linestyle='--', alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de estratégia de pneus salva em: {output_path}")
    plt.close()

def create_compound_distribution_chart(driver_data, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de distribuição de compostos de pneus usados na sessão.
    
    Args:
        driver_data: Dicionário com dados processados por piloto
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if not driver_data:
        print("Aviso: Nenhum dado de piloto disponível para visualização de distribuição de compostos")
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
    
    # Contar uso de cada composto
    compound_counts = {}
    
    for driver, data in driver_data.items():
        for stint in data['stints']:
            compound = stint['compound']
            if compound in compound_counts:
                compound_counts[compound] += 1
            else:
                compound_counts[compound] = 1
    
    # Remover entrada 'UNKNOWN' se existir
    if 'UNKNOWN' in compound_counts:
        del compound_counts['UNKNOWN']
    
    # Criar figura
    plt.figure(figsize=(12, 8), dpi=100)
    
    # Criar gráfico de barras
    compounds = list(compound_counts.keys())
    counts = [compound_counts[c] for c in compounds]
    
    # Definir cores para as barras
    colors = [COMPOUND_COLORS.get(c, 'gray') for c in compounds]
    
    # Criar gráfico de barras
    bars = plt.bar(compounds, counts, color=colors, edgecolor='black')
    
    # Adicionar valores acima das barras
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 str(int(height)), ha='center', va='bottom', fontsize=10, color=text_color)
    
    # Configurar título e rótulos
    plt.title(f"Tire Compound Distribution - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel("Compound", fontsize=12, color=text_color)
    plt.ylabel("Count", fontsize=12, color=text_color)
    
    # Adicionar grade para facilitar a leitura
    plt.grid(axis='y', linestyle='--', alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de distribuição de compostos salva em: {output_path}")
    plt.close()

def create_stint_duration_chart(driver_data, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de duração dos stints por composto.
    
    Args:
        driver_data: Dicionário com dados processados por piloto
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if not driver_data:
        print("Aviso: Nenhum dado de piloto disponível para visualização de duração de stints")
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
    
    # Extrair duração dos stints por composto
    stint_durations = {}
    
    for driver, data in driver_data.items():
        for stint in data['stints']:
            compound = stint['compound']
            duration = stint['duration']
            
            if compound not in stint_durations:
                stint_durations[compound] = []
            
            stint_durations[compound].append(duration)
    
    # Remover entrada 'UNKNOWN' se existir
    if 'UNKNOWN' in stint_durations:
        del stint_durations['UNKNOWN']
    
    # Criar figura
    plt.figure(figsize=(12, 8), dpi=100)
    
    # Preparar dados para boxplot
    data = []
    labels = []
    colors = []
    
    for compound in sorted(stint_durations.keys()):
        durations = stint_durations[compound]
        data.append(durations)
        labels.append(compound)
        colors.append(COMPOUND_COLORS.get(compound, 'gray'))
    
    # Criar boxplot
    boxplot = plt.boxplot(data, patch_artist=True, labels=labels)
    
    # Colorir boxes de acordo com os compostos
    for patch, color in zip(boxplot['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    # Adicionar média como ponto
    for i, compound_data in enumerate(data):
        plt.scatter([i+1] * len(compound_data), compound_data, 
                    color='black', alpha=0.5, s=20, zorder=5)
    
    # Configurar título e rótulos
    plt.title(f"Stint Duration by Compound - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel("Compound", fontsize=12, color=text_color)
    plt.ylabel("Duration (minutes)", fontsize=12, color=text_color)
    
    # Adicionar grade para facilitar a leitura
    plt.grid(axis='y', linestyle='--', alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de duração de stints salva em: {output_path}")
    plt.close()

def main():
    """Função principal do script."""
    # Processar argumentos de linha de comando
    args = parse_args()
    
    # Extrair argumentos
    meeting_key = args.meeting
    session_key = args.session
    selected_drivers = args.drivers
    top_n = args.top
    dark_mode = args.dark_mode
    
    # Definir o diretório de saída
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Usar o diretório de visualizações dentro dos dados processados
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/tires")
    
    # Criar diretório de saída se não existir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Definir nomes para o evento e sessão
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Carregar dados de pneus
        entries_df, history_df = load_tire_data(meeting_key, session_key)
        
        # Carregar informações dos pilotos
        driver_info = load_driver_info(meeting_key, session_key)
        
        # Carregar dados de posição/classificação
        position_data = load_position_data(meeting_key, session_key)
        
        # Processar dados de pneus
        driver_data = process_tire_data(history_df, entries_df, position_data, driver_info, 
                                         selected_drivers, top_n)
        
        if not driver_data:
            print("Erro: Não foi possível processar dados de pneus")
            return 1
        
        # Criar visualização da estratégia de pneus
        strategy_path = output_dir / f"tire_strategy_{meeting_key}_{session_key}.png"
        create_tire_strategy_chart(driver_data, race_name, session_name, strategy_path, dark_mode)
        
        # Criar visualização da distribuição de compostos
        distribution_path = output_dir / f"compound_distribution_{meeting_key}_{session_key}.png"
        create_compound_distribution_chart(driver_data, race_name, session_name, distribution_path, dark_mode)
        
        # Criar visualização da duração dos stints
        duration_path = output_dir / f"stint_duration_{meeting_key}_{session_key}.png"
        create_stint_duration_chart(driver_data, race_name, session_name, duration_path, dark_mode)
        
        print("Todas as visualizações de pneus foram geradas com sucesso!")
        print(f"As visualizações estão disponíveis em: {output_dir}")
        
    except Exception as e:
        print(f"Erro ao criar visualizações: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())