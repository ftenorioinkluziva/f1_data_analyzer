#!/usr/bin/env python3
"""
racecontrol_visualizer.py - Script para visualizar mensagens do controle de corrida F1

Este script lê os dados processados de RaceControlMessages do F1 Data Analyzer e gera
visualizações das mensagens oficiais transmitidas durante uma sessão de F1.

Uso:
    python racecontrol_visualizer.py --meeting 1264 --session 1297 --output-dir visualizations
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
import re

# Constantes e configurações
FIG_SIZE = (16, 10)  # Tamanho padrão para gráficos
DPI = 300  # Resolução para salvar imagens

# Cores para diferentes categorias de mensagens
CATEGORY_COLORS = {
    'Flag': 'red',
    'CarEvent': 'blue',
    'Drs': 'green',
    'Incident': 'orange',
    'Other': 'purple',
    'Unknown': 'gray'
}

# Cores para diferentes tipos de bandeiras
FLAG_COLORS = {
    'YELLOW': '#FFFF00',  # Amarelo
    'RED': '#FF0000',     # Vermelho
    'GREEN': '#00FF00',   # Verde
    'BLUE': '#0000FF',    # Azul
    'WHITE': '#FFFFFF',   # Branco
    'BLACK': '#000000',   # Preto
    'CHEQUERED': '#CCCCCC',  # Xadrez (cinza para representar)
    'DOUBLE YELLOW': '#FFDD00',  # Amarelo mais escuro para duplo amarelo
    'SC': '#FFA500',      # Laranja para safety car
    'VSC': '#FFC0CB',     # Rosa para virtual safety car
    'Unknown': '#808080'  # Cinza para bandeiras desconhecidas
}

def parse_args():
    """Processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='Visualizar mensagens do controle de corrida F1')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Chave do evento (ex: 1264 para Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Chave da sessão (ex: 1297 para corrida principal)')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Diretório para salvar visualizações')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Nome personalizado para o evento')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Nome personalizado para a sessão')
    
    parser.add_argument('--dark-mode', action='store_true', default=False,
                        help='Usar tema escuro para visualizações')
    
    parser.add_argument('--filter', type=str, default=None,
                        help='Filtrar mensagens por categoria (Flag, CarEvent, Drs, Incident)')
    
    return parser.parse_args()

def load_racecontrol_data(meeting_key, session_key):
    """
    Carrega as mensagens do controle de corrida da sessão.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        pd.DataFrame: DataFrame contendo as mensagens ou None se não disponível
    """
    # Verificar arquivo de mensagens
    messages_file = f"f1_data/processed/{meeting_key}/{session_key}/RaceControlMessages/race_control_messages.csv"
    
    if not os.path.exists(messages_file):
        print(f"Aviso: Mensagens do controle de corrida não encontradas: {messages_file}")
        return None
    
    try:
        print(f"Carregando mensagens de: {messages_file}")
        df = pd.read_csv(messages_file)
        
        # Se o arquivo existe mas está vazio
        if df.empty:
            print(f"Aviso: Arquivo de mensagens vazio: {messages_file}")
            return None
        
        # Padronizar nomes de colunas (podem variar dependendo da fonte)
        column_mapping = {
            'category': 'category',
            'Category': 'category',
            'message': 'message',
            'Message': 'message',
            'flag': 'flag',
            'Flag': 'flag',
            'scope': 'scope',
            'Scope': 'scope',
            'sector': 'sector',
            'Sector': 'sector',
            'driver_number': 'driver_number',
            'DriverNumber': 'driver_number'
        }
        
        # Aplicar mapeamento para colunas existentes
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and new_name not in df.columns:
                df[new_name] = df[old_name]
        
        # Tentar converter timestamp para datetime se não for já
        if 'timestamp' in df.columns:
            try:
                # Verificar formato do timestamp
                sample_timestamp = df['timestamp'].iloc[0]
                
                # Se for string no formato HH:MM:SS.mmm
                if isinstance(sample_timestamp, str) and ':' in sample_timestamp:
                    # Verificar se já temos uma data completa ou apenas tempo
                    if 'T' in sample_timestamp or '-' in sample_timestamp:
                        # Parece ser um timestamp ISO completo
                        df['datetime'] = pd.to_datetime(df['timestamp'])
                    else:
                        # Apenas tempo, adicionar data fictícia para plotagem
                        base_date = '2023-01-01 '  # Data fictícia
                        df['datetime'] = pd.to_datetime(base_date + df['timestamp'])
            except Exception as e:
                print(f"Aviso: Não foi possível converter timestamps para datetime: {str(e)}")
        
        # Ordenar por timestamp
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
        
        print(f"Mensagens do controle de corrida carregadas: {len(df)} registros")
        return df
    except Exception as e:
        print(f"Erro ao carregar mensagens do controle de corrida: {str(e)}")
        return None

def categorize_messages(messages_df):
    """
    Categoriza mensagens para facilitar a análise.
    
    Args:
        messages_df: DataFrame com mensagens do controle de corrida
        
    Returns:
        pd.DataFrame: DataFrame com categorias adicionadas
    """
    if messages_df is None or messages_df.empty:
        return None
    
    # Criar cópia para não modificar o original
    df = messages_df.copy()
    
    # Se já temos a coluna categoria, verificar valores
    if 'category' in df.columns:
        # Garantir que todas as categorias têm valor
        if df['category'].isna().any():
            # Tentar inferir categoria a partir da mensagem
            for idx, row in df[df['category'].isna()].iterrows():
                if pd.notna(row.get('flag')) and row.get('flag') != '':
                    df.at[idx, 'category'] = 'Flag'
                elif 'DRS' in str(row.get('message', '')).upper():
                    df.at[idx, 'category'] = 'Drs'
                elif 'CAR' in str(row.get('message', '')).upper():
                    df.at[idx, 'category'] = 'CarEvent'
                elif 'INCIDENT' in str(row.get('message', '')).upper():
                    df.at[idx, 'category'] = 'Incident'
                else:
                    df.at[idx, 'category'] = 'Other'
    else:
        # Criar coluna de categoria
        df['category'] = 'Unknown'
        
        # Classificar mensagens em categorias
        for idx, row in df.iterrows():
            if pd.notna(row.get('flag')) and row.get('flag') != '':
                df.at[idx, 'category'] = 'Flag'
            elif 'DRS' in str(row.get('message', '')).upper():
                df.at[idx, 'category'] = 'Drs'
            elif 'CAR' in str(row.get('message', '')).upper():
                df.at[idx, 'category'] = 'CarEvent'
            elif 'INCIDENT' in str(row.get('message', '')).upper():
                df.at[idx, 'category'] = 'Incident'
            else:
                df.at[idx, 'category'] = 'Other'
    
    # Normalizar bandeiras
    if 'flag' in df.columns:
        for idx, row in df.iterrows():
            if pd.isna(row['flag']) or row['flag'] == '':
                df.at[idx, 'flag'] = 'None'
            else:
                # Converter para maiúsculas e limpar
                flag_text = str(row['flag']).upper().strip()
                
                # Simplificar alguns nomes comuns
                if 'SAFETY CAR' in flag_text or flag_text == 'SC':
                    df.at[idx, 'flag'] = 'SC'
                elif 'VIRTUAL SAFETY CAR' in flag_text or flag_text == 'VSC':
                    df.at[idx, 'flag'] = 'VSC'
                elif 'DOUBLE YELLOW' in flag_text:
                    df.at[idx, 'flag'] = 'DOUBLE YELLOW'
                elif 'YELLOW' in flag_text:
                    df.at[idx, 'flag'] = 'YELLOW'
                elif 'RED' in flag_text:
                    df.at[idx, 'flag'] = 'RED'
                elif 'GREEN' in flag_text:
                    df.at[idx, 'flag'] = 'GREEN'
                elif 'BLUE' in flag_text:
                    df.at[idx, 'flag'] = 'BLUE'
                elif 'WHITE' in flag_text:
                    df.at[idx, 'flag'] = 'WHITE'
                elif 'BLACK' in flag_text and 'WHITE' not in flag_text:
                    df.at[idx, 'flag'] = 'BLACK'
                elif 'CHEQUERED' in flag_text or 'CHECKERED' in flag_text:
                    df.at[idx, 'flag'] = 'CHEQUERED'
                else:
                    df.at[idx, 'flag'] = flag_text
    
    # Extrair números de pilotos das mensagens se não existirem
    if 'driver_number' not in df.columns or df['driver_number'].isna().all():
        df['driver_number'] = None
        
        # Padrão para extrair números de pilotos (ex: "Car 44", "Driver 77", etc.)
        pattern = r'(?:car|driver)\s+(\d+)'
        
        for idx, row in df.iterrows():
            if pd.notna(row['message']):
                # Buscar padrão na mensagem
                match = re.search(pattern, str(row['message']).lower())
                if match:
                    df.at[idx, 'driver_number'] = match.group(1)
    
    # Extrair números de volta se mencionados nas mensagens
    df['lap_number'] = None
    
    # Padrão para extrair números de volta (ex: "Lap 12", "Turn 3, Lap 45", etc.)
    pattern = r'lap\s+(\d+)'
    
    for idx, row in df.iterrows():
        if pd.notna(row['message']):
            # Buscar padrão na mensagem
            match = re.search(pattern, str(row['message']).lower())
            if match:
                df.at[idx, 'lap_number'] = int(match.group(1))
    
    return df

def create_messages_timeline(messages_df, race_name, session_name, output_path, dark_mode=False, filter_category=None):
    """
    Cria visualização da linha do tempo das mensagens do controle de corrida.
    
    Args:
        messages_df: DataFrame com mensagens categorizadas
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
        filter_category: Categoria específica para filtrar (opcional)
    """
    if messages_df is None or messages_df.empty:
        print("Aviso: Nenhuma mensagem disponível para visualização")
        return
    
    # Filtrar por categoria se especificado
    if filter_category:
        messages_df = messages_df[messages_df['category'] == filter_category].copy()
        if messages_df.empty:
            print(f"Aviso: Nenhuma mensagem na categoria '{filter_category}'")
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
    
    # Determinar eixo X (tempo)
    if 'datetime' in messages_df.columns:
        x = messages_df['datetime']
        x_formatter = mdates.DateFormatter('%H:%M')
        x_label = 'Time'
    else:
        # Usar índices se datetime não estiver disponível
        x = np.arange(len(messages_df))
        x_formatter = None
        x_label = 'Message Sequence'
    
    # Criar eixo Y baseado em categorias
    categories = messages_df['category'].unique()
    cat_to_y = {cat: i for i, cat in enumerate(categories)}
    
    # Configurar cores e marcadores
    colors = []
    markers = []
    sizes = []
    
    for _, row in messages_df.iterrows():
        cat = row['category']
        colors.append(CATEGORY_COLORS.get(cat, CATEGORY_COLORS['Unknown']))
        
        # Diferentes marcadores para diferentes tipos
        if cat == 'Flag':
            markers.append('^')  # Triângulo para bandeiras
            sizes.append(100)    # Maior para bandeiras
        elif cat == 'Incident':
            markers.append('s')  # Quadrado para incidentes
            sizes.append(80)     # Médio para incidentes
        elif cat == 'Drs':
            markers.append('d')  # Diamante para DRS
            sizes.append(70)     # Menor para DRS
        else:
            markers.append('o')  # Círculo para outros
            sizes.append(60)     # Padrão para outros
    
    # Plotar pontos para cada mensagem
    for i, (_, row) in enumerate(messages_df.iterrows()):
        y = cat_to_y[row['category']]
        plt.scatter(x[i], y, 
                   color=colors[i], 
                   marker=markers[i], 
                   s=sizes[i],
                   edgecolor='black',
                   alpha=0.8)
    
    # Configurar eixo Y (categorias)
    plt.yticks(range(len(categories)), categories)
    
    # Configurar eixo X
    if x_formatter:
        plt.gca().xaxis.set_major_formatter(x_formatter)
        plt.gcf().autofmt_xdate()
    
    # Se tivermos dados de lap, adicionar pontos de referência no eixo X
    if 'lap_number' in messages_df.columns and not messages_df['lap_number'].isna().all():
        # Marcar algumas voltas no eixo X secundário
        lap_refs = messages_df[~messages_df['lap_number'].isna()].copy()
        if not lap_refs.empty:
            ax2 = plt.gca().twiny()
            ax2.scatter(lap_refs['datetime'] if 'datetime' in lap_refs else range(len(lap_refs)), 
                        [-0.5] * len(lap_refs), marker='|', color='gray', alpha=0.5)
            
            # Tentar adicionar alguns rótulos de volta
            unique_laps = lap_refs['lap_number'].unique()
            if len(unique_laps) <= 10:  # Não sobrecarregar com muitos rótulos
                for lap in unique_laps:
                    lap_point = lap_refs[lap_refs['lap_number'] == lap].iloc[0]
                    x_pos = lap_point['datetime'] if 'datetime' in lap_point else lap_point.name
                    plt.text(x_pos, -0.7, f"Lap {int(lap)}", ha='center', va='top', 
                             fontsize=8, color='gray', alpha=0.8)
    
    # Adicionar legenda para categorias
    legend_elements = [Line2D([0], [0], marker='^', color='w', markerfacecolor=CATEGORY_COLORS['Flag'],
                              markersize=10, label='Flag'),
                      Line2D([0], [0], marker='s', color='w', markerfacecolor=CATEGORY_COLORS['Incident'],
                              markersize=10, label='Incident'),
                      Line2D([0], [0], marker='d', color='w', markerfacecolor=CATEGORY_COLORS['Drs'],
                              markersize=10, label='DRS'),
                      Line2D([0], [0], marker='o', color='w', markerfacecolor=CATEGORY_COLORS['CarEvent'],
                              markersize=10, label='Car Event'),
                      Line2D([0], [0], marker='o', color='w', markerfacecolor=CATEGORY_COLORS['Other'],
                              markersize=10, label='Other')]
    
    plt.legend(handles=legend_elements, loc='upper right')
    
    # Configurar título e rótulos
    filter_text = f" - {filter_category}" if filter_category else ""
    plt.title(f"Race Control Messages{filter_text} - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel(x_label, fontsize=12, color=text_color)
    plt.ylabel("Message Category", fontsize=12, color=text_color)
    
    # Adicionar grade para facilitar a leitura
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização da linha do tempo de mensagens salva em: {output_path}")
    plt.close()

def create_flag_frequency_chart(messages_df, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de frequência das bandeiras usadas durante a sessão.
    
    Args:
        messages_df: DataFrame com mensagens categorizadas
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if messages_df is None or messages_df.empty:
        print("Aviso: Nenhuma mensagem disponível para visualização de bandeiras")
        return
    
    # Filtrar apenas mensagens com bandeiras
    flag_df = messages_df[(messages_df['category'] == 'Flag') & (messages_df['flag'] != 'None')].copy()
    
    if flag_df.empty:
        print("Aviso: Nenhuma mensagem de bandeira encontrada")
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
    
    # Contar frequência de cada bandeira
    flag_counts = flag_df['flag'].value_counts()
    
    # Criar figura
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Cores personalizadas para cada bandeira
    colors = [FLAG_COLORS.get(flag, FLAG_COLORS['Unknown']) for flag in flag_counts.index]
    
    # Ajustar cores para tema escuro
    if dark_mode and 'BLACK' in flag_counts.index:
        # Índice da bandeira preta
        black_idx = flag_counts.index.get_loc('BLACK')
        # Mudar para um tom mais claro para ser visível em fundo escuro
        colors[black_idx] = '#808080'  # Cinza médio
    
    # Criar gráfico de barras
    bars = plt.bar(flag_counts.index, flag_counts.values, color=colors, edgecolor='black')
    
    # Adicionar rótulos com contagem
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 0.1,
                 str(int(height)), ha='center', va='bottom', fontsize=10, color=text_color)
    
    # Configurar título e rótulos
    plt.title(f"Flag Usage - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.ylabel("Frequency", fontsize=12, color=text_color)
    plt.xlabel("Flag Type", fontsize=12, color=text_color)
    
    # Adicionar grade para facilitar a leitura
    plt.grid(axis='y', linestyle='--', alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de frequência de bandeiras salva em: {output_path}")
    plt.close()

def create_message_categories_chart(messages_df, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de distribuição das categorias de mensagens.
    
    Args:
        messages_df: DataFrame com mensagens categorizadas
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if messages_df is None or messages_df.empty:
        print("Aviso: Nenhuma mensagem disponível para visualização de categorias")
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
    
    # Contar mensagens por categoria
    category_counts = messages_df['category'].value_counts()
    
    # Criar figura
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Cores para cada categoria
    colors = [CATEGORY_COLORS.get(cat, CATEGORY_COLORS['Unknown']) for cat in category_counts.index]
    
    # Criar gráfico de pizza
    plt.pie(category_counts.values, labels=category_counts.index, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=90)
    
    # Garantir que o círculo seja um círculo
    plt.axis('equal')
    
    # Configurar título
    plt.title(f"Message Categories - {race_name} - {session_name}", fontsize=14, color=text_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de categorias de mensagens salva em: {output_path}")
    plt.close()

def create_flag_timeline(messages_df, race_name, session_name, output_path, dark_mode=False):
    """
    Cria linha do tempo específica para bandeiras com duração.
    
    Args:
        messages_df: DataFrame com mensagens categorizadas
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if messages_df is None or messages_df.empty:
        print("Aviso: Nenhuma mensagem disponível para visualização de bandeiras")
        return
    
    # Filtrar apenas mensagens de bandeiras
    flag_df = messages_df[(messages_df['category'] == 'Flag') & (messages_df['flag'] != 'None')].copy()
    
    if flag_df.empty:
        print("Aviso: Nenhuma mensagem de bandeira encontrada")
        return
    
    # Verificar se temos datetime para criar a linha do tempo
    if 'datetime' not in flag_df.columns:
        print("Aviso: Dados de datetime não disponíveis para linha do tempo de bandeiras")
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
    
    # Determinar tipos de bandeiras únicos
    unique_flags = flag_df['flag'].unique()
    flag_to_y = {flag: i for i, flag in enumerate(unique_flags)}
    
    # Configurar marcadores para eventos de bandeira
    previous_flags = {}  # Rastrear últimas ocorrências de cada bandeira
    
    # Dados para plotagem
    x_start = []
    x_end = []
    y_pos = []
    colors = []
    labels = []
    
    # Percorrer mensagens em ordem cronológica
    for i, (_, row) in enumerate(flag_df.iterrows()):
        flag = row['flag']
        y = flag_to_y[flag]
        
        if flag in previous_flags:
            # Adicionar barra de duração desde a última ocorrência
            x_start.append(previous_flags[flag])
            x_end.append(row['datetime'])
            y_pos.append(y)
            colors.append(FLAG_COLORS.get(flag, FLAG_COLORS['Unknown']))
            labels.append(flag)
        
        # Atualizar última ocorrência
        previous_flags[flag] = row['datetime']
        
        # Plotar marcador para este evento
        plt.scatter(row['datetime'], y, color=FLAG_COLORS.get(flag, FLAG_COLORS['Unknown']), 
                   marker='o', s=50, edgecolor='black', zorder=5)
    
    # Plotar segmentos horizontais para duração de bandeiras
    for i in range(len(x_start)):
        plt.hlines(y=y_pos[i], xmin=x_start[i], xmax=x_end[i], 
                  colors=colors[i], linewidth=8, alpha=0.6)
    
    # Configurar eixo Y (tipos de bandeiras)
    plt.yticks(range(len(unique_flags)), unique_flags)
    
    # Configurar eixo X (tempo)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gcf().autofmt_xdate()
    
    # Configurar título e rótulos
    plt.title(f"Flag Timeline - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel("Time", fontsize=12, color=text_color)
    plt.ylabel("Flag Type", fontsize=12, color=text_color)
    
    # Adicionar grade para facilitar a leitura
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização da linha do tempo de bandeiras salva em: {output_path}")
    plt.close()

def main():
    """Função principal do script."""
    # Processar argumentos de linha de comando
    args = parse_args()
    
    # Extrair argumentos
    meeting_key = args.meeting
    session_key = args.session
    dark_mode = args.dark_mode
    filter_category = args.filter
    
    # Definir o diretório de saída
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Usar o diretório de visualizações dentro dos dados processados
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/race_control")
    
    # Criar diretório de saída se não existir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Definir nomes para o evento e sessão
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Carregar mensagens
        messages_df = load_racecontrol_data(meeting_key, session_key)
        
        if messages_df is None:
            print("Erro: Não foi possível carregar mensagens do controle de corrida")
            return 1
        
        # Categorizar mensagens
        messages_df = categorize_messages(messages_df)
        
        if messages_df is None:
            print("Erro: Não foi possível categorizar mensagens")
            return 1
        
        # Criar visualizações
        # 1. Linha do tempo de todas as mensagens
        timeline_path = output_dir / f"message_timeline_{meeting_key}_{session_key}.png"
        create_messages_timeline(messages_df, race_name, session_name, timeline_path, dark_mode)
        
        # 2. Gráfico de frequência de bandeiras
        flags_path = output_dir / f"flag_frequency_{meeting_key}_{session_key}.png"
        create_flag_frequency_chart(messages_df, race_name, session_name, flags_path, dark_mode)
        
        # 3. Gráfico de categorias de mensagens
        categories_path = output_dir / f"message_categories_{meeting_key}_{session_key}.png"
        create_message_categories_chart(messages_df, race_name, session_name, categories_path, dark_mode)
        
        # 4. Linha do tempo específica para bandeiras
        flag_timeline_path = output_dir / f"flag_timeline_{meeting_key}_{session_key}.png"
        create_flag_timeline(messages_df, race_name, session_name, flag_timeline_path, dark_mode)
        
        # 5. Se um filtro foi especificado, criar linha do tempo filtrada
        if filter_category:
            filtered_path = output_dir / f"filtered_{filter_category}_{meeting_key}_{session_key}.png"
            create_messages_timeline(messages_df, race_name, session_name, filtered_path, dark_mode, filter_category)
        
        print("Todas as visualizações de mensagens de controle de corrida foram geradas com sucesso!")
        print(f"As visualizações estão disponíveis em: {output_dir}")
        
    except Exception as e:
        print(f"Erro ao criar visualizações: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())