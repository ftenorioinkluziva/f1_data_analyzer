#!/usr/bin/env python3
"""
weather_visualizer.py - Script para visualizar dados meteorológicos de sessões F1

Este script lê os dados processados de WeatherData do F1 Data Analyzer e gera
visualizações detalhadas das condições meteorológicas durante sessões da F1.

Uso:
    python weather_visualizer.py --meeting 1264 --session 1297 --output-dir visualizations
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime, timedelta
from matplotlib.gridspec import GridSpec

# Constantes e configurações
FIG_SIZE = (16, 10)  # Tamanho padrão para gráficos
DPI = 300  # Resolução para salvar imagens

# Cores para diferentes métricas
COLORS = {
    'air_temp': 'red',
    'track_temp': 'orange',
    'humidity': 'blue',
    'pressure': 'purple',
    'rainfall': 'cyan',
    'wind_speed': 'green',
    'wind_direction': 'brown'
}

def parse_args():
    """Processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='Visualizar dados meteorológicos de sessões F1')
    
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
    
    parser.add_argument('--compare-sessions', type=str, nargs='+', default=None,
                        help='Lista de session_keys adicionais para comparar dados meteorológicos')
    
    return parser.parse_args()

def load_weather_data(meeting_key, session_key):
    """
    Carrega os dados meteorológicos da sessão.
    
    Args:
        meeting_key: Chave do evento
        session_key: Chave da sessão
        
    Returns:
        pd.DataFrame: DataFrame contendo os dados meteorológicos ou None se não disponível
    """
    # Verificar arquivo de dados meteorológicos
    weather_file = f"f1_data/processed/{meeting_key}/{session_key}/WeatherData/weather_data.csv"
    
    if not os.path.exists(weather_file):
        print(f"Aviso: Dados meteorológicos não encontrados: {weather_file}")
        return None
    
    try:
        print(f"Carregando dados meteorológicos de: {weather_file}")
        df = pd.read_csv(weather_file)
        
        # Se o arquivo existe mas está vazio
        if df.empty:
            print(f"Aviso: Arquivo de dados meteorológicos vazio: {weather_file}")
            return None
        
        # Padronizar nomes de colunas (podem variar dependendo da fonte)
        column_mapping = {
            'AirTemp': 'air_temp',
            'TrackTemp': 'track_temp',
            'Humidity': 'humidity',
            'Pressure': 'pressure',
            'Rainfall': 'rainfall',
            'WindSpeed': 'wind_speed',
            'WindDirection': 'wind_direction'
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
        
        print(f"Dados meteorológicos carregados: {len(df)} registros")
        return df
    except Exception as e:
        print(f"Erro ao carregar dados meteorológicos: {str(e)}")
        return None

def create_temperature_chart(weather_df, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de temperatura do ar e da pista.
    
    Args:
        weather_df: DataFrame com dados meteorológicos
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if weather_df is None or weather_df.empty:
        print("Aviso: Dados insuficientes para gráfico de temperatura")
        return
    
    # Verificar se temos dados de temperatura
    required_columns = ['air_temp', 'track_temp']
    missing_columns = [col for col in required_columns if col not in weather_df.columns]
    
    if missing_columns:
        print(f"Aviso: Dados de temperatura não contêm colunas necessárias: {missing_columns}")
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
    if 'datetime' in weather_df.columns:
        x = weather_df['datetime']
        x_formatter = mdates.DateFormatter('%H:%M')
        x_label = 'Time'
    else:
        # Usar índices se datetime não estiver disponível
        x = np.arange(len(weather_df))
        x_formatter = None
        x_label = 'Time (data points)'
    
    # Plotar temperatura do ar
    plt.plot(x, weather_df['air_temp'], color=COLORS['air_temp'], linewidth=2, label='Air Temperature')
    
    # Plotar temperatura da pista
    plt.plot(x, weather_df['track_temp'], color=COLORS['track_temp'], linewidth=2, label='Track Temperature')
    
    # Configurar eixo X
    if x_formatter:
        plt.gca().xaxis.set_major_formatter(x_formatter)
        plt.gcf().autofmt_xdate()
    
    # Configurar título e rótulos
    plt.title(f"Temperature Data - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel(x_label, fontsize=12, color=text_color)
    plt.ylabel('Temperature (°C)', fontsize=12, color=text_color)
    
    # Adicionar legenda
    plt.legend()
    
    # Adicionar grade para facilitar a leitura
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de temperatura salva em: {output_path}")
    plt.close()

def create_humidity_rainfall_chart(weather_df, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de umidade e precipitação.
    
    Args:
        weather_df: DataFrame com dados meteorológicos
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if weather_df is None or weather_df.empty:
        print("Aviso: Dados insuficientes para gráfico de umidade/precipitação")
        return
    
    # Verificar se temos pelo menos dados de umidade
    if 'humidity' not in weather_df.columns:
        print("Aviso: Dados de umidade não disponíveis")
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
    fig, ax1 = plt.subplots(figsize=FIG_SIZE, dpi=100)
    
    # Determinar eixo X (tempo)
    if 'datetime' in weather_df.columns:
        x = weather_df['datetime']
        x_formatter = mdates.DateFormatter('%H:%M')
        x_label = 'Time'
    else:
        # Usar índices se datetime não estiver disponível
        x = np.arange(len(weather_df))
        x_formatter = None
        x_label = 'Time (data points)'
    
    # Plotar umidade no eixo principal
    ax1.plot(x, weather_df['humidity'], color=COLORS['humidity'], linewidth=2, label='Humidity')
    ax1.set_ylabel('Humidity (%)', fontsize=12, color=COLORS['humidity'])
    ax1.tick_params(axis='y', labelcolor=COLORS['humidity'])
    
    # Configurar eixo X
    if x_formatter:
        ax1.xaxis.set_major_formatter(x_formatter)
        fig.autofmt_xdate()
    
    # Se temos dados de precipitação, plotar no eixo secundário
    if 'rainfall' in weather_df.columns:
        ax2 = ax1.twinx()
        
        # Se todos os valores de precipitação são zero, plotar uma linha horizontal
        if weather_df['rainfall'].sum() == 0:
            ax2.axhline(y=0, color=COLORS['rainfall'], linewidth=1, linestyle='--', label='Rainfall (None)')
            max_rainfall = 1  # Valor arbitrário para escala
        else:
            ax2.plot(x, weather_df['rainfall'], color=COLORS['rainfall'], linewidth=2, label='Rainfall')
            max_rainfall = weather_df['rainfall'].max() * 1.1  # 10% a mais para margem
        
        ax2.set_ylabel('Rainfall (mm)', fontsize=12, color=COLORS['rainfall'])
        ax2.tick_params(axis='y', labelcolor=COLORS['rainfall'])
        ax2.set_ylim(0, max_rainfall)
    
    # Configurar título e rótulos
    ax1.set_title(f"Humidity & Rainfall - {race_name} - {session_name}", fontsize=14, color=text_color)
    ax1.set_xlabel(x_label, fontsize=12, color=text_color)
    
    # Adicionar legenda
    lines, labels = ax1.get_legend_handles_labels()
    if 'rainfall' in weather_df.columns:
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc='upper right')
    else:
        ax1.legend(loc='upper right')
    
    # Adicionar grade para facilitar a leitura
    ax1.grid(True, alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de umidade/precipitação salva em: {output_path}")
    plt.close()

def create_wind_chart(weather_df, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico de velocidade e direção do vento.
    
    Args:
        weather_df: DataFrame com dados meteorológicos
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if weather_df is None or weather_df.empty:
        print("Aviso: Dados insuficientes para gráfico de vento")
        return
    
    # Verificar se temos pelo menos dados de velocidade do vento
    if 'wind_speed' not in weather_df.columns:
        print("Aviso: Dados de velocidade do vento não disponíveis")
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
    fig, ax1 = plt.subplots(figsize=FIG_SIZE, dpi=100)
    
    # Determinar eixo X (tempo)
    if 'datetime' in weather_df.columns:
        x = weather_df['datetime']
        x_formatter = mdates.DateFormatter('%H:%M')
        x_label = 'Time'
    else:
        # Usar índices se datetime não estiver disponível
        x = np.arange(len(weather_df))
        x_formatter = None
        x_label = 'Time (data points)'
    
    # Plotar velocidade do vento no eixo principal
    ax1.plot(x, weather_df['wind_speed'], color=COLORS['wind_speed'], linewidth=2, label='Wind Speed')
    ax1.set_ylabel('Wind Speed (km/h)', fontsize=12, color=COLORS['wind_speed'])
    ax1.tick_params(axis='y', labelcolor=COLORS['wind_speed'])
    
    # Configurar eixo X
    if x_formatter:
        ax1.xaxis.set_major_formatter(x_formatter)
        fig.autofmt_xdate()
    
    # Se temos dados de direção do vento, plotar no eixo secundário
    if 'wind_direction' in weather_df.columns:
        ax2 = ax1.twinx()
        ax2.plot(x, weather_df['wind_direction'], color=COLORS['wind_direction'], linewidth=2, label='Wind Direction')
        ax2.set_ylabel('Wind Direction (degrees)', fontsize=12, color=COLORS['wind_direction'])
        ax2.tick_params(axis='y', labelcolor=COLORS['wind_direction'])
        
        # Configurar limites e ticks para direção (0-360 graus)
        ax2.set_ylim(0, 360)
        ax2.set_yticks([0, 90, 180, 270, 360])
        ax2.set_yticklabels(['N', 'E', 'S', 'W', 'N'])
    
    # Configurar título e rótulos
    ax1.set_title(f"Wind Data - {race_name} - {session_name}", fontsize=14, color=text_color)
    ax1.set_xlabel(x_label, fontsize=12, color=text_color)
    
    # Adicionar legenda
    lines, labels = ax1.get_legend_handles_labels()
    if 'wind_direction' in weather_df.columns:
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc='upper right')
    else:
        ax1.legend(loc='upper right')
    
    # Adicionar grade para facilitar a leitura
    ax1.grid(True, alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de vento salva em: {output_path}")
    plt.close()

def create_weather_summary(weather_df, race_name, session_name, output_path, dark_mode=False):
    """
    Cria gráfico resumo com todas as métricas meteorológicas principais.
    
    Args:
        weather_df: DataFrame com dados meteorológicos
        race_name: Nome do evento para o título
        session_name: Nome da sessão para o título
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if weather_df is None or weather_df.empty:
        print("Aviso: Dados insuficientes para resumo meteorológico")
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
    
    # Criar figura com grid para subplots
    fig = plt.figure(figsize=(16, 12), dpi=100)
    gs = GridSpec(3, 2, figure=fig)
    
    # Determinar eixo X (tempo) comum
    if 'datetime' in weather_df.columns:
        x = weather_df['datetime']
        x_formatter = mdates.DateFormatter('%H:%M')
        x_label = 'Time'
    else:
        # Usar índices se datetime não estiver disponível
        x = np.arange(len(weather_df))
        x_formatter = None
        x_label = 'Time (data points)'
    
    # 1. Gráfico de temperatura (ocupando a primeira linha inteira)
    ax1 = fig.add_subplot(gs[0, :])
    if 'air_temp' in weather_df.columns:
        ax1.plot(x, weather_df['air_temp'], color=COLORS['air_temp'], linewidth=2, label='Air Temperature')
    if 'track_temp' in weather_df.columns:
        ax1.plot(x, weather_df['track_temp'], color=COLORS['track_temp'], linewidth=2, label='Track Temperature')
    ax1.set_title('Temperature', fontsize=12, color=text_color)
    ax1.set_ylabel('Temperature (°C)', fontsize=10, color=text_color)
    ax1.grid(True, alpha=0.3, color=grid_color)
    if x_formatter:
        ax1.xaxis.set_major_formatter(x_formatter)
    ax1.legend(loc='upper right')
    
    # 2. Gráfico de umidade
    ax2 = fig.add_subplot(gs[1, 0])
    if 'humidity' in weather_df.columns:
        ax2.plot(x, weather_df['humidity'], color=COLORS['humidity'], linewidth=2)
        ax2.set_title('Humidity', fontsize=12, color=text_color)
        ax2.set_ylabel('Humidity (%)', fontsize=10, color=text_color)
        ax2.grid(True, alpha=0.3, color=grid_color)
        if x_formatter:
            ax2.xaxis.set_major_formatter(x_formatter)
    
    # 3. Gráfico de precipitação
    ax3 = fig.add_subplot(gs[1, 1])
    if 'rainfall' in weather_df.columns:
        # Se todos os valores de precipitação são zero, plotar uma linha horizontal
        if weather_df['rainfall'].sum() == 0:
            ax3.axhline(y=0, color=COLORS['rainfall'], linewidth=1, linestyle='--')
            max_rainfall = 1  # Valor arbitrário para escala
        else:
            ax3.plot(x, weather_df['rainfall'], color=COLORS['rainfall'], linewidth=2)
            max_rainfall = weather_df['rainfall'].max() * 1.1  # 10% a mais para margem
        
        ax3.set_title('Rainfall', fontsize=12, color=text_color)
        ax3.set_ylabel('Rainfall (mm)', fontsize=10, color=text_color)
        ax3.set_ylim(0, max_rainfall)
        ax3.grid(True, alpha=0.3, color=grid_color)
        if x_formatter:
            ax3.xaxis.set_major_formatter(x_formatter)
    
    # 4. Gráfico de velocidade do vento
    ax4 = fig.add_subplot(gs[2, 0])
    if 'wind_speed' in weather_df.columns:
        ax4.plot(x, weather_df['wind_speed'], color=COLORS['wind_speed'], linewidth=2)
        ax4.set_title('Wind Speed', fontsize=12, color=text_color)
        ax4.set_ylabel('Wind Speed (km/h)', fontsize=10, color=text_color)
        ax4.set_xlabel(x_label, fontsize=10, color=text_color)
        ax4.grid(True, alpha=0.3, color=grid_color)
        if x_formatter:
            ax4.xaxis.set_major_formatter(x_formatter)
    
    # 5. Gráfico de direção do vento
    ax5 = fig.add_subplot(gs[2, 1])
    if 'wind_direction' in weather_df.columns:
        ax5.plot(x, weather_df['wind_direction'], color=COLORS['wind_direction'], linewidth=2)
        ax5.set_title('Wind Direction', fontsize=12, color=text_color)
        ax5.set_ylabel('Wind Direction', fontsize=10, color=text_color)
        ax5.set_xlabel(x_label, fontsize=10, color=text_color)
        # Configurar limites e ticks para direção (0-360 graus)
        ax5.set_ylim(0, 360)
        ax5.set_yticks([0, 90, 180, 270, 360])
        ax5.set_yticklabels(['N', 'E', 'S', 'W', 'N'])
        ax5.grid(True, alpha=0.3, color=grid_color)
        if x_formatter:
            ax5.xaxis.set_major_formatter(x_formatter)
    
    # Título geral
    fig.suptitle(f"Weather Summary - {race_name} - {session_name}", fontsize=16, color=text_color)
    
    # Ajustar layout
    plt.tight_layout(rect=[0, 0, 1, 0.96])  # Ajustar para deixar espaço para o título geral
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de resumo meteorológico salva em: {output_path}")
    plt.close()

def create_temperature_comparison(weather_dfs, race_name, session_names, output_path, dark_mode=False):
    """
    Cria gráfico comparativo de temperatura entre várias sessões.
    
    Args:
        weather_dfs: Lista de DataFrames com dados meteorológicos de diferentes sessões
        race_name: Nome do evento para o título
        session_names: Lista de nomes das sessões para legenda
        output_path: Caminho para salvar a visualização
        dark_mode: Se True, usa tema escuro para a visualização
    """
    if not weather_dfs:
        print("Aviso: Nenhum dado disponível para comparação de temperatura")
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
    
    # Cores para diferentes sessões
    session_colors = ['blue', 'red', 'green', 'orange', 'purple', 'cyan']
    
    # Plotar temperatura do ar para cada sessão
    for i, (df, session_name) in enumerate(zip(weather_dfs, session_names)):
        if df is not None and not df.empty and 'air_temp' in df.columns:
            # Normalizar o eixo X para percentual da sessão
            x = np.linspace(0, 100, len(df))
            color = session_colors[i % len(session_colors)]
            plt.plot(x, df['air_temp'], color=color, linewidth=2, label=f'{session_name} - Air')
    
    # Configurar título e rótulos
    plt.title(f"Temperature Comparison - {race_name}", fontsize=14, color=text_color)
    plt.xlabel('Session Progress (%)', fontsize=12, color=text_color)
    plt.ylabel('Temperature (°C)', fontsize=12, color=text_color)
    
    # Adicionar legenda
    plt.legend()
    
    # Adicionar grade para facilitar a leitura
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Salvar a visualização
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Visualização de comparação de temperatura salva em: {output_path}")
    plt.close()

def main():
    """Função principal do script."""
    # Processar argumentos de linha de comando
    args = parse_args()
    
    # Extrair argumentos
    meeting_key = args.meeting
    session_key = args.session
    dark_mode = args.dark_mode
    compare_sessions = args.compare_sessions
    
    # Definir o diretório de saída
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Usar o diretório de visualizações dentro dos dados processados
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/weather")
    
    # Criar diretório de saída se não existir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Definir nomes para o evento e sessão
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Carregar dados meteorológicos
        weather_df = load_weather_data(meeting_key, session_key)
        
        if weather_df is None:
            print("Erro: Não foi possível carregar dados meteorológicos")
            return 1
        
        # Criar visualizações individuais
        temp_path = output_dir / f"temperature_{meeting_key}_{session_key}.png"
        create_temperature_chart(weather_df, race_name, session_name, temp_path, dark_mode)
        
        humidity_path = output_dir / f"humidity_rainfall_{meeting_key}_{session_key}.png"
        create_humidity_rainfall_chart(weather_df, race_name, session_name, humidity_path, dark_mode)
        
        wind_path = output_dir / f"wind_{meeting_key}_{session_key}.png"
        create_wind_chart(weather_df, race_name, session_name, wind_path, dark_mode)
        
        summary_path = output_dir / f"weather_summary_{meeting_key}_{session_key}.png"
        create_weather_summary(weather_df, race_name, session_name, summary_path, dark_mode)
        
        # Se houver sessões para comparar, criar visualização comparativa
        if compare_sessions:
            # Carregar dados das sessões adicionais
            compare_dfs = []
            session_names = [session_name]
            
            for comp_session in compare_sessions:
                comp_df = load_weather_data(meeting_key, comp_session)
                compare_dfs.append(comp_df)
                session_names.append(f"Session_{comp_session}")
            
            # Adicionar a sessão principal à lista
            compare_dfs.insert(0, weather_df)
            
            # Criar visualização comparativa
            comparison_path = output_dir / f"temperature_comparison_{meeting_key}.png"
            create_temperature_comparison(compare_dfs, race_name, session_names, comparison_path, dark_mode)
        
        print("Todas as visualizações meteorológicas foram geradas com sucesso!")
        print(f"As visualizações estão disponíveis em: {output_dir}")
        
    except Exception as e:
        print(f"Erro ao criar visualizações: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())