# F1 Data Analyzer

Um sistema completo para coleta, processamento e análise de dados de corridas da Fórmula 1, utilizando os feeds de dados oficiais.

## Visão Geral

O F1 Data Analyzer permite:

1. **Explorar** dados de eventos de F1 disponíveis para anos recentes
2. **Coletar** dados brutos de sessões específicas (treinos, classificação, corrida)
3. **Processar** os dados brutos em formatos utilizáveis (CSV)
4. **Analisar** dados para extrair informações sobre desempenho, estratégia e telemetria
5. **Importar** dados para um banco de dados Supabase (opcional)

## Índice

- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Como Usar](#como-usar)
  - [Explorar Dados Disponíveis](#explorar-dados-disponíveis)
  - [Coletar Dados Brutos](#coletar-dados-brutos)
  - [Processar Dados](#processar-dados)
  - [Conectar ao Supabase](#conectar-ao-supabase)
- [Tipos de Dados Disponíveis](#tipos-de-dados-disponíveis)
- [Estrutura de Diretórios](#estrutura-de-diretórios)
- [Fluxo de Trabalho Típico](#fluxo-de-trabalho-típico)
- [Módulos e Processadores](#módulos-e-processadores)
- [APIs OpenF1](#apis-openf1)
- [Solução de Problemas](#solução-de-problemas)

## Requisitos

- Python 3.8+
- pip (gerenciador de pacotes Python)
- Conexão com a internet para acessar os dados da F1
- (Opcional) Conta no Supabase para armazenamento em banco de dados

## Instalação

1. Clone este repositório ou baixe os arquivos:

```bash
git clone <url-do-repositório>
cd f1-data-analyzer
```

2. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração

1. O sistema funcionará sem configuração adicional para coleta e processamento local.

2. Para utilizar o armazenamento no Supabase (opcional), crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```
SUPABASE_URL=https://your-supabase-url.supabase.co
SUPABASE_KEY=your-supabase-key
```

## Como Usar

### Explorar Dados Disponíveis

Para verificar quais corridas e sessões estão disponíveis, use o script `f1_explorer.py`:

```bash
python f1_explorer.py
```

Isso irá buscar e listar eventos disponíveis para anos recentes, gerando arquivos JSON na pasta `f1_data_explorer/`.

### Coletar Dados Brutos

Use o script `f1_collector.py` para baixar dados brutos. Há três maneiras de usá-lo:

1. Listar todas as corridas e sessões disponíveis:

```bash
python f1_collector.py --list
```

2. Coletar uma sessão específica usando as chaves (valor numérico):

```bash
python f1_collector.py --meeting 1264 --session 1297
```

Onde:
- `--meeting` é a chave da corrida (ex: 1264 para Miami GP 2025)
- `--session` é a chave da sessão específica (ex: 1297 para a corrida principal)

3. Ou use o script principal `main.py`:

```bash
python main.py collect --meeting 1264 --session 1297
```

Os dados brutos serão salvos em `f1_data/raw/[meeting_key]/[session_key]/`.

### Processar Dados

Após a coleta, processe os dados brutos em formatos utilizáveis:

```bash
python main.py process --race 1264 --session 1297
```

Para processar apenas tópicos específicos:

```bash
python main.py process --race 1264 --session 1297 --topics TimingData WeatherData
```

Os dados processados serão salvos em `f1_data/processed/[meeting_key]/[session_key]/`.

### Conectar ao Supabase

Se configurado, dados específicos como meteorologia, informações de pilotos e telemetria dos carros podem ser automaticamente importados para o Supabase:

```bash
python main.py import
```

Também é possível utilizar os scripts específicos da pasta `openf1/` para importar dados de uma API alternativa:

```bash
python openf1/openf1_import.py --session 9980 --import-weather --import-drivers
```

## Tipos de Dados Disponíveis

O sistema pode coletar e processar diversos tipos de dados:

- **TimingData**: Tempos de volta, tempos de setor, velocidades
- **CarData.z**: Telemetria dos carros (RPM, velocidade, aceleração, freio, etc.)
- **CurrentTyres**: Informações sobre os pneus utilizados
- **PitLaneTimeCollection**: Dados de pit stops
- **WeatherData**: Condições meteorológicas
- **DriverList**: Informações sobre pilotos
- **Position.z**: Posições dos carros na pista
- **RaceControlMessages**: Mensagens oficiais do controle de corrida
- **TeamRadio**: Comunicações de rádio entre equipes e pilotos
- **TimingAppData**: Dados adicionais de tempo e estratégia de pneus

## Estrutura de Diretórios

```
.
├── f1_data/                  # Diretório principal de dados
│   ├── raw/                  # Dados brutos coletados
│   │   └── [meeting_key]/    # Dados organizados por corrida (ex: 1264)
│   │       └── [session_key]/# Dados específicos da sessão (ex: 1297)
│   └── processed/            # Dados processados (CSV, visualizações)
│       └── [meeting_key]/    # Processados por corrida
│           └── [session_key]/# Processados por sessão
├── f1_data_explorer/         # Metadados de corridas e sessões
├── src/                      # Código-fonte
│   └── processors/           # Processadores específicos por tipo de dado
├── openf1/                   # Scripts para API alternativa OpenF1
└── .env                      # Configuração do Supabase (opcional)
```

## Fluxo de Trabalho Típico

1. **Explorar**: Descubra quais corridas e sessões estão disponíveis
   ```
   python f1_explorer.py
   ```

2. **Listar**: Veja as chaves específicas para as sessões
   ```
   python f1_collector.py --list
   ```

3. **Coletar**: Baixe os dados brutos de uma sessão específica
   ```
   python main.py collect --meeting 1264 --session 1297
   ```

4. **Processar**: Transforme os dados em formatos analisáveis
   ```
   python main.py process --race 1264 --session 1297
   ```

5. **Importar** (opcional): Envie dados para o banco de dados Supabase
   ```
   python main.py import
   ```

## Módulos e Processadores

### Processadores Principais:

- **BaseProcessor**: Classe base com funcionalidades comuns
- **TimingDataProcessor**: Processa tempos de volta e posições
- **CarDataProcessor**: Processa telemetria dos carros
- **CurrentTyresProcessor**: Analisa estratégias de pneus
- **PitLaneProcessor**: Analisa dados de pit stops
- **WeatherDataProcessor**: Processa dados meteorológicos
- **DriverListProcessor**: Gerencia informações de pilotos
- **PositionProcessor**: Analisa posições na pista
- **RaceControlMessagesProcessor**: Processa mensagens oficiais
- **TeamRadioProcessor**: Processa comunicações de rádio
- **StintAnalyzer**: Correlaciona tempos de volta com estratégias de pneus

### Scripts Utilitários:

- **f1_explorer.py**: Explora dados disponíveis
- **f1_collector.py**: Coleta dados brutos
- **main.py**: Interface principal
- **import_races_to_supabase.py**: Importa dados para Supabase

## APIs OpenF1

O diretório `openf1/` contém scripts para interagir com a API alternativa OpenF1:

- **openf1_import.py**: Script principal para importação
- **openf1_meetings_importer.py**: Importa dados de corridas
- **openf1_sessions_importer.py**: Importa dados de sessões
- **openf1_drivers_importer.py**: Importa dados de pilotos
- **openf1_weather_importer.py**: Importa dados meteorológicos
- **openf1_car_data_importer.py**: Importa telemetria dos carros
- **openf1_race_control_importer.py**: Importa mensagens do controle de corrida
- **auto_update_weather.py**: Automatiza atualizações incrementais de dados meteorológicos

## Solução de Problemas

### Problemas de Conexão

Se encontrar erros ao acessar dados:
- Verifique sua conexão com a internet
- Confirme se a URL base ainda está correta (raramente muda)
- Verifique se as chaves de meeting/session estão corretas

### Erros de Processamento

- Verifique se os dados brutos foram coletados corretamente
- Assegure-se de que as dependências estão instaladas
- Para compressão, verifique se as bibliotecas zlib e base64 estão funcionando

### Erros do Supabase

Se a importação para o Supabase falhar:
- Confirme se o arquivo `.env` está configurado corretamente
- Verifique se seu banco de dados Supabase tem as tabelas esperadas
- Confirme se suas credenciais têm permissão de escrita

### Dados Incompletos

Os dados de F1 são atualizados em tempo real durante as sessões. Se quiser dados completos:
- Colete dados após o término das sessões
- Para sessões em andamento, use atualizações incrementais com `auto_update_weather.py`

---

## Exemplos de Uso

### Coletar e processar dados do GP de Miami 2025

```bash
# Explorar dados disponíveis
python f1_explorer.py

# Listar sessões
python f1_collector.py --list

# Coletar dados da corrida (session 1297)
python main.py collect --meeting 1264 --session 1297

# Processar todos os tópicos
python main.py process --race 1264 --session 1297

# Processar apenas telemetria e pneus
python main.py process --race 1264 --session 1297 --topics CarData.z CurrentTyres
```

### Importar usando API OpenF1 (alternativa)

```bash
# Importar dados meteorológicos para uma sessão
python openf1/openf1_weather_importer.py --session 9980

# Atualizar dados meteorológicos incrementalmente a cada minuto
python openf1/auto_update_weather.py --session 9980 --interval 60
```

---

Se precisar de ajuda adicional ou tiver sugestões, sinta-se à vontade para abrir uma issue no repositório do projeto.