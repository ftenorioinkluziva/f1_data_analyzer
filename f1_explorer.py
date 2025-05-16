# f1_explorer.py
import requests
import json
import time
from pathlib import Path

def check_url_exists(url):
    try:
        print(f"Verificando URL: {url}")
        response = requests.get(url)
        status = response.status_code
        print(f"Status: {status}")
        return status == 200, response.content
    except Exception as e:
        print(f"Erro ao acessar URL: {str(e)}")
        return False, None

def fix_utf8_bom(content):
    """Corrige o problema de UTF-8 BOM nos arquivos JSON."""
    try:
        # Decodifica o conteúdo considerando o BOM
        text = content.decode('utf-8-sig')
        # Converte para JSON
        data = json.loads(text)
        return data
    except Exception as e:
        print(f"Erro ao processar JSON: {str(e)}")
        return None

def explore_f1_years():
    base_url = "https://livetiming.formula1.com/static"
    output_dir = Path("f1_data_explorer")
    output_dir.mkdir(exist_ok=True)
    
    # Vamos verificar anos: 2025, 2024, 2023, 2022
    years_to_check = [2025]
    found_years = []
    
    print("Procurando índices de anos disponíveis...\n")
    
    for year in years_to_check:
        year_url = f"{base_url}/{year}"
        exists, content = check_url_exists(f"{year_url}/Index.json")
        
        if exists:
            print(f"✅ Encontrado índice para o ano: {year}")
            
            # Processar o JSON corrigindo o BOM
            index_data = fix_utf8_bom(content)
            
            if not index_data:
                print(f"❌ Não foi possível processar os dados do ano {year}.")
                continue
            
            # Salvar o índice do ano
            with open(output_dir / f"index_{year}.json", "w") as f:
                json.dump(index_data, f, indent=2)
            
            print(f"Índice do ano {year} salvo em {output_dir}/index_{year}.json")
            found_years.append(year)
        else:
            print(f"❌ Índice do ano {year} não encontrado.")
    
    if found_years:
        print(f"\nArquivos de índice foram gerados para os seguintes anos: {', '.join(map(str, found_years))}")
        print("\nPróximos passos:")
        print("1. Os arquivos index_YEAR.json contêm informações sobre todas as corridas e sessões do ano.")
        print("2. Você pode usá-los para extrair os caminhos das sessões e as chaves necessárias para o seu projeto.")
        print("3. Exemplo para extrair informações do GP de Miami 2025:")
        print("   - Abra o arquivo index_2025.json")
        print("   - Procure pela seção do 'Miami Grand Prix'")
        print("   - Extraia os caminhos e chaves das sessões desejadas (Practice, Qualifying, Race)")
    else:
        print("\nNenhum índice encontrado para os anos verificados.")

if __name__ == "__main__":
    explore_f1_years()