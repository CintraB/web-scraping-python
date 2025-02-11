import pandas as pd
import re
import json
import requests
import subprocess
import sys
from datetime import datetime


def log_message(log_file, message):
    """Registra mensagens no log sem acentuacoes."""
    message = message.encode('ascii', 'ignore').decode('ascii')  # Remove acentuacoes
    with open(log_file, 'a') as log:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.write(f"[{timestamp}] {message}\n")

def extract_nodes(node, manager_name=None):
    """Extrai informacoes de cada no recursivamente."""
    extracted_data = []
    extracted_data.append({
        "id": node.get("id", "null"),
        "name": node.get("name", "null"),
        "email": node.get("email", "null"),
        "jobTitle": node.get("jobTitle", node.get("job_title", "null")),
        "id_manager": node.get("id_manager", "null")
    })
    for child in node.get("children", []):
        extracted_data.extend(extract_nodes(child, manager_name=node.get("name", "null")))
    return extracted_data

def extract_hierarchy(data):
    """Extrai dados hierarquicos de JSONs corrigidos."""
    extracted_data = []
    for root in data:
        extracted_data.extend(extract_nodes(root))
    return pd.DataFrame(extracted_data)

def parse_html_to_json(html_content, log_file):
    """Busca JSONs completos no HTML sem validacao de formato."""
    try:
        json_blocks = re.findall(r'\{"id":.*?"children":\[[^\]]*\]\}', html_content)
        log_message(log_file, f"{len(json_blocks)} blocos JSON encontrados no HTML.")
        return json_blocks
    except Exception as e:
        log_message(log_file, f"Erro ao processar HTML: {e}")
        return []

def process_raw_json(raw_json):
    """Processa e corrige objetos JSON diretamente do conteudo bruto."""
    corrected_data = []
    errors = []

    # Identificar blocos de JSON validos usando logica aprimorada
    json_blocks = []
    start_idx = 0

    while True:
        start_idx = raw_json.find('{', start_idx)
        if start_idx == -1:
            break

        end_idx = raw_json.find('}', start_idx)
        while end_idx != -1:
            try:
                json_block = raw_json[start_idx:end_idx + 1]
                parsed_json = json.loads(json_block)
                json_blocks.append(parsed_json)
                break
            except Exception:
                end_idx = raw_json.find('}', end_idx + 1)

        start_idx += 1

    return json_blocks

def process_files_with_cookies(ids_file, page_url, cookies_file, output_new, output_disconnected_csv, output_leaders_csv, raw_json_output, problematic_json_output, new_ids_file, log_file):
    """Pipeline completo de processamento."""
    log_message(log_file, "Processo iniciado.")

    # Carregar IDs antigos
    try:
        with open(ids_file, 'r') as file:
            old_ids = {line.strip() for line in file.readlines()}
        log_message(log_file, f"IDs antigos carregados de {ids_file}.")
    except Exception as e:
        log_message(log_file, f"Erro ao carregar IDs antigos: {e}")
        return

    
    try:
        # Atualizar o arquivo de cookies chamando programa paralelo dedicado
        subprocess.run([sys.executable,"playwright_cookies.py"]) # Executa e aguarda a conclusao
    except Exception as e:
        log_message(log_file, f"Algoritmo separado rodando: Erro ao atualizar cookies: {e}")

    # Carregar cookies
    try:
        with open(cookies_file, 'r') as file:
            cookies = json.load(file)
        session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
        log_message(log_file, f"Cookies carregados de {cookies_file}.")
    except Exception as e:
        log_message(log_file, f"Erro ao carregar cookies: {e}")
        return

    # Requisicao a pagina
    try:
        response = requests.get(page_url, cookies=session_cookies)
        response.raise_for_status()
        html_content = response.text
        log_message(log_file, f"Requisicao para {page_url} bem-sucedida.")
    except Exception as e:
        log_message(log_file, f"Erro ao fazer requisicao: {e}")
        return

    # Extrair JSONs do HTML
    json_blocks = parse_html_to_json(html_content, log_file)

    # Salvar JSON bruto
    try:
        with open(raw_json_output, 'w', encoding='utf-8') as raw_file:
            json.dump(json_blocks, raw_file, ensure_ascii=False, indent=4)
        log_message(log_file, f"Arquivo JSON bruto salvo em {raw_json_output}.")
    except Exception as e:
        log_message(log_file, f"Erro ao salvar JSON bruto: {e}")

    # Corrigir JSONs extraidos
    corrected_nodes = []
    errors = []
    for raw_block in json_blocks:
        corrected_json = process_raw_json(raw_block)
        corrected_nodes.extend(corrected_json)

    # Salvar JSON problematico
    try:
        with open(problematic_json_output, 'w', encoding='utf-8') as error_file:
            json.dump(errors, error_file, ensure_ascii=False, indent=4)
        log_message(log_file, f"Erros JSON salvos em {problematic_json_output}.")
    except Exception as e:
        log_message(log_file, f"Erro ao salvar erros de JSON: {e}")

    # Extrair dados hierarquicos
    try:
        df_hierarchy = extract_hierarchy(corrected_nodes)
        df_hierarchy["id"] = df_hierarchy["id"].astype(str)
        log_message(log_file, "Dados hierarquicos extraidos com sucesso.")
    except Exception as e:
        log_message(log_file, f"Erro ao extrair dados hierarquicos: {e}")
        return

    # Identificar IDs desligados e novos IDs
    try:
        current_ids = set(df_hierarchy["id"].dropna())
        old_ids = {str(id) for id in old_ids}
        new_ids = current_ids - old_ids

        log_message(log_file, f"IDs identificados como novos: {new_ids}")

        # Salvar IDs desligados como CSV
        disconnected_ids = old_ids - current_ids
        df_disconnected = pd.DataFrame({"ID": list(disconnected_ids)})
        df_disconnected.to_csv(output_disconnected_csv, index=False, sep=';', encoding='utf-8')
        log_message(log_file, f"Arquivo com IDs desligados salvo em {output_disconnected_csv}.")

        # Salvar IDs novos
        with open(new_ids_file, 'w') as file:
            file.writelines(f"{id}\n" for id in new_ids)
        log_message(log_file, f"Arquivo com IDs novos salvo em {new_ids_file}.")

        # Filtrar dados de novos funcionarios
        df_new_employees = df_hierarchy[df_hierarchy["id"].isin(new_ids)][["id", "name", "email", "jobTitle", "id_manager"]]
        df_new_employees["email"] = df_new_employees["email"].fillna("null")
        df_new_employees["jobTitle"] = df_new_employees["jobTitle"].fillna("null")
        df_new_employees["id_manager"] = df_new_employees["id_manager"].fillna("null").astype(str).str.replace(r"\.0", "", regex=True)

        # Renomear colunas e remover acentuacoes
        df_new_employees.columns = ["ID", "NOME", "EMAIL", "CARGO", "ID_GESTORES"]
        df_new_employees = df_new_employees.apply(lambda col: col.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii') if col.dtype == 'object' else col)

        # Separar gestores
        leadership_keywords = ["lider", "gestor", "head", "coordenador", "CEO", "diretor", "manager", "supervisor", "gerente"]
        pattern = '|'.join(leadership_keywords)
        df_leaders = df_new_employees[df_new_employees["CARGO"].str.contains(pattern, case=False, na=False)]

        # Remover gestores do DataFrame geral
        df_new_employees = df_new_employees[~df_new_employees["CARGO"].str.contains(pattern, case=False, na=False)]

        # Salvar arquivos separados
        df_new_employees.to_csv(output_new, index=False, sep=';', encoding='utf-8')
        
        # Remover coluna ID_GESTORES de gestores antes de salvar
        df_leaders.drop(columns=["ID_GESTORES"], inplace=True)
        df_leaders.to_csv(output_leaders_csv, index=False, sep=';', encoding='utf-8')

        log_message(log_file, f"Tabela de novos funcionarios salva em {output_new}.")
        log_message(log_file, f"Tabela de gestores salva em {output_leaders_csv}.")

        # Atualizar o arquivo ID.txt
        try:
            with open(ids_file, 'w') as id_file:
                id_file.writelines(f"{id}\n" for id in current_ids)
            log_message(log_file, f"Arquivo {ids_file} atualizado com os IDs atuais.")
        except Exception as e:
            log_message(log_file, f"Erro ao atualizar o arquivo {ids_file}: {e}")

    except Exception as e:
        log_message(log_file, f"Erro ao identificar novos funcionarios e IDs desligados: {e}")

if __name__ == "__main__":
    process_files_with_cookies(
        ids_file="ID.txt",
        page_url="https://app.feedz.com.br/organograma",
        cookies_file="cookies.json",
        output_new="dados_ids_novos.csv",
        output_disconnected_csv="ids_desligados.csv",
        output_leaders_csv="gestores.csv",
        raw_json_output="json_bruto_encontrado.json",
        problematic_json_output="json_com_erros.json",
        new_ids_file="ids_novos.txt",
        log_file="processo.log"
    )

