import json
import logging
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

def log_message(log_file, message):
    """Registra mensagens no log usando codificação utf-8."""
    with open(log_file, 'a', encoding='utf-8') as log:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.write(f"[{timestamp}] {message}  \n")
        

def load_config(config_file):
    """Carrega configuracoes de um arquivo de texto."""
    config = {}
    try:
        with open(config_file, 'r', encoding='utf-8') as file:  
            for line in file:
                key, value = line.strip().split('=')
                config[key.strip()] = value.strip()
    except Exception as e:
        raise ValueError(f"Erro ao carregar configuracoes do arquivo {config_file}: {e}")
    return config

def extract_cookies_with_playwright(login_url, target_url, email, password, cookies_file, log_file):
    """Realiza login com Playwright e extrai todos os cookies da página alvo."""
    try:
        log_message(log_file, "Iniciando navegador com Playwright...")
        with sync_playwright() as p:
            # Iniciar navegador
            browser = p.chromium.launch(headless=False, args=[  # Defina `headless=True` para rodar em segundo plano
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--disable-blink-features=AutomationControlled",
                "--ignore-certificate-errors",
                "--allow-insecure-localhost",
                "--disable-web-security",
                "--enable-logging", #logs
                "--v=1", #logs
                "--disable-popup-blocking",
                "--disable-plugins-discovery",
                "--disable-notifications",
                "--disable-background-networking",
                "--disable-extensions",
                "--disable-sync",
                "--disable-images",  # Desativar imagens
                "--disable-default-apps",
            ])  
            context = browser.new_context()

            # Configurar o agente de usuário (User-Agent)
            context.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Linux; Android 10; SM-G960F Build/QP1A.190711.020) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Mobile Safari/537.36"
            })

            # Abrir nova página
            page = context.new_page()

            # Acessar página de login
            page.goto(login_url)
            log_message(log_file, "Página de login carregada.")

            # Preencher o formulário de login
            page.fill('input#login_email', email)
            page.fill('input#passInput', password)
            page.click('#enter-login')
            log_message(log_file, "Formulário preenchido com sucesso.")
            
            time.sleep(10);
            # Navegar para a página de destino
            page.goto(target_url, timeout=60000,wait_until="domcontentloaded") # 60 segundos

            # Capturar cookies
            cookies = context.cookies()
            log_message(log_file, "Cookies capturados.")

            # Salvar cookies no formato JSON
            with open(cookies_file, "w", encoding="utf-8") as file:  # Salvar cookies em utf-8
                json.dump(cookies, file, indent=4)
            log_message(log_file, f"Cookies salvos em {cookies_file}.")

            # Fechar navegador
            browser.close()

    except Exception as e:
        log_message(log_file, f"Erro inesperado: {e}")

if __name__ == "__main__":
    CONFIG_FILE = "config.txt"
    COOKIES_FILE = "cookies.json"
    LOG_FILE = "log_cookies_processo.log"
    LOGIN_URL = "https://app.feedz.com.br"
    TARGET_URL = "https://app.feedz.com.br/organograma"

    try:
        # Carregar configurações
        config = load_config(CONFIG_FILE)
        email = config.get("email")
        password = config.get("senha")
        if not email or not password:
            raise ValueError("Configurações de email ou senha não encontradas no arquivo de configuração.")

        # Extrair cookies com Playwright
        extract_cookies_with_playwright(LOGIN_URL, TARGET_URL, email, password, COOKIES_FILE, LOG_FILE)
    except Exception as e:
        with open(LOG_FILE, 'a', encoding='utf-8') as log:  
            log.write(f"Erro durante o processo: {e}\n")
