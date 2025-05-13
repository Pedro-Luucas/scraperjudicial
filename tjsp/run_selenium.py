from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import json
import time
import threading
import os

# Configurações globais
PREFIXO = "SP"
NUM_THREADS = 32
LOCK = threading.Lock()
TAMANHO_LOTE = 1000  # Quantidade de OABs por arquivo
DELAY_ENTRE_LOTES = 5  # Segundos entre lotes

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.page_load_strategy = 'eager'

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

def extrair_dados_processos(oab, driver):
    try:
        elementos = driver.find_elements(By.CSS_SELECTOR, "div[id^='divProcesso']")
        processos = []
        
        for processo in elementos:
            dados = {
                "oab": oab,
                "numero_processo": safe_extract(processo, By.CLASS_NAME, "nuProcesso"),
                "advogado": safe_extract(processo, By.CSS_SELECTOR, "div.nomeParte"),
                "classe_processo": safe_extract(processo, By.CLASS_NAME, "classeProcesso"),
                "assunto": safe_extract(processo, By.CLASS_NAME, "assuntoPrincipalProcesso"),
                "link_processo": safe_extract(processo, By.CSS_SELECTOR, "a.linkProcesso", attr="href")
            }
            
            data_local = safe_extract(processo, By.CLASS_NAME, "dataLocalDistribuicaoProcesso")
            if data_local:
                parts = data_local.split(" - ", 1)
                dados["data_recebimento"] = parts[0] if parts else None
                dados["vara"] = parts[1] if len(parts) > 1 else None
            else:
                dados["data_recebimento"] = None
                dados["vara"] = None
            
            processos.append(dados)
        
        return processos
    except Exception as e:
        print(f"Erro ao extrair processos para OAB {oab}: {str(e)}")
        return []

def safe_extract(element, by, value, attr=None):
    try:
        found = element.find_element(by, value)
        return found.get_attribute(attr) if attr else found.text.strip()
    except:
        return None

def pesquisar_oab(oab, driver):
    try:
        url = f"https://esaj.tjsp.jus.br/cpopg/search.do?conversationId=&cbPesquisa=NUMOAB&dadosConsulta.valorConsulta={oab}&cdForo=-1"
        driver.get(url)
        time.sleep(0.5)  # Delay reduzido
        
        processos = extrair_dados_processos(oab, driver)
        
        # Verifica paginação
        paginas = driver.find_elements(By.CSS_SELECTOR, "a.paginacao")
        
        for pagina in range(2, len(paginas) + 2):
            url_pagina = f"{url}&paginaConsulta={pagina}"
            driver.get(url_pagina)
            time.sleep(0.2)
            processos.extend(extrair_dados_processos(oab, driver))
        
        return processos
    except Exception as e:
        print(f"Erro ao pesquisar OAB {oab}: {str(e)}")
        return []

def worker(oabs, thread_id, resultados):
    driver = setup_driver()
    
    try:
        for oab_num in oabs:
            oab_formatada = f"{str(oab_num).zfill(6)}{PREFIXO}"
            processos = pesquisar_oab(oab_formatada, driver)
            
            with LOCK:
                resultados.extend(processos)
                
            print(f"Thread {thread_id} concluiu OAB {oab_formatada} - {len(processos)} processos")
    finally:
        driver.quit()

def processar_lote(inicio, fim):
    todos_processos = []
    oabs_por_thread = (fim - inicio + 1) // NUM_THREADS
    threads = []
    
    for i in range(NUM_THREADS):
        start = inicio + i * oabs_por_thread
        end = start + oabs_por_thread - 1 if i < NUM_THREADS - 1 else fim
        oabs = range(start, end + 1)
        
        t = threading.Thread(target=worker, args=(oabs, i+1, todos_processos))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    return todos_processos

def salvar_resultados(resultados, inicio, fim):
    nome_arquivo = f"processos_OAB{str(inicio).zfill(6)}_OAB{str(fim).zfill(6)}.json"
    caminho = os.path.join("./processos", nome_arquivo)
    
    os.makedirs("./processos", exist_ok=True)
    
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    
    print(f"\nArquivo salvo: {caminho} - {len(resultados)} processos")

def main():
    inicio = 18001  # Pode ser ajustado para continuar de onde parou
    fim_total = inicio + 20000  # Total desejado
    
    while inicio <= fim_total:
        fim_lote = min(inicio + TAMANHO_LOTE - 1, fim_total)
        print(f"\nProcessando lote: OAB{inicio} a OAB{fim_lote}")
        
        resultados = processar_lote(inicio, fim_lote)
        salvar_resultados(resultados, inicio, fim_lote)
        
        inicio = fim_lote + 1
        
        if inicio <= fim_total:
            print(f"\nAguardando {DELAY_ENTRE_LOTES} segundos antes do próximo lote...")
            time.sleep(DELAY_ENTRE_LOTES)

if __name__ == "__main__":
    main()