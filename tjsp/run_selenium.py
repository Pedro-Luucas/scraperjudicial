from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import json
import time
import threading

# Configurações globais
OAB_INICIAL = 3901
OAB_FINAL = 5000
PREFIXO = "SP"
NUM_THREADS = 24
LOCK = threading.Lock()
TODOS_PROCESSOS = []

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.page_load_strategy = 'eager'

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

def extrair_dados_processos(oab, driver):
    processos_pagina = []
    elementos_processos = driver.find_elements(By.CSS_SELECTOR, "div[id^='divProcesso']")
    
    for processo in elementos_processos:
        dados_processo = {}
        
        try:
            # Número do processo
            num_processo = processo.find_element(By.CLASS_NAME, "nuProcesso").text.strip()
            dados_processo["numero_processo"] = num_processo
            
            # Nome do advogado
            try:
                advogado = processo.find_element(By.CSS_SELECTOR, "div.nomeParte").text.strip()
                dados_processo["advogado"] = advogado
            except:
                dados_processo["advogado"] = None
            
            dados_processo["oab"] = oab
            # Classe do processo
            classe = processo.find_element(By.CLASS_NAME, "classeProcesso").text.strip()
            dados_processo["classe_processo"] = classe
            
            # Assunto principal
            assunto = processo.find_element(By.CLASS_NAME, "assuntoPrincipalProcesso").text.strip()
            dados_processo["assunto"] = assunto
            
            # Data e local de distribuição
            data_local = processo.find_element(By.CLASS_NAME, "dataLocalDistribuicaoProcesso").text.strip()
            dados_processo["data_recebimento"] = data_local.split(" - ")[0] if data_local else None
            dados_processo["vara"] = data_local.split(" - ")[1] if data_local and " - " in data_local else None
            
            # Link do processo
            try:
                link = processo.find_element(By.CSS_SELECTOR, "a.linkProcesso").get_attribute("href")
                dados_processo["link_processo"] = link
            except:
                dados_processo["link_processo"] = None
            
            processos_pagina.append(dados_processo)
            
        except Exception as e:
            print(f"Erro ao extrair dados de um processo: {str(e)}")
            continue
    
    return processos_pagina

def pesquisar_oab(oab, driver):
    url = f"https://esaj.tjsp.jus.br/cpopg/search.do?conversationId=&cbPesquisa=NUMOAB&dadosConsulta.valorConsulta={oab}&cdForo=-1"
    driver.get(url)
    time.sleep(0.86)
    
    processos_oab = []
    
    # Verifica paginação
    paginas = driver.find_elements(By.CSS_SELECTOR, "a.paginacao")
    total_paginas = len(paginas) if paginas else 1
    
    for pagina in range(1, total_paginas + 1):
        if pagina > 1:
            url_pagina = f"{url}&paginaConsulta={pagina}"
            driver.get(url_pagina)
            time.sleep(0.2)
        
        print(f"OAB {oab} - Processando página {pagina}/{total_paginas}")
        processos_pagina = extrair_dados_processos(oab, driver)
        processos_oab.extend(processos_pagina)
    
    return processos_oab

def worker(oabs, thread_id):
    print(f"Thread {thread_id} iniciando...")
    driver = setup_driver()
    processos_thread = []
    
    try:
        for oab_num in oabs:
            oab_formatada = f"{str(oab_num).zfill(6)}{PREFIXO}"
            print(f"Thread {thread_id} pesquisando OAB {oab_formatada}")
            
            processos_encontrados = pesquisar_oab(oab_formatada, driver)
            
            if processos_encontrados:
                print(f"Thread {thread_id}: Encontrados {len(processos_encontrados)} processos para OAB {oab_formatada}")
                processos_thread.extend(processos_encontrados)
            else:
                print(f"Thread {thread_id}: Nenhum processo encontrado para OAB {oab_formatada}")
        
        with LOCK:
            TODOS_PROCESSOS.extend(processos_thread)
            
    except Exception as e:
        print(f"Erro na thread {thread_id}: {str(e)}")
    finally:
        driver.quit()
        print(f"Thread {thread_id} finalizada")

def main():
    # Divide o range de OABs entre as threads
    oabs_por_thread = (OAB_FINAL - OAB_INICIAL + 1) // NUM_THREADS
    threads = []
    
    for i in range(NUM_THREADS):
        start = OAB_INICIAL + i * oabs_por_thread
        end = start + oabs_por_thread - 1 if i < NUM_THREADS - 1 else OAB_FINAL
        oabs = range(start, end + 1)
        
        t = threading.Thread(target=worker, args=(oabs, i+1))
        threads.append(t)
        t.start()
    
    # Aguarda todas as threads terminarem
    for t in threads:
        t.join()
    
    # Salva os resultados
    with open(f'processos_tjsp_OAB{OAB_INICIAL}_OAB{OAB_FINAL}.json', 'w', encoding='utf-8') as f:
        json.dump(TODOS_PROCESSOS, f, ensure_ascii=False, indent=4)
    
    print(f"\nProcesso concluído! Total de processos coletados: {len(TODOS_PROCESSOS)}")
    print(f"Dados salvos em 'processos_tjsp_range.json'")

if __name__ == "__main__":
    main()