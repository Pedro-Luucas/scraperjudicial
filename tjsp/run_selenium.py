from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import sqlite3
import time
import threading
import os

# Configurações globais
PREFIXO = "SP"
NUM_THREADS = 12
LOCK = threading.Lock()
DB_LOCK = threading.Lock()  # Separate lock for database operations
TAMANHO_LOTE = 100  # Quantidade de OABs por lote
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

def inicializar_banco():
    with DB_LOCK:
        try:
            conn = sqlite3.connect("processos.db")
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processos (
                    oab TEXT,
                    numero_processo TEXT,
                    assunto TEXT,
                    link_processo TEXT,
                    data_recebimento TEXT,
                    documents_url TEXT
            )""")
            conn.commit()
            print("Banco de dados inicializado com sucesso.")
        except Exception as e:
            print(f"Erro ao inicializar banco: {str(e)}")
        finally:
            if 'conn' in locals():
                conn.close()

def safe_extract(element, by, value, attr=None):
    try:
        found = element.find_element(by, value)
        return found.get_attribute(attr) if attr else found.text.strip()
    except:
        return None

def extrair_dados_processos(oab, driver):
    try:
        elementos = driver.find_elements(By.CSS_SELECTOR, "div[id^='divProcesso']")
        processos = []
        
        for processo in elementos:
            dados = {
                "oab": oab,
                "numero_processo": safe_extract(processo, By.CLASS_NAME, "nuProcesso"),
                "assunto": f"{safe_extract(processo, By.CLASS_NAME, 'assuntoPrincipalProcesso')} {safe_extract(processo, By.CLASS_NAME, 'classeProcesso')}",
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

def pesquisar_oab(oab, driver):
    try:
        url = f"https://esaj.tjsp.jus.br/cpopg/search.do?conversationId=&cbPesquisa=NUMOAB&dadosConsulta.valorConsulta={oab}&cdForo=-1"
        driver.get(url)
        time.sleep(0.5)

        processos = extrair_dados_processos(oab, driver)
        
        # Debug output
        print(f"Encontrados {len(processos)} processos para OAB {oab}")
        if processos:
            print("Exemplo de processo encontrado:", processos[0])
        
        # Paginação
        paginas = driver.find_elements(By.CSS_SELECTOR, "a.paginacao")
        
        for pagina in range(2, len(paginas) + 2):
            url_pagina = f"{url}&paginaConsulta={pagina}"
            driver.get(url_pagina)
            time.sleep(0.2)
            novos_processos = extrair_dados_processos(oab, driver)
            processos.extend(novos_processos)
        
        return processos
    except Exception as e:
        print(f"Erro ao pesquisar OAB {oab}: {str(e)}")
        return []

def worker(oabs, thread_id):
    driver = setup_driver()
    
    try:
        for oab_num in oabs:
            oab_formatada = f"{str(oab_num).zfill(6)}{PREFIXO}"
            print(f"Thread {thread_id} processando OAB {oab_formatada}...")
            
            try:
                processos = pesquisar_oab(oab_formatada, driver)
                
                # Salva imediatamente após cada pesquisa
                if processos:
                    with DB_LOCK:
                        salvar_resultados(processos)
                        print(f"Thread {thread_id} salvou {len(processos)} processos para OAB {oab_formatada}")
                
            except Exception as e:
                print(f"Erro ao processar OAB {oab_formatada} na thread {thread_id}: {str(e)}")
                continue
                
    finally:
        driver.quit()

def processar_lote(inicio, fim):
    oabs_por_thread = (fim - inicio + 1) // NUM_THREADS
    threads = []
    
    for i in range(NUM_THREADS):
        start = inicio + i * oabs_por_thread
        end = start + oabs_por_thread - 1 if i < NUM_THREADS - 1 else fim
        oabs = range(start, end + 1)
        
        t = threading.Thread(target=worker, args=(oabs, i + 1))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()

def salvar_resultados(resultados):
    if not resultados:
        return
        
    try:
        conn = sqlite3.connect("processos.db")
        cursor = conn.cursor()
        
        data_to_insert = []
        for processo in resultados:
            data_to_insert.append((
                processo.get("oab"),
                processo.get("numero_processo"),
                processo.get("assunto"),
                processo.get("link_processo"),
                processo.get("data_recebimento")
            ))
        
        cursor.executemany("""
            INSERT OR IGNORE INTO processos (
                oab,
                numero_processo,
                assunto,
                link_processo,
                data_recebimento
            ) VALUES (?, ?, ?, ?, ?)
        """, data_to_insert)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erro ao salvar no banco de dados: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    print("Iniciando processo de scraping...")
    inicializar_banco()
    inicio = 18001
    fim_total = inicio + 200

    while inicio <= fim_total:
        fim_lote = min(inicio + TAMANHO_LOTE - 1, fim_total)
        print(f"\nProcessando lote: OAB{inicio} a OAB{fim_lote}")

        resultados = processar_lote(inicio, fim_lote)
        
        # Debug output
        print(f"\nResumo do lote {inicio}-{fim_lote}:")

        inicio = fim_lote + 1

        if inicio <= fim_total:
            print(f"\nAguardando {DELAY_ENTRE_LOTES} segundos antes do próximo lote...")
            time.sleep(DELAY_ENTRE_LOTES)

    print("\nProcesso de scraping concluído.")

if __name__ == "__main__":
    main()