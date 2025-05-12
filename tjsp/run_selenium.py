from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import json
import time

# Configurações do navegador
chrome_options = Options()
chrome_options.add_argument("--start-maximized")

# Inicializa o driver
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

# Função para extrair dados dos processos
def extrair_dados_processos(oab):
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

# Função para pesquisar uma OAB específica
def pesquisar_oab(oab):
    url = f"https://esaj.tjsp.jus.br/cpopg/search.do?conversationId=&cbPesquisa=NUMOAB&dadosConsulta.valorConsulta={oab}&cdForo=-1"
    driver.get(url)
    time.sleep(0.86)  # Espera inicial
    
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
        processos_pagina = extrair_dados_processos(oab)
        processos_oab.extend(processos_pagina)
    
    return processos_oab

# Range de OABs a pesquisar
oab_inicial = 100  # 077826SP
oab_final = 300    # 077832SP
prefixo = "SP"
todos_processos = []

try:
    for numero in range(oab_inicial, oab_final + 1):
        oab_formatada = f"{str(numero).zfill(6)}{prefixo}"  # Formata com zeros à esquerda
        print(f"\nIniciando pesquisa para OAB {oab_formatada}")
        
        processos_encontrados = pesquisar_oab(oab_formatada)
        
        if processos_encontrados:
            print(f"Encontrados {len(processos_encontrados)} processos para OAB {oab_formatada}")
            todos_processos.extend(processos_encontrados)
        else:
            print(f"Nenhum processo encontrado para OAB {oab_formatada}")
        
        # Limpa memória explicitamente (embora o Python faça garbage collection)
        del processos_encontrados
    
    # Salva todos os dados coletados em um arquivo JSON
    with open('processos_tjsp_range.json', 'w', encoding='utf-8') as f:
        json.dump(todos_processos, f, ensure_ascii=False, indent=4)
    
    print(f"\nProcesso concluído! Total de processos coletados: {len(todos_processos)}")
    print(f"Dados salvos em 'processos_tjsp_range.json'")

finally:
    driver.quit()