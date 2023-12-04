# coleta de dados
import sidrapy
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains

# manipulação dos dados
import pandas as pd
import json

# sistema
import time
import os
import datetime
from time import sleep
import constants as c

# github
from github import Github
from github import Auth

# ************************
# CONSTANTES
# ************************

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36(KHTML, like Gecko) '
                         'Chrome/118.0.0.0 Safari/537.36'}

git_doc_path = 'support_files/doc-links.json'

script_path = os.getcwd()
dir_name = c.dir_name
dir_path = os.path.join(script_path, dir_name)

errors = {}
n_downs = 0

# criação do diretório
if not os.path.exists(dir_path):
    os.mkdir(dir_path)

# ************************
# EXTRAÇÃO DE DADOS POR MÉTODO DE COLETA WEB SCRAPING
# ************************

# coleta de informações sobre os gráficos
doc = pd.read_json(git_doc_path)
doc_scraping = doc.loc[doc['method'] == 'scraping'].reset_index(drop=True)

for fig, url in zip(doc_scraping['figure'], doc_scraping['url']):
    if fig.startswith('Gráfico 7.1'):
        try:
            print(f'A baixar o arquivo da url:\n {url} ...')

            # downloading do arquivo
            xpath = '/html/body/form/div[3]/div[2]/section/div[2]/div/span/div[2]/div[1]/div[5]/div/div/div/div[' \
                    '2]/div/table/tbody/tr/td[2]/a/@href'
            html = c.get_html(url)
            html_urls = html.xpath(xpath).getall()
            url_to_get = [item for item in html_urls if item.endswith('.xlsx')][0]
            url_to_get = 'https://www.epe.gov.br' + url_to_get
            file = c.open_url(url_to_get)

            c.to_file(dir_path, 'epe-anuario-energia.xlsx', file.content)
            n_downs += 1

        except Exception as e:
            errors[url] = str(e)

    elif fig.startswith('Gráfico 8.1'):
        try:
            print(f'A baixar o arquivo da url:\n {url} ...')

            # downloading do arquivo
            xpath = '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div/ul[1]/li[2]/a/@href'
            html = c.get_html(url)
            url_to_get = html.xpath(xpath).get()
            file = c.open_url(url_to_get)

            c.to_file(dir_path, 'anp_producao_petroleo.csv', file.content)
            n_downs += 1

        except Exception as e:
            errors[url] = str(e)

    elif fig.startswith('Gráfico 8.2'):
        try:
            print(f'A baixar o arquivo da url:\n {url} ...')

            # downloading do arquivo
            xpath = '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div/ul[3]/li[2]/a/@href'
            html = c.get_html(url)
            url_to_get = html.xpath(xpath).get()
            file = c.open_url(url_to_get)

            c.to_file(dir_path, 'anp_producao_gas.csv', file.content)

            xpath = '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div/ul[2]/li[2]/a/@href'
            url_to_get = html.xpath(xpath).get()
            file = c.open_url(url_to_get)

            c.to_file(c.open_url('anp_producao_lgn.csv'))

            n_downs += 1

        except Exception as e:
            errors[url] = str(e)


# ************************
# EXTRAÇÃO DE DADOS POR MÉTODO DE COLETA API
# IBGE CONTAS REGIONAIS, IBGE INDICADORES SOCIAIS, SICONFI TESOURO
# ************************

doc_api = doc.loc[doc['method'] == 'API'].reset_index(drop=True)
api_url = set(doc_api['url'].to_list())

for url in api_url:

    # downloading dos arquivos com fonte: sistema de contas regionais
    if url.endswith('Contas_Regionais'):
        try:
            print(f'A baixar o arquivo da url:\n {url} ...')
            response = c.open_url(url)
            df = pd.DataFrame(response.json())

            # pequisa pela publicação mais recente --> inicia com '2' e possui 4 caracteres
            df = df.loc[(df['name'].str.startswith('2')) &
                        (df['name'].str.len() == 4),
                        ['name', 'path']].sort_values(by='name', ascending=False).reset_index(drop=True)

            # obtém o caminho da publicação mais recente e adiciona à url de acesso aos arquivos
            url_to_get = df['path'][0][-5:] + '/xls'
            response = c.open_url(url + url_to_get)
            df = pd.DataFrame(response.json())
            url_to_get_pib = df.loc[df['name'].str.startswith('PIB_Otica_Renda'), 'url'].values[0]
            url_to_get_esp = df.loc[(df['name'].str.startswith('Especiais_2010')) &
                                    (df['name'].str.endswith('.zip')), 'url'].values[0]

            # downloading e organização do arquivo pib pela ótica da renda
            file = c.open_url(url_to_get_pib)
            c.to_file(dir_path, 'ibge_pib_otica_renda.xls', file.content)

            # downloading e organização do arquivo especiais 2010
            file = c.open_url(url_to_get_esp)
            c.to_file(dir_path, 'ibge_especiais.zip', file.content)
            n_downs += 1

        except Exception as e:
            errors[url] = str(e)

    elif url.endswith('Sintese_de_Indicadores_Sociais'):
        try:
            print(f'A baixar o arquivo da url:\n {url} ...')
            response = c.open_url(url)

            # verifica a última publicação
            url_to_last_pub = url + '/' + response.json()[-2]['path'].split('/')[-1] + '/xls'
            response = c.open_url(url_to_last_pub)

            # coleta o link do arquivo
            url_to_file = response.json()[0]['url']
            file = c.open_url(url_to_file)

            c.to_file(dir_path, 'ibge_indicadores_sociais.zip', file.content)
            n_downs += 1

        except Exception as e:
            errors[url] = str(e)

    elif url.startswith('https://apidatalake.tesouro.gov.br'):
        try:
            print(f'A baixar o arquivo da url:\n {url} ...')

            # definição dos anos de referência
            base_year = 2015
            current_year = datetime.datetime.now().year

            dfs = []
            for y in range(base_year, current_year + 1):

                siconfi_url = f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf?an_exercicio={y}&" \
                              f"in_periodicidade=Q&nr_periodo=3&co_tipo_demonstrativo=RGF&no_anexo=RGF-Anexo%2002&" \
                              f"co_esfera=E&co_poder=E&id_ente=28"

                response = c.open_url(siconfi_url)
                if response.status_code == 200 and len(response.json()['items']) > 1:
                    print(f'A coletar dados do siconfi referentes ao exercício de {y} ...\nNível regional: Sergipe')

                    # seleção das colunas de interesse
                    # seleção do quadrimestre de interesse
                    # seleção das contas de interesse
                    df = pd.DataFrame(response.json()['items'])
                    df = df.loc[:, ['exercicio', 'instituicao', 'uf', 'coluna', 'conta', 'valor']]
                    df = df.loc[df['coluna'] == 'Até o 3º Quadrimestre']
                    df = df.loc[(df['conta'].str.startswith('DÍVIDA CONSOLIDADA LÍQUIDA (DCL)')) |
                                (df['conta'].str.startswith('RECEITA CORRENTE LÍQUIDA - RCL')) |
                                (df['conta'].str.startswith('% da DCL sobre a RCL'))]

                    dfs.append(df)
                else:
                    print(f'Não foram encontrados dados referentes ao exercício de {y}!')

            # união dos dfs
            df_concat = pd.concat(dfs, ignore_index=True)

            df_concat['conta'] = df_concat['conta'].apply(
                lambda x: '% da DCL sobre a RCL' if x.startswith('% da DCL sobre a RCL') else (
                    'DÍVIDA CONSOLIDADA LÍQUIDA' if x.startswith('DÍVIDA CONSOLIDADA LÍQUIDA') else
                    'RECEITA CORRENTE LÍQUIDA'))

            # ordenação dos valores
            df_concat.sort_values(by=['uf', 'exercicio', 'conta'], ascending=[True] * 3, inplace=True)

            # classificação dos dados
            df_concat[df_concat.columns[0]] = pd.to_datetime(df_concat[df_concat.columns[0]], format='%Y')
            df_concat[['conta', 'uf']] = df_concat[['conta', 'uf']].astype('str')
            df_concat['valor'] = df_concat['valor'].astype('float')

            # conversão em arquivo csv
            c.to_csv(df_concat, dir_path, 'grafico_11-11_siconfi_tesouro.csv')
            n_downs += 1

        except Exception as e:
            errors[url] = str(e)


# ************************
# EXTRAÇÃO DE DADOS POR MÉTODO DE COLETA API
# TABELAS SIDRA
# ************************

# informações das figuras para o looping de requisições
sidra_figures = {
    'figure': ['grafico_4-2', 'grafico_5-4', 'grafico_13-6', 'tabela_13-2'],
    'table': ['5603', '1761', '6402', '5434'],
    'variable': ['631,706', '631,1243', '4099', '4090,4108'],
    'class': ['', '', '86', '888'],
    'class_val': ['', '', '95251', '47947,47948,47949,47950,56622,56623,56624,60032']
}

# códigos nos níveis regionais
# regions = [('1', 'all'), ('2', '2'), ('3', '28')]

df_sidra = pd.DataFrame(sidra_figures)

# looping de requisições para cada figura
for i in df_sidra.index:
    print(f'A baixar tabelas do(a) {df_sidra["figure"][i]}')
    # looping das figuras que não dispõem de classificação
    if df_sidra['class'][i] == '':
        try:
            regions = [('3', '28')]
            dfs = []
            # looping de requisições para cada tabela da figura
            for reg in regions:
                data = sidrapy.get_table(table_code=df_sidra['table'][i], territorial_level=reg[0],
                                         ibge_territorial_code=reg[1],
                                         variable=df_sidra['variable'][i], period="all")

                # remoção da linha 0, dados para serem usados como rótulos das colunas
                # não foram usados porque variam de acordo com a tabela
                # seleção das colunas de interesse
                data.drop(0, axis='index', inplace=True)
                data = data[['D1N', 'D2N', 'D3N', 'V']]
                dfs.append(data)

                # acrescenta delay às requests
                c.delay_requests(2)

            # união dos dfs
            # renomeação das colunas
            # filtragem de dados a partir do ano 2010
            df_concat = pd.concat(dfs, ignore_index=True)
            df_concat.columns = ['Região', 'Ano', 'Variável', 'Valor']
            df_concat = df_concat.loc[df_concat['Ano'] >= '2010']

            # classificação dos dados
            df_concat[['Região', 'Variável']] = df_concat[['Região', 'Variável']].astype('str')
            df_concat['Ano'] = pd.to_datetime(df_concat['Ano'], format='%Y')
            df_concat['Valor'] = df_concat['Valor'].astype('int64')

            # adicionado após conversa
            df_concat = df_concat.pivot(index=['Região', 'Ano'], columns='Variável', values='Valor').reset_index()
            df_concat.index.name = None

            # conversão em arquivo csv
            c.to_csv(df_concat, dir_path, df_sidra['figure'][i] + f'_sidra_tb{df_sidra["table"][i]}.csv')
            n_downs += 1

        except Exception as e:
            errors[f'Tabela {df_sidra["table"][i]}'] = str(e)

    else:
        # looping das figuras que dispõem de classificação
        if df_sidra['figure'][i] != 'tabela_13-2':
            try:
                regions = [('1', 'all'), ('2', '2'), ('3', '28')]
                dfs = []
                for reg in regions:
                    data = sidrapy.get_table(table_code=df_sidra['table'][i], territorial_level=reg[0],
                                             ibge_territorial_code=reg[1],
                                             variable=df_sidra['variable'][i],
                                             classifications={df_sidra['class'][i]: df_sidra['class_val'][i]},
                                             period="all")

                    # remoção da linha 0, dados para serem usados como rótulos das colunas
                    # não foram usados porque variam de acordo com a tabela
                    # seleção das colunas de interesse
                    data.drop(0, axis='index', inplace=True)
                    data = data[['D1N', 'D2N', 'D3N', 'D4N', 'V']]
                    dfs.append(data)

                    # acrescenta delay às requests
                    c.delay_requests(2)

                # união dos dfs
                # renomeação das colunas
                # filtragem de dados referentes ao 4º trimestre de cada ano
                # seleção dos dígitos referentes ao ano
                df_concat = pd.concat(dfs, ignore_index=True)
                df_concat.columns = ['Região', 'Ano', 'Variável', 'Classe', 'Valor']
                df_concat = df_concat.loc[df_concat['Ano'].str.startswith('4º trimestre')]
                df_concat['Ano'] = df_concat['Ano'].apply(lambda x: x[-4:])

                # classificação dos dados
                df_concat[df_concat.columns[:-1]] = df_concat[df_concat.columns[:-1]].astype('str')
                df_concat['Valor'] = df_concat['Valor'].replace('...', '0.0')
                df_concat['Valor'] = df_concat['Valor'].astype('float64')
                df_concat['Ano'] = pd.to_datetime(df_concat['Ano'], format='%Y')

                # conversão em arquivo csv
                c.to_csv(df_concat, dir_path, df_sidra['figure'][i] + f'_sidra_tb{df_sidra["table"][i]}.csv')
                n_downs += 1

            except Exception as e:
                errors[f'Tabela {df_sidra["table"][i]}'] = str(e)

        else:
            try:
                regions = [('3', '28')]
                dfs = []
                for reg in regions:
                    data = sidrapy.get_table(table_code=df_sidra['table'][i], territorial_level=reg[0],
                                             ibge_territorial_code=reg[1], variable=df_sidra['variable'][i],
                                             classifications={df_sidra['class'][i]: df_sidra['class_val'][i]},
                                             period="all")

                    # remoção da linha 0, dados para serem usados como rótulos das colunas
                    # não foram usados porque variam de acordo com a tabela
                    # seleção das colunas de interesse
                    data.drop(0, axis='index', inplace=True)
                    data = data[['MN', 'D1N', 'D2N', 'D3N', 'D4N', 'V']]

                    # separação de valores; valores inteiros e percentuais estão armazenados na mesma coluna
                    data_ab = data.loc[data['MN'] == 'Mil pessoas']
                    data_per = data.loc[data['MN'] == '%']
                    data = data_ab.iloc[:, 1:]
                    data['Percentual'] = data_per.loc[:, 'V'].to_list()
                    dfs.append(data)

                    # acrescenta delay às requests
                    c.delay_requests(2)

                # união dos dfs
                # renomeação das colunas
                # filtragem de dados referentes ao 4º trimestre de cada ano
                df_concat = pd.concat(dfs, ignore_index=True)
                df_concat.columns = ['Região', 'Ano', 'Variável', 'Atividade', 'Valor', 'Percentual']
                df_concat = df_concat.loc[df_concat['Ano'].str.startswith('4º trimestre')]
                df_concat['Ano'] = df_concat['Ano'].apply(lambda x: x[-4:])
                df_concat.drop('Variável', axis='columns', inplace=True)

                # classificação dos dados
                df_concat[df_concat.columns[:-2]] = df_concat[df_concat.columns[:-2]].astype('str')
                df_concat[df_concat.columns[-2:]] = df_concat[df_concat.columns[-2:]].astype('float64')
                df_concat['Ano'] = pd.to_datetime(df_concat['Ano'], format='%Y')

                # conversão em arquivo csv
                c.to_csv(df_concat, dir_path, df_sidra['figure'][i] + f'_sidra_tb{df_sidra["table"][i]}.csv')
                n_downs += 1

            except Exception as e:
                errors[f"Tabela {df_sidra['table'][i]}"] = str(e)

# ************************
# TABELA 17.2
# ************************

# download da última publicação
url = 'https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados'
try:
    print(f'A baixar o arquivo da url:\n {url} ...')

    # acesso ao site com a aplicação selenium
    options = webdriver.ChromeOptions()
    prefs = {'download.default_directory': dir_path}
    options.add_experimental_option('prefs', prefs)
    options.add_argument('--headless=new')

    browser = webdriver.Chrome(options=options)
    browser.get(url)

    # encontra o botão de login e o fecha
    login_btn = browser.find_element(By.ID, 'govbr-login-overlay-wrapper')
    browser.implicitly_wait(2)
    ActionChains(browser).move_to_element(login_btn).click(login_btn).perform()
    browser.implicitly_wait(2)

    # encontra o botão dos cookies e aceita
    cookies_btn = browser.find_element(By.XPATH, '/html/body/div[5]/div/div/div/div/div[2]/button[2]')
    browser.implicitly_wait(2)
    cookies_btn.click()

    # encontra o ano da última publicação
    year_element = browser.find_element(By.XPATH,
                                        '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[1]/div[1]/div[1]')
    last_year = year_element.text

    # encontra o elemento que armazena o link de download do arquivo com dados a nível estadual e macrorregional
    link_element = browser.find_element(By.XPATH,
                                        '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div[1]/div/div['
                                        '2]/div/div/div/div[2]/ul/li/a[1]')
    link = link_element.get_attribute('href')
    browser.implicitly_wait(2)

    # baixa o arquivo; sergipe e nordeste
    browser.get(link)
    sleep(3)

    # altera para a página das publicações anteriores
    next_link_element = browser.find_element(By.XPATH, '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[1]/div['
                                                       '1]/div[2]/a')
    browser.implicitly_wait(2)
    ActionChains(browser).move_to_element(next_link_element).click(next_link_element).perform()

    # encontra o elemento que armazena o link de download do arquivo com dados a nível estadual e macrorregional
    link_element = browser.find_element(By.XPATH,
                                        '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div[2]/div/div['
                                        '2]/div/div/div/div[2]/ul/li/a')
    link = link_element.get_attribute('href')
    browser.implicitly_wait(2)

    # baixa o arquivo; sergipe e nordeste
    browser.get(link)
    sleep(3)
    browser.quit()
    n_downs += 1

except Exception as e:
    errors[url] = str(e)

# ************************
# GRÁFICO 18.6
# ************************

url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/nruf.def'
try:
    print(f'A baixar o arquivo da url:\n {url} ...')

    # configurações do driver
    options = webdriver.ChromeOptions()
    prefs = {'download.default_directory': dir_path}
    options.add_experimental_option('prefs', prefs)
    options.add_argument('--headless=new')

    browser = webdriver.Chrome(options=options)
    browser.get(url)
    browser.implicitly_wait(2)

    # seleção de botões e elementos da páginas
    lin_btn = browser.find_element(By.XPATH, '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]')
    ActionChains(browser).move_to_element(lin_btn).click(lin_btn).perform()
    browser.implicitly_wait(2)

    col_btn = browser.find_element(By.XPATH, '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[7]')
    ActionChains(browser).move_to_element(col_btn).click(col_btn).perform()
    browser.implicitly_wait(2)

    cont_btn = browser.find_element(By.XPATH, '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[15]')
    ActionChains(browser).move_to_element(cont_btn).click(cont_btn).perform()
    browser.implicitly_wait(2)

    per_btn_1 = browser.find_element(By.XPATH, '/html/body/div/div/center/div/form/div[3]/div/select/option[1]')
    ActionChains(browser).move_to_element(per_btn_1).click(per_btn_1).perform()
    browser.implicitly_wait(2)

    per_btn_2 = browser.find_element(By.XPATH, '/html/body/div/div/center/div/form/div[3]/div/select/option[189]')
    ActionChains(browser).key_down(Keys.SHIFT).click(per_btn_2).key_up(Keys.SHIFT).perform()
    browser.implicitly_wait(2)

    option_btn = browser.find_element(By.XPATH,
                                      '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]')
    ActionChains(browser).move_to_element(option_btn).click(option_btn).perform()
    browser.implicitly_wait(2)

    show_btn = browser.find_element(By.XPATH, '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')
    ActionChains(browser).move_to_element(show_btn).click(show_btn).perform()
    browser.implicitly_wait(2)

    windows = browser.window_handles
    browser.switch_to.window(windows[-1])
    print('A esperar o carregamento da página ...')
    download_btn = WebDriverWait(browser, 600).until(
        EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div[3]/table/tbody/tr/td[1]/a'))
    )
    print('Página carregada!')
    ActionChains(browser).move_to_element(download_btn).click(download_btn).perform()
    time.sleep(3)
    browser.quit()
    n_downs += 1

except Exception as e:
    errors[url] = str(e)

# ************************
# GRÁFICO 19.2
# ************************

url = 'https://forumseguranca.org.br/anuario-brasileiro-seguranca-publica/'
try:
    print(f'A baixar o arquivo da url:\n {url} ...')

    html = c.get_html(url)
    to_get = html.xpath('/html/body/div[1]/section[3]/div/div[1]/div/section[2]/div/div/div/section/div/div['
                        '2]/div/div/div/a/@href').get()
    file = c.open_url(to_get)

    c.to_file(dir_path, 'anuario_seguranca_publica.xls', file.content)
    n_downs += 1

except Exception as e:
    errors[url] = str(e)

# ************************
# GRÁFICO 19.12
# ************************

url = 'https://dados.mj.gov.br/dataset/sistema-nacional-de-estatisticas-de-seguranca-publica/resource/feeae05e-faba' \
      '-406c-8a4a-512aec91a9d1'
try:
    print(f'A baixar o arquivo da url:\n {url} ...')

    html = c.get_html(url)
    to_get = html.xpath('/html/body/div[2]/div/div[3]/section/div[1]/p/a/@href').get()
    file = c.open_url(to_get)

    c.to_file(dir_path, 'sinesp_ocorrencias_criminais.xls', file.content)
    n_downs += 1

except Exception as e:
    errors[url] = str(e)


# ************************
# TABELA 19.2
# ************************

url = 'https://forumseguranca.org.br/anuario-brasileiro-seguranca-publica/'
try:
    print(f'A baixar o arquivo da url:\n {url} ...')

    html = c.get_html(url)
    to_get = html.xpath('/html/body/div[1]/section[3]/div/div[1]/div/section[2]/div/div/div/section/div/div['
                        '2]/div/div/div/a/@href').get()
    file = c.open_url(to_get)

    c.to_file(dir_path, 'anuario_seguranca_publica.xls', file.content)
    n_downs += 1

except Exception as e:
    errors[url] = str(e)

if errors:
    try:
        with open(os.path.join(dir_name, 'errors.json'), 'w', encoding='utf-8') as f:
            f.write(json.dumps(errors, indent=4, ensure_ascii=False))
    except Exception as e:
        print(e)
else:
    print('Erros: ', errors)

print(f'Arquivos baixados: {n_downs}')

# # ************************
# # UPLOAD DE ARQUIVOS PARA REPOSITÓRIO NO GITHUB
# # ************************
#
# # definição do horário para registro de upload ou update dos arquivos
# now = datetime.datetime.now().strftime('%d/%m/%Y, %H:%M:%S')
#
# # caminhos de diretórios
# repo_path = 'anuariosocieconomico/T25'
# data_git_path = 'data'
# script_git_path = 'script'
# doc_git_path = 'doc'
#
# # inicialização do repositório
# auth = Auth.Token(c.git_token)
# g = Github(auth=auth)
# repo = g.get_repo(repo_path)
# contents = repo.get_contents('')
#
# # diretórios no git
# git_cont = []
# for content_file in contents:
#     git_cont.append(content_file.path)
#
# # verifica se há a pasta 'data' no diretório
# if 'data' not in git_cont:
#     print('Pasta não encontrada. A criar nova pasta no diretório ...')
#
#     # upload dos arquivos csv
#     csv_folder = 'to_github/data'
#     for csv_file in os.listdir(csv_folder):
#         csv_path = os.path.join(csv_folder, csv_file)
#
#         if os.path.isfile(csv_path):
#             with open(csv_path, 'r', encoding='utf-8') as file:
#                 csv_content = file.read()
#
#             repo.create_file(f'data/{csv_file}', f'Arquivo criado em {now}.', csv_content)
#             print(f'Arquivo {csv_file} criado no novo diretório.')
#             sleep(1)
# else:
#     print('\nPasta encontrada. A atualizar os arquivos no diretório ...')
#
#     # update dos arquivos csv
#     my_folder = repo.get_contents(data_git_path)
#     csv_folder = 'to_github/data'
#     for csv_file in os.listdir(csv_folder):
#         csv_path = os.path.join(csv_folder, csv_file)
#
#         if os.path.isfile(csv_path):
#             with open(csv_path, 'r', encoding='utf-8') as file:
#                 csv_content = file.read()
#
#             file_in_folder = next((csv_f for csv_f in my_folder if csv_f.name == csv_file), None)
#
#             # verifica se se arquivo local já existe no diretório, para definir se deve criá-lo ou atualizá-lo
#             if file_in_folder:
#                 repo.update_file(f'data/{csv_file}', f'Arquivo atualizado em {now}.',
#                                  csv_content, file_in_folder.sha)
#                 print(f'Arquivo {csv_file} atualizado no novo diretório.')
#                 sleep(1)
#             else:
#                 repo.create_file(f'data/{csv_file}', f'Arquivo criado em {now}.', csv_content)
#                 sleep(1)
#
# if 'script' not in git_cont:
#     # upload do script
#     script_path = 'get_data.py'
#     with open(script_path, 'r', encoding='utf-8') as f:
#         text = f.read()
#
#     repo.create_file('script/script.txt', f'Arquivo criado em {now}', text)
#     sleep(1)
# else:
#     # update do script
#     my_folder = repo.get_contents(script_git_path)
#     script_path = 'get_data.py'
#     with open(script_path, 'r', encoding='utf-8') as f:
#         text = f.read()
#
#     file_in_folder = next((script for script in my_folder if script.name == 'script.txt'), None)
#
#     if file_in_folder:
#         repo.update_file('script/script.txt', f'Arquivo atualizado em {now}', text, file_in_folder.sha)
#         print('Script atualizado no diretório.')
#         sleep(1)
#     else:
#         print('script não atualizado no diretório.')
#
# if 'doc' not in git_cont:
#     doc_path = 'support_files/documentação.json'
#     with open(doc_path, 'r', encoding='utf-8') as f:
#         data = json.load(f)
#     repo.create_file('doc/documentação.txt', f'Arquivo criado em {now}.',
#                      json.dumps(data, indent=4, ensure_ascii=False))
#     sleep(1)
# else:
#     my_folder = repo.get_contents(doc_git_path)
#     doc_path = 'support_files/documentação.json'
#     with open(doc_path, 'r', encoding='utf-8') as f:
#         data = json.load(f)
#
#     file_in_folder = next((doc_f for doc_f in my_folder if doc_f.name == 'documentação.json'), None)
#     if file_in_folder:
#         repo.update_file(f'doc/documentação.json', f'Arquivo atualizado em {now}.',
#                          json.dumps(data, indent=4, ensure_ascii=False), file_in_folder.sha)
#         print('Documentação atualizada no diretório.')
#         sleep(1)
#     else:
#         print('Documentação não atualizada no diretório.')
#
# g.close()
