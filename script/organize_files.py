# manipulação dos dados
import shutil

import pandas as pd
import numpy as np
import re
import json

# sistema
import os
import constants as c
import datetime
from time import sleep

# github
from github import Github
from github import Auth

git_doc_path = 'support_files/doc-links.json'
script_path = os.getcwd()
dir_name = c.dir_name2
dir_path = os.path.join(script_path, dir_name)
ideb = ''

errors = {}
n_figs = 0

# criação do diretório
if not os.path.exists(dir_path):
    os.mkdir(dir_path)


# coleta de informações sobre os gráficos
doc = pd.read_json(git_doc_path)
doc_scraping = doc.loc[doc['method'] == 'scraping'].reset_index(drop=True)

for csv_name in os.listdir(os.path.join(script_path, c.dir_name)):
    if csv_name.startswith('epe'):
        try:
            print('A organizar o arquivo da figura: Gráfico 7.1 ...')
            # organização do arquivo
            # seleção das colunas e das linhas de interesse
            df = c.open_file(os.path.join(script_path, c.dir_name), csv_name,
                             'xls', sheet_name='Tabela 3.63', skiprows=8)
            df = df.iloc[:, 1:12]
            df = df.loc[df[' '].isin(['Brasil', 'Nordeste', 'Sergipe'])]

            # reordenação da variável ano para o eixo y
            # renomeação do rótulo da coluna de ' ' para 'Região'
            # ordenação alfabética da coluna 'Região' e cronológica da coluna 'Ano'
            df_melted = df.melt(id_vars=[' '], value_vars=[str(y) for y in np.arange(2013, 2023)], var_name=['Ano'],
                                value_name='Valor')
            df_melted.rename(columns={' ': 'Região'}, inplace=True)
            df_melted.sort_values(by=['Região', 'Ano'], ascending=[True, True], inplace=True)

            # classificação dos valores em cada coluna
            df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
            df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]], format='%Y')
            df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('float64')

            # tratamento do título da figura para nomeação do arquivo csv
            c.to_excel(df_melted, dir_path, 'g7.1.xlsx')
            n_figs += 1

        except Exception as e:
            errors['Gráfico 7.1'] = str(e)

    elif csv_name.endswith('petroleo.csv'):
        try:
            print('A organizar o arquivo da figura: Gráfico 8.1 ...')
            # organização do arquivo
            # dados sobre sergipe
            # remoção de variáveis não utilizáveis
            # adição da identificação da região
            df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'csv', sep=';')
            df_se = df.loc[df['UNIDADE DA FEDERAÇÃO'] == 'SERGIPE']
            df_se = df_se.drop(['GRANDE REGIÃO', 'UNIDADE DA FEDERAÇÃO', 'PRODUTO'], axis='columns')
            df_se['REGIÃO'] = 'SERGIPE'

            # adicionado após comentário das linhas acima
            month_mapping = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
                             'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
            df_concat = df_se
            df_concat['month'] = df_concat['MÊS'].str.lower().map(month_mapping)
            df_concat['date'] = df_concat['ANO'].astype('str') + '-' + df_concat['month'].astype('str')
            df_concat['ANO'] = df_concat['date']
            df_concat.drop(['month', 'date', 'MÊS'], axis='columns', inplace=True)

            # classificação dos dados
            df_concat[df_concat.columns[0]] = pd.to_datetime(df_concat[df_concat.columns[0]], format='%Y-%m')
            df_concat[df_concat.columns[1]] = df_concat[df_concat.columns[1]].astype('str')
            df_concat[df_concat.columns[2]] = df_concat[df_concat.columns[2]].astype('float64')
            df_concat[df_concat.columns[-1]] = df_concat[df_concat.columns[-1]].astype('str')

            # tratamento do título da figura para nomeação do arquivo csv
            c.to_excel(df_concat, dir_path, 'g8.1.xlsx')
            n_figs += 1

        except Exception as e:
            errors['Gráfico 8.1'] = str(e)

    elif csv_name.endswith('gas.csv'):
        try:
            print('A organizar o arquivo da figura: Gráfico 8.2 ...')
            # abertura arquivo 1
            # seleção das variáveis de interesse
            # adição da variável 'REGIÃO'
            df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'csv', sep=';')
            df_se = df.loc[df['UNIDADE DA FEDERAÇÃO'] == 'SERGIPE']
            df_se = df_se.groupby(['ANO', 'MÊS', 'LOCALIZAÇÃO', 'PRODUTO'])['PRODUÇÃO'].sum().reset_index()
            df_se['REGIÃO'] = 'SERGIPE'
            df_gas = df_se

            # abertura arquivo 2
            # seleção das variáveis de interesse
            # adição da variável 'LOCALIZAÇÃO'
            # adição da variável 'REGIÃO'
            df = c.open_file(os.path.join(script_path, c.dir_name), 'anp_producao_lgn.csv', 'csv', sep=';')
            df_se = df.loc[df['UNIDADE DA FEDERAÇÃO'] == 'SERGIPE']
            df_se = df_se.groupby(['ANO', 'MÊS', 'PRODUTO'])['PRODUÇÃO'].sum().reset_index()
            df_se['LOCALIZAÇÃO'] = 'NÃO SE APLICA'
            df_se['REGIÃO'] = 'SERGIPE'

            df_lgn = df_se
            df_lgn = df_lgn[['ANO', 'MÊS', 'LOCALIZAÇÃO', 'PRODUTO', 'PRODUÇÃO', 'REGIÃO']]

            # união das tabelas de ambos os arquivos
            # classificação dos dados
            df_concat = pd.concat([df_gas, df_lgn], ignore_index=True)

            # adicionado após comentários acima
            month_mapping = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
                             'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
            df_concat['month'] = df_concat['MÊS'].str.lower().map(month_mapping)
            df_concat['date'] = df_concat['ANO'].astype('str') + '-' + df_concat['month'].astype('str')
            df_concat['ANO'] = df_concat['date']
            df_concat.drop(['month', 'date', 'MÊS'], axis='columns', inplace=True)

            df_concat[df_concat.columns[0]] = pd.to_datetime(df_concat[df_concat.columns[0]], format='%Y-%m')
            df_concat[df_concat.columns[1:3]] = df_concat[df_concat.columns[1:3]].astype('str')
            df_concat[df_concat.columns[-2]] = df_concat[df_concat.columns[-2]].astype('float64')
            df_concat[df_concat.columns[-1]] = df_concat[df_concat.columns[-1]].astype('str')

            # tratamento do título da figura para nomeação do arquivo
            # conversão em arquivo csv
            c.to_excel(df_concat, dir_path, 'g8.2.xlsx')
            n_figs += 1

        except Exception as e:
            errors['Gráfico 8.2'] = str(e)

    elif csv_name.endswith('otica_renda.xls'):
        df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'xls', skiprows=8)
        tables = ['Tabela1', 'Tabela10', 'Tabela18']

        mapping = {
            'grafico_1-5': 'Salários',
            'grafico_1-6': 'Contribuição social',
            'grafico_1-7': 'Impostos sobre produto, líquidos de subsídios',
            'grafico_1-8': 'Excedente Operacional Bruto (EOB) e Rendimento Misto (RM)'
        }

        # seleção das tabelas e componentes de interesse
        for k, v in mapping.items():
            try:
                print(f'A organizar o arquivo da figura: {k} ...')
                dfs = []
                for tb in tables:
                    # seleção de linhas não vazias
                    # renomeação da coluna
                    df_tb = df[tb]
                    df_tb = df_tb.iloc[:9]
                    df_tb = df_tb.rename(columns={'Unnamed: 0': 'Componente'})

                    # reordenação da variável ano para o eixo y
                    # seleção das linhas e das colunas de interesse
                    df_melted = pd.melt(df_tb, id_vars=['Componente'], value_vars=df_tb.columns[1:], var_name=['Ano'],
                                        value_name='Valor')
                    df_melted = df_melted.loc[(df_melted['Componente'] == v) &
                                              (df_melted['Ano'].str.endswith('.1'))]

                    # remoção do ".1" ao final dos valores de Ano
                    # decorrentes da ordenação padrão das variáveis Ano como colunas
                    df_melted.loc[:, 'Ano'] = df_melted.loc[:, 'Ano'].apply(lambda x: x[:-2])

                    # adição da variável região
                    df_melted['Região'] = 'Brasil' if tb.endswith('1') else (
                        'Nordeste' if tb.endswith('10') else 'Sergipe')

                    # classificação dos dados
                    df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
                    df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]])
                    df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('float64')
                    df_melted[df_melted.columns[3]] = df_melted[df_melted.columns[3]].astype('str')

                    dfs.append(df_melted)

                # conversão para arquivo csv
                df_concat = pd.concat(dfs, ignore_index=True)
                c.to_excel(df_concat, dir_path, k[0] + k.split('_')[1].replace('-', '.') + '.xlsx')
                n_figs += 1

            except Exception as e:
                g = k.split('_')[0].capitalize()
                g = g.replace('a', 'á')
                n = k.split('_')[1]
                n = n.replace('-', '.')
                errors[g + ' ' + n] = str(e)

    elif csv_name.endswith('especiais.zip'):
        try:
            print('A organizar o arquivo da figura: Tabela 1.1 ...')
            df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'zip', excel_name='tab07.xls', skiprows=4)
            # seleção das tabelas de interesse
            tables = ['Tabela7.1', 'Tabela7.10', 'Tabela7.18']
            dfs = []
            for tb in tables:
                df_tb = df[tb]

                # renomeação do coluna de 'Unnamed: 0' para 'Atividade'
                # seleção das linhas de interesse
                df_tb.rename(columns={'Unnamed: 0': 'Atividade'}, inplace=True)
                df_tb = df_tb.iloc[2:23]

                # reordenação da variável ano para o eixo y
                df_melted = pd.melt(df_tb, id_vars=['Atividade'], value_vars=[y for y in np.arange(2010, 2021)],
                                    var_name=['Ano'], value_name='Valor')

                # classificação dos dados por setor econômico

                '''
                Originalmente, os dados referentes ao setor e às atividades estão armazenados na mesma coluna.
                A cada ocorrência do valor 'Agropecuária', por exemplo, será coletado o seu índice,
                ponto de início da seleção dos valores.
                O índice, então, é somado à quantia de atividades deste setor (neste caso 3) mais 1,
                ponto de término da seleção dos valores.
                A mesma operação é realizada para os outros dois setores econômicos.
                '''

                df_melted['Setor'] = ''

                agro_index = df_melted.loc[df_melted['Atividade'] == 'Agropecuária'].index
                ind_index = df_melted.loc[df_melted['Atividade'] == 'Indústria'].index
                serv_index = df_melted.loc[df_melted['Atividade'] == 'Serviços'].index

                for agro, ind, serv in zip(agro_index, ind_index, serv_index):
                    df_melted.iloc[agro:agro + 4, -1] = 'Agropecuária'
                    df_melted.iloc[ind:ind + 5, -1] = 'Indústria'
                    df_melted.iloc[serv:serv + 12, -1] = 'Serviços'

                # remoção dos valores referentes ao setor da coluna 'Atividade'
                # adição da variável região
                df_melted.drop(list(agro_index) + list(ind_index) + list(serv_index), axis='index', inplace=True)
                df_melted['Região'] = 'Brasil' if tb.endswith('7.1') else (
                    'Nordeste' if tb.endswith('7.10') else 'Sergipe')

                dfs.append(df_melted)

            # união dos dfs
            # reordenação das colunas
            df_concat = pd.concat(dfs, ignore_index=True)
            df_concat = df_concat[['Região', 'Setor', 'Atividade', 'Ano', 'Valor']]

            # classificação dos dados
            df_concat[df_concat.columns[0:3]] = df_concat[df_concat.columns[0:3]].astype('str')
            df_concat[df_concat.columns[-2]] = pd.to_datetime(df_concat[df_concat.columns[-2]], format='%Y')
            df_concat[df_concat.columns[-1]] = df_concat[df_concat.columns[-1]].astype('float64')

            # conversão em arquivo csv
            c.to_excel(df_concat, dir_path, 't1.1.xlsx')
            n_figs += 1

        except Exception as e:
            errors['Tabela 1.1'] = str(e)

    elif csv_name.endswith('indicadores_sociais.zip'):
        try:
            print('A organizar o arquivo da figura: Gráfico 13.8 ...')
            # seleção das abas de interesse
            df = c.open_file(os.path.join(script_path, c.dir_name), csv_name,
                             'zip', excel_name='Tabela 1.40', skiprows=6)
            # seleção das abas de interesse
            tables = []
            for table in df.keys():
                if not table.endswith('(CV)'):
                    tables.append(table)

            # união das tabelas de interesse
            dfs = []
            for tb in tables:
                # seleção das linhas e colunas de interesse
                # renomeação da coluna de 'Unnamed: 0' para 'Região' e remoção do .1 dos rótulos das colunas,
                # decorrentes da repetição de valores como rótulos
                df_tb = df[tb]
                df_tb = df_tb.iloc[2:35, [0] + list(np.arange(5, 9))]
                df_tb.columns = ['Região'] + [col[:-2] for col in df_tb.columns if col != 'Unnamed: 0']

                # reordenação da variável ocupação para o eixo y
                df_melted = pd.melt(df_tb, id_vars=['Região'], value_vars=df_tb.columns[1:],
                                    var_name=['Ocupação'], value_name='Valor')

                # dados sergipe, nordeste e brasil
                # ordenação dos valores
                # adição da variável ano
                # df_melted = df_melted.loc[df_melted['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])]
                df_melted = df_melted.loc[df_melted['Região'] == 'Sergipe']
                df_melted = df_melted.sort_values(by=['Região', 'Ocupação'], ascending=[True] * 2)
                df_melted['Ano'] = pd.to_datetime(tb, format='%Y')

                dfs.append(df_melted)

            # união dos dfs
            # ordenação dos valores a partir das variáveis 'Região', 'Ano' e 'Ocupação'
            # classificação dos dados
            df_concat = pd.concat(dfs, ignore_index=True)
            df_concat.sort_values(by=['Região', 'Ano', 'Ocupação'], ascending=[True] * 3, inplace=True)
            df_concat[df_concat.columns[:2]] = df_concat[df_concat.columns[:2]].astype('str')
            df_concat[df_concat.columns[-2]] = df_concat[df_concat.columns[-2]].astype('float64')

            # conversão em arquivo csv
            c.to_excel(df_concat, dir_path, 'g13.8.xlsx')
            n_figs += 1

        except Exception as e:
            errors['Gráfico 13.8'] = str(e)

    elif 'ideb' in csv_name and ideb != 'OK':
        try:
            print('A organizar o arquivo da figura: Tabela 17.2 ...')
            ideb_files = [f for f in os.listdir(os.path.join(script_path, c.dir_name)) if 'ideb' in f]
            ideb_files.sort(reverse=True)

            df = c.open_file(os.path.join(script_path, c.dir_name), ideb_files[1],
                             'zip', excel_name='divulgacao_regioes_ufs_ideb', skiprows=9)
            dfs_old = []
            for i, tb in enumerate(df.keys()):
                # seleção das linhas e colunas de interesse
                # indicação da série com base na aba aberta
                df_tb = df[tb]
                df_tb = df_tb.loc[df_tb[df_tb.columns[0]] == 'Sergipe', df_tb.iloc[:, [0, 1] + list(
                    np.arange(-16, -0))].columns]

                # renomeação das colunas para indicação do ano a que se refere o dado, para posterior tratamento
                # índice IDEB
                base_year = 2005
                for col in df_tb[df_tb.columns[-17:-9]].columns:
                    df_tb.rename(columns={col: 'IDEB ' + str(base_year)}, inplace=True)
                    base_year += 2
                # projeções
                base_year = 2007
                for col in df_tb[df_tb.columns[-9:-1]].columns:
                    df_tb.rename(columns={col: 'Projeção ' + str(base_year)}, inplace=True)
                    base_year += 2

                # reorganização da tabela
                df_melted = pd.melt(df_tb, id_vars=list(df_tb.iloc[:, [0, 1, -1]].columns),
                                    value_vars=list(df_tb.columns[2:-1]), var_name=['var'], value_name='val')
                df_melted['Classe'] = df_melted['var'].apply(lambda x: x[:-5])
                df_melted['Ano'] = df_melted['var'].apply(lambda x: x[-4:])
                df_melted.drop('var', axis='columns', inplace=True)

                # renomeação das colunas
                # reordenação das colunas
                df_melted.columns = ['Região', 'Rede', 'Série', 'Valor', 'Classe', 'Ano']
                df_melted = df_melted[['Ano', 'Região', 'Série', 'Rede', 'Classe', 'Valor']]

                dfs_old.append(df_melted)

            df_old = pd.concat(dfs_old, ignore_index=True)
            year = df_old['Ano'].to_list()
            year.sort(reverse=True)

            df = c.open_file(os.path.join(script_path, c.dir_name), ideb_files[0],
                             'zip', excel_name='divulgacao_regioes_ufs_ideb', skiprows=9)
            dfs_last = []
            for i, tb in enumerate(df.keys()):
                # seleção das linhas e colunas de interesse
                df_tb = df[tb]
                df_tb = df_tb.loc[df_tb[df_tb.columns[0]] == 'Sergipe',
                                  df_tb.iloc[:, [0, 1, -1]].columns]

                # indicação da série com base na aba aberta
                # indicação do ano com base no valor da última publicação
                # indicação do tipo do dado; as últimas publicações dispõem apenas de dados, não há projeções
                df_tb['Série'] = 'Fundamental - Anos Iniciais' if i == 0 else (
                    'Fundamental - Anos Finais' if i == 1 else 'Ensino Médio')
                df_tb['Ano'] = str(int(year[0]) + 2)
                df_tb['Classe'] = 'IDEB'

                # renomeação das colunas
                # reordenação das colunas
                df_tb.columns = ['Região', 'Rede', 'Valor', 'Série', 'Ano', 'Classe']
                df_tb = df_tb[['Ano', 'Região', 'Série', 'Rede', 'Classe', 'Valor']]

                dfs_last.append(df_tb)

            df_last = pd.concat(dfs_last, ignore_index=True)

            # união de todas as publicações
            df_united = pd.concat([df_last, df_old], ignore_index=True)
            df_united.sort_values(by=['Ano', 'Região', 'Classe', 'Série'], ascending=[True] * 4, inplace=True)

            # classificação dos dados
            df_united[df_united.columns[0]] = pd.to_datetime(df_united[df_united.columns[0]], format='%Y')
            df_united[df_united.columns[1:-1]] = df_united[df_united.columns[1:-1]].astype('str')
            df_united[df_united.columns[-1]] = df_united[df_united.columns[-1]].astype('float64')

            # coversão em arquivo csv
            c.to_excel(df_united, dir_path, 't17.2.xlsx')
            n_figs += 1
            ideb = 'OK'

        except Exception as e:
            errors['Tabela 17.2'] = str(e)

    elif csv_name.startswith('sih_cnv'):
        try:
            print('A organizar o arquivo da figura: Gráfico 18.6 ...')
            df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'csv',
                             sep=';', encoding='cp1252', skiprows=3)

            df_melted = df.melt(id_vars=[df.columns[0]], value_vars=list(df.columns[1:-1]), var_name=['Ano'],
                                value_name='Valor')
            df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].str.replace(r'^\.\. ', '', regex=True)

            df_melted = df_melted.loc[df_melted[df_melted.columns[0]].isin(['Total', 'Região Nordeste', 'Sergipe'])]
            df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].replace('Total', 'Brasil')

            df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
            df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]], format='%Y')
            df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('float64')

            c.to_excel(df_melted, dir_name, 'g18.6.xlsx')
            n_figs += 1

        except Exception as e:
            errors['Tabela 18.6'] = str(e)

    elif csv_name.startswith('anuario_seguranca_publica'):
        try:
            print('A organizar o arquivo da figura: Gráfico 19.2 ...')
            df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'xls', sheet_name='T03', skiprows=6)

            # remoção de linhas vazias
            # seleção das linhas de interesse
            df.dropna(axis='index', inplace=True)
            df = df.loc[df['Unnamed: 0'].isin(['Brasil', 'Região Nordeste', 'Sergipe'])]

            # renomeação da coluna sem rótulo
            # reorganização da tabela; variável ano passa para o eixo y
            df.rename(columns={'Unnamed: 0': 'Região'}, inplace=True)
            df_melted = pd.melt(df, id_vars=['Região'], value_vars=list(df.columns[1:]),
                                var_name=['Ano'], value_name='Valor')

            # reordenação das variáveis
            # classificação dos dados
            df_melted.sort_values(by=['Região', 'Ano'], ascending=[True] * 2, inplace=True)
            df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
            df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]], format='%Y')
            df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('int64')

            # conversão em arquivo csv
            c.to_excel(df_melted, dir_name, 'g19.2.xlsx')
            n_figs += 1

        except Exception as e:
            errors['Gráfico 19.2'] = str(e)

        '''
        Bloco de comando ignorado por falta de dados
        '''
        continue
        # try:
        #     print('A organizar o arquivo da figura: Tabela 19.2 ...')
        #     df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'xls', sheet_name=None)
        #
        #     # coleta de dados referentes à despesa per capita com segurança pública
        #     # seleção das linhas e colunas de interesse
        #     # renomeação das colunas
        #     df_despesas = df['G65']
        #     df_despesas = df_despesas.iloc[6:33, :2]
        #     df_despesas.columns = ['Região', 'Valor']
        #
        #     # dados sergipe
        #     df_despesas_se = df_despesas.loc[df_despesas['Região'] == 'Sergipe']
        #
        #     # dados nordeste
        #     ne_states = ['Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba',
        #                  'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe']
        #     df_despesas_ne = df_despesas.loc[df_despesas['Região'].isin(ne_states)]
        #     df_despesas_ne.iloc[:, 0] = 'Nordeste'
        #     df_despesas_ne = df_despesas_ne.groupby('Região')['Valor'].sum().reset_index()
        #
        #     # dados brasil
        #     df_despesas_br = df_despesas.iloc[:]
        #     df_despesas_br.iloc[:, 0] = 'Brasil'
        #     df_despesas_br = df_despesas_br.groupby('Região')['Valor'].sum().reset_index()
        #
        #     # união dos dfs
        #     # adição da variável ano
        #     # adição da variável classe
        #     df_despesas_united = pd.concat([df_despesas_br, df_despesas_ne, df_despesas_se], ignore_index=True)
        #     df_despesas_united['Ano'] = df['G65'].iloc[0, 0][-4:]
        #     df_despesas_united['Classe'] = 'Despesa per capita com Segurança Pública'
        #
        #     # coleta de dados referentes à participação das despesas com segurança pública
        #     # renomeação das colunas
        #     # seleção das linhas de interesse
        #     # remoção de linhas vazias
        #     df_participacao = df['T55']
        #     df_participacao.columns = df_participacao.iloc[4]
        #     df_participacao = df_participacao.iloc[6:]
        #     df_participacao = df_participacao.dropna(axis='index')
        #
        #     # reorganização da tabela; variável ano passada para o eixo y
        #     df_participacao_melted = df_participacao.melt(id_vars=df_participacao.columns[0],
        #                                                   value_vars=list(df_participacao.columns[1:]),
        #                                                   var_name=['Ano'], value_name='Valor')
        #     df_participacao_melted['Ano'] = df_participacao_melted['Ano'].astype('int64')
        #
        #     # dados sergipe
        #     df_participacao_se = df_participacao_melted.loc[
        #         df_participacao_melted[df_participacao_melted.columns[0]] == 'Sergipe']
        #
        #     # dados nordeste
        #     df_participacao_ne = df_participacao_melted.loc[
        #         df_participacao_melted[df_participacao_melted.columns[0]].isin(ne_states)]
        #     df_participacao_ne.iloc[:, 0] = 'Nordeste'
        #     df_participacao_ne = df_participacao_ne.groupby([df_participacao_ne.columns[0],
        #                                                      df_participacao_ne.columns[1]])[
        #         'Valor'].mean().reset_index()
        #
        #     # dados brasil
        #     df_participacao_br = df_participacao_melted.loc[
        #         df_participacao_melted[df_participacao_melted.columns[0]] == 'União']
        #     df_participacao_br.iloc[:, 0] = 'Brasil'
        #
        #     # união dos dfs
        #     df_participacao_united = pd.concat([df_participacao_br, df_participacao_ne, df_participacao_se],
        #                                        ignore_index=True)
        #     df_participacao_united['Classe'] = 'Participação das despesas com Segurança Pública'
        #     df_participacao_united = df_participacao_united.rename(
        #         columns={df_participacao_united.columns[0]: 'Região'})
        #
        #     # coleta de dados referentes à razão preso/vaga
        #     # organização da tabela para seleção das linhas de interesse
        #     df_preso = df['T75']
        #     df_preso.columns = df_preso.iloc[5]
        #     df_preso = df_preso.dropna(axis='index')
        #     df_preso = df_preso.iloc[:, [0, -2, -1]]
        #     df_preso.columns = [str(int(col)) if not pd.isna(col) else 'Região' for col in df_preso.columns]
        #     df_preso = df_preso.melt(id_vars=['Região'], value_vars=list(df_preso.columns[1:]),
        #                              var_name=['Ano'], value_name='Valor')
        #
        #     # dados sergipe
        #     df_preso_se = df_preso.loc[df_preso['Região'] == 'Sergipe']
        #
        #     # dados sergipe
        #     df_preso_ne = df_preso.loc[df_preso['Região'].isin(ne_states)]
        #     df_preso_ne.iloc[:, 0] = 'Nordeste'
        #     df_preso_ne = df_preso_ne.groupby(['Região', 'Ano'])['Valor'].mean().reset_index()
        #
        #     # dados brasil
        #     df_preso_br = df_preso.loc[df_preso['Região'].str.startswith('Brasil')]
        #
        #     # união dos dfs
        #     df_preso_united = pd.concat([df_preso_br, df_preso_ne, df_preso_se], ignore_index=True)
        #     df_preso_united['Classe'] = 'Razão preso/vaga'
        #
        #     # união dos dfs de cada aba de interesse
        #     df_united = pd.concat([df_despesas_united, df_participacao_united, df_preso_united], ignore_index=True)
        #     df_united['Região'] = df_united['Região'].apply(lambda x: re.sub(r'\(\d+\)', '', x))
        #     df_united = df_united.sort_values(by=['Região', 'Ano', 'Classe'], ascending=[True] * 3)
        #
        #     # classificação dos dados
        #     df_united[df_united.columns[0]] = df_united[df_united.columns[0]].astype('str')
        #     df_united[df_united.columns[1]] = df_united[df_united.columns[1]].astype('float64')
        #     df_united[df_united.columns[2]] = pd.to_datetime(df_united[df_united.columns[2]], format='%Y')
        #     df_united[df_united.columns[3]] = df_united[df_united.columns[3]].astype('str')
        #
        #     # adicionado após conversa para remoção de dados agregados manualmente
        #     df_united = df_united.loc[df_united['Região'] == 'Sergipe']
        #
        #     # conversão em arquivo csv
        #     c.to_excel(df_united, dir_name, 'tabela_19-2_anuario_seguranca_publica.xlsx')
        #     n_figs += 1
        #
        # except Exception as e:
        #     errors['Tabela 19.2'] = str(e)

    elif csv_name.startswith('sinesp'):
        '''
        Bloco de comando ignorado por falta de dados
        '''
        continue
        # try:
        #     print('A organizar o arquivo da figura: Gráfico 19.12 ...')
        #     df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'xls', sheet_name='Ocorrências')
        #
        #     # dados sergipe
        #     df_se = df.loc[(df['UF'] == 'Sergipe') & (df['Tipo Crime'] == 'Estupro')]
        #
        #     # dados nordeste
        #     df_ne = df.loc[(df['UF'].isin(['Alagoas', 'Bahia', 'Ceará', 'Maranhão',
        #                                    'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe'])) &
        #                    (df['Tipo Crime'] == 'Estupro')]
        #     df_ne.iloc[:len(df_ne), 0] = 'Nordeste'
        #     df_ne = df_ne.groupby(['UF', 'Tipo Crime', 'Ano', 'Mês'])['Ocorrências'].sum().reset_index()
        #
        #     # dados brasil
        #     df_br = df.loc[df['Tipo Crime'] == 'Estupro']
        #     df_br.iloc[:len(df_br), 0] = 'Brasil'
        #     df_br = df_br.groupby(['UF', 'Tipo Crime', 'Ano', 'Mês'])['Ocorrências'].sum().reset_index()
        #
        #     # união dos dfs
        #     df_united = pd.concat([df_br, df_ne, df_se], ignore_index=True)
        #     df_united.rename(columns={'UF': 'Região'}, inplace=True)
        #
        #     # classificação dos dados
        #     df_united[df_united.columns[:2]] = df_united[df_united.columns[:2]].astype('str')
        #     df_united[df_united.columns[2]] = pd.to_datetime(df_united[df_united.columns[2]], format='%Y')
        #     df_united[df_united.columns[3]] = df_united[df_united.columns[3]].astype('str')
        #     df_united[df_united.columns[-1]] = df_united[df_united.columns[-1]].astype('int64')
        #
        #     # adicionado após conversa para remoção de dados agregados manualmente
        #     df_united = df_united.loc[df_united['Região'] == 'Sergipe']
        #
        #     # conversão em csv
        #     c.to_excel(df_united, dir_name, 'grafico-19-12_sinesp_ocorrencias_criminais.xlsx')
        #     n_figs += 1
        #
        # except Exception as e:
        #     errors['Gráfico 19.12'] = str(e)
        #
        # try:
        #     print('A organizar o arquivo da figura: Tabela 19.1 ...')
        #     df = c.open_file(os.path.join(script_path, c.dir_name), csv_name, 'xls', sheet_name='Ocorrências')
        #
        #     crimes = ['Roubo a instituição financeira', 'Roubo de carga', 'Roubo de veículo', 'Furto de veículo']
        #     ne_states = ['Alagoas', 'Bahia', 'Ceará', 'Maranhão',
        #                  'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe']
        #
        #     # dados sergipe
        #     df_se = df.loc[(df['UF'] == 'Sergipe') & df['Tipo Crime'].isin(crimes)]
        #
        #     # dados nordeste
        #     df_ne = df.loc[(df['UF'].isin(ne_states)) & (df['Tipo Crime'].isin(crimes))]
        #     df_ne.iloc[:len(df_ne), 0] = 'Nordeste'
        #     df_ne = df_ne.groupby(['UF', 'Tipo Crime', 'Ano', 'Mês'])['Ocorrências'].sum().reset_index()
        #
        #     # dados brasil
        #     df_br = df.loc[df['Tipo Crime'].isin(crimes)]
        #     df_br.iloc[:len(df_br), 0] = 'Brasil'
        #     df_br = df_br.groupby(['UF', 'Tipo Crime', 'Ano', 'Mês'])['Ocorrências'].sum().reset_index()
        #
        #     # união dos dfs
        #     df_united = pd.concat([df_br, df_ne, df_se], ignore_index=True)
        #     df_united.rename(columns={'UF': 'Região'}, inplace=True)
        #
        #     # classificação dos dados
        #     df_united[df_united.columns[:2]] = df_united[df_united.columns[:2]].astype('str')
        #     df_united[df_united.columns[2]] = pd.to_datetime(df_united[df_united.columns[2]], format='%Y')
        #     df_united[df_united.columns[3]] = df_united[df_united.columns[3]].astype('str')
        #     df_united[df_united.columns[-1]] = df_united[df_united.columns[-1]].astype('int64')
        #
        #     # adicionado após conversa para remoção de dados agregados manualmente
        #     df_united = df_united.loc[df_united['Região'] == 'Sergipe']
        #
        #     # conversão em csv
        #     c.to_excel(df_united, dir_name, 'tabela_19-1_sinesp_ocorrencias_criminais.xlsx')
        #     n_figs += 1
        #
        # except Exception as e:
        #     errors['Tabela 19.1'] = str(e)

if errors:
    try:
        with open(os.path.join(dir_name, 'errors.json'), 'w', encoding='utf-8') as f:
            f.write(json.dumps(errors, indent=4, ensure_ascii=False))
        print('Relatório de erros gerado!')
    except Exception as e:
        print(e)
else:
    print('Erros: ', errors)

n_files = len([f for f in os.listdir(c.dir_name) if not "tabela" in f and not "grafico" in f])
print(f'Total de arquivos a organizar: {n_files}')
print(f'Total de arquivos organizados: {n_figs}')

to_copy = [f for f in os.listdir(c.dir_name) if 'grafico' in f or 'tabela' in f]

for f in to_copy:
    from_path = os.path.join(c.dir_name, f)
    df = pd.read_csv(from_path, encoding='utf-8', decimal=',')
    df.to_excel(os.path.join(dir_name, f[0] + f.split('_')[1].replace('-', '.') + '.xlsx'), index=False)

# ************************
# UPLOAD DE ARQUIVOS PARA REPOSITÓRIO NO GITHUB
# ************************

# definição do horário para registro de upload ou update dos arquivos
now = datetime.datetime.now().strftime('%d/%m/%Y, %H:%M:%S')

# caminhos de diretórios
repo_path = c.repo_path2
git_token = c.git_token2
data_git_path = 'data'
script_git_path = 'script'
doc_git_path = 'doc'
script_path = os.getcwd()
dir_name = c.dir_name2

# inicialização do repositório
auth = Auth.Token(git_token)
g = Github(auth=auth)
repo = g.get_repo(repo_path)
contents = repo.get_contents('')

# diretórios no git
git_cont = []
for content_file in contents:
    git_cont.append(content_file.path)

# upload ou update de arquivos xlsx
try:
    my_folder = repo.get_contents(data_git_path)
except:
    my_folder = None

csv_folder = dir_name
for csv_file in os.listdir(os.path.join(script_path, csv_folder)):
    csv_path = os.path.join(script_path, csv_folder, csv_file)

    if os.path.isfile(csv_path):
        with open(csv_path, 'rb') as file:
            csv_content = file.read()

        if my_folder:
            file_in_folder = next((csv_f for csv_f in my_folder if csv_f.name == csv_file), None)
        else:
            file_in_folder = None

        # verifica se se arquivo local já existe no diretório, para definir se deve criá-lo ou atualizá-lo
        if file_in_folder:
            repo.update_file(f'data/{csv_file}', f'Arquivo atualizado em {now}.',
                             csv_content, file_in_folder.sha)
            print(f'Arquivo {csv_file} atualizado no diretório.')
            sleep(1)
        else:
            repo.create_file(f'data/{csv_file}', f'Arquivo criado em {now}.', csv_content)
            print(f'Arquivo {csv_file} criado no diretório.')
            sleep(1)

# upload ou update do script
try:
    my_folder = repo.get_contents(script_git_path)
except:
    my_folder = None

script_path = 'get_files.py'
script_path2 = 'organize_files.py'

with open(script_path, 'r', encoding='utf-8') as f:
    text = f.read()
with open(script_path2, 'r', encoding='utf-8') as f:
    text2 = f.read()

if my_folder:
    file_in_folder = next((script for script in my_folder if script.name == script_path), None)
    file_in_folder2 = next((script for script in my_folder if script.name == script_path2), None)
else:
    file_in_folder = None
    file_in_folder2 = None

if file_in_folder:
    repo.update_file(f'script/{script_path}', f'Arquivo atualizado em {now}',
                     text, file_in_folder.sha)
    print(f'Script {script_path} atualizado no diretório.')
    sleep(1)
else:
    repo.create_file(f'script/{script_path}', f'Arquivo criado em {now}', text)
    print(f'Script {script_path} criado no diretório.')
    sleep(1)

if file_in_folder2:
    repo.update_file(f'script/{script_path2}', f'Arquivo atualizado em {now}',
                     text2, file_in_folder2.sha)
    print(f'Script {script_path2} atualizado no diretório.')
    sleep(1)
else:
    repo.create_file(f'script/{script_path2}', f'Arquivo criado em {now}', text2)
    print(f'Script {script_path2} criado no diretório.')
    sleep(1)

# upload o update da documentação
try:
    my_folder = repo.get_contents(doc_git_path)
except:
    my_folder = None

doc_path = 'support_files/documentação.json'

with open(doc_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

if my_folder:
    file_in_folder = next((doc_f for doc_f in my_folder if doc_f.name == 'documentação.txt'), None)
else:
    file_in_folder = None

if file_in_folder:
    repo.update_file('doc/documentação.txt', f'Arquivo atualizado em {now}.',
                     json.dumps(data, indent=4, ensure_ascii=False), file_in_folder.sha)
    print('Documentação atualizada no diretório.')
    sleep(1)
else:
    repo.create_file('doc/documentação.txt', f'Arquivo criado em {now}.',
                     json.dumps(data, indent=4, ensure_ascii=False))
    print('Documentação criada no diretório.')
    sleep(1)

g.close()
