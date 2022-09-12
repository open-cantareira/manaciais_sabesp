#!/usr/bin/env python
# coding: utf-8

import os
import json
import time
import zlib
import requests
import calendar
import pandas as pd
import urllib.request
from datetime import date, datetime


def create_df(start=date(1970, 1, 1), end=None):
    """
    Function to create date table, with only on colum named 'Data' as index.
    With no 'end' parameter is passed, the function will return a table until today
    With no 'start' parameter is passed, the function will return a table staring in firts day of de 70's.
    
    The function return two more parameters to used in filenames:
    filename_start > first day of table 
    filename_end   > last day of table as string    
    """    
    
    if end is None:
        end=date.today()
        
    else:
        pass
    
    # Dataframe to get dates
    df = pd.DataFrame(
        pd.date_range(pd.to_datetime(start), end=end),
        columns=['Data']
    ).set_index('Data')

    return (
        df,
        start.strftime('%Y.%m.%d'),
        end.strftime('%Y.%m.%d'),
    )


def get_json(url):
    # Get Array with data
    #webURL = urllib.request.urlopen(url)
    #my_bytes = webURL.read()

    # Transform Array into JSON
    #my_json = my_bytes.decode('utf8')
    #data = json.loads(my_json)  
    
    r = requests.get(url, verify=False)
    decompressed_data=zlib.decompress(r.content, 16+zlib.MAX_WBITS)
    data = json.loads(decompressed_data)
    return json.dumps(data, indent=2, sort_keys=True)


def json2df(jsn):
    # Create dataframe
    df = pd.read_json(jsn)

    # Delete columns
    return df.drop(['FlagHasError', 'Message'], axis=1)


def get_system(df):
    # JSON to dataframe    
    data = df.loc['SistemaId']['ReturnObj']
    
    # Results
    return data


def get_enddate(df):
    # JSON to dataframe    
    data = df.loc['DataFinal']['ReturnObj']   
   
    # Results
    return datetime.strptime(data, '%d/%m/%Y').date()


def get_startdate(df):
    # JSON to dataframe    
    data = df.loc['DataInicial']['ReturnObj']   
    
    # Results
    return datetime.strptime(data, '%d/%m/%Y').date()


def get_manobras(df):
    # JSON to dataframe    
    lst = df.loc['ListaManobras']['ReturnObj']
    
    # Results
    return pd.json_normalize(lst)


def list_represas(df):
    # JSON to dataframe
    lst = df.loc['ListaRepresas']['ReturnObj']
    df = pd.json_normalize(lst)

    # Delete columns
    df = df.drop(['temChuva','temNivel', 'temQjus', 'temQnat', 'temVolume'], axis=1)

    # Results
    return df


def list_estruturas(df):
    # JSON to dataframe
    lst = df.loc['ListaLocais']['ReturnObj']
    df = pd.json_normalize(lst)

    # Delete columns
    df = df.drop(['Maximo','Minimo',
                  'Data','Dia',
                  'Valor','Unidade'],
                 axis=1)


    # Transform columns to list and reorder list
    col = df.columns.to_list()
    col.insert(0, col.pop(col.index('ComponenteId')))

    # Reindex Columns
    df = df.reindex(columns=col)

    # Results
    return df


def rename_field(x):
    return(x.replace('/', '-').
           replace(' (', '-').
           replace('-', '_').
           replace(')', '').
           replace('Cesp', 'CESP').
           replace('Represa ', '').
           replace(' ', '')
          )


def list_volumes(df):
    # JSON to dataframe
    lst = df.loc['ListaDados']['ReturnObj']
    df = pd.json_normalize(lst, 'Dados')

    # Define pivot to create new tables
    fields = df['Nome']
    fields = sorted(list(set(fields)))

    # Transform columns to list and reorder list
    col = df.columns.to_list()
    col.insert(0, col.pop(col.index('Data')))

    # Reindex Columns
    df = df.reindex(columns=col)

    # Create a blank table
    df_full,start,end = create_df()

    for i in fields:
        # Define Nomes e Nomes de Tabelas
        j = rename_field(i)
        df_name = 'df_dados_{}'.format(j)
        
        # Filtra e cria das tabelas por fields (represas)
        locals()[df_name] = df[df['Nome'] == i]

        # Deleta colunas
        locals()[df_name] = locals()[df_name].drop(['FlagConsolidado',
                                                    'NAMaxMax','NAMinMin',
                                                    'QJusanteMax','QJusanteMin',
                                                    'NivelUltimoDia',
                                                    'SistemaId','ComponenteId',
                                                    'UltimoDia',
                                                    'VazaoJusantePrincipal','VazaoJusanteSecundaria',
                                                    'VolumeOperacionalUltimoDia','VolumePorcentagemUltimoDia',
                                                    'VolumeTotalUltimoDia','Nome'], axis=1)

        # Renomeia as colunas
        locals()[df_name].columns = [x if x=='Data' else j+'_'+x for x in locals()[df_name].columns]

        # Convert Data Column (object) to datatime colum
        locals()[df_name]['Data'] = pd.to_datetime(locals()[df_name]['Data'])

        # Merge all tables
        df_full = pd.merge(df_full, locals()[df_name], on='Data', how='left')

    # Results
    df_full = df_full.set_index('Data')
    df_full.dropna(how='all', inplace=True)
    
    return df_full


def list_vazao(df):
    # Represas
    df_represas = list_represas(df)
    
    # JSON to dataframe
    lst = df.loc['ListaDados']['ReturnObj']
    df = pd.json_normalize(lst, 'Qnat')

    # Merge Tables
    df = pd.merge(df, df_represas, on='ComponenteId', how='outer')

    # Define pivot to create new tables
    fields = df['Nome']
    fields = sorted(list(set(fields)))
    
    # Transform columns to list and reorder list
    col = df.columns.to_list()
    col.insert(0, col.pop(col.index('Data')))

    # Reindex Columns
    df = df.reindex(columns=col)

    # Create a blank table
    df_full,start,end = create_df()

    for i in fields:
        # Define Nomes e Nomes de Tabelas
        j = rename_field(i)
        df_name = 'df_vazao'+'_'+j
        
        # Filtra e cria das tabelas por fields (represas)
        locals()[df_name] = df[df['Nome'] == i]

        # Deleta colunas
        locals()[df_name] = locals()[df_name].drop(['ComponenteId','Nome',
                                                    'VazaoAfluenteMax','VazaoAfluenteMin',
                                                    'VazaoNaturalMax','VazaoNaturalMin'], axis=1)

        # Renomeia as colunas
        locals()[df_name].columns = [x if x=='Data' else j+'_'+x for x in locals()[df_name].columns]

        # Convert Data Column (object) to datatime colum
        locals()[df_name]['Data'] = pd.to_datetime(locals()[df_name]['Data'])

        # Merge all tables
        df_full = pd.merge(df_full,locals()[df_name], on='Data', how='left')

    # Results
    df_full = df_full.set_index('Data')
    df_full.dropna(how='all', inplace=True)
    
    return df_full


def list_SE(df):
    # JSON to dataframe
    lst = df.loc['ListaDados']['ReturnObj']
    df = pd.json_normalize(lst)

    # Delete columns
    df = df.drop(['Dados','Data','Qnat'], axis=1)

    # Transform columns to list and reorder list
    col = df.columns.to_list()

    # Functions to rename
    col = ['SE_{}'.format(x) for x in col]
    col = [x.replace('SistemaEquivalente.', '').replace('SE_Data', 'Data') for x in col]

    # Rename Columns
    df.columns = col

    # Convert Data Column (object) to datatime colum
    df['Data'] = pd.to_datetime(df['Data'])

    # Results
    df = df.set_index('Data')
    df.dropna(how='all', inplace=True)
    
    return df


def list_SC(df):
    # JSON to dataframe
    lst = df.loc['ListaDadosSistema']['ReturnObj']
    df = pd.json_normalize(lst)
    
    # Delete columns
    df = df.drop(['objSistema.SistemaId', 'objQETA', 'objSistema.Data'], axis=1, errors='ignore')
    
    # Transform columns to list and reorder list
    col = df.columns.to_list()

    # Functions to rename
    col = ['SC_{}'.format(x) for x in col]
    col = [x.replace('objSistema', '') for x in col]
    col = [x.replace('.', '') for x in col]
    col = [x.replace('SC_Data', 'Data') for x in col]
    
    # Rename Columns
    df.columns = col
    
    # Convert Data Column (object) to datatime colum
    df['Data'] = pd.to_datetime(df['Data'])

    # Results
    df = df.set_index('Data')
    df.dropna(how='all', inplace=True)

    # Results
    return df


def list_vazaoestruturas(df):
    # JSON to dataframe
    lst = df.loc['ListaDadosLocais']['ReturnObj']
    # Descobri que deixou de funcionar pois a partir de 01.06.2021 teve falhas no Q"SC-PS"
    #df = pd.json_normalize(lst, 'Dados')
    list_d = []
    for parte1 in lst:
        for parte2 in parte1['Dados']:    
            if isinstance(parte2, dict):
                list_d.append(parte2)
    df = pd.DataFrame(list_d)

    # Define pivot to create new tables
    fields = df['Abreviatura']
    fields = sorted(list(set(fields)))

    # Transform columns to list and reorder list
    col = df.columns.to_list()
    col.insert(0, col.pop(col.index('Data')))
    col.append(col.pop(col.index('Unidade')))

    # Reindex Columns
    df = df.reindex(columns=col)

    # Create a blank table
    df_full,start,end = create_df()

    for i in fields:
        # Define Nomes e Nomes de Tabelas
        j = rename_field(i)
        df_name = 'df_dados_{}'.format(j)
        
        # Filtra e cria das tabelas por fields (represas)
        locals()[df_name] = df[df['Abreviatura'] == i].copy()
        #locals()[df_name] = df.loc[:, 'Abreviatura' == i]

        # Deleta colunas
        locals()[df_name].drop(
            [
                'Maximo', 'Minimo', 'Dia',
                'Abreviatura', 'ComponenteId',
                'LocalMedicaoId', 'Nome', 'SistemaId'
            ],
            axis=1,
            inplace=True
        )

        # Renomeia as colunas
        locals()[df_name].columns = [x if x=='Data' else '{}_{}'.format(j, x) for x in locals()[df_name].columns]

        # Convert Data Column (object) to datatime colum
        #locals()[df_name]['Data'] = pd.to_datetime(locals()[df_name]['Data'])
        locals()[df_name].loc[:, 'Data'] = pd.to_datetime(locals()[df_name]['Data'])

        # Merge all tables
        df_full = pd.merge(df_full,locals()[df_name], on='Data', how='left')

    # Results
    df_full.set_index('Data', inplace=True)
    df_full.dropna(how='all', inplace=True)
    
    return df_full


def list_EEAB(df):
    # JSON to dataframe
    lst = df.loc['ListaEspecial']['ReturnObj']
    df = pd.json_normalize(lst)
    
    # Results
    return df


def list_vazao_EEAB(df):
    # JSON to dataframe
    lst = df.loc['ListaDadosEspecial']['ReturnObj']
    df = pd.json_normalize(lst, 'Dados')
    
    # Results
    return df


def list_etas(df):
    # JSON to dataframe
    lst = df.loc['ListaETAs']['ReturnObj']
    df = pd.json_normalize(lst)

    # Results
    return df


def list_etas_dados(df):
    # JSON to dataframe
    lst = df.loc['ListaDadosSistema']['ReturnObj']
    df = pd.json_normalize(lst, 'objQETA')
    
    return df




