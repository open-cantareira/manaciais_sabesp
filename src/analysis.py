#!/usr/bin/env python
# coding: utf-8

#!pip install plotly


import os
import psycopg2
import pandas as pd

import plotly.offline as py
import plotly.graph_objs as go
import plotly.graph_objects as go

from plotly.offline import plot
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta


try:
    from sabesp import settings
    from dados.models import cantareira
    from django.conf import settings
    print('In Django:\n{}'.format(settings.BASE_DIR))
    in_django = True
except Exception as e:
    print('Not in Django\n{}'.format(e))
    py.init_notebook_mode(connected=True)
    in_django = False


def m3s_2_hm3(m3s):
    "Converte m3/s em um dia (86400 segundos) para hm3"
    x = m3s * (24*60*60) * 0.000001
    return x


def hm3_2_m3s(hm3):
    "Converte mhm3 para m3/s em um dia (86400 segundos)"
    x = hm3 / (24*60*60) / 0.000001
    return x


def get_middle_date(start_date, end_date):
    """
    Pega a data do meio entre duas datas.
    Adequado para os labels de gráficos
    Textos inseridos em string
    Retorno em dataetime
    """
    # Get Middle Date
    #start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    #end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    x = (end_date-start_date).days
    x = start_date + timedelta(x/2)
    return x


def get_df_compiled(in_django):
    # Create blank list
    dfs = []

    # Files
    files = [
        'tab_Cantareira_1990.01.01__1999.12.31.csv',
        'tab_Cantareira_2000.01.01__2009.12.31.csv',
        'tab_Cantareira_2010.01.01__2019.12.31.csv',
        'tab_Cantareira_2020.01.01__2020.07.14.csv',
    ]

    if in_django:
        filepath = os.path.join(settings.BASE_DIR, 'staticfiles', 'data')
    else:
        filepath = os.path.join('..', 'data')

    # Read all Files
    for file in files:
        df = pd.read_csv(
            os.path.join(filepath, file),
            sep=';',
            decimal=','
        )

        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='ignore')
        dfs.append(df)

    # Concat
    dfs = pd.concat(dfs)

    # Se no Django...
    if in_django:
        # Read all DB
        query = cantareira.objects.all().values()

        if query.count() > 0:
            df = pd.DataFrame.from_records(query)
            df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d', errors='ignore')

            # Get Max value from csv files
            max_date_csv = max(dfs['data'])

            # Seleciona o deadline
            df = df[df['data'] > max_date_csv]

            # Concat
            df = pd.concat([dfs, df])

    else:
        df = pd.concat([dfs])

    # Results
    return df.sort_values('data')


def concat_dfdb(df):
    # Dados do Banco de Dados (Heroku)
    host = 'ec2-34-239-241-25.compute-1.amazonaws.com'
    database = 'da818piepcotea'
    user = 'aohjlkbcflpjkk'
    port = '5432'
    password='211406869e5c3e2fe81b84ad37f45c0f5c645f4ef834271a70f3918dda00d500'
    
    # Conection
    db = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )
    cur = db.cursor()

    # Query
    sql = """
    SELECT *
    FROM dados_cantareira
    """
    cur.execute(sql)

    # Transform in Dataframe
    df_db = pd.read_sql_query(sql, db)

    # Close Conection
    db.close()

    # Ajusta da Tabela
    df_db.drop(['id'], axis=1, inplace=True, errors='ignore')
    df_db['data'] = pd.to_datetime(df_db['data'], format='%Y-%m-%d')

    # Seleciona o deadline
    #deadline
    #deadline = '2020-07-10'
    #df = df[df['data'] >= deadline]

    # Concat
    df = pd.concat([df, df_db])

    # Results
    return df


def save_df(df, data_path):
    # Save
    df.to_csv(
        os.path.join(data_path, 'tab_Cantareira_compiled.csv'),
        index=False,
        header=True,
        encoding='UTF-8-SIG',
        sep=';',
        decimal=',',
        date_format='%d/%m/%Y'
    )
    return print('Tabela Salva')


def set_periodo(data):
    """
    Define o periodo a partir de uma data qualquer, em formato texto
    """
    # Definição do Período Seco
    periodoseco_start = date(data.year, 6, 1)
    periodoseco_end = date(data.year, 11, 30)

    # Ifs
    if data >= periodoseco_start and data <= periodoseco_end:
        periodo = 'Período Seco'
        days2end_periodo = (periodoseco_end - data).days
        
    elif data < periodoseco_start:
        periodo = 'Período Úmido'
        days2end_periodo = (periodoseco_start - data).days
        
    elif data > periodoseco_end:
        periodo = 'Período Úmido'
        x = periodoseco_start + relativedelta(years=1)-timedelta(1)
        days2end_periodo = (x - data).days
        
    else:
        periodo = 'Erro'
        
    return periodo, days2end_periodo, periodoseco_start, periodoseco_end


def set_faixas(vol_porcentagem):
    # Seleciona a Faixa
    if vol_porcentagem >= 0.6:
        faixa = 'Faixa 1 (Normal)'
        faixa_id = 1
    elif vol_porcentagem >= 0.4:
        faixa = 'Faixa 2 (Atenção)'
        faixa_id = 2
    elif vol_porcentagem >= 0.3:
        faixa = 'Faixa 3 (Alerta)'
        faixa_id = 3
    elif vol_porcentagem >= 0.2:
        faixa = 'Faixa 4 (Restrição)'
        faixa_id = 4
    elif vol_porcentagem < 0.2:
        faixa = 'Faixa 5 (Especial)'
        faixa_id = 5
        
    return faixa, faixa_id


def get_faixas(data, df):
    """
    Define o periodo a partir de uma data qualquer, em formato texto
    """

    # Filtra Tabela
    #df = df[df['data'] == pd.Timestamp(data)]
    #df.loc[:, 'data' == pd.Timestamp(data)]
    df.loc[df['data'] == pd.Timestamp(data)]

    # Calcula Campo
    df['sc_calc_volumeporcentagem'] = (
        (df['jaguari_jacarei_volumeoperacional'] +
        df['cachoeira_volumeoperacional'] +
        df['atibainha_volumeoperacional'] +
        df['paivacastro_volumeoperacional']) / 
        (
            (df['jaguari_jacarei_volumemaximo'] +
            df['cachoeira_volumemaximo'] +
            df['atibainha_volumemaximo'] +
            df['paivacastro_volumemaximo'])
            -
            (df['jaguari_jacarei_volumeminimo'] +
            df['cachoeira_volumeminimo'] +
            df['atibainha_volumeminimo'] +
            df['paivacastro_volumeminimo'])
    ))
    
    # Define o Valor
    vol_porcentagem = df.iloc[0]['sc_calc_volumeporcentagem']
    
    # Set Faixas
    faixa, faixa_id = set_faixas(vol_porcentagem)

    # Round
    vol_porcentagem = round(vol_porcentagem*100, 2)

    return (
        data.strftime('%d.%m.%Y'),
        vol_porcentagem,
        faixa,
        faixa_id
    )


def get_graph_faixas(df):
    # Filtra Tabela
    start_date = date(2017, 5, 29)
    df = df[df['data'] >= pd.Timestamp(start_date)].copy()

    # Calcula Campo
    df['sc_calc_volumeporcentagem'] = (
        (df['jaguari_jacarei_volumeoperacional'] +
        df['cachoeira_volumeoperacional'] +
        df['atibainha_volumeoperacional'] +
        df['paivacastro_volumeoperacional']) / 
        (
            (df['jaguari_jacarei_volumemaximo'] +
            df['cachoeira_volumemaximo'] +
            df['atibainha_volumemaximo'] +
            df['paivacastro_volumemaximo'])
            -
            (df['jaguari_jacarei_volumeminimo'] +
            df['cachoeira_volumeminimo'] +
            df['atibainha_volumeminimo'] +
            df['paivacastro_volumeminimo'])
    ))

    # Get Middle Date
    end_date = date.today()
    x = get_middle_date(start_date, end_date)

    # Create Graph
    fig = go.Figure()

    # Create scatter trace of data
    fig.add_trace(
        go.Scatter(
            x=df['data'],
            y=df['sc_calc_volumeporcentagem'],
            name='V.Útil',
            text='Volume Útil (%)',
            # hover_name='sss',
            line={
                'color': '#000066',
                #'dash': 'dash'
            }
        )
    )
    
    # Create scatter trace of text labels
    fig.add_trace(
        go.Scatter(
            x=[x, x, x, x, x],
            y=[0.8, 0.5, 0.35, 0.25, 0.1],
            text=['Faixa 1: Normal',
                  'Faixa 2: Atenção',
                  'Faixa 3: Alerta',
                  'Faixa 4: Restrição',
                  'Faixa 5: Especial',
                  ],
            mode='text',
            showlegend=False,
            hoverinfo='none',
        )
    )
    
    fig.add_shape(
        dict(
            type='rect',
            x0=start_date,
            y0=0.6,
            x1=max(df['data']),
            y1=1,
            fillcolor='#476ab4',
            opacity=0.5,
            layer='below',
            line_width=0,
            line=dict(
                color="Green",
                width=1
            ),
        ))

    fig.add_shape(
        dict(
            type='rect',
            x0=start_date,
            y0=0.4,
            x1=max(df['data']),
            y1=0.6,
            fillcolor='#97d4ae',
            opacity=0.5,
            layer='below',
            line_width=0,
            line=dict(
                color='Green',
                width=1
            ),
        ))

    fig.add_shape(
        dict(
            type='rect',
            x0=start_date,
            y0=0.3,
            x1=max(df['data']),
            y1=0.4,
            fillcolor='#f4f3ca',
            opacity=0.5,
            layer='below',
            line_width=0,
            line=dict(
                color='Green',
                width=1
            ),
        ))

    fig.add_shape(
        dict(
            type='rect',
            x0=start_date,
            y0=0.2,
            x1=max(df['data']),
            y1=0.3,
            fillcolor='#ffa75f',
            opacity=0.5,
            layer='below',
            line_width=0,
            line=dict(
                color='Green',
                width=1
            ),
        ))

    fig.add_shape(
        dict(
            type='rect',
            x0=start_date,
            y0=0,
            x1=max(df['data']),
            y1=0.2,
            fillcolor='#bd3b3b',
            opacity=0.5,
            layer='below',
            line_width=0,
            line=dict(
                color='Green',
                width=1
            ),
        ))

    # Layout
    fig.update_layout(
        # title='Faixas de Operação',
        yaxis={'title': 'Volume Útil (%)'},
        # xaxis={'title': 'Data'}
    )
    fig.update_layout(yaxis_tickformat='%')
    fig.update_layout(yaxis_tickformat='.2%')
    fig.update_layout(xaxis_tickformat='%d %B<br>%Y')
    fig.update_layout(showlegend=False)

    # Results
    plt = plot(fig, output_type='div', include_plotlyjs=False)
    return plt, fig


def get_graph_reservatorios(date_series, data_plot):
    # Graph
    fig = go.Figure()
    scatter = go.Scatter(
        x=date_series,
        y=data_plot,
        mode='lines',
        name='test',
        opacity=1,
        marker_color='blue'
    )

    fig.add_trace(scatter)
    plt = plot(fig, output_type='div', include_plotlyjs=False)
    return plt, fig


def get_forecast_data(df):
    # Soma a vazão de jusante
    df['qjusante'] = (
            df['jaguari_jacarei_qjusante'] +
            df['cachoeira_qjusante'] +
            df['atibainha_qjusante']
    )

    # Converte a vazão de jusante para hm3
    df['hm3jusante'] = df['qjusante'].apply(m3s_2_hm3)

    # Seleciona Colunas de Interesse
    df = df[['data', 'qjusante', 'hm3jusante']].copy()

    # Definição do Período Seco
    dia = date.today().strftime('%Y-%m-%d')
    dia = date.today()
    periodo, days2end_periodo, periodoseco_start, periodoseco_end = set_periodo(dia)

    if periodo == 'Período Seco':
        max_date = max(df['data'])
        df_periodoseco = df[df['data'] >= pd.Timestamp(periodoseco_start)]
        hm3_today = sum(df_periodoseco['hm3jusante'])
        hm3_periodoseco = 158.1
        hm3_balance = hm3_periodoseco-hm3_today
        hm3_forecast = hm3_balance/days2end_periodo
        m3s_forecast = hm3_2_m3s(hm3_forecast)

        # Define Parâmetros para criar uma tabela iniciando com o dia de amanhã
        start = (date.today() + timedelta(1)).strftime('%Y-%m-%d')
        end = periodoseco_end.strftime('%Y-%m-%d')
        df_forecast = pd.DataFrame(pd.date_range(pd.to_datetime(start), end=pd.to_datetime(end)), columns=['data'])
        df_forecast['qjusante'] = m3s_forecast
        df_forecast['hm3jusante'] = hm3_forecast

        # Adiciona a tabela
        df = df.append(df_forecast, ignore_index=True)
        df = df.sort_values('data')

        # Variaveis para Exportar
        hm3_periodoseco = round(hm3_periodoseco, 2)
        hm3_today = round(hm3_today, 2)
        hm3_balance = round(hm3_balance, 2)
        hm3_forecast = round(hm3_forecast, 2)
        m3s_forecast = round(m3s_forecast, 2)
        max_date = max_date.strftime('%d.%m.%Y')

        return (
            df,
            hm3_periodoseco,
            hm3_today,
            hm3_balance,
            hm3_forecast,
            m3s_forecast,
            max_date,
        )
    if periodo == 'Período Úmido':
        #TODO: Ajustar toda essa sessão sobre periodo úmido.
        max_date = max(df['data'])
        df_periodoseco = df[df['data'] >= pd.Timestamp(periodoseco_start)]
        hm3_today = sum(df_periodoseco['hm3jusante'])
        hm3_periodoseco = 158.1
        hm3_balance = hm3_periodoseco-hm3_today
        hm3_forecast = hm3_balance/days2end_periodo
        m3s_forecast = hm3_2_m3s(hm3_forecast)

        # Define Parâmetros para criar uma tabela iniciando com o dia de amanhã
        start = (date.today() + timedelta(1)).strftime('%Y-%m-%d')
        end = periodoseco_end.strftime('%Y-%m-%d')
        df_forecast = pd.DataFrame(pd.date_range(pd.to_datetime(start), end=pd.to_datetime(end)), columns=['data'])
        df_forecast['qjusante'] = m3s_forecast
        df_forecast['hm3jusante'] = hm3_forecast

        # Adiciona a tabela
        df = df.append(df_forecast, ignore_index=True)
        df = df.sort_values('data')

        # Variaveis para Exportar
        hm3_periodoseco = round(hm3_periodoseco, 2)
        hm3_today = round(hm3_today, 2)
        hm3_balance = round(hm3_balance, 2)
        hm3_forecast = round(hm3_forecast, 2)
        m3s_forecast = round(m3s_forecast, 2)
        max_date = max_date.strftime('%d.%m.%Y')

        return (
            df,
            hm3_periodoseco,
            hm3_today,
            hm3_balance,
            hm3_forecast,
            m3s_forecast,
            max_date,
        )


def get_graph_forecast(date_series, data_plot):
    # Graph Forecast
    fig = go.Figure()
    scatter = go.Scatter(
        x=date_series,
        y=data_plot,
        mode='lines',
        name='test',
        opacity=1,
        marker_color='blue'
    )

    fig.add_trace(scatter)
    plt = plot(fig, output_type='div', include_plotlyjs=False)    
    return plt, fig


def get_graph_forecast(date_series, data_plot, max_date_db):
    # Definição do Período Seco
    periodo, days2end_periodo, periodoseco_start, periodoseco_end = set_periodo(date.today())

    # Get Middle Date
    x1 = get_middle_date(min(date_series).date(), max_date_db)
    x2 = get_middle_date(max_date_db, periodoseco_end)

    # Graph
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=date_series,
            y=data_plot,
            mode='lines',
            name='QJus',
            text='m³/s',
            showlegend=False,
            line=dict(
                shape='linear',
                color='#000066',
                width=0.8,
            )
        ))
    fig.add_shape(
        dict(
            type='rect',
            x0=min(date_series),
            x1=max_date_db,
            yref='paper',
            y0=0,
            y1=1,
            fillcolor='#6ed69f',
            opacity=0.5,
            layer='below',
            line_width=0,
            line=dict(
                color='#f4f3ca',
                width=1
            ),
        )
    )
    fig.add_shape(
        dict(
            type='rect',
            x0=max_date_db,
            x1=periodoseco_end,
            yref='paper',
            y0=0,
            y1=1,
            fillcolor='#ecb50c',
            opacity=0.5,
            layer='below',
            line_width=0,
            line=dict(
                color='#f4f3ca',
                width=1
            ),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[x1, x2],
            y=[10, 2],
            text=['Passado', 'Futuro'],
            mode='text',
            showlegend=False,
            hoverinfo='none',
        ))
    start_date = date(2017, 5, 29)
    fig.update_layout(
        yaxis={
            'title': 'Vazão (m³/s)',
            'range': [0, 14]
        },
        xaxis={'range': [start_date, max(date_series)]},
        yaxis_tickformat='.2',
        xaxis_tickformat='%d %b<br>%Y',
        showlegend=False,
        hovermode='x',
    )
    config = {'displaylogo': False}
    plt = plot(fig, output_type='div', include_plotlyjs=False, config=config)
    return plt, fig


def get_qmin_data(df):
    # Filtra Tabela
    start_date = '2017-05-29'
    df = df[df['data'] >= pd.Timestamp(start_date)]

    # Vazão dos Cursos d'água
    df['jaguari_qjusante'] = df['jaguari_jacarei_qjusante']
    df['atibaia_qjusante'] = (df['cachoeira_qjusante'] + df['atibainha_qjusante'])
    df['juqueri_qjusante'] = df['paivacastro_qjusante']
    
    return df


def get_graph_qmin(date_series, data_plot, qminima):
    # Graph Q minima
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=date_series,
            y=data_plot,
            mode='lines',
            name='test',
            opacity=1,
            marker_color='blue'
        )
    )
    fig.add_shape(
        dict(
            type='line',
            x0=min(date_series),
            y0=qminima,
            x1=max(date_series),
            y1=qminima,
            layer='above',
            line=dict(
                color='Red',
                width=2,
                dash='dashdot',
            ),
        ))

    # Layout
    fig.update_layout(
        yaxis={'title': 'Vazão (m³/s)'},
    )
    fig.update_layout(yaxis_tickformat='.2')
    fig.update_layout(xaxis_tickformat='%d %B<br>%Y')
    fig.update_layout(showlegend=False)

    plt = plot(fig, output_type='div', include_plotlyjs=False)

    return plt, fig


def set_limiteretirada(faixa_id):
    # Seleciona a Faixa
    if faixa_id == 1:
        limite_retirada = 33
    elif faixa_id == 2:
        limite_retirada = 31
    elif faixa_id == 3:
        limite_retirada = 27
    elif faixa_id == 4:
        limite_retirada = 23
    elif faixa_id == 5:
        limite_retirada = 15.5
        
    return limite_retirada


def get_qretirada_data(df):
    # Filtra Tabela
    start_date = '2017-05-29'
    df = df[df['data'] >= pd.Timestamp(start_date)]
    
    # Filtra Colunas
    df = df[[
        'data',
        'jaguari_jacarei_volume', 'cachoeira_volume', 'atibainha_volume', 'paivacastro_volume',
        'jaguari_jacarei_volumemaximo', 'cachoeira_volumemaximo',
        'atibainha_volumemaximo', 'paivacastro_volumemaximo',
        'qesi_valor', 'sc_vazaoretirada', 'sc_vazaojusante',
    ]]       
    
    # Calcula Campo
    df['sc_calc_volumeporcentagem'] = (
        df['jaguari_jacarei_volume'] +
        df['cachoeira_volume'] +
        df['atibainha_volume'] +
        df['paivacastro_volume']) / (
        df['jaguari_jacarei_volumemaximo'] +
        df['cachoeira_volumemaximo'] +
        df['atibainha_volumemaximo'] +
        df['paivacastro_volumemaximo']
    )

    df[['faixa', 'faixa_id']] = df.apply(lambda x: set_faixas(x['sc_calc_volumeporcentagem']),
                                          axis=1, result_type='expand')
    df['limiteretirada'] = df.apply(lambda x: set_limiteretirada(x['faixa_id']), axis=1)
    
    return df


def get_graph_qretirada(df):
    # Graph Q minima
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df['data'],
            y=df['sc_vazaoretirada'],
            mode='lines',
            name='Q ETA Guaraú',
            opacity=1,
            marker_color='blue'
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df['data'],
            y=df['limiteretirada'],
            mode='lines',
            name='Limite Retirada RMSP',
            opacity=1,
            marker_color='black'
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df['data'],
            y=df['qesi_valor'],
            mode='lines',
            name='Q ESI',
            opacity=1,
            marker_color='red'
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df['data'],
            y=df['sc_vazaojusante'],
            mode='lines',
            name='Q Jusante',
            opacity=1,
            marker_color='green'
        )
    )

    # Layout
    fig.update_layout(
        yaxis={'title': 'Vazão (m³/s)'},
    )
    #fig.update_layout(yaxis_tickformat='.2')
    fig.update_layout(xaxis_tickformat='%d %B<br>%Y')
    fig.update_layout(showlegend=False)

    plt = plot(fig, output_type='div', include_plotlyjs=False)

    return plt, fig


#df = df.groupby(['ano', 'mes']).mean()
#df = df.reset_index()




