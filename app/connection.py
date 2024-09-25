# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 12:22:00 2023

@author: Ignacio Carvajal
"""

import pandas as pd 
import numpy as np
import random
import os
import psycopg2
from datetime import datetime


def connectionDB(query):

    # Datos de conexión
    host = "190.171.188.230"
    port = "5432"  # Puerto predeterminado de PostgreSQL
    database = "topusDB"  # Reemplazar por el nombre real de la base de datos
    user = "user_solo_lectura"
    password = "4l13nW4r3.C0ntr4s3n4.S0l0.L3ctur4"
    
    #connection 
    try:
        # Establecer la conexión
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        print("Conexión exitosa a la base de datos PostgreSQL")
    except (Exception, psycopg2.Error) as error:
        print("Error al conectarse a la base de datos PostgreSQL:", error)
        
    # execute the query 
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    
    #close the cursor
    cursor.close()
    return rows


def connectionDB_todf(query):

    # Datos de conexión
    host = "190.171.188.230"
    port = "5432"  # Puerto predeterminado de PostgreSQL
    database = "topusDB"  # Reemplazar por el nombre real de la base de datos
    user = "user_solo_lectura"
    password = "4l13nW4r3.C0ntr4s3n4.S0l0.L3ctur4"
    #connection 
    try:
        # Establecer la conexión
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        print("Conexión exitosa a la base de datos PostgreSQL")
    except (Exception, psycopg2.Error) as error:
        print("Error al conectarse a la base de datos PostgreSQL:", error)
    # execute the query 
    cursor = connection.cursor()
    cursor.execute(query)

    cursor = connection.cursor()
    cursor.execute(query)

    # Obtener los resultados en un DataFrame
    column_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=column_names)
    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()

    return df



def transform_dataframe(df):
    # Reemplazar etapa_tipo nulos o vacíos por 0
    # df['etapa_tipo'].fillna(0, inplace=True)
    df['etapa_tipo'] = df['etapa_tipo'].fillna(0)
    df['etapa_tipo'] = df['etapa_tipo'].replace('', 0)

    # Reemplazar etapa_titulo, etapa_1_fecha, etapa_1_hora nulos o vacíos por ''
    df[['etapa_titulo', 'etapa_1_fecha', 'etapa_1_hora']] = df[['etapa_titulo', 'etapa_1_fecha', 'etapa_1_hora']].fillna('')

    # Identificar registros con etapa_tipo=2 y etapa_tipo=3 en el mismo fk_servicio
    # etapa_2 = df[df['etapa_tipo'] == 2]
    etapa_2 = df[df['etapa_tipo'] == 2][['fk_servicio', 'etapa_1_fecha', 'etapa_1_hora']]
    etapa_3 = df[df['etapa_tipo'] == 3]
    
    # Merge etapa_2 con etapa_3 para combinar fechas y horas
    merged = pd.merge(df[df['etapa_tipo'] == 3][['fk_servicio']], etapa_2, on='fk_servicio', how='inner')
    
    # print('ETAPA 2 ANTES:', etapa_2)
    # print('ETAPA 3 ANTES:', df[df['etapa_tipo'] == 3][['fk_servicio', 'etapa_1_fecha', 'etapa_1_hora']])

    # Combinar etapa_1_fecha y etapa_1_hora de etapa_tipo=2 en etapa_tipo=3 para el mismo fk_servicio
    # for servicio_id in set(etapa_2['fk_servicio']).intersection(etapa_3['fk_servicio']):
    #     etapa_3_indices = etapa_3.index[etapa_3['fk_servicio'] == servicio_id]
    #     etapa_2_indices = etapa_2.index[etapa_2['fk_servicio'] == servicio_id]

    #     if len(etapa_2_indices) > 0 and len(etapa_3_indices) > 0:
    #         fecha_hora = etapa_2.loc[etapa_2_indices[0], ['etapa_1_fecha', 'etapa_1_hora']]
    #         df.loc[etapa_3_indices, ['etapa_1_fecha', 'etapa_1_hora']] = fecha_hora.values
    
    etapa_3.update(merged.set_index('fk_servicio')[['etapa_1_fecha', 'etapa_1_hora']])
    
    df[df['etapa_tipo'] == 3] = etapa_3
    
    # print('ETAPA 3 MERGED:', merged)

    return df




def rename_df(df):
    columnas_seleccionadas = ['fk_servicio', 'estado', 'eta_fecha', 'etapa_tipo', 'etapa_titulo',
           'etapa_1_fecha', 'etapa_1_hora', 'direccion_id_salida',
           'direccion_id_llegada', 'tiempo_minutos', 'distancia_mts',
           'posicion_tipo', 'cont_tamano', 'contenedor_peso_carga', 'comuna_nombre']
    
    new_columns = {
        'fk_servicio':'id', 
        'estado':'estado', 
        'eta_fecha':'eta_fecha', 
        'etapa_tipo':'etapa_tipo', 
        'etapa_titulo':'etapa_titulo',
        'etapa_1_fecha':'etapa_1_fecha', 
        'etapa_1_hora':'hora_presentacion', 
        'direccion_id_salida':'id_salida',
        'direccion_id_llegada':'id_llegada', 
        'tiempo_minutos':'tiempo_minutos', 
        'distancia_mts':'dist',
        'posicion_tipo':'posicion', 
        'cont_tamano':'cont_tamano', 
        'contenedor_peso_carga':'contenedor_peso', 
        'comuna_nombre':'comuna_nombre',
        'cli_desp_nombre':'cli_desp_nombre',
        'percentil_70_tiempo_cliente':'percentil_70_tiempo_cliente'
        }

    df.rename(columns=new_columns, inplace=True)
    
    return df 




def merged(date, df):
    with open( "app/queries/new_travels.txt", "r") as archivo:
        contenido = archivo.read()
        
    query = contenido.format(date)
    
    #df = connectionDB_todf(query)
    #print("hola",df)
    #df = transform_dataframe(df)   
    #print("hola2",df)
    
    
    columnas_seleccionadas = ['fk_servicio', 'estado', 'eta_fecha', 'etapa_tipo', 
                              'etapa_titulo', 'etapa_1_fecha', 'etapa_1_hora', 
                              'direccion_id_salida', 'direccion_id_llegada', 'tiempo_minutos', 
                              'distancia_mts', 'posicion_tipo', 'cont_tamano', 'contenedor_peso_carga', 
                              'comuna_nombre', 'cli_desp_nombre', 'comercial_nombre']
    
    df = df[columnas_seleccionadas]

    
    
    with open( "app/queries/tiempos_presentaciones.txt", "r") as archivo:
        contenido = archivo.read()
    query = contenido
    
    df2 = connectionDB_todf(query)

    
    # Suponiendo que 'tiempo_estadia' es una columna con valores de tiempo en formato de cadena
    # Convierte la columna 'tiempo_estadia' a un formato numérico (en minutos)
    df2['tiempo_estadia'] = pd.to_timedelta(df2['tiempo_estadia']).dt.total_seconds() / 60
    
    # Definir los percentiles para identificar outliers (por ejemplo, 5% y 95%)
    percentile_low = 10
    percentile_high = 80
    
    # Calcular los percentiles para identificar los valores límite
    low_limit = df2['tiempo_estadia'].quantile(percentile_low / 100)
    
    high_limit = df2['tiempo_estadia'].quantile(percentile_high / 100)
    high_limit = 300
    # Filtrar el DataFrame para eliminar outliers
    filtered_df = df2[(df2['tiempo_estadia'] >= low_limit) & (df2['tiempo_estadia'] <= high_limit)]
    
    # Realizar el groupby por 'cli_desp_nombre' en el DataFrame filtrado y calcular el percentil 90
    grouped = filtered_df.groupby('fk_cliente_despacho')['tiempo_estadia'].quantile(0.8)
    
    # Resetear el índice para obtener un DataFrame plano
    grouped = grouped.reset_index()
    
    # Renombrar la columna resultante
    grouped.columns = ['fk_cliente_despacho', 'percentil_70_tiempo']
    
    # Realizar left join de grouped sobre df utilizando 'cli_desp_nombre' como clave de unión
    merged_df = df.merge(grouped, left_on='cli_desp_nombre', right_on='fk_cliente_despacho', how='left')
    
    # Renombrar la columna 'percentil_70_tiempo' después del join
    merged_df.rename(columns={'percentil_70_tiempo': 'percentil_70_tiempo_cliente'}
                     , inplace=True)
    
    # Eliminar la columna duplicada 'fk_cliente_despacho' si es necesario
    merged_df.drop('fk_cliente_despacho', axis=1, inplace=True)
    
    # Reemplazar los valores NaN por 3 horas (180 minutos) en 'percentil_70_tiempo_cliente'
    merged_df.fillna({'percentil_70_tiempo_cliente': 180}, inplace=True)
    
    # Exportar el DataFrame merged_df a un archivo Excel en el directorio actual
    #merged_df.to_excel('percentile_73_data_without_outliers.xlsx', index=False)
    
    print("DataFrame exportado exitosamente a 'percentile_73_data_without_outliers.xlsx'")
    return merged_df

