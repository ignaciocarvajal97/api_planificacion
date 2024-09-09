# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 09:32:04 2023

@author: Ignacio Carvajal
"""

import pandas as pd 
import os
import shutil
import pandas as pd
import difflib
#from scrapper import download_secuences

directorio_actual = os.getcwd()
dpw_dir = directorio_actual + "\secuencias\\dpw"
tps_dir = directorio_actual + "\secuencias\\tps"
sti_dir = directorio_actual + "\secuencias\\sti"


def eliminar_contenido_directorio(ruta_directorio):
    try:
        for elemento in os.listdir(ruta_directorio):
            elemento_ruta = os.path.join(ruta_directorio, elemento)
            if os.path.isfile(elemento_ruta):
                os.unlink(elemento_ruta)
            elif os.path.isdir(elemento_ruta):
                shutil.rmtree(elemento_ruta)
        print(f"Contenido de '{ruta_directorio}' eliminado correctamente.")
    except Exception as e:
        print(f"Error al eliminar contenido de '{ruta_directorio}': {e}")



import pandas as pd

def concat_dataframes(dataframes_list):
    """
    Concatenates a list of DataFrames vertically (one after another),
    or returns the single DataFrame if there's only one in the list.
    
    Args:
        dataframes_list (list of pandas.DataFrame): List of DataFrames to be concatenated.
        
    Returns:
        pandas.DataFrame: Concatenated DataFrame if there are multiple DataFrames,
                          or the single DataFrame if there's only one.
    """
    if len(dataframes_list) < 1:
        # Crear un DataFrame vacío con las columnas requeridas
        column_names = ['contenedor', 'fecha', 'comuna', 'empresa']
        df = pd.DataFrame(columns=column_names)
        return df
    
    elif len(dataframes_list) == 1:
        return dataframes_list[0]
    
    elif len(dataframes_list) >1:
        #print(dataframes_list)
        concatenated_df = pd.concat(dataframes_list, ignore_index=True)
        return concatenated_df
    else:
        return pd.DataFrame()

def calcular_verificador_contenedor(numero_contenedor):
    letras = {
        'A': 10, 'B': 12, 'C': 13, 'D': 14, 'E': 15, 'F': 16, 'G': 17,
        'H': 18, 'I': 19, 'J': 20, 'K': 21, 'L': 23, 'M': 24, 'N': 25,
        'O': 26, 'P': 27, 'Q': 28, 'R': 29, 'S': 30, 'T': 31, 'U': 32,
        'V': 34, 'W': 35, 'X': 36, 'Y': 37, 'Z': 38, '?': 39, '0':0, 
        '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9
    }

    digitos_base = numero_contenedor[:10]
    suma = 0

    for i, caracter in enumerate(digitos_base):
        if caracter in letras:
            valor = letras[caracter]
            
            suma += valor * (2 ** i)

    verificador = suma % 11
    return str(verificador) if verificador < 10 else "0"

def transform_sti(input_df):
    """
    Transforms the input DataFrame by combining "Sigla" and "Numero" columns,
    formatting the "Programacion Despacho" column, and adding "comuna" column.
    
    Args:
        input_df (pandas.DataFrame): DataFrame with columns "Sigla", "Numero", "Dv", and "Programacion Despacho".
        
    Returns:
        pandas.DataFrame: Transformed DataFrame with columns "contenedor", "fecha", and "comuna".
    """
    
    input_df = input_df.dropna(subset=['Programacion Despacho'])
    transformed_df = pd.DataFrame()
    #input_df['Dv'] = input_df['Sigla'] + input_df['Numero'].astype(str)
    
    # Aplicar la función a la columna 'Dv'
    #input_df['Dv'] = input_df['Dv'].apply(calcular_verificador_contenedor)

    # Combine "Sigla" and "Numero" columns into "contenedor"
    
    transformed_df['contenedor'] = input_df['Sigla'] + input_df['Numero'].astype(str) + input_df['Dv'].astype(str) 
    # Combine "Sigla" and "Numero" columns into "contenedor" and fill "Numero" with leading zeros
    transformed_df['contenedor'] = input_df['Sigla'] + input_df['Numero'].astype(str).str.zfill(6) + input_df['Dv'].astype(str)

    
    # Format "Programacion Despacho" column and add "fecha" column
    # Elimina espacios en blanco adicionales en los valores de la columna 'Programacion Despacho'
    # Utiliza .loc para eliminar espacios en blanco adicionales en los valores de la columna 'Programacion Despacho'
    input_df.loc[:, 'Programacion Despacho'] = input_df['Programacion Despacho'].str.strip()

    

    transformed_df['fecha'] = pd.to_datetime(input_df['Programacion Despacho'], format="%d/%m/%Y %H:%M", errors='coerce')
 
    
    # Add "comuna" column with constant value 'San Antonio'
    transformed_df['comuna'] = 'San Antonio'
    transformed_df['empresa'] = 'sti'

    return transformed_df

def transform_tps(input_df):
    """
    Transforms the input DataFrame by selecting columns and adding "comuna" column.
    
    Args:
        input_df (pandas.DataFrame): DataFrame with columns "N°", "FECHA", "HORA", "CONTENEDOR", "TIPO", "ALM", "IMO".
        
    Returns:
        pandas.DataFrame: Transformed DataFrame with columns "contenedor", "fecha", and "comuna".
    """
    # Filter out rows without relevant columns
    #print(input_df.columns)
    filtered_df = input_df.dropna(subset=['CONTENEDOR'], how='all')
    
    transformed_df = pd.DataFrame()
    
    transformed_df['contenedor'] = filtered_df['CONTENEDOR']


    # Convert "FECHA" and "HORA" columns to datetime
    try:
        transformed_df['fecha'] = pd.to_datetime(
            filtered_df['FECHA'].astype(str) + ' ' + filtered_df['HORA'].astype(str),
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce'
        )
    except Exception as e:
        print("Error converting date-time:", e)
        print("Problematic rows:")
        problematic_rows = filtered_df[~filtered_df.apply(lambda row: row['FECHA'] == '' or row['HORA'] == '', axis=1)]
        
    
    # Add "comuna" column with constant value 'Valparaíso'
    transformed_df['comuna'] = 'Valparaíso'
    transformed_df['empresa'] = 'tps'
    return transformed_df


def transform_dpw(input_df):
    """
    Transforms the input DataFrame by selecting columns and adding "comuna" column.
    
    Args:
        input_df (pandas.DataFrame): DataFrame with columns "Unit Nbr", "Type", "Appt Time".
        
    Returns:
        pandas.DataFrame: Transformed DataFrame with columns "contenedor", "fecha", and "comuna".
    """
    transformed_df = pd.DataFrame()
    
    # Use "Unit Nbr" column for "contenedor"
    transformed_df['contenedor'] = input_df['Unit Nbr']
    
    # Format "Appt Time" column and add "fecha" column
    transformed_df['fecha'] = pd.to_datetime(input_df['Appt Time'], format='%d-%b-%y %H%M')
    
    # Add "comuna" column with constant value 'San Antonio'
    transformed_df['comuna'] = 'San Antonio'
    # Add "comuna" column with constant value 'San Antonio'
    transformed_df['empresa'] = 'dpw'
    
    return transformed_df



def calcular_similitud(texto1, texto2):
    # Elimina espacios y convierte a minúsculas
    texto1 = texto1.replace(" ", "").lower()
    texto2 = texto2.replace(" ", "").lower()
    
    # Calcula la similitud entre las cadenas
    similaridad = difflib.SequenceMatcher(None, texto1, texto2).ratio()

    return similaridad

def encontrar_fila_con_similitud(df, patron):
    mejor_similitud = 0
    numero_de_fila = None

    for index, row in df.iterrows():
        header = " ".join(map(str, row))
        similitud = calcular_similitud(patron, header)
        
        if similitud > mejor_similitud:
            mejor_similitud = similitud
            numero_de_fila = index + 1   # Suma 1 porque el índice comienza en 0

    return numero_de_fila

def excel_to_df():
    directorio_actual = os.getcwd()
    dpw_dir = directorio_actual + "\secuencias\dpw"
    tps_dir = directorio_actual + "\secuencias\\tps"
    sti_dir = directorio_actual + "\secuencias\sti"
    
    dfs = {}
    dfs["sti"] = []
    dfs["tps"] = []
    dfs["dpw"] = []
    

    #dpw
    for archivo in os.listdir(dpw_dir):
        if archivo.endswith(".xlsx"):
            archivo_ruta = os.path.join(dpw_dir, archivo)
            df1 = pd.read_excel(archivo_ruta, header=0)  # Leer el archivo Excel y convertirlo en DataFrame
            # Patrón a buscar en el encabezado
            patron = "N° FECHA HORA OBSERVACIONES CONTENEDOR TIPO ALM IMO"
            # Encuentra el número de fila con la mejor similitud
            numero_de_fila = encontrar_fila_con_similitud(df1, patron)
            print("hoalsd", numero_de_fila)
            df = pd.read_excel(archivo_ruta, header=numero_de_fila)
           
            dfs["dpw"].append(df)

    #tps
    for archivo in os.listdir(tps_dir):
        if archivo.endswith(".xlsx"):
           
            archivo_ruta = os.path.join(tps_dir, archivo)
            
            df1 = pd.read_excel(archivo_ruta, header=0)  # Leer el archivo Excel y convertirlo en DataFrame
            # Patrón a buscar en el encabezado
            patron = "N° FECHA HORA OBSERVACIONES CONTENEDOR TIPO ALM IMO"
            # Encuentra el número de fila con la mejor similitud
            numero_de_fila = encontrar_fila_con_similitud(df1, patron)
            #print("hoalsd", numero_de_fila)
            df = pd.read_excel(archivo_ruta, header=numero_de_fila)
            dfs["tps"].append(df)
            
        elif archivo.endswith(".xls"):
          
            archivo_ruta = os.path.join(tps_dir, archivo)
            df1 = pd.read_excel(archivo_ruta, header=0)  # Leer el archivo Excel y convertirlo en DataFrame
            
            patron = "N° FECHA HORA OBSERVACIONES CONTENEDOR TIPO ALM IMO"
            # Encuentra el número de fila con la mejor similitud
            numero_de_fila = encontrar_fila_con_similitud(df1, patron)
            df = pd.read_excel(archivo_ruta, header=numero_de_fila)
            
            dfs["tps"].append(df)

    #sti
    for archivo in os.listdir(sti_dir):
        if archivo.endswith(".xls"):
            archivo_ruta = os.path.join(sti_dir, archivo)
            df1 = pd.read_excel(archivo_ruta, header=0)  # Leer el archivo Excel y convertirlo en DataFrame
            # Patrón a buscar en el encabezado
            patron = "N° FECHA HORA OBSERVACIONES CONTENEDOR TIPO ALM IMO"
            # Encuentra el número de fila con la mejor similitud
            numero_de_fila = encontrar_fila_con_similitud(df1, patron)
            #print("hoalsd", numero_de_fila)
            df = pd.read_excel(archivo_ruta, header=numero_de_fila)
           
            dfs["sti"].append(df)

    dfs["sti"] = concat_dataframes(dfs["sti"])
    dfs["tps"] = concat_dataframes(dfs["tps"])
    dfs["dpw"] = concat_dataframes(dfs["dpw"])
    
    return dfs




def df_standardizer(dfs):
    df_list = []
    try:
        df_list.append(transform_sti(dfs["sti"]))
    except:
        print("Error de formato en sti")
        pass
    try:
        df_list.append(transform_tps(dfs["tps"]))
    except:
        print("Error de formato en tps")
        pass
    
    try:
        df_list.append(transform_dpw(dfs["dpw"]))
    except:
        print("Error de formato en dpw")
        pass
 
    return concat_dataframes(df_list)



def filter_containers(input_df, contenedores):
    """
    Filters the input DataFrame to include only rows with specified containers.
    
    Args:
        input_df (pandas.DataFrame): DataFrame with columns "contenedor", "fecha", "comuna".
        contenedores (list): List of container IDs to filter by.
        
    Returns:
        pandas.DataFrame: Filtered DataFrame containing rows with specified containers.
    """
    #filtered_df = input_df[input_df['contenedor'].isin(contenedores)]
    
    # Combinar los DataFrames basado en la columna 'contenedores'
    merged_df = input_df.merge(contenedores, on='contenedor')
    merged_df = merged_df.drop_duplicates(subset='contenedor')
    return merged_df
    

def remove_dashes_and_convert(elements_list):
    """
    Removes dashes from each element in the input list, converts to uppercase strings.
    
    Args:
        elements_list (list): List of elements.
        
    Returns:
        list: New list with elements without dashes and converted to uppercase strings.
    """
    cleaned_elements = [element.replace('-', '').upper() for element in elements_list]
    return cleaned_elements

    
from app.connection import connectionDB
    
                            
                            
def query_contenedores():
    
    query_contenedores =  """
            SELECT s.id, s.numero_contenedor, ct.conttam_tamano AS cont_tamano, s.contenedor_peso_carga
            FROM public.servicios AS s
            INNER JOIN public.contenedores_tamanos AS ct ON s.fk_contenedor_tamano = ct.conttam_id
            WHERE s."createdAt" >= NOW() - INTERVAL '75 days';

    """
                                
    #AND ser.estado != 2 AND ser.estado != 999
    rows = connectionDB(query_contenedores)
    
    contenedor = []
    servicios = []
    cont_tamano = []
    contenedor_peso_carga = []
    for row in rows:
        servicios.append(row[0])
        contenedor.append(row[1])
        cont_tamano.append(row[2])
        contenedor_peso_carga.append(row[3])
    contenedor = remove_dashes_and_convert(contenedor)
    
    dict_ = {"servicios":servicios,
             "contenedor":contenedor,
             "cont_tamano":cont_tamano,
             "contenedor_peso":contenedor_peso_carga}
    
    contenedores = pd.DataFrame(dict_)
    
    return contenedores


def filter_by_date(input_df, start_date, end_date):
    """
    Filters the input DataFrame based on the date range.
    
    Args:
        input_df (pandas.DataFrame): DataFrame to be filtered.
        start_date (str or pandas.Timestamp): Start date of the date range.
        end_date (str or pandas.Timestamp): End date of the date range.
        
    Returns:
        pandas.DataFrame: Filtered DataFrame containing only rows within the specified date range.
    """
    mask = (input_df['fecha'] >= start_date) & (input_df['fecha'] <= end_date)
    filtered_df = input_df[mask]
    return filtered_df

# Configurar Pandas para mostrar todo el contenido de los DataFrames
pd.set_option('display.max_columns', None)  # Mostrar todas las columnas
pd.set_option('display.max_rows', None)     # Mostrar todas las filas




def df_portuarios(start_date, end_date, download=True):
    if download == True:
        
        #download_secuences()
        contenedores = query_contenedores()
        dfs = excel_to_df()
        retiros = df_standardizer(dfs)
        retiros = filter_by_date(retiros, start_date, end_date)
    else:
        contenedores = query_contenedores()
        dfs = excel_to_df()
        retiros = df_standardizer(dfs)
        retiros = filter_by_date(retiros, start_date, end_date)
    filter_containersretiros = filter_containers(retiros, contenedores)
    filter_containersretiros.to_excel("retiros//retiros_puerto.xlsx", index=False)
    return filter_containersretiros


"""
# Input date string
start_string = '2023-09-21 00:00:00'
end_string = '2023-09-21 23:59:00'

# Convert to a pandas datetime object
start_date = pd.to_datetime(start_string)
end_date = pd.to_datetime(end_string)
df = df_portuarios(start_date, end_date, True)

#dfs = excel_to_df()

#print(df)


#print(len(df))
"""