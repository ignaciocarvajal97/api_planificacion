

# import all the libraries
import time
import pandas as pd
import numpy as np
import random
import os
from datetime import datetime
from datetime import datetime, timedelta
import datetime
#from travel_dataframe import *

comunas_santiago_chile = [
    "Santiago",
    "Providencia",
    "Las Condes",
    "Vitacura",
    "Ñuñoa",
    "La Reina",
    "Macul",
    "La Florida",
    "Maipú",
    "Peñalolén",
    "Estación Central",
    "Quilicura",
    "Recoleta",
    "Independencia",
    "Conchalí",
    "Huechuraba",
    "Quinta Normal",
    "Cerrillos",
    "Pudahuel",
    "Lo Prado",
    "San Miguel",
    "San Joaquín",
    "La Cisterna",
    "San Ramón",
    "La Granja",
    "La Pintana",
    "El Bosque",
    "Pedro Aguirre Cerda",
    "Lo Espejo",
    "Cerro Navia",
    "Renca",
    "Lo Barnechea",
    "Puente Alto",
    "La Florida",
    "La Pintana",
    "El Bosque",
    "Lo Espejo",
    "Cerro Navia",
    "Renca",
    "Lo Barnechea",
    "Puente Alto"
]

def preprocess(df1):
    # crear copia
    df = df1.copy()
    pd.set_option('display.max_columns', None)

    # preprocesar dataframe
    df = df[df["hora_presentacion"].astype(bool)]
    df = df.dropna(subset=['etapa_1_fecha'])
    df = df.drop(df[df['etapa_1_fecha'] == '0'].index)
    df = df.fillna(180)

    return df

def date_filter(df1, fecha_referencia, fecha_referencia_fin):
    
    # crear copia
    df = df1.copy()
    
    #print("hola1", df)
    pd.set_option('display.max_columns', None)

    #creamos dos listas a partir de las columnas
    hora_presentacion = list(df['hora_presentacion'])
    fecha = list(df['etapa_1_fecha'])
    
    #por cada elemento del df
    for idx in range(len(df)):
        
        #si la hora de llegada dice cero lo pasamos a formato de hora
        if hora_presentacion[idx] == '0':
            hora_presentacion[idx] = '00:00'
        #concatenamos 
        hora_presentacion[idx] = str(fecha[idx]) + ' ' + str(hora_presentacion[idx])
    
    df["hora_presentacion"] = hora_presentacion
    # Convertir la columna 'hora_llegada' a tipo 'datetime'
    try:
        df['hora_presentacion'] = pd.to_datetime(df['hora_presentacion'], format='%d-%m-%Y %H:%M')
    except:
        print("hora del planeta de los simios")
        
    # Crear la columna 'hora_llegada_timestamp' como timestamps
    df['hora_llegada_timestamp'] = df['hora_presentacion'].apply(lambda x: x.timestamp())
    
    # Convertir la fecha de referencia a timestamp
    timestamp_referencia = fecha_referencia.timestamp()
    timestamp_referencia_fin = fecha_referencia_fin.timestamp()
    #print("hola2", df)
    
    print('hola' , df['hora_llegada_timestamp'], timestamp_referencia)

    # Convertir timestamp_referencia a datetime
    timestamp_referencia_dt = pd.to_datetime(timestamp_referencia, unit='s')  # Ajusta 's' si es en segundos o 'ms' si es en milisegundos
    # Convertir timestamp_referencia a datetime
    timestamp_referencia_fin_dt = pd.to_datetime(timestamp_referencia_fin, unit='s')  # Ajusta 's' si es en segundos o 'ms' si es en milisegundos

    #df['hora_llegada_timestamp'] = pd.to_datetime(df['hora_llegada_timestamp'])

    # Luego realiza la comparación
    df = df[df['hora_llegada_timestamp'] > timestamp_referencia]
    df = df[df['hora_llegada_timestamp']< timestamp_referencia_fin] 
    #print("hola3", df)
    return df

def time_filler(df1, df_portuarios, T_estimado_retiros=100,  T_estimado_presentacion=175, T_estimado_descargas=30, T_viaje_retiros_SAI=50, T_viaje_retiros_STGO=160, T_viaje_retiros_VAL=150, T_estimado_devoluciones=55, T_viajes_devolucion_SAI=40, T_viajes_devolucion_VAL=120, T_viajes_devolucion_STGO=160):#T_estimado_retiros=40,  T_estimado_presentacion=150, T_estimado_descargas=10, T_viaje_retiros_SAI=40, T_viaje_retiros_STGO=160, T_viaje_retiros_VAL=120, T_estimado_devoluciones=40, T_viajes_devolucion_SAI=40, T_viajes_devolucion_VAL=120, T_viajes_devolucion_STGO=160):
    
    
    df = df1.copy()
    idservice = list(df["id"])
    hora_salida = list(df['hora_presentacion'])
    hora_llegada = list(df['hora_llegada_timestamp'])
    tiempo_minutos = list(df['tiempo_minutos'])
    etapa_tipo = list(df['etapa_tipo'])
    comuna = list(df['comuna_nombre'])
    cont_tamano = list(df['cont_tamano'])
    peso_cont = list(df['contenedor_peso'])
    tiempo_en_cliente = list(df['percentil_70_tiempo_cliente'])
    
    
    
    #print(len(idservice), len(cont_tamano), len(peso_cont))
    
    #por cada elemento del df
    df_visualization = {}
    df_visualization["id"] = []
    df_visualization["etapa"] = []
    df_visualization["DT inicio"] =  []
    df_visualization["DT final"] =  []
    df_visualization["cont_tamano"] = []
    df_visualization["peso_cont"] = []

    for idx in range(len(df)):
        
        if type(hora_salida[idx]) == float:
            hora_salida[idx] = datetime.datetime.fromtimestamp(hora_salida[idx])
        if type(hora_llegada[idx]) == float:
            hora_llegada[idx] = datetime.datetime.fromtimestamp(hora_llegada[idx])
        
        #presentacion
        if etapa_tipo[idx] == 2:#presentaciones
        
            #creando la instancia de trayecto
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("trayecto")
            df_visualization["DT inicio"].append(hora_salida[idx] - timedelta(minutes=tiempo_minutos[idx]))
            df_visualization["DT final"].append(hora_salida[idx])

            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
            
            #creando instancia de presentacion en cliente 
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("presentacion")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_salida[idx] + timedelta(minutes=tiempo_en_cliente[idx]))
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
            #creando instancia devolucion de vacio
            
            #hora de llegada es igual a la hora en que se llega al lugar de presentacion
            #mas el tiempo de presentacion, i.e. el tiempo de desconsolidacion
            #mas el tiempo estimado por google para el regreso
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=tiempo_en_cliente[idx]) 
            
            #el tiempo de salida es igual al tiempo en que debemos llegar a cumplir 
            #menos el tiempo de viaje estimado por google 
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=tiempo_minutos[idx])
        #devolucion de vacios 
        elif etapa_tipo[idx] == 3 and comuna[idx] == 'San Antonio':#devolucion vacio 
            hora_salida[idx] = hora_salida[idx] + timedelta(minutes=tiempo_en_cliente[idx]) 
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=tiempo_minutos[idx]) + timedelta(minutes=T_estimado_devoluciones) + timedelta(minutes=T_viajes_devolucion_SAI)
            
            #creando instancia de devolucion de vacio
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("devolucion_vacio_sai")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])

        elif etapa_tipo[idx] == 3 and comuna[idx] == 'Valparaíso' or comuna[idx] == 'Cartagena':#devolucion vacio 
            hora_salida[idx] = hora_salida[idx] + timedelta(minutes=tiempo_en_cliente[idx]) 
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=tiempo_minutos[idx]) + timedelta(minutes=T_estimado_devoluciones) + timedelta(minutes=T_viajes_devolucion_VAL)
            
            #creando instancia de devolucion de vacio
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("devolucion_vacio_val")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
        
        elif etapa_tipo[idx] == 3 and comuna[idx] in comunas_santiago_chile:#devolucion vacio 
            hora_salida[idx] = hora_salida[idx] + timedelta(minutes=tiempo_en_cliente[idx]) 
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=tiempo_minutos[idx]) + timedelta(minutes=T_estimado_devoluciones) + timedelta(minutes=T_viajes_devolucion_STGO)
            
            #creando instancia de devolucion de vacio
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("devolucion_vacio_stgo")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])

        #retiros de full
        elif etapa_tipo[idx] == 1 and comuna[idx] == 'San Antonio': #retiro de contenedores full
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=T_estimado_retiros) 
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=T_viaje_retiros_SAI)
            #creando instancia de retiros full
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("retiro_full_sai")
            df_visualization["DT inicio"].append( hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
            
        elif etapa_tipo[idx] == 1 and (comuna[idx] == 'Valparaíso' or comuna[idx] == 'Cartagena'):#retiro de contenedores full     
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=T_estimado_retiros) 
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=T_viaje_retiros_VAL)
            
            #creando instancia de retiros full
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("retiro_full_val")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
        
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
            
        elif etapa_tipo[idx] == 1 and comuna[idx] in comunas_santiago_chile: #retiro de contenedores full
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=T_estimado_retiros) 
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=T_viaje_retiros_STGO)
        
            #creando instancia de retiros full
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("retiro_full_stgo")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
        
        
        #modificar para manana 
        elif etapa_tipo[idx] == 0: #almacenamiento
   
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=T_estimado_descargas)
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=tiempo_minutos[idx])
           
            #creando instancia de presentacion en cliente 
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("almacenamiento")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
    
    #print(len(df_visualization["id"]), len(df_visualization["cont_tamano"]), len(df_visualization["peso_cont"]))
    
############################################################# retiros desde portuarios ###########################3

    df2 = df_portuarios.copy()
    
    idservice = idservice + list(df2["servicios"])
    hora_salida = hora_salida + list(df2['fecha'])
    hora_llegada = hora_llegada + list(df2['fecha'])
    comuna = comuna + list(df2['comuna'])
    cont_tamano = cont_tamano + list(df2['cont_tamano'])
    peso_cont = peso_cont + list(df2['contenedor_peso'])
    
    #print(len(idservice), len(cont_tamano), len(peso_cont))
    
    for idx in range(len(df), len(df_portuarios) + len(df)): 
        #retiros de full
        if  comuna[idx] == 'San Antonio': #retiro de contenedores full
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=T_estimado_retiros) 
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=T_viaje_retiros_SAI)
            #creando instancia de retiros full
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("retiro_full_sai")
            df_visualization["DT inicio"].append( hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
            
        elif (comuna[idx] == 'Valparaíso' or comuna[idx] == 'Cartagena'):#retiro de contenedores full     
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=T_estimado_retiros) 
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=T_viaje_retiros_VAL)
            
            #creando instancia de retiros full
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("retiro_full_val")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
            
        elif comuna[idx] in comunas_santiago_chile:#retiro de contenedores full
            hora_llegada[idx] = hora_salida[idx] + timedelta(minutes=T_estimado_retiros) 
            hora_salida[idx] = hora_salida[idx] - timedelta(minutes=T_viaje_retiros_STGO)
        
            #creando instancia de retiros full
            df_visualization["id"].append(idservice[idx])
            df_visualization["etapa"].append("retiro_full_stgo")
            df_visualization["DT inicio"].append(hora_salida[idx])
            df_visualization["DT final"].append(hora_llegada[idx])
            
            df_visualization["cont_tamano"].append(cont_tamano[idx])
            df_visualization["peso_cont"].append(peso_cont[idx])
    
    #print(len(df_visualization["id"]), len(df_visualization["cont_tamano"]), len(df_visualization["peso_cont"]))
    
    # Convert dictionary to DataFrame
    df_visualization = pd.DataFrame(df_visualization)
    df_visualization = df_visualization.drop_duplicates()
    

    
    #print(df)
    
    #print(len(hora_salida))
    #print(len(hora_llegada))
    print("_------------------{")
    #print(df_portuarios)
    df_model = pd.DataFrame()

    df_model["id"] = idservice 
    df_model['hora_salida'] = hora_salida
    df_model['hora_llegada'] = hora_llegada
    df_model = df_model.drop_duplicates()
    #print("model", len(df_model))
    #print("port", len(df_portuarios)-1)
    #print("vis", len(df_visualization))
    
    return df_model, df_visualization 

import os
import pandas as pd

def group_by_id(df):
    # Imprimir el DataFrame filtrado print(df_filtrado)
    #print(df)
    grouped_df = df.groupby('id')

    min_hora_inicio = grouped_df['DT inicio'].min()
    max_hora_salida = grouped_df['DT final'].max()

    # Configurar pandas para mostrar todas las columnas
    pd.set_option('display.max_columns', None)

    df2 = pd.DataFrame({
        'id': df['id'].unique(),
        'DT inicio': min_hora_inicio.values,
        'DT final': max_hora_salida.values
    })
    
    directory = os.getcwd()
    #df2.to_excel(directory + '\\static\\tmp\\planificacion3.xlsx')
    return df2, min_hora_inicio, max_hora_salida

def merge(df1, df2):
    
    # Realizar el merge basado en las columnas 'ID_Servicio' y 'ID_Serv'
    df_resultado = pd.merge(df1, df2, left_on='id', right_on='id', how='left')
    
    return df_resultado

def process_result(df_resultado):
    
    #identifica el tipo de contenedor: de 40, 45, nulo, 20 liviano, 20 pesado
    tipo_cont = []
    for tamano, peso in zip(df_resultado["cont_tamano"], df_resultado["peso_cont"]):
        
        if tamano == "40":
            tipo_cont.append("40")
            
        elif tamano == "45":
            tipo_cont.append("45")
            
        elif tamano == "20":
            if peso > 10000:
                tipo_cont.append("20 pesado")
                
            else:
                tipo_cont.append("20 liviano")
        else:
            tipo_cont.append("lcl?")
                
    df_resultado["tipo_cont"] = tipo_cont
    # filtra solo las columnas necesarias
    selected_columns = ["id", "Trackers", "etapa", "DT inicio",	"DT final",	"tipo_cont"]
    df_resultado = df_resultado[selected_columns]
    return df_resultado


def delete(directory):
    # Comprobar si el archivo existe y eliminarlo si es necesario
    file_path = os.path.join(directory, 'static', 'tmp', 'planificacion2.xlsx')
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            #print(f"Archivo '{file_path}' eliminado con éxito.")
        except OSError as e:
            print(f"No se pudo eliminar el archivo '{file_path}': {e}")

    
    
    
import requests

def obtener_hora_pais(pais):
    # URL de la API de World Time API
    url = f'http://worldtimeapi.org/api/timezone/{pais}'
    
    try:
        # Realizar la solicitud GET a la API
        response = requests.get(url)
        
        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Parsear la respuesta JSON
            data = response.json()
            # Obtener la hora actual
            hora_actual = data['datetime']
            return pd.to_datetime(hora_actual)#.strftime('%d-%m-%Y %H:%M:%S')
        else:
            return f"No se pudo obtener la hora para {pais}. Código de estado: {response.status_code}"
    except Exception as e:
        return f"Error al realizar la solicitud: {e}"