# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 16:24:33 2023

@author: Ignacio Carvajal
"""
#from planificación import 
import os 
from app.connection import *
from app.funciones import *
import numpy as np


#toma un numero de servicio y entrega una query con todas las etapas de este
def query_serv(n_servicio):
    n_serv = n_servicio
    directory = os.getcwd()
    with open("app/queries/travel.txt", "r") as archivo:
        contenido = archivo.read()
    query = contenido.format(n_serv)
    serv_df = connectionDB_todf(query)
    return serv_df


def procesar_duracion(duracion, valor_por_defecto):
    # Verificar si duracion es un número válido (int o float) y no NaN
    if isinstance(duracion, (int, float)) and not np.isnan(duracion):
        return duracion + 60
    else:
        return valor_por_defecto

#toma un numero de servicio, saca la query de las etapas
#saca la direccion de la presentacion
#saca el tiempo de estadia del servicio
#simula los minutos que se demora antes de la presentacion y despues 
def simulador(n_servicio):
    serv_df = query_serv(n_servicio)
    id_direccion = list(serv_df[serv_df['etapa_tipo']==2]['cli_desp_nombre'])[0]
    directory = os.getcwd()
    
    with open("app/queries/tiempo_estadia.txt", "r") as archivo:
        contenido = archivo.read()
    query = contenido.format(id_direccion)
    #print(query)
    tiempos_estadia_df = connectionDB_todf(query)

    tiempos_estadia = list(tiempos_estadia_df['tiempo_estadia'])
    
    
    tiempos_estadia_df['tiempo_estadia'] = pd.to_timedelta(tiempos_estadia_df['tiempo_estadia']).dt.total_seconds() / 60
    
    # Definir los percentiles para identificar outliers (por ejemplo, 5% y 95%)
    percentile_low = 10
    percentile_high = 80
    
    # Calcular los percentiles para identificar los valores límite
    low_limit = tiempos_estadia_df['tiempo_estadia'].quantile(percentile_low / 100)
    
    high_limit = tiempos_estadia_df['tiempo_estadia'].quantile(percentile_high / 100)
    high_limit = 300
    # Filtrar el DataFrame para eliminar outliers
    filtered_df = tiempos_estadia_df[(tiempos_estadia_df['tiempo_estadia'] >= low_limit) & (tiempos_estadia_df['tiempo_estadia'] <= high_limit)]
    
    tiempos_estadia = list(filtered_df['tiempo_estadia'])
    if len(tiempos_estadia) == 0:
        tiempos_estadia = [180]
    

    tiempos_en_minutos = []
    for tiempo in tiempos_estadia:
        #tiempo_dt = datetime.strptime(tiempo, '%H:%M:%S')
        #minutos = tiempo_dt.hour * 60 + tiempo_dt.minute + tiempo_dt.second / 60
        tiempos_en_minutos.append(tiempo)


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
    if len(tiempos_en_minutos)>0:
        # Calcular el percentil 60
        percentil_tiempo_estadia = np.percentile(tiempos_en_minutos, 100)
    else:
        percentil_tiempo_estadia = 240
        
    comuna_devolucion = list(serv_df[serv_df['etapa_tipo']==3]['comuna_nombre'])[0]
    if comuna_devolucion == 'San Antonio':
        T_viaje_retorno = 40
    elif (comuna_devolucion == 'Valparaíso' or comuna_devolucion == 'Cartagena'):
        T_viaje_retorno = 120
    elif comuna_devolucion in comunas_santiago_chile:
        T_viaje_retorno = 160
    else:
        T_viaje_retorno = 160
    T_estimado_devolucion = 55
    T_viaje_devolucion = list(serv_df[serv_df['etapa_tipo']==3]['tiempo_minutos'])[0]
    T_viaje_presentacion = list(serv_df[serv_df['etapa_tipo']==2]['tiempo_minutos'])[0]
    
    duracion_salida = T_viaje_presentacion
    
    duracion_llegada = (
    (percentil_tiempo_estadia or 180) + 
    (T_viaje_retorno or 120) + 
    (T_estimado_devolucion or 60) + 
    (T_viaje_devolucion or 60)
    )

    #duracion_llegada = percentil_tiempo_estadia + T_viaje_retorno + T_estimado_devolucion + T_viaje_devolucion 
    if list(serv_df[serv_df['etapa_tipo']==2]['cont_tamano'])[0] == "20" and int(list(serv_df[serv_df['etapa_tipo']==2]["contenedor_peso"])[0])<10000 :
        print("es de 20 pesado...")
        
    elif list(serv_df[serv_df['etapa_tipo']==2]['cont_tamano'])[0] == "20":
        print("es de 20...")

    else: 
        print("es de 40")
        
    #Procesar duracion_salida y duracion_llegada con valores por defecto
    duracion_salida = procesar_duracion(duracion_salida, 230)
    duracion_llegada = procesar_duracion(duracion_llegada, 300)

    return {'duracion_salida': duracion_salida , 'duracion_llegada': duracion_llegada }


#simula todas las ventanas horarias que usaria el servicio 
#entrega una lista de listas donde cada lista trae el inicio en el primer elememto, 
#horario de presentacion en el segundo elemento y el horario de final del ultimo elemento 
def simulador_de_horarios(fecha_lista, dict_simulador):
    ventanas_horarios = []
    for fecha_hora in fecha_lista:
        fecha_obj = datetime.strptime(fecha_hora, '%d-%m-%Y %H:%M:%S')
        inicio = fecha_obj - timedelta(minutes=dict_simulador['duracion_salida'])
        final = fecha_obj + timedelta(minutes=dict_simulador['duracion_llegada'])
        # Formatear las fechas en el nuevo formato
        inicio_str = inicio.strftime('%d-%m-%Y %H:%M:%S')
        fecha_obj_str = fecha_obj.strftime('%d-%m-%Y %H:%M:%S')
        final_str = final.strftime('%d-%m-%Y %H:%M:%S')
        ventanas_horarios.append([inicio_str, fecha_obj_str, final_str])

    return ventanas_horarios

def simulador_de_horarios(fecha_lista, dict_simulador):
    #print("listafechas", fecha_lista)
    ventanas_horarios = []
    for fecha_hora in fecha_lista:
        fecha_obj = datetime.strptime(fecha_hora, '%d-%m-%Y %H:%M:%S')
        inicio = fecha_obj - timedelta(minutes=dict_simulador['duracion_salida'])
        final = fecha_obj + timedelta(minutes=dict_simulador['duracion_llegada'])
        
        # Definir los límites de tiempo en el mismo día
        limite_inferior = fecha_obj.replace(hour=5, minute=0, second=0, microsecond=0)
        limite_superior = (fecha_obj + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
     
        print(inicio, final)
        # Filtrar las ventanas de tiempo basadas en los límites
        if inicio >= limite_inferior and final <= limite_superior:
            # Formatear las fechas en el nuevo formato
            inicio_str = inicio.strftime('%d-%m-%Y %H:%M:%S')
            fecha_obj_str = fecha_obj.strftime('%d-%m-%Y %H:%M:%S')
            final_str = final.strftime('%d-%m-%Y %H:%M:%S')
            ventanas_horarios.append([inicio_str, fecha_obj_str, final_str])
    
    # Entrega el inicio, hora de presentacion y final estimado del viaje
    return ventanas_horarios
"""
def simulador_de_horarios(fecha_lista, dict_simulador):
    
    ventanas_horarios = []
    for fecha_hora in fecha_lista:
        fecha_obj = datetime.strptime(fecha_hora, '%d-%m-%Y %H:%M:%S')
        inicio = fecha_obj - timedelta(minutes=dict_simulador['duracion_salida'])
        final = fecha_obj + timedelta(minutes=dict_simulador['duracion_llegada'])
        
        # Filtrar las horas menores a las 5 AM y mayores a las 21 horas
        if inicio.hour >= 5  and final.hour <= 21:
            # Formatear las fechas en el nuevo formato
            inicio_str = inicio.strftime('%d-%m-%Y %H:%M:%S')
            fecha_obj_str = fecha_obj.strftime('%d-%m-%Y %H:%M:%S')
            final_str = final.strftime('%d-%m-%Y %H:%M:%S')
            ventanas_horarios.append([inicio_str, fecha_obj_str, final_str])
    #entrega el inicio, hora de presentacion y final estimado del viaje
    return ventanas_horarios









# Ajustar la opción para mostrar todos los valores
pd.set_option('display.max_rows', None)

serv_df = query_serv(85115)
print(list(serv_df[serv_df['etapa_tipo']==2]['cli_desp_nombre']))
print(simulador(85115))

date = '18-10-2023'
dates_with_hours = generate_hours_for_date(date)

print(simulador_de_horarios(dates_with_hours, simulador(85115)))
#print(serv_df[serv_df['etapa_tipo']==3]['tiempo_minutos'])

"""