# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 10:12:20 2023

@author: Ignacio Carvajal
"""

import pandas as pd 
import numpy as np 
import time
import os
from app.connection import merged, connectionDB_todf, transform_dataframe
from app.utils2 import preprocess, time_filler, date_filter, group_by_id, merge
#from gantt import *
from app.dfconsumer import df_portuarios, filter_by_date
from app.data_scrapper import * 
from datetime import datetime
from datetime import datetime, timedelta
from app.funciones import  encontrar_ventanas_sin_coincidencia, find_hours_of_max_values, find_hours_of_max_values_20, generate_hours_for_date, generate_availability_matrix, sum_columns_in_matrix
import psycopg2


#from funciones import *
from app.query_Serv import simulador, simulador_de_horarios, rename_df


class Planning:
    def __init__(self, T_estimado_retiros=40,  T_estimado_presentacion=180, T_estimado_descargas=10, T_viaje_retiros_SAI=30, T_viaje_retiros_STGO=160, T_viaje_retiros_VAL=120):

        self.df = {}
        self.df_visualization = {}
        self.min_hora_inicio = np.zeros(len(self.df))
        self.max_hora_salida = np.zeros(len(self.df))
        self.trackers = []
        self.Iv = []
        self.inicios = {}
        self.Fv = {}
        self.duration = {}
        self.capacidad = 50
        self.no_hay_viajes = False
        
    def insert_day(self, day):
        self.day = day
        # Input date string
        start_string = self.day + ' 00:00:00'
        end_string = self.day + ' 23:59:00'

        # Convert to a pandas datetime object
        start_date = pd.to_datetime(start_string)
        end_date = pd.to_datetime(end_string)

        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        
        
    def Querys(self):
        self.no_hay_viajes = False
        #me situo en el directorio actual
        directory = os.getcwd()
        # Directorio donde crear la carpeta
        directorio_base =  "/static/tmp/"  # Ruta base donde deseas crear la carpeta
        
        # Obtener la fecha que vamos a correr
        fecha = self.start_date 

        # Formatear la fecha como una cadena (por ejemplo, "2023-09-22")
        fecha_formateada = fecha.strftime("%Y-%m-%d")
        self.fecha_formateada = fecha.strftime("%d-%m-%Y")
       
        # Comprobar si la carpeta ya existe antes de crearla
        if not os.path.exists(os.path.join(directorio_base, fecha_formateada)):
            os.mkdir(os.path.join(directorio_base, fecha_formateada))
        
        
        with open( "app/queries/new_travels.txt", "r") as archivo:
            contenido = archivo.read()
        query = contenido.format(self.fecha_formateada)
        
        fecha_ahora = datetime.now()

        # Formatear la fecha y hora como una cadena
        fecha_hora_formateada = fecha_ahora.strftime("%Y-%m-%d %H:%M:%S")
        # Reemplazar ":" por "_" en la hora
        fecha_hora_formateada = fecha_hora_formateada.replace(":", "-")        
        
        df = connectionDB_todf(query)

        if df.empty:
            self.no_hay_viajes = True
            pass
        else:
            #df.to_excel( "/static/tmp/" + str(fecha_formateada) + "/query_travels" + str(fecha_hora_formateada) + ".xlsx", index=False)
            df = transform_dataframe(df)
            pd.set_option('display.float_format', '{:.0f}'.format)
            df = merged(self.fecha_formateada, df)
            df = rename_df(df)
            self.df = df
            df = pd.DataFrame(self.df)

            trackers = []

            trackers = sorted(trackers, key=lambda x: x[2])
            self.trackers = []
            for trucker in trackers:
                self.trackers.append(str((trucker[0], trucker[1])))

 
    def preprocessing(self):
        
        try: 
            df_port = filter_by_date(data_scrapper(), self.start_date, self.end_date)# df_portuarios(self.start_date, self.end_date, self.download)
        except:
            print('Error al descargar directos diferidos')
            df_port = pd.DataFrame(columns=['contenedor', 'fecha', 'comuna', 'empresa', 'servicios', 'cont_tamano', 'contenedor_peso'])
        
        if self.no_hay_viajes:
            pass
        else:

            self.df = preprocess(self.df)

            self.df = date_filter(self.df, self.start_date, self.end_date)
            self.df, self.df_visualization = time_filler(self.df, df_port)
            
            self.df = self.df[["id", "hora_salida", "hora_llegada"]]
            self.df, self.min_hora_inicio, self.max_hora_salida = group_by_id(
                self.df_visualization)
        
    def dict_creator(self):
        
        if self.no_hay_viajes:
            pass
        else:
            #se generan la lista de horas 
            self.dates_with_hours = generate_hours_for_date(self.fecha_formateada)
            
            #creo copia del dataframe
            df_20 = self.df_visualization.copy()
            self.df_20 = df_20[df_20["cont_tamano"]=="20"]   
            self.df_20_pesados = self.df_20[self.df_20['peso_cont']<10000]
            
            # Llama a la función para obtener la matriz de disponibilidad
            #si la hora de self.dates_with_hours tiene un servicio activo, 
            #es decir, esta entre la hora de inicio y fin de un viaje, el valor en la matriz es 1
            self.availability_matrix = generate_availability_matrix(self.df_visualization, self.dates_with_hours)
            self.availability_matrix_20 = generate_availability_matrix(self.df_20, self.dates_with_hours)
            self.availability_matrix_20_pesados = generate_availability_matrix(self.df_20_pesados, self.dates_with_hours)
        
            
            # Llama a la función para obtener el diccionario
            #suma todos los unos que hayan por hora. es decir que el diccionario tiene el total de servicios activos a cada hora
            self.hour_sum_dict = sum_columns_in_matrix(self.availability_matrix)
            self.hour_sum_dict_20 = sum_columns_in_matrix(self.availability_matrix_20)
            self.hour_sum_dict_20_pesados = sum_columns_in_matrix(self.availability_matrix_20_pesados)
            return self.hour_sum_dict
        
    def dict_creator_merged(self):
        
        if self.no_hay_viajes:
            pass
        else:
            #se generan la lista de horas 
            self.dates_with_hours = generate_hours_for_date(self.fecha_formateada)
            
            #creo copia del dataframe
            df_20 = self.df_visualization.copy()
            self.df_20 = df_20[df_20["cont_tamano"]=="20"]   
            self.df_20_pesados = self.df_20[self.df_20['peso_cont']<10000]
            
            # Llama a la función para obtener la matriz de disponibilidad
            #si la hora de self.dates_with_hours tiene un servicio activo, 
            #es decir, esta entre la hora de inicio y fin de un viaje, el valor en la matriz es 1
            self.availability_matrix = generate_availability_matrix(self.df_visualization, self.dates_with_hours)
            self.availability_matrix_20 = generate_availability_matrix(self.df_20, self.dates_with_hours)
            self.availability_matrix_20_pesados = generate_availability_matrix(self.df_20_pesados, self.dates_with_hours)
        
            
            # Llama a la función para obtener el diccionario
            #suma todos los unos que hayan por hora. es decir que el diccionario tiene el total de servicios activos a cada hora
            self.hour_sum_dict = sum_columns_in_matrix(self.availability_matrix)
            self.hour_sum_dict_20 = sum_columns_in_matrix(self.availability_matrix_20)
            self.hour_sum_dict_20_pesados = sum_columns_in_matrix(self.availability_matrix_20_pesados)
            
            merged_dict = {}
            
            for key in list(self.hour_sum_dict.keys()):
                merged_dict[key] = {
                    'activos': self.hour_sum_dict[key],
                    '20': self.hour_sum_dict_20[key],
                    '20_pesados': self.hour_sum_dict_20_pesados[key],
                }
            
            print("DICCIONARIO:", merged_dict)
            return merged_dict
    
    
    def simular_disponibilidad(self, fk_servicio, capacidad, capacidad_20, capacidad_20_pesados):
        if self.no_hay_viajes:
            return generate_hours_for_date(self.fecha_formateada), generate_hours_for_date(self.fecha_formateada), generate_hours_for_date(self.fecha_formateada)
        else:
            #se toma un servicio, se simula su inicio y fin con simulador y simulador_de_horarios entrega las horas a las que se puede agendar
            self.capacidad = capacidad
            self.capacidad_20 = capacidad_20
            self.capacidad_20_pesados = capacidad_20_pesados
            
            ventanas_horarias = simulador_de_horarios(self.dates_with_hours, simulador(fk_servicio))

            print("DICCIONARIO:", self.hour_sum_dict)
            fechas = find_hours_of_max_values(self.hour_sum_dict, self.capacidad)
            fechas_20 = find_hours_of_max_values_20(self.hour_sum_dict_20, self.capacidad_20)
            fechas_20_pesados = find_hours_of_max_values_20(self.hour_sum_dict_20_pesados, self.capacidad_20_pesados)
            #print(fechas)
            elementos_seleccionados = encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias)
            elementos_seleccionados_20 = encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias)
            elementos_seleccionados_20_pesados = encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias)

            return elementos_seleccionados, elementos_seleccionados_20, elementos_seleccionados_20_pesados
        
        
    def simular_disponibilidad_con_dict(self, lista_dict, fk_servicio, capacidad, capacidad_20, capacidad_20_pesados):
        if self.no_hay_viajes:
            return generate_hours_for_date(self.fecha_formateada), generate_hours_for_date(self.fecha_formateada), generate_hours_for_date(self.fecha_formateada)
        else:
            #se toma un servicio, se simula su inicio y fin con simulador y simulador_de_horarios entrega las horas a las que se puede agendar
            self.capacidad = capacidad
            self.capacidad_20 = capacidad_20
            self.capacidad_20_pesados = capacidad_20_pesados
            
            self.dates_with_hours = generate_hours_for_date(self.fecha_formateada)
            
            ventanas_horarias = simulador_de_horarios(self.dates_with_hours, simulador(fk_servicio))
            
            hour_sum_from_dict = {}
            hour_sum_from_dict_20 = {}
            hour_sum_from_dict_20_pesados = {}
            
            for diccionario in lista_dict:
                dt = datetime.strptime(diccionario['fecha'], '%Y-%m-%d %H:%M:%S')
                hora = dt.strftime("%d-%m-%Y %H:%M:%S")
                
                hour_sum_from_dict[hora] = diccionario['n_servicios_activos']
                hour_sum_from_dict_20[hora] = diccionario['n_servicios_20_activos']
                hour_sum_from_dict_20_pesados[hora] = diccionario['n_servicios_20_pesados_activos']

            fechas = find_hours_of_max_values(hour_sum_from_dict, self.capacidad)
            fechas_20 = find_hours_of_max_values_20(hour_sum_from_dict_20, self.capacidad_20)
            fechas_20_pesados = find_hours_of_max_values_20(hour_sum_from_dict_20_pesados, self.capacidad_20_pesados)
            #print(fechas)
            elementos_seleccionados = encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias)
            elementos_seleccionados_20 = encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias)
            elementos_seleccionados_20_pesados = encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias)

            return elementos_seleccionados, elementos_seleccionados_20, elementos_seleccionados_20_pesados
    
     
    def hour_sum_dict_generator(self, capacity):
        
        dates_with_hours = generate_hours_for_date(self.fecha_formateada)
 
        # Llama a la función para obtener la matriz de disponibilidad
        availability_matrix = generate_availability_matrix(self.df, dates_with_hours)
        
        # Llama a la función para obtener el diccionario
        hour_sum_dict = sum_columns_in_matrix(availability_matrix)
        
        # Ejemplo de uso con el diccionario hour_sum_dict
        max_hour, max_value = find_max_hour(hour_sum_dict)
        
        return find_hours_of_max_values(hour_sum_dict, capacity)
    
    def refresh(self):
        self.Querys()
        self.preprocessing()
        self.dict_creator()
        
    
    
    def travel_simulator(self):
        #traer el viaje individual en base a n_serv
        #insertar 
        #
        while True:
            self.capacidad = int(input("Ingrese la capacidad estimada: "))
            self.capacidad_20 = int(input("Ingrese la capacidad estimada de 20: "))
            self.capacidad_20_pesados = int(input("Ingrese la capacidad estimada de 20 pesados: "))
            fk_servicio = int(input("Ingrese numero de servicio: "))
            
            horarios, horarios_20, horarios_20_pesados = planning.simular_disponibilidad(fk_servicio, self.capacidad, self.capacidad_20, self.capacidad_20_pesados)
            print(horarios)#, horarios_20, horarios_20_pesados)
            hora = input("Inserte horario: ")
            
            if hora in horarios and hora in horarios_20 and hora in horarios_20_pesados:
                print("Confirmado!")
                print("Procesando...")
                self.Querys()
                self.preprocessing()
                self.refresh()
            else:
                if not (hora in horarios) and (hora in horarios_20_pesados or hora in horarios_20):
                    print("Faltan camiones!")
                    codigo = int(input("Porfavor ingrese el codigo: "))
                    if codigo == 1234:
                        print("Confirmado!")
                        
                        print("Procesando...")
                        self.refresh()
                        
                    else:
                        print("Te pillamos po compadre...")     
                        
                elif not (hora in horarios_20_pesados):
                    print("Faltan chasis de 20 para pesados!")
                    codigo = int(input("Porfavor ingrese el codigo: "))
                    if codigo == 1234:
                        print("Confirmado!")
                        print("Procesando...")
                        self.refresh()
                    else:
                        print("Te pillamos po compadre...")     
                        
                elif not (hora in horarios_20):
                    print("Faltan chasis o multis!")
                    codigo = int(input("Porfavor ingrese el codigo: "))
                
                    if codigo == 1234:
                        print("Confirmado!")
                        print("Procesando...")
                        self.refresh()

                    else:
                        print("Te pillamos po compadre...")                   

                
        return 



'''
# Input date string v12-10-2023
start_string = '2024-09-02 00:00:00'
end_string = '2024-09-02 23:59:00'
day = '2024-09-02'

# Convert to a pandas datetime object
start_date = pd.to_datetime(start_string)
end_date = pd.to_datetime(end_string)

planning = Planning() #define el objeto planificador
planning.insert_day(day) #define los atributos de hora de inicio y cierre en base al dia ingresado

planning.Querys()
planning.preprocessing()
planning.dict_creator()

planning.travel_simulator()

/simular_disponibilidad/{100202}/{'2024-07-12'}/{67}

Planning es una clase que tiene como finalidad principal recibir los viajes que han sido 
agendados hasta el momento y pueda aprobar o rechazar el agendamiento de los viajes. 

class Planning:
    def __init__(self, percentile, n_carriers, date):
        self.percentile = percentile
        self.n_carriers = n_carriers
        self.date = date
        
    def service_simulator(self, n_service):
        
        return start_hour, end_hour
    
    def service_count(self, n_service, date):
        simultaneous_trips = {}
        return simultaneous_trips

    def service_blocker(self, n_service, date):
        availability = {}
        
        return availability
    
'''

