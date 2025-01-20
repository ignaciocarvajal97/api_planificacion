# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 09:25:30 2023

@author: Ignacio Carvajal
"""
from datetime import datetime
from datetime import datetime, timedelta
import time as t
import pandas as pd
from app.utils2 import obtener_hora_pais
def ajustar_valores(diccionario):
    # Encuentra la key con el mayor valor
    max_key = max(diccionario, key=diccionario.get)
    max_value = diccionario[max_key]
    
    # Convierte las keys a una lista y encuentra el índice de la key con el mayor valor
    keys = list(diccionario.keys())
    index = keys.index(max_key)
    
    # Ajusta los valores de las dos keys anteriores y las dos keys posteriores
    for i in range(index-4, index+5):
        if 0 <= i < len(keys):
            diccionario[keys[i]] = max_value
            
    return diccionario
"""
#genera una lista con las horas del dia date. las horas son todas cada 5 minutos 
def generate_hours_for_date(date):
    # Convert the date into a datetime object
    date_obj = datetime.strptime(date, '%d-%m-%Y')
    
    # List of hours in 5-minute intervals
    hours = []
    for hour in range(5, 23):
        for minute in range(0, 60, 15):
            hour_formatted = f'{hour:02d}:{minute:02d}:{00:02d}'
            hours.append(hour_formatted)
    pais = "America/Santiago"
    resultado = obtener_hora_pais(pais)

    # Generate the list of dates with hours
    dates_with_hours = [date_obj + timedelta(hours=int(h.split(':')[0]), minutes=int(h.split(':')[1])) for h in hours]
    
    # Format the dates in the desired format 'dd-mm-yyyy HH:MM'
    formatted_dates = [f'{date_obj.strftime("%d-%m-%Y")} {hour.strftime("%H:%M:%S")}' for hour in dates_with_hours]
    
    return formatted_dates
"""

import pytz  # Necesario para manejar zonas horarias
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo  # Utiliza zoneinfo para manejar zonas horarias
from app.time_ntp import now

def generate_hours_for_date(date):
    # Convert the date into a datetime object
    date_obj = datetime.strptime(date, '%d-%m-%Y')
    
    pais = "America/Santiago"
    resultado = now()# obtener_hora_pais(pais)
    print(resultado)
    resultado = resultado.replace(tzinfo=None)

    # Calculate the limit time (resultado + 3 hours)
    limit_time = resultado# + timedelta(hours=2.5)
    print(resultado)
    # List of hours in 5-minute intervals
    hours = []
    for hour in range(5, 23):  # Updated to include all hours of the day
        for minute in range(0, 60, 5):  # 5-minute intervals
            hour_formatted = f'{hour:02d}:{minute:02d}:{00:02d}'
            hours.append(hour_formatted)
    
    # Generate the list of datetime objects for the given date with timezone
    dates_with_hours = [date_obj + timedelta(hours=int(h.split(':')[0]), minutes=int(h.split(':')[1])) for h in hours]
    
    # Filter out dates that are less than or equal to the limit_time
    filtered_dates = [dt for dt in dates_with_hours ]#if dt > limit_time]
    
    # Format the dates in the desired format 'dd-mm-yyyy HH:MM:SS'
    formatted_dates = [f'{dt.strftime("%d-%m-%Y %H:%M:%S")}' for dt in filtered_dates]
    
    print("FORMATTED DATES:", formatted_dates)
    
    return formatted_dates
#genera una matriz que representa la planificacion del dia. lo hace poniendo un 1 si hay un viaje activo a esa hora
def generate_availability_matrix(dataframe, dates_with_hours):
    # Inicializar la matriz con ceros
    availability_matrix = pd.DataFrame(0, columns=dates_with_hours, index=dataframe.index)
    
    # Iterar sobre las filas del DataFrame
    print("generate_availability_matrix")
    # id                          107710
    # etapa              retiro_full_val
    # DT inicio      2024-09-10 13:00:00
    # DT final       2024-09-10 15:55:00
    # cont_tamano                     40
    # peso_cont                    12630
    t1 = t.perf_counter(), t.process_time()
    for index, row in dataframe.iterrows():
        #start_time = datetime.strptime(row['DT inicio'], '%Y-%m-%d %H:%M:%S')  # Convertir la hora de inicio a datetime
        #end_time = datetime.strptime(row['DT final'], '%Y-%m-%d %H:%M:%S')  # Convertir la hora de finalización a datetime
        
        start_time = row['DT inicio']  # Convertir la hora de inicio a datetime
        end_time = row['DT final']  # Convertir la hora de finalización a datetime
        
        # Marcar las horas en la matriz como 1 si están en el rango de tiempo
        for hour in dates_with_hours:
            hour_dt = datetime.strptime(hour, '%d-%m-%Y %H:%M:%S')
            if start_time <= hour_dt <= end_time:
                availability_matrix.loc[index, hour] = 1
    t2 = t.perf_counter(), t.process_time()
    print(f" Real time: {t2[0] - t1[0]:.2f} seconds")
    print(f" CPU time: {t2[1] - t1[1]:.2f} seconds")
    
    return availability_matrix



#suma las columnas de la matriz de representacion y mete los valores en un diccionario
#
def sum_columns_in_matrix(matrix):
    # Suma las columnas de la matriz
    column_sums = matrix.sum()
    
    # Convierte las sumas en un diccionario
    hour_sum_dict = column_sums.to_dict()
    #print(hour_sum_dict)
    # plot_dict(hour_sum_dict, len(hour_sum_dict.items()) )
    return ajustar_valores(hour_sum_dict)


#encuentra las horas en el diccionario que tienen el mayor numero de viajes activos 
#devuelve la hora y el valor maximo en cuestion
def find_max_hour(hour_sum_dict):
    max_hour = max(hour_sum_dict, key=hour_sum_dict.get)
    max_value = hour_sum_dict[max_hour]
    return max_hour, max_value 


def check_threshold(hour_sum_dict, threshold):
    for hour, value in hour_sum_dict.items():
        if value > threshold :
            return False
    return True

#encuentra las horas en el diccionario que tienen el mayor numero de viajes activos 
#devuelve las horas que superan la capacidad establecida
def find_hours_of_max_values(hour_sum_dict, capacity):
    """
    
    Parameters
    ----------
    hour_sum_dict : dict
        DESCRIPTION.

    Returns list of hours with the maximum of travels
    -------
    max_hours : list
        DESCRIPTION.

    """
    max_value = max(hour_sum_dict.values())
    max_hours = [hour for hour, value in hour_sum_dict.items() if value >= capacity]
    print("max_hours:", max_hours, "capacidad:", capacity)
    return max_hours

def find_hours_of_max_values_20(hour_sum_dict, capacity):
    """
    
    Parameters
    ----------
    hour_sum_dict : dict
        DESCRIPTION.

    Returns list of hours with the maximum of travels
    -------
    max_hours : list
        DESCRIPTION.

    """
    max_value = max(hour_sum_dict.values())
    max_hours = [hour for hour, value in hour_sum_dict.items() if value + 1 >= capacity]
    print("max_hours_20:", max_hours, "capacidad:", capacity)
    return max_hours

def initializator(df,date):

    dates_with_hours = generate_hours_for_date(date)
    
    # Llama a la función para obtener la matriz de disponibilidad
    availability_matrix = generate_availability_matrix(df, dates_with_hours)
    
    # Llama a la función para obtener el diccionario
    hour_sum_dict = sum_columns_in_matrix(availability_matrix)
    
    # Ejemplo de uso con el diccionario hour_sum_dict
    max_hour, max_value = find_max_hour(hour_sum_dict)

    return max_value 


import matplotlib.pyplot as plt



def plot_dict(dictionary, max_labels= 24):
    keys = list(dictionary.keys())
    values = list(dictionary.values())

    if max_labels and len(keys) > max_labels:
        step = len(keys) // max_labels
        keys = keys[::step]
        values = values[::step]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(keys, values)
    plt.xlabel('Claves')
    plt.ylabel('Valores')
    plt.title('Gráfico de Valores de Diccionario')
    plt.xticks(rotation=45, ha='right')

    for bar, value in zip(bars, values):
        plt.annotate(str(value), (bar.get_x() + bar.get_width() / 2, bar.get_height()), ha='center', va='bottom')

    plt.tight_layout()
    plt.show()

def encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias, devolucion_sai):
    indices = []
    #print("fecha : ", fechas)
    #print("ventanas horarias: ", ventanas_horarias)
    #ventanas horarias son todos los inicios y finales posibles para este viaje
    
    for i, ventana in enumerate(ventanas_horarias):
        inicio_ventana = datetime.strptime(ventana[0], '%d-%m-%Y %H:%M:%S')
        hora_presentacion = datetime.strptime(ventana[1], '%d-%m-%Y %H:%M:%S')
        final_ventana = datetime.strptime(ventana[2], '%d-%m-%Y %H:%M:%S')
        
        coincide = False
        
        
        for fecha in fechas:
            fecha_obj = datetime.strptime(fecha, '%d-%m-%Y %H:%M:%S')
            if inicio_ventana <= fecha_obj <= final_ventana:
                coincide = True
                break

        if not coincide:
            if devolucion_sai == 1: 
                indices.append(i)
            else:
                if hora_presentacion.hour < 17:
                    indices.append(i)
            print(ventanas_horarias[i][1])
    elementos_seleccionados = [ventanas_horarias[i][1] for i in indices]
    return elementos_seleccionados






"""
# Example of usage
date = '19-10-2023'


# Especifica la ruta del archivo Excel
archivo_excel = 'C:\\Users\\Usuario\\Desktop\\planificación\\static\\tmp\\planificacion3.xlsx'

# Utiliza pd.read_excel para leer el archivo Excel y crear un DataFrame
df = pd.read_excel(archivo_excel)

print(initializator(df,date))


dates_with_hours = generate_hours_for_date(date)
print(dates_with_hours)

# Llama a la función para obtener la matriz de disponibilidad
availability_matrix = generate_availability_matrix(df, dates_with_hours)
print(availability_matrix)

# Llama a la función para obtener el diccionario
hour_sum_dict = sum_columns_in_matrix(availability_matrix)

print(hour_sum_dict)

plot_dict(hour_sum_dict)
# Ejemplo de uso con el diccionario hour_sum_dict
max_hour, max_value = find_max_hour(hour_sum_dict)
print(find_hours_of_max_values(hour_sum_dict, 70))

print(f"La hora con el valor máximo es {max_hour} y el valor máximo es {max_value}.")


"""





# Example of usage
