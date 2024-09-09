# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 11:30:29 2023

@author: Ignacio Carvajal
"""

from app.funciones import *
from app.query_Serv import *
import os 
# Example of usage
date = '26-06-2024'


# Especifica la ruta del archivo Excel
archivo_excel = os.getcwd() +  '\\static\\tmp\\planificacion3.xlsx'

# Utiliza pd.read_excel para leer el archivo Excel y crear un DataFrame
df = pd.read_excel(archivo_excel)
df_20 = df.copy()
#df_20 = df_20[df_20["cont_tamano"]==20]
print(df_20.columns)


#se generan la lista de horas 
dates_with_hours = generate_hours_for_date(date)
print(dates_with_hours)

# Llama a la función para obtener la matriz de disponibilidad
availability_matrix = generate_availability_matrix(df, dates_with_hours)
print(availability_matrix)

# Llama a la función para obtener el diccionario
hour_sum_dict = sum_columns_in_matrix(availability_matrix)

#print(hour_sum_dict)

#plot_dict(hour_sum_dict)
# Ejemplo de uso con el diccionario hour_sum_dict
#max_hour, max_value = find_max_hour(hour_sum_dict)
#print(find_hours_of_max_values(hour_sum_dict, 73))


# Ajustar la opción para mostrar todos los valores
pd.set_option('display.max_rows', None)

serv_df = query_serv(101022)
#print(list(serv_df[serv_df['etapa_tipo']==2]['cli_desp_nombre']))
#print(serv_df)


#entrega todos los inicios y finales posibles en el dia para este servicio
ventanas_horarias = simulador_de_horarios(dates_with_hours, simulador(101022))
#print(serv_df[serv_df['etapa_tipo']==3]['tiempo_minutos'])
fechas = find_hours_of_max_values(hour_sum_dict, 45)


elementos_seleccionados = encontrar_ventanas_sin_coincidencia(fechas, ventanas_horarias)

print(elementos_seleccionados)
