# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 10:50:50 2023

@author: Ignacio Carvajal
"""





import pandas as pd 
import numpy as np 
import os
from connection import merged, connectionDB_todf, transform_dataframe
from utils2 import preprocess, time_filler, date_filter, group_by_id, merge
#from gantt import *
from dfconsumer import df_portuarios
from datetime import datetime
from datetime import datetime, timedelta
from funciones import  encontrar_ventanas_sin_coincidencia, find_hours_of_max_values, generate_hours_for_date, generate_availability_matrix, sum_columns_in_matrix
import psycopg2
from query_Serv import simulador
from planner import Planning
#from funciones import *
from query_Serv import simulador_de_horarios, rename_df



# Input date string
start_string = '2023-11-20 00:00:00'
end_string = '2023-11-20 23:59:00'
day = '2023-11-20'


    
    
def simular_servicio(dictionary):
    
    fk_servicio = dictionary["fk_servicio"]
    day = dictionary["day"]
    capacidad = dictionary["capacidad"]
    planning = Planning()
    planning.insert_day(str(day))
    planning.Querys()
    planning.preprocessing()
    dictionary = planning.dict_creator()
    horarios, horarios_20, horarios_20_pesados = planning.simular_disponibilidad(fk_servicio, capacidad)
    return {"horarios": horarios, "horarios_20": horarios_20, "horarios_20_pesados": horarios_20_pesados}

dictionary = {"fk_servicio": 87587, "day": '27-11-2023', "capacidad": 48}
print(simular_servicio(dictionary).items())