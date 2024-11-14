# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 11:41:11 2023

@author: Ignacio Carvajal
"""


import gc
from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from app.planner import Planning

# Obtén la fecha actual
today = datetime.now()

# Calcula la fecha de mañana
tomorrow = today + timedelta(days=1)

# Convierte la fecha del día siguiente en una cadena
day = tomorrow.strftime('%Y-%m-%d')

app = FastAPI()

# Configura el middleware CORS para permitir cualquier origen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Puedes ajustar esto según tus necesidades
    allow_methods=["*"],
    allow_headers=["*"],
)

class RequerimientoPlanificacion(BaseModel):
    n_servicio: int
    day: str

@app.get("/")
def read_root():
    try:
        planning = Planning() 
        planning.insert_day(day)
        planning.Querys()
        planning.preprocessing()
        dictionary = planning.dict_creator()
        return {"message": dictionary}
    finally:
        # Libera los objetos grandes
        del dictionary
        del planning
        gc.collect()  # Forzar recolección de basura

@app.get("/simular_disponibilidad/{fk_servicio}/{day}/{capacidad}/{capacidad_20}/{capacidad_20_pesados}")
def simular_servicio(fk_servicio: int, day: str, capacidad: int, capacidad_20: int, capacidad_20_pesados: int):
    try:
        planning = Planning() 
        planning.insert_day(day)
        planning.Querys()
        planning.preprocessing()
        dictionary = planning.dict_creator()

        horarios, horarios_20, horarios_20_pesados = planning.simular_disponibilidad(fk_servicio, capacidad, capacidad_20, capacidad_20_pesados)
        return {"horarios": horarios, "horarios_20": horarios_20, "horarios_20_pesados": horarios_20_pesados}
    finally:
        # Libera los objetos y llama al recolector de basura
        
        del planning
        gc.collect()

@app.post("/simular_disponibilidad_con_dict/{fk_servicio}/{day}/{capacidad}/{capacidad_20}/{capacidad_20_pesados}")
def simular_servicio(diccionario_horarios: dict[str, list[dict[str, str | int]]], fk_servicio: int, day: str, capacidad: int, capacidad_20: int, capacidad_20_pesados: int):
    try:
        planning = Planning() 
        planning.insert_day(day)
        planning.Querys()
        planning.preprocessing()
        # dictionary = planning.dict_creator()

        horarios, horarios_20, horarios_20_pesados = planning.simular_disponibilidad_con_dict(diccionario_horarios['data'], fk_servicio, capacidad, capacidad_20, capacidad_20_pesados)
        return {"horarios": horarios, "horarios_20": horarios_20, "horarios_20_pesados": horarios_20_pesados}
    finally:
        del diccionario_horarios
        del planning
        gc.collect()

@app.get("/diccionario/{fecha}")
def diccionario(fecha: str = Path(..., pattern=r"^\d{4}-\d{2}-\d{2}$", description="Fecha en formato 'YYYY-MM-DD'")):
    try:
        planning = Planning() 
        planning.insert_day(fecha)
        planning.Querys()
        planning.preprocessing()
        return planning.dict_creator_merged()
    finally:
        del planning
        gc.collect()  # Forzar la liberación de la memoria
