# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 11:41:11 2023

@author: Ignacio Carvajal
"""
from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel
from datetime import datetime, timedelta, date
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from app.planner import Planning  # Asegúrate de importar correctamente desde app.planner

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

planning = Planning()

@app.get("/")
def read_root():
    planning.insert_day(day)
    planning.Querys()
    planning.preprocessing()
    dictionary = planning.dict_creator()
    return {"message": dictionary}

@app.get("/simular_disponibilidad/{fk_servicio}/{day}/{capacidad}/{capacidad_20}/{capacidad_20_pesados}")
def simular_servicio(fk_servicio: int, day: str, capacidad: int, capacidad_20: int, capacidad_20_pesados: int):
    planning.insert_day(day)
    planning.Querys()
    planning.preprocessing()
    dictionary = planning.dict_creator()

    horarios, horarios_20, horarios_20_pesados = planning.simular_disponibilidad(fk_servicio, capacidad, capacidad_20, capacidad_20_pesados)
    return {"horarios": horarios, "horarios_20": horarios_20, "horarios_20_pesados": horarios_20_pesados}

@app.post("/simular_disponibilidad_con_dict/{fk_servicio}/{day}/{capacidad}/{capacidad_20}/{capacidad_20_pesados}")
def simular_servicio(diccionario_horarios: dict[str, list[dict[str, str | int]]], fk_servicio: int, day: str, capacidad: int, capacidad_20: int, capacidad_20_pesados: int):
    planning.insert_day(day)
    planning.Querys()
    planning.preprocessing()
    # dictionary = planning.dict_creator()

    horarios, horarios_20, horarios_20_pesados = planning.simular_disponibilidad_con_dict(diccionario_horarios['data'], fk_servicio, capacidad, capacidad_20, capacidad_20_pesados)
    return {"horarios": horarios, "horarios_20": horarios_20, "horarios_20_pesados": horarios_20_pesados}

@app.get("/diccionario/{fecha}")
def diccionario(fecha: str = Path(..., pattern=r"^\d{4}-\d{2}-\d{2}$", description="Fecha en formato 'YYYY-MM-DD'")):
    planning.insert_day(fecha)
    planning.Querys()
    planning.preprocessing()
    
    return planning.dict_creator_merged()
