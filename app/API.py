# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 11:07:23 2023

@author: Ignacio Carvajal
"""

from fastapi import FastAPI
from flask import Flask

app = FastAPI()

# Define tus rutas y controladores FastAPI aquí.

flask_app = Flask(__name__)

if __name__ == "__main__":
    
    Flask.run(app, host="0.0.0.0", port=8000)


