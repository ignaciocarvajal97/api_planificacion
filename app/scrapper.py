# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 10:01:54 2023

@author: Ignacio Carvajal
"""

import os
import time
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import stat
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import shutil
from datetime import datetime, timedelta


directorio_actual = os.getcwd()
dpw_dir = directorio_actual + "\secuencias\\dpw"
tps_dir = directorio_actual + "\secuencias\\tps"
sti_dir = directorio_actual + "\secuencias\\sti"
print(dpw_dir)

def delete_dir_content():
    directorio_actual = os.getcwd()
    dpw_dir = directorio_actual + "\secuencias\\dpw"
    tps_dir = directorio_actual + "\secuencias\\tps"
    sti_dir = directorio_actual + "\secuencias\\sti"
    
    rutas = [tps_dir, sti_dir, dpw_dir]
    for ruta_directorio in rutas:
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




def download_dpw():
    directorio_actual = os.getcwd()
    options = Options()
    options.add_argument("start-maximized")
    options.add_experimental_option("prefs", {
        "download.default_directory": directorio_actual + "\secuencias\\dpw",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    except: 
        driver = webdriver.Chrome(options=options)
        
    driver.get("https://www.dpworldchile.com/programacion-faenas-contenedores/despacho-de-directo-diferido/")
    
    time.sleep(3)
    # Esperar hasta 1 segundos para que aparezca el elemento
    wait = WebDriverWait(driver, 5)
    element = wait.until(EC.presence_of_element_located((By.XPATH, '//html/body/div/div/div/div/div/a[contains(@href, ".xlsx")]')))
    elementos = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//html/body/div/div/div/div/div/a[contains(@href, ".xlsx")]')))



    try:
        
        time.sleep(3)
        # Esperar hasta 1 segundos para que aparezca el elemento
        wait = WebDriverWait(driver, 5)
        elements = wait.until(EC.presence_of_element_located((By.XPATH, '//html/body/div/div/div/div/div/a[contains(@href, ".xlsx")]')))
        elementos = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//html/body/div/div/div/div/div/a[contains(@href, ".xlsx")]')))


        for element in elementos:
            time.sleep(1)
            print(element.text)
            element.click()
            time.sleep(1)
        
    except:
        pass

    

    
    driver.quit()
    

def query_NAVES():
    
    query_naves = """SELECT DISTINCT COALESCE(nave.nave_nombre, '') AS servicio_nave_nombre
    FROM
        public.servicios AS ser
        INNER JOIN public.usuarios AS comer ON ser.fk_comercial = comer.usu_rut
        LEFT JOIN public.clientes AS cli_fact ON ser.fk_cliente_facturacion = cli_fact.cli_codigo
        LEFT JOIN public.clientes AS cli_desp ON ser.fk_cliente_despacho = cli_desp.cli_codigo
        LEFT JOIN public.naves AS nave ON ser.fk_nave = nave.nave_id
        LEFT JOIN public.naves_etas AS eta ON ser.fk_eta = eta.eta_id
        LEFT JOIN public.contenedores_tipos AS cont_tip ON ser.fk_tipo_contenedor = cont_tip.cont_id
        LEFT JOIN public.contenedores_tamanos AS cont_tam ON ser.fk_contenedor_tamano = cont_tam.conttam_id
        LEFT JOIN public.servicios_etapas AS eta_1 ON ser.id = eta_1.fk_servicio
        LEFT JOIN public.direcciones AS dir_1 ON eta_1.fk_direccion = dir_1.id
        LEFT JOIN public.comunas AS com_1 ON dir_1."comunaComunaId" = com_1.comuna_id
        LEFT JOIN public.servicios_etapas_conductores AS cond_eta_1 ON eta_1.id = cond_eta_1.fk_etapa
        LEFT JOIN public.usuarios AS cond_1 ON cond_eta_1.fk_conductor = cond_1.usu_rut
        LEFT JOIN public.taller_equipos AS tract_1 ON cond_eta_1.fk_tracto = tract_1.id
        LEFT JOIN public.servicios_etapas AS eta_0 ON eta_1.fk_etapa_anterior = eta_0.id
    WHERE
        ser.estado = 1
        AND eta_1.tipo = 1
        AND TO_DATE(eta_1.fecha, 'DD-MM-YYYY') > (CURRENT_DATE - INTERVAL '1 month')
    ORDER BY
        servicio_nave_nombre; """
                                    
    #AND ser.estado != 2 AND ser.estado != 999
    rows = connectionDB(query_naves)
    
    NAVES = []
    for row in rows:
        NAVES.append(row[0])
    
    return NAVES

from datetime import datetime, timedelta

def esta_dentro_del_rango(fecha_hora_str):
    try:
        # Convierte la cadena de fecha y hora en un objeto de fecha y hora
        fecha_hora_obj = datetime.strptime(fecha_hora_str, "%d/%m/%Y %H:%M")

        # Obtiene la fecha de hoy
        hoy = datetime.now()

        # Calcula la fecha de hace un mes desde hoy
        hace_un_mes = hoy - timedelta(days=30)

        # Compara si la fecha y hora está entre hoy y hace un mes
        return hace_un_mes <= fecha_hora_obj <= hoy
    except ValueError:
        # En caso de que la cadena no tenga el formato correcto
        return False

import difflib

def calcular_similitud(texto1, texto2):
    texto1 = texto1.replace(" ", "").lower()
    texto2 = texto2.replace(" ", "").lower()
    return difflib.SequenceMatcher(None, texto1, texto2).ratio()

def tiene_similitud_con_lista(texto, lista):
    for elemento in lista:
        similitud = calcular_similitud(texto, elemento)
        if similitud > 0.5:
            return True
    return False



from connection import connectionDB







def download_sti():
    directorio_actual = os.getcwd()
    options = Options()
    options.add_argument("start-maximized")
    options.add_experimental_option("prefs", {
        "download.default_directory": directorio_actual + "\secuencias\sti" ,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    except: 
        driver = webdriver.Chrome(options=options)
        
    driver.get("https://www.stiport.com/sti_en_linea/transportistas/informedespachonav.php")
    
    
    # Esperar hasta 1 segundos para que aparezca el elemento
    time.sleep(3)
    wait = WebDriverWait(driver, 10)
    user = wait.until(EC.presence_of_element_located((By.XPATH, '//html/body/div/header/div/div/form/div/fieldset/input[@name="userd"]')))# '//html/body/div/div/div[@class="texto"]/table/tbody')))
    password = wait.until(EC.presence_of_element_located((By.XPATH, '//html/body/div/header/div/div/form/div/fieldset/input[@name="pass"]')))
    submit =  wait.until(EC.presence_of_element_located((By.XPATH, '//html/body/div/header/div/div/form/div/fieldset[@class="actions"]/input[@type="submit"]')))
    
    user.send_keys('12782631-5')
    password.send_keys('OLGA1975')
    submit.click()
    
    
    # Obtener la URL actual
    current_url_sti = driver.current_url
    print(current_url_sti)
    
    #tabla = wait.until(EC.presence_of_element_located((By.XPATH, '//html/body/div/div/div[@class="texto"]/table/tbody')))
    
    page_source = driver.page_source
    
    
    # Crear un objeto de árbol HTML
    tree = html.fromstring(page_source)
    
    # Utilizar XPath para extraer los enlaces
    links = tree.xpath('//tr/td/a')
    arrivals = tree.xpath('//tr/td[4]')
    
    NAVES = query_NAVES()
    print(len(NAVES))
          
    count = 0
    # Imprimir los enlaces
    for link, arrival in zip(links, arrivals):
        if count <= len(NAVES):
            href = link.get('href')
            text = link.text_content()
            if esta_dentro_del_rango(str(arrival.text_content())):
                if tiene_similitud_con_lista(str(text), NAVES):
                    print(str(100*count/len(NAVES))[:4]+"%")
                    download_page = "https://www.stiport.com" + str(href)
                    enlace = driver.get(download_page)
                    time.sleep(1)
                    descargar =  wait.until(EC.presence_of_element_located((By.XPATH, '//html/body/div/div/div/div/a[@class="tipo_excel"]')))
                    
                    
                    descargar.click()
                    time.sleep(1)
                    driver.back()
                    count += 1
        else:
            print(count)
            break
    
    driver.quit()




def download_tps():
    
    # URL de la página que deseas scrape
    url = "https://www.tps.cl/tps/site/edic/base/port/entregac.html"
    directorio_actual = os.getcwd()
    # Carpeta donde guardar los archivos descargados
    directorio_descargas = directorio_actual + "\secuencias\\tps"
    
    
    # Crear la carpeta de descargas si no existe
    if not os.path.exists(directorio_descargas):
        os.makedirs(directorio_descargas)
    
    # Realizar una solicitud HTTP a la página
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")


    # Encontrar todos los enlaces <a> con clase "descarga"
    enlaces_descarga_tps = soup.find_all("a", class_="descarga")
    enlaces_descarga_dpw = soup.find_all("a", class_="elementor-button-link.elementor-button.elementor-size-sm")
    
    # Iterar a través de los enlaces y descargar los archivos Excel
    for enlace in enlaces_descarga_tps:
        enlace_href = enlace.get("href")
        nombre_archivo = enlace_href.split("/")[-1]  # Obtener el nombre del archivo del enlace
        
        # Evitar descargar archivos que contengan "cambio_de_condicion" en el nombre
        if "cambio_de_condicion" not in nombre_archivo:
            enlace_completo = urljoin(url, enlace_href)
            archivo_ruta = os.path.join(directorio_descargas, nombre_archivo)
            
            with open(archivo_ruta, "wb") as archivo:
                archivo_descarga = requests.get(enlace_completo)
                archivo.write(archivo_descarga.content)
                
            #print(f"Archivo '{nombre_archivo}' descargado.")
        else:
            print(f"Archivo '{nombre_archivo}' evitado.")

    # Iterar a través de los enlaces y descargar los archivos Excel
    for enlace in enlaces_descarga_dpw:
        enlace_href = enlace.get("href")
       
        nombre_archivo = enlace_href.split("/")[-1]  # Obtener el nombre del archivo del enlace
        
        # Evitar descargar archivos que contengan "cambio_de_condicion" en el nombre
        if "cambio_de_condicion" not in nombre_archivo:
            enlace_completo = urljoin(url, enlace_href)
            archivo_ruta = os.path.join(directorio_descargas, nombre_archivo)
            
            with open(archivo_ruta, "wb") as archivo:
                archivo_descarga = requests.get(enlace_completo)
                archivo.write(archivo_descarga.content)
               
        else:
            print(f"Archivo '{nombre_archivo}' evitado.")



def download_secuences():
    try:
        delete_dir_content()
    except:
        print("Error al eliminar archivos de secuencia")
    
    try:
        download_tps()
    except:
        print("Error al descargar tps")
    
    try:
        download_dpw()
    except:
        print("Error al descargar dpw")
    
    try:
        download_sti()
    except:
        print("Error al descargar sti")

    
    
    
#download_secuences()

                            






