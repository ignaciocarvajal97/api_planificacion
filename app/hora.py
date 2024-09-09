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
            return f"La hora actual en {pais} es: {hora_actual}"
        else:
            return f"No se pudo obtener la hora para {pais}. Código de estado: {response.status_code}"
    except Exception as e:
        return f"Error al realizar la solicitud: {e}"

# Ejemplo de uso
pais = "America/Santiago"
print(obtener_hora_pais(pais))