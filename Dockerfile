# Usa una imagen base de Python
FROM python:3.10-slim

# Establece el directorio de trabajo en /app
WORKDIR /app

# Ejemplo de copia de archivos en el Dockerfile
COPY app /app
# Copia el archivo requirements.txt en el directorio de trabajo
COPY requirements.txt .

# Instala las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt

# Añade esta línea al Dockerfile para crear el directorio
RUN mkdir -p /static/tmp/

RUN mkdir -p /queries/

# Copia todo el contenido del directorio actual en el contenedor
COPY . .


# Expone el puerto en el que se ejecutará la aplicación
EXPOSE 8000

# Comando para ejecutar la aplicación
CMD ["uvicorn", "app.myapp:app", "--host", "0.0.0.0", "--port", "8000"]
