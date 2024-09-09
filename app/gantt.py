import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import pandas as pd
import os
import base64
import psycopg2

import os
import paramiko


def upload_file_to_server(local_file_path, remote_directory, server_ip, server_username, server_password):
    try:
        # Establish SSH connection on port 34
        ssh = paramiko.SSHClient()
        print("paso1")
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("paso2")
        ssh.connect(server_ip, port=21, username=server_username,
                    password=server_password)
        print("paso3")
        # Create SFTP client
        sftp = ssh.open_sftp()
        print("paso4")
        # Upload the local file to the remote server
        remote_file_path = os.path.join(
            remote_directory, os.path.basename(local_file_path))
        print("paso5")
        sftp.put(local_file_path, remote_file_path)
        print("paso6")
        # Close the SFTP session and SSH connection
        sftp.close()
        print("paso7")
        ssh.close()
        print("paso8")
        print(f"File uploaded to: {remote_file_path}")
        return True

    except Exception as e:
        print("An error occurred:", e)
        return False


def convert(date):
    # Ajustamos el formato de fecha
    return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")


def insert_image(filename, encoded_image):
    connection = None
    try:
        # Establecer la conexión a la base de datos
        connection = psycopg2.connect(
            host="3.91.152.225",
            port="5432",
            user="postgres",
            password="ignacio",
            database="modelo_opti"
        )

        cursor = connection.cursor()

        # Obtener el ID del último registro de la tabla parametros
        get_latest_id_query = "SELECT id FROM parametros ORDER BY id DESC LIMIT 1;"
        cursor.execute(get_latest_id_query)
        latest_id = cursor.fetchone()[0]

        # Actualizar los campos nombre_archivo y picture en el último registro
        update_query = "UPDATE parametros SET nombre_archivo = %s, picture = %s WHERE id = %s;"
        cursor.execute(update_query, (filename, encoded_image, latest_id))

        # Confirmar la transacción
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error al actualizar en la tabla parametros:", error)

    finally:
        # Cerrar la conexión
        if connection:
            cursor.close()
            connection.close()

# trackers


def carta_gantt_trackers(datos, start, end, mostrar_info):
    directory = os.getcwd()
    # Asegúrate de que el nombre del archivo sea correcto
    datos = pd.read_excel(directory + '\\static\\tmp\\planificacion2.xlsx')

    # Convierte las fechas a objetos datetime si no lo están o ajusta el formato
    if datos['DT inicio'].dtypes == 'datetime64[ns]':
        datos['Inicio'] = datos['DT inicio']
        datos['hora_inicio'] = datos['DT inicio'].dt.strftime(
            '%H:%M')  # Mostramos solo la hora y minuto
    else:
        datos['Inicio'] = datos['DT inicio'].apply(convert)
        datos['hora_inicio'] = datos['DT inicio'].apply(
            lambda x: x.strftime('%H:%M'))

    if datos['DT final'].dtypes == 'datetime64[ns]':
        datos['Fin'] = datos['DT final']
        datos['hora_fin'] = datos['DT final'].dt.strftime(
            '%H:%M')  # Mostramos solo la hora y minuto
    else:
        datos['Fin'] = datos['DT final'].apply(convert)
        datos['hora_fin'] = datos['DT final'].apply(
            lambda x: x.strftime('%H:%M'))

    # Calculate the last service end time for each trucker
    last_service_end_time = datos.groupby(
        'Trackers')['DT final'].max().reset_index()
    last_service_end_time.rename(
        columns={'DT final': 'last_end_time'}, inplace=True)

    # Merge the last service end time back to the original data
    datos = pd.merge(datos, last_service_end_time, on='Trackers')

    # Ordenar los servicios según la columna "last_end_time"
    datos.sort_values(by='last_end_time', ascending=True, inplace=True)

    datos['DT duracion'] = datos['DT final'] - datos['DT inicio']

    # Crear una lista de conductores
    trackers = datos['Trackers'].unique()
    tracker_positions = {tracker: pos for pos, tracker in enumerate(trackers)}

    datos.reset_index(inplace=True)
    hitos = datos[datos['DT duracion'] == pd.Timedelta(seconds=0)]
    nrows = datos.shape[0]

    # Crear una lista de todos los momentos en que inician y terminan los servicios
    service_times = pd.concat([datos['DT inicio'], datos['DT final']])

    # Ordenar y eliminar duplicados para obtener todos los momentos únicos
    service_times = service_times.sort_values().drop_duplicates()

    # Crear una lista para contar cuántos servicios están ocurriendo en cada momento
    concurrent_services = []

    # Recorrer cada momento y contar cuántos servicios están ocurriendo en ese momento
    for time in service_times:
        concurrent = ((datos['DT inicio'] <= time) &
                      (datos['DT final'] > time)).sum()
        concurrent_services.append(concurrent)

    # Encontrar el punto con el máximo de servicios ocurriendo simultáneamente
    max_concurrent_index = np.argmax(concurrent_services)
    max_concurrent_time = service_times.iloc[max_concurrent_index]

    # Convertir la columna 'datetime_column' al formato '%H:%M'
    max_concurrent_time_str = max_concurrent_time.strftime('%H:%M')

    # Plot the vertical red line
    fig, ax = plt.subplots(1, 1, figsize=(
        20, len(trackers) * 0.3), constrained_layout=True, sharex=False)
    ax.invert_yaxis()

    # Crear una lista para contar cuántos servicios están ocurriendo en cada momento
    concurrent_services_20_pesados = []
    datos_20_pesados = datos[datos["tipo_cont"] == "20 pesado"]

    # Recorrer cada momento y contar cuántos servicios están ocurriendo en ese momento
    for time in service_times:
        concurrent_20_pesados = ((datos_20_pesados['DT inicio'] <= time) & (
            datos_20_pesados['DT final'] > time)).sum()
        concurrent_services_20_pesados.append(concurrent_20_pesados)

    # Encontrar el punto con el máximo de servicios ocurriendo simultáneamente
    max_concurrent_index_20_pesados = np.argmax(concurrent_services_20_pesados)
    max_concurrent_time_20_pesados = service_times.iloc[max_concurrent_index_20_pesados]

    # Convertir la columna 'datetime_column' al formato '%H:%M'
    max_concurrent_time_str = max_concurrent_time_20_pesados.strftime('%H:%M')

    # Plot the vertical red line
    #fig, ax2 = plt.subplots(1, 1, figsize=(20, len(trackers) * 0.3), constrained_layout=True, sharex=False)
    # ax2.invert_yaxis()

    # Crear una lista para contar cuántos servicios están ocurriendo en cada momento
    concurrent_services_20_pesados_livianos = []
    datos_20_pesados_livianos = datos[datos["tipo_cont"].isin(
        ["20 pesado", "20 liviano"])]

    # Recorrer cada momento y contar cuántos servicios están ocurriendo en ese momento
    for time in service_times:
        concurrent_20_pesados_livianos = ((datos_20_pesados_livianos['DT inicio'] <= time) & (
            datos_20_pesados_livianos['DT final'] > time)).sum()
        concurrent_services_20_pesados_livianos.append(
            concurrent_20_pesados_livianos)

    # Encontrar el punto con el máximo de servicios ocurriendo simultáneamente
    max_concurrent_index_20_pesados_livianos = np.argmax(
        concurrent_services_20_pesados_livianos)
    max_concurrent_time_20_pesados_livianos = service_times.iloc[
        max_concurrent_index_20_pesados_livianos]

    # Convertir la columna 'datetime_column' al formato '%H:%M'
    max_concurrent_time_str = max_concurrent_time_20_pesados_livianos.strftime(
        '%H:%M')

    # Colores para cada etapa
    colores_etapas = {
        'trayecto': 'lightgreen',
        'presentacion': 'deepskyblue',
        'devolucion_vacio_val': 'plum',
        'devolucion_vacio_sai': 'lightgreen',
        'devolucion_vacio_stgo': 'bisque',
        'retiro_full_val': 'darkviolet',
        'retiro_full_sai': 'darkgreen',
    }

    servicios_etiquetados = set()  # Conjunto para rastrear los servicios ya etiquetados

    for etapa, color in colores_etapas.items():
        datos_etapa = datos[datos['etapa'] == etapa]
        ax.barh(datos_etapa['Trackers'].map(tracker_positions),
                datos_etapa['DT duracion'],
                left=datos_etapa['DT inicio'],
                color=color,  # Colorear cada etapa diferente
                height=0.35,
                alpha=0.7,
                label=etapa)  # Agregar etiqueta al gráfico para cada etapa


        datos2 = datos[datos["etapa"].isin(["presentacion", "retiro_full_sai", "retiro_full_val"])]
        # Calculate the last service end time for each service
        last_service_end_time = datos2.groupby('id')['DT final'].max().reset_index()
        
        # Cambia el nombre de la columna 'DT final' a 'final_servicio'
        last_service_end_time.rename(columns={'DT final': 'final_servicio'}, inplace=True)
        
        # Merge the last service end time back to the original data
        datos_etapa = pd.merge(datos_etapa, last_service_end_time, on='id')




        # Agregar etiqueta con asteriscos en la parte derecha de las barras
        for index, row in datos_etapa.iterrows():
            servicio = row['id']
            if servicio not in servicios_etiquetados:
                # Cambia el carácter de asterisco (*) por un carácter de estrella (☆)
                asteriscos = "★" if row['tipo_cont'] == '20 liviano' else "★★" if row['tipo_cont'] == '20 pesado' else ""

        

                # Ajustar el valor para mover las etiquetas más a la derecha
                label_x = row['final_servicio'] - pd.Timedelta(minutes=60)
      
                # Ajusta el valor para mover las etiquetas al medio de la presentación
                label_x = row['DT inicio'] + (row['final_servicio'] - row['DT inicio']) / 2


                ax.text(label_x, tracker_positions[row['Trackers']], asteriscos,
                        color='black', ha='left', va='center', fontsize=12)

                ax.text(row['DT inicio'], tracker_positions[row['Trackers']],
                        f"serv:{servicio}", color='black', ha='right', va='bottom', fontsize=8)
                servicios_etiquetados.add(servicio)
                
        """
        # Agregar etiqueta con el número de servicio al inicio de cada barra
        for index, row in datos_etapa.iterrows():
            servicio = row['id']
            if servicio not in servicios_etiquetados:
                ax.text(row['DT inicio'], tracker_positions[row['Trackers']],
                        f"serv:{servicio}", color='black', ha='right', va='bottom', fontsize=8)
                servicios_etiquetados.add(servicio)
        """
        # Después de crear el gráfico, define las etiquetas y colores para la leyenda
    legend_labels = ['20 liviano', '20 pesado']
    legend_colors = ['black', 'black']
    
    # Crea la leyenda
    ax.legend(legend_labels, title="Tipo de Contenedor", loc="upper right", labels=['★', '★★'], title_fontsize=14)
    etapa_counts = datos['etapa'].value_counts()
    presentaciones_count = etapa_counts.get('presentacion', 0)
    retiros_count = etapa_counts.get(
        'retiro_full_val', 0) + etapa_counts.get('retiro_full_sai', 0)

    ax.axvline(x=max_concurrent_time, color='red', linestyle='--',
               label='Max Concurrent Services: ' + max_concurrent_time_str)
    ax.axvline(x=max_concurrent_time_20_pesados, color='blue', linestyle='--',
               label='Max Concurrent Services 20 (heavy): ' + max_concurrent_time_str)
    ax.axvline(x=max_concurrent_time_20_pesados_livianos, color='green', linestyle='--',
               label='Max Concurrent Services 20 (heavy): ' + max_concurrent_time_str)

    # Calcular la posición ideal para mostrar el número "30" en el gráfico
    min_y_position = np.min(list(tracker_positions.values()))
    max_y_position = np.max(list(tracker_positions.values()))
    y_position_to_show_number = min_y_position + \
        0.2 * (max_y_position - min_y_position)

    # Mostrar el número "30" en la esquina superior izquierda del gráfico

    hora_actual = datetime.now()
    dia_actual = datetime.now().date()

    day = dia_actual
    hora = hora_actual.hour
    minutos = hora_actual.minute
    ax.text(pd.Timestamp(start) + timedelta(hours=5), len(trackers)+1, "fecha: " + str(start) + "  corrio:" +
            str(day) + " " + str(hora) + ":" + str(minutos), color='black', ha='left', va='top', fontsize=27)
    ax.text(pd.Timestamp(start) + timedelta(hours=5), len(trackers)+3, "Minimo " + str(len(trackers)) + " transportistas.", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start) + timedelta(hours=5), len(trackers)+4, "Minimo " + str(np.max(concurrent_services_20_pesados_livianos)) + " chasis de veinte + multi. ", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start) + timedelta(hours=5), len(trackers)+5, "Minimo " + str(np.max(concurrent_services_20_pesados)) + " chasis de veinte", color='black', ha='left', va='top', fontsize=20)

    
    
    
    
    ax.text(pd.Timestamp(start) + timedelta(hours=5), len(trackers)+6,
            f"Presentaciones: {presentaciones_count}", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start) + timedelta(hours=5), len(trackers)+7,
            f"Retiros: {retiros_count}", color='black', ha='left', va='top', fontsize=20)

    ax.xaxis.set_major_locator(mdates.HourLocator(
        interval=1))  # Mostrar marcas cada hora
    ax.xaxis.set_major_formatter(mdates.DateFormatter(
        '%H:%M'))  # Formato de hora y minutos
    ax.set_xlim(pd.Timestamp(start), pd.Timestamp(end))  # Límites del eje x

    # Habilitar el eje x superior
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position('top')

    # Configuraciones adicionales para el eje y
    ax.set_yticks(range(len(trackers)))
    ax.set_yticklabels(trackers)
    # Aumentamos el espaciado entre etiquetas y eje y
    ax.tick_params(axis='y', pad=15)

    # Calculate the difference between the final of all services and the start of the first service for each conductor
    first_service_start_time = datos.groupby(
        'Trackers')['DT inicio'].min().reset_index()
    first_service_start_time.rename(
        columns={'DT inicio': 'first_start_time'}, inplace=True)
    datos = pd.merge(datos, first_service_start_time, on='Trackers')
    datos['DT duracion'] = datos['last_end_time'] - datos['first_start_time']

    # Remove duplicates from the dataframe
    unique_trackers_data = datos.drop_duplicates(subset=['Trackers'])

    # Create a table with unique trackers data
    valores_tabla = unique_trackers_data[['Trackers']]

    table = ax.table(cellText=valores_tabla.to_numpy(),
                     loc='left',
                     # Ajusta las etiquetas de las columnas
                     colLabels=['Transportista'],
                     colWidths=[0.02, 0.02, 0.02, 0.03],
                     bbox=(-0.6, 0, 0.6, (len(unique_trackers_data) + 1) / len(unique_trackers_data)))
    ax.margins(y=0.005)
    table.auto_set_font_size(False)
    table.set_fontsize(9)

    hours_of_interest = [14, 15, 16]
    tracker_counts_after_hours = []

    for hour in hours_of_interest:

        trackers_after_hour = datos[(datos['last_end_time'].dt.hour < hour) & (
            datos['last_end_time'].dt.hour > hour-10)]['Trackers'].drop_duplicates().count()
        trackers_available_after_hour = datos[datos['last_end_time'].dt.hour >=
                                              hour]['Trackers'].drop_duplicates().count()

        total_trackers = trackers_after_hour  # + trackers_available_after_hour
        tracker_counts_after_hours.append(total_trackers)

    # Calculate the number of retiros separated by time periods
    retiros_val_before_12 = datos[(datos['etapa'] == 'retiro_full_val') & (
        datos['DT inicio'].dt.hour < 12)].shape[0]
    retiros_val_between_12_15 = datos[(datos['etapa'] == 'retiro_full_val') & (
        datos['DT inicio'].dt.hour >= 12) & (datos['DT inicio'].dt.hour < 15)].shape[0]
    retiros_val_after_15 = datos[(datos['etapa'] == 'retiro_full_val') & (
        datos['DT inicio'].dt.hour >= 15)].shape[0]

    retiros_sai_before_12 = datos[(datos['etapa'] == 'retiro_full_sai') & (
        datos['DT inicio'].dt.hour < 12)].shape[0]
    retiros_sai_between_12_15 = datos[(datos['etapa'] == 'retiro_full_sai') & (
        datos['DT inicio'].dt.hour >= 12) & (datos['DT inicio'].dt.hour < 15)].shape[0]
    retiros_sai_after_15 = datos[(datos['etapa'] == 'retiro_full_sai') & (
        datos['DT inicio'].dt.hour >= 15)].shape[0]

    # Calculate the number of presentaciones separated by time periods
    presentaciones_before_12 = datos[(datos['etapa'] == 'presentacion') & (
        datos['DT inicio'].dt.hour < 12)].shape[0]
    presentaciones_between_12_15 = datos[(datos['etapa'] == 'presentacion') & (
        datos['DT inicio'].dt.hour >= 12) & (datos['DT inicio'].dt.hour < 15)].shape[0]
    presentaciones_after_15 = datos[(datos['etapa'] == 'presentacion') & (
        datos['DT inicio'].dt.hour >= 15)].shape[0]

    adjusted_start = start - timedelta(hours=1)

    # Show retiros information
    ax.text(pd.Timestamp(start - timedelta(hours=14)), len(trackers) + 1,
            "Retiros (Val):", color='black', ha='left', va='top', fontsize=25)
    ax.text(pd.Timestamp(start - timedelta(hours=14)), len(trackers) + 3,
            f"Antes de 12h: {retiros_val_before_12}", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start - timedelta(hours=14)), len(trackers) + 5,
            f"Entre 12-15h: {retiros_val_between_12_15}", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start - timedelta(hours=14)), len(trackers) + 7,
            f"Después de 15h: {retiros_val_after_15}", color='black', ha='left', va='top', fontsize=20)

    ax.text(pd.Timestamp(start - timedelta(hours=8)), len(trackers) + 1,
            "Retiros (SAI):", color='black', ha='left', va='top', fontsize=25)
    ax.text(pd.Timestamp(start - timedelta(hours=8)), len(trackers) + 3,
            f"Antes de 12h: {retiros_sai_before_12}", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start - timedelta(hours=8)), len(trackers) + 5,
            f"Entre 12-15h: {retiros_sai_between_12_15}", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start - timedelta(hours=8)), len(trackers) + 7,
            f"Después de 15h: {retiros_sai_after_15}", color='black', ha='left', va='top', fontsize=20)

    # Show presentaciones information
    ax.text(pd.Timestamp(start - timedelta(hours=2)), len(trackers) + 1,
            "Presentaciones:", color='black', ha='left', va='top', fontsize=25)
    ax.text(pd.Timestamp(start - timedelta(hours=2)), len(trackers) + 3,
            f"Antes de 12h: {presentaciones_before_12}", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start - timedelta(hours=2)), len(trackers) + 5,
            f"Entre 12-15h: {presentaciones_between_12_15}", color='black', ha='left', va='top', fontsize=20)
    ax.text(pd.Timestamp(start - timedelta(hours=2)), len(trackers) + 7,
            f"Después de 15h: {presentaciones_after_15}", color='black', ha='left', va='top', fontsize=20)

    if mostrar_info == True:
        # Show trackers without more active trips after certain hours
        ax.text(pd.Timestamp(start - timedelta(hours=14)), len(trackers) + 9,
                f"Trackers sin más viajes después de las 14h: {tracker_counts_after_hours[0]}", color='black', ha='left', va='top', fontsize=20)
        ax.text(pd.Timestamp(start - timedelta(hours=14)), len(trackers) + 11,
                f"Trackers sin más viajes después de las 15h: {tracker_counts_after_hours[1]}", color='black', ha='left', va='top', fontsize=20)
        ax.text(pd.Timestamp(start - timedelta(hours=14)), len(trackers) + 13,
                f"Trackers sin más viajes después de las 16h: {tracker_counts_after_hours[2]}", color='black', ha='left', va='top', fontsize=20)

    ax.margins(y=0.005)
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    ax.grid(True)
    ax.yaxis.set_ticklabels([])
    plt.legend()  # Mostrar la leyenda con los nombres de las etapas

    # Obtener la fecha y hora actual
    fecha = start # datetime.datetime.now()

    

    
    # Formatear la fecha como una cadena (por ejemplo, "2023-09-22")
    fecha_formateada = fecha.strftime("%Y-%m-%d")


    # Generar la ruta y el nombre de archivo para guardar la imagen
    filename = f"{start.day}_{start.month}_corrio_{day}_hora_{hora}{minutos}.png"
    filepath = os.path.join(directory, 'static', 'tmp',fecha_formateada, filename)

    # Guardar el gráfico como una imagen en el archivo especificado
    plt.savefig(filepath, bbox_inches='tight')
    plt.close()  # Cerrar la figura para liberar memoria



"""
# Input date string
start_string = '2023-09-25 00:00:00'
end_string = '2023-09-25 23:59:00'

# Convert to a pandas datetime object
start_date = pd.to_datetime(start_string)
end_date = pd.to_datetime(end_string)

directory = os.getcwd()
# Asegúrate de que el nombre del archivo sea correcto
datos = pd.read_excel(directory + '\\static\\tmp\\planificacion.xlsx')

df_visualization = pd.read_excel(
    directory + '\\static\\tmp\\planificacion2.xlsx')
# print(datos)
# print(self.df)

mostrar_info = False

carta_gantt_trackers(datos, start_date, end_date, mostrar_info)



directory = os.getcwd()
datos = pd.read_excel(directory + '\\static\\tmp\\planificacion2.xlsx')  # Asegúrate de que el nombre del archivo sea correcto

#carta_gantt_trackers(datos)

"""
