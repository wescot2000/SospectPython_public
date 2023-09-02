import boto3
import psycopg2
import csv
import os

# Configuración de AWS y PostgreSQL
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']
bucket_name = os.environ['BUCKET_NAME']
postgres_host = os.environ['POSTGRES_HOST']
postgres_port = os.environ['POSTGRES_PORT']
postgres_user = os.environ['POSTGRES_USER']
postgres_password = os.environ['POSTGRES_PASSWORD']
postgres_db = os.environ['POSTGRES_DB']

# Lista de tablas y acciones
tablas = []
with open('tablas.txt', 'r') as f:
    for linea in f:
        nombre, accion = linea.strip().split(',')
        tablas.append({'nombre': nombre, 'accion': accion})

# Conéctate a la base de datos de PostgreSQL
conn = psycopg2.connect(host=postgres_host, port=postgres_port, user=postgres_user, password=postgres_password, dbname=postgres_db)
cursor = conn.cursor()

# Crea un cliente de S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

# Procesa cada tabla
for tabla in tablas:
    nombre_tabla = tabla['nombre']
    accion = tabla['accion']
    file_name = nombre_tabla + '.csv'
    
    # Extrae los datos de la tabla
    cursor.execute(f"SELECT * FROM {nombre_tabla};")
    rows = cursor.fetchall()

    # Escribe los datos en un archivo CSV
    with open('/tmp/' + file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    # Carga el archivo a S3
    with open('/tmp/' + file_name, 'rb') as f:
        s3.upload_fileobj(f, bucket_name, 'dwh/' + file_name)
    
    # Verifica que todos los datos se hayan transferido correctamente
    original_count = len(rows)

    s3.download_file(bucket_name, file_name, '/tmp/' + file_name)

    with open('/tmp/' + file_name, 'r') as f:
        transferred_count = sum(1 for _ in f)

    if original_count == transferred_count:
        if accion == 'insertar_eliminar':
            # Elimina los datos de la tabla de origen
            cursor.execute(f"DELETE FROM {nombre_tabla};")
            conn.commit()
    else:
        print(f"Error en la transferencia de datos de la tabla {nombre_tabla}.")
    
    # Elimina el archivo temporal
    os.remove('/tmp/' + file_name)

# Cierra la conexión a la base de datos
cursor.close()
conn.close()
