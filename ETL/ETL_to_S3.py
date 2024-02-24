import boto3
import psycopg2
import pandas as pd
import os
from datetime import datetime
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# Configuración de AWS y PostgreSQL
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']
os.environ['AWS_ACCESS_KEY_ID'] = os.environ['AWS_ACCESS_KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ['AWS_SECRET_KEY']
bucket_name = os.environ['BUCKET_NAME']
postgres_host = os.environ['POSTGRES_HOST']
postgres_port = os.environ['POSTGRES_PORT']
postgres_user = os.environ['POSTGRES_USER']
postgres_password = os.environ['POSTGRES_PASSWORD']
postgres_db = os.environ['POSTGRES_DB']

# Crea un cliente de S3 y Athena
s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
athena = boto3.client('athena', region_name='us-east-1')

# Conéctate a la base de datos de PostgreSQL
conn = psycopg2.connect(host=postgres_host, port=postgres_port, user=postgres_user, password=postgres_password, dbname=postgres_db)

# Lee la lista de tablas y acciones
tablas = []
with open('/wwwroot/sospectETL/tablas.txt', 'r') as f:
    for linea in f:
        nombre, accion = linea.strip().split(',')
        tablas.append({'nombre': nombre, 'accion': accion})

# Procesa cada tabla
for tabla in tablas:
    nombre_tabla = tabla['nombre']
    accion = tabla['accion']
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    file_name = f"{nombre_tabla}_{fecha_actual}.parquet"
    s3_path = f'dwh/{nombre_tabla}/ano={fecha_actual[:4]}/mes={fecha_actual[5:7]}/dia={fecha_actual[8:10]}/{file_name}'

    # Extrae los datos de la tabla en un DataFrame
    df = pd.read_sql(f"SELECT * FROM {nombre_tabla};", conn)

    # Convierte el DataFrame a Parquet
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    # Sube el archivo Parquet a S3
    s3.put_object(Bucket=bucket_name, Key=s3_path, Body=buf.getvalue())

    # Si la acción es 'insertar_eliminar', elimina los datos de la tabla de origen
    #if accion == 'insertar_eliminar':
    #    cursor = conn.cursor()
    #    cursor.execute(f"DELETE FROM {nombre_tabla};")
    #    conn.commit()
    #    cursor.close()

    # Ejecuta el comando ALTER TABLE ADD PARTITION en Athena
    query = (
        f"ALTER TABLE {nombre_tabla} ADD IF NOT EXISTS PARTITION (ano='{fecha_actual[:4]}', mes='{fecha_actual[5:7]}', dia='{fecha_actual[8:10]}') "
        f"LOCATION 's3://{bucket_name}/dwh/{nombre_tabla}/ano={fecha_actual[:4]}/mes={fecha_actual[5:7]}/dia={fecha_actual[8:10]}/';"
    )
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'dwh'},
        ResultConfiguration={'OutputLocation': f's3://{bucket_name}/dwh/AthenaDWHResults/'}
    )

# Cierra la conexión a la base de datos
conn.close()
