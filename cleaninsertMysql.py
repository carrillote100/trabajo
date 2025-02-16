from sqlalchemy import create_engine
import pandas as pd
import mysql.connector

def clean_dia(df_dia):
    print('clean dia')
    # Eliminar duplicados
    df_dia = df_dia.drop_duplicates()

    # Eliminar filas donde el nombre empiece por '('
    df_dia = df_dia[~df_dia['nombre'].str.startswith('(')]

    # Eliminar filas donde alguno de los valores esté vacío
    df_dia = df_dia.dropna(subset=['nombre', 'precio', 'volumen'])

    print(df_dia.dtypes)
    # Reemplazar el punto por una coma en la columna 'precio'
    df_dia['precio'] = df_dia['precio'].str.replace(',', '.', regex=False)
    df_dia['precio'] = pd.to_numeric(df_dia['precio'], errors='coerce')

    return df_dia


def clean_carrefour(df_carrefour):
    print('clean carrefour')
    # Eliminar duplicados
    df_carrefour = df_carrefour.drop_duplicates()

    # Eliminar filas donde el nombre empiece por '('
    df_carrefour = df_carrefour[~df_carrefour['nombre'].str.startswith('(')]

    # Eliminar filas donde alguno de los valores esté vacío
    df_carrefour = df_carrefour.dropna(subset=['nombre', 'precio'])
    print(df_carrefour.dtypes)

    return df_carrefour

def clean_alcampo(df_alcampo):
    print('clean carrefour')
    # Eliminar duplicados
    df_alcampo = df_alcampo.drop_duplicates()

    # Eliminar filas donde el nombre empiece por '('
    df_alcampo = df_alcampo[~df_alcampo['nombre'].str.startswith('(')]

    # Eliminar filas donde alguno de los valores esté vacío
    df_alcampo = df_alcampo.dropna(subset=['nombre', 'precio'])
    print(df_alcampo.dtypes)

    return df_alcampo

def historico(df_dia, df_carrefour, df_alcampo, timestamp_especifico):

    timestamp_actual = pd.to_datetime('now')

    df_dia_hist = df_dia.copy()
    df_dia_hist['timestamp'] = timestamp_actual

    df_carrefour_hist = df_carrefour.copy()
    df_carrefour_hist['timestamp'] = timestamp_actual

    df_alcampo_hist = df_alcampo.copy()
    df_alcampo_hist['timestamp'] = timestamp_actual



    return df_dia_hist, df_carrefour_hist , df_alcampo_hist


def truncate_tables():
    # Conexión a la base de datos
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='12345',
        database='supermercados'
    )

    # Crear un cursor para ejecutar la consulta SQL
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE dia")
    cursor.execute("TRUNCATE TABLE carrefour")
    cursor.execute("TRUNCATE TABLE alcampo")

    return cursor

# Leer el archivo CSV
df_dia = pd.read_csv('dia.csv', sep=';')
df_carrefour = pd.read_csv('carrefour.csv', sep=';')
df_alcampo = pd.read_csv('alcampo.csv', sep=';')


df_dia = clean_dia(df_dia)
df_carrefour = clean_carrefour(df_carrefour)
df_alcampo = clean_alcampo(df_alcampo)

timestamp_especifico = pd.to_datetime('now')
df_dia_hist, df_carrefour_hist , df_alcampo_hist = historico(df_dia, df_carrefour, df_alcampo, timestamp_especifico)

# Crear el engine de SQLAlchemy para conectar a MySQL
usuario = 'root'
contraseña = '12345'
host = 'localhost:3306'
base_de_datos = 'supermercados'

# Cadena de conexión para MySQL
engine = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@{host}/{base_de_datos}')


# Truncar tablas
truncate_tables()

# Insertar el DataFrame en la tabla MySQL
df_dia.to_sql('dia', con=engine, if_exists='append', index=False)
df_carrefour.to_sql('carrefour', con=engine, if_exists='append', index=False)
df_alcampo.to_sql('alcampo', con=engine, if_exists='append', index=False)

df_dia_hist.to_sql('dia_hist', con=engine, if_exists='append', index=False)
df_carrefour_hist.to_sql('carrefour_hist', con=engine, if_exists='append', index=False)
df_alcampo_hist.to_sql('alcampo_hist', con=engine, if_exists='append', index=False)