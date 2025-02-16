from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
import re
from rapidfuzz import process, fuzz
import numpy as np

# Conexión a la base de datos
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345',
    database='supermercados'
)

# Crear un cursor para ejecutar la consulta SQL


df_dia = pd.read_sql("SELECT nombre,precio FROM dia where nombre is not null and precio is not null", conn)
df_carrefour = pd.read_sql("SELECT * FROM carrefour where nombre is not null and precio is not null", conn)
df_alcampo = pd.read_sql("SELECT * FROM alcampo where nombre is not null and precio is not null", conn)

# # Agregar columna de supermercado
# df_dia["supermercado"] = "DIA"
# df_carrefour["supermercado"] = "CARREFOUR"
# df_alcampo["supermercado"] = "ALCAMPO"
#
# # Unir todos los DataFrames en uno solo
# df_total = pd.concat([df_dia, df_carrefour, df_alcampo], ignore_index=True)
#
#
# # Función para normalizar nombres
# def normalizar_nombre(nombre):
#     nombre = nombre.lower()
#     nombre = re.sub(r'[^a-z0-9 ]', '', nombre)  # Quitar caracteres especiales
#     nombre = " ".join(sorted(nombre.split()))  # Ordenar palabras
#     return nombre
#
#
# df_total["nombre_normalizado"] = df_total["nombre"].apply(normalizar_nombre)
#
# # Asignar identificadores únicos a productos similares
# unique_id = 1
# matched_products = {}
# df_total["id_producto"] = None
#
# for idx, row in df_total.iterrows():
#     nombre = row["nombre_normalizado"]
#     precio = row["precio"]
#
#     found_match = False
#     for key, values in matched_products.items():
#         existing_nombre = values["nombre"]
#         existing_precio = values["precio"]
#
#         similarity = fuzz.ratio(nombre, existing_nombre)
#         price_diff = abs(precio - existing_precio) / existing_precio
#         if similarity > 80 and price_diff < 0.05:
#         #if similarity > 85 and price_diff < 0.18:  # Similaridad >80% y diferencia de precio <20%  61
#             df_total.at[idx, "id_producto"] = key
#             found_match = True
#             break
#
#     if not found_match:
#         matched_products[unique_id] = {"nombre": nombre, "precio": precio}
#         df_total.at[idx, "id_producto"] = unique_id
#         unique_id += 1
#
# # Asegurar que haya una sola entrada por id_producto y supermercado (evita duplicados)
# df_total = df_total.groupby(["id_producto", "supermercado"], as_index=False).agg({
#     "nombre": "first",  # Mantener el primer nombre
#     "precio": "min"  # Tomar el precio más bajo si hay duplicados
# })
#
# # Ahora sí podemos hacer el pivot sin error
# df_pivot = df_total.pivot(index="id_producto", columns="supermercado", values=["nombre", "precio"]).reset_index()
#
# # Renombrar columnas correctamente
# df_pivot.columns = ["id_producto", "nombre_carrefour", "nombre_dia", "nombre_alcampo",
#                     "precio_carrefour", "precio_dia", "precio_alcampo"]
#
# # Filtrar los productos que estén en los tres supermercados
# df_final = df_pivot.dropna().copy()
#
# # Obtener el precio más bajo y el supermercado correspondiente
# df_total["precio_supermercado"] = df_total["supermercado"]  # Guardamos la info del supermercado
# df_mejor_precio = df_total.loc[
#     df_total.groupby("id_producto")["precio"].idxmin(), ["id_producto", "precio", "precio_supermercado"]]
#
# # Unir con los datos finales
# df_final = pd.merge(df_final, df_mejor_precio, on="id_producto", how="left")
#
# # Guardar los productos que NO están en los tres supermercados en df_errores
# df_errores = df_pivot[df_pivot.isnull().any(axis=1)].copy()
#
# # Mostrar resultados
# print("📌 Productos en los 3 supermercados:")
# print(df_final)
# print("\n❌ Productos faltantes en algún supermercado (df_errores):")
# print(df_errores)

# # Agregar columna de supermercado
# df_dia["supermercado"] = "DIA"
# df_carrefour["supermercado"] = "CARREFOUR"
# df_alcampo["supermercado"] = "ALCAMPO"
#
# # Unir todos los DataFrames en uno solo
# df_total = pd.concat([df_dia, df_carrefour, df_alcampo], ignore_index=True)
#
# # Función para normalizar nombres
# def normalizar_nombre(nombre):
#     nombre = nombre.lower()
#     nombre = re.sub(r'[^a-z0-9 ]', '', nombre)  # Quitar caracteres especiales
#     nombre = " ".join(sorted(nombre.split()))  # Ordenar palabras
#     return nombre
#
# df_total["nombre_normalizado"] = df_total["nombre"].apply(normalizar_nombre)
#
# # Asignar identificadores únicos a productos similares
# unique_id = 1
# matched_products = {}
# df_total["id_producto"] = None
#
# for idx, row in df_total.iterrows():
#     nombre = row["nombre_normalizado"]
#     precio = row["precio"]
#
#     found_match = False
#     for key, values in matched_products.items():
#         existing_nombre = values["nombre"]
#         existing_precio = values["precio"]
#
#         similarity = fuzz.ratio(nombre, existing_nombre)
#         price_diff = abs(precio - existing_precio) / existing_precio
#         if similarity > 80 and price_diff < 0.05:
#             df_total.at[idx, "id_producto"] = key
#             found_match = True
#             break
#
#     if not found_match:
#         matched_products[unique_id] = {"nombre": nombre, "precio": precio}
#         df_total.at[idx, "id_producto"] = unique_id
#         unique_id += 1
#
# # Asegurar una sola entrada por id_producto y supermercado
# df_total = df_total.groupby(["id_producto", "supermercado"], as_index=False).agg({
#     "nombre": "first",
#     "precio": "min"
# })
#
# # Hacer el pivot sin eliminar productos con valores nulos
# df_pivot = df_total.pivot(index="id_producto", columns="supermercado", values=["nombre", "precio"]).reset_index()
# df_pivot.columns = ["id_producto", "nombre_carrefour", "nombre_dia", "nombre_alcampo",
#                     "precio_carrefour", "precio_dia", "precio_alcampo"]
#
# # Rellenar valores faltantes con NaN
# df_pivot = df_pivot.fillna(np.nan)
#
# # 🔹 FILTRAR: Mantener solo productos con AL MENOS 2 supermercados con datos
# df_pivot["num_supermercados"] = df_pivot[["precio_carrefour", "precio_dia", "precio_alcampo"]].notnull().sum(axis=1)
# df_pivot = df_pivot[df_pivot["num_supermercados"] >= 2].drop(columns=["num_supermercados"])
#
# # Obtener el mejor precio y supermercado correspondiente
# df_total["precio_supermercado"] = df_total["supermercado"]
# df_mejor_precio = df_total.loc[df_total.groupby("id_producto")["precio"].idxmin(), ["id_producto", "precio", "precio_supermercado"]]
#
# # Unir los datos con el mejor precio
# df_final = pd.merge(df_pivot, df_mejor_precio, on="id_producto", how="left")
#
# # Mostrar resultados
# print("📌 Productos con al menos dos supermercados:")
# print(df_final)

# Agregar columna de supermercado
df_dia["supermercado"] = "DIA"
df_carrefour["supermercado"] = "CARREFOUR"
df_alcampo["supermercado"] = "ALCAMPO"

# Unir todos los DataFrames en uno solo
df_total = pd.concat([df_dia, df_carrefour, df_alcampo], ignore_index=True)

# Función para normalizar nombres
def normalizar_nombre(nombre):
    nombre = nombre.lower()
    nombre = re.sub(r'[^a-z0-9 ]', '', nombre)  # Quitar caracteres especiales
    nombre = " ".join(sorted(nombre.split()))  # Ordenar palabras
    return nombre

df_total["nombre_normalizado"] = df_total["nombre"].apply(normalizar_nombre)

# Asignar identificadores únicos a productos similares
unique_id = 1
matched_products = {}
df_total["id_producto"] = None

for idx, row in df_total.iterrows():
    nombre = row["nombre_normalizado"]
    precio = row["precio"]

    found_match = False
    for key, values in matched_products.items():
        existing_nombre = values["nombre"]
        existing_precio = values["precio"]

        similarity = fuzz.ratio(nombre, existing_nombre)
        price_diff = abs(precio - existing_precio) / existing_precio
        if similarity > 80 and price_diff < 0.05:
            df_total.at[idx, "id_producto"] = key
            found_match = True
            break

    if not found_match:
        matched_products[unique_id] = {"nombre": nombre, "precio": precio}
        df_total.at[idx, "id_producto"] = unique_id
        unique_id += 1

# Asegurar una sola entrada por id_producto y supermercado
df_total = df_total.groupby(["id_producto", "supermercado"], as_index=False).agg({
    "nombre": "first",
    "precio": "min"  # Tomar el precio más bajo si hay duplicados
})

# Hacer el pivot sin eliminar productos con valores nulos
df_pivot = df_total.pivot(index="id_producto", columns="supermercado", values=["nombre", "precio"]).reset_index()
df_pivot.columns = ["id_producto", "nombre_carrefour", "nombre_dia", "nombre_alcampo",
                    "precio_carrefour", "precio_dia", "precio_alcampo"]

# Rellenar valores faltantes con NaN
df_pivot = df_pivot.fillna(np.nan)

# 🔹 FILTRAR: Mantener solo productos con AL MENOS 2 supermercados con datos
df_pivot["num_supermercados"] = df_pivot[["precio_carrefour", "precio_dia", "precio_alcampo"]].notnull().sum(axis=1)
df_pivot = df_pivot[df_pivot["num_supermercados"] >= 2].drop(columns=["num_supermercados"])

# Obtener el mejor precio y supermercado correspondiente
df_total["precio_supermercado"] = df_total["supermercado"]
df_mejor_precio = df_total.loc[df_total.groupby("id_producto")["precio"].idxmin(), ["id_producto", "precio", "precio_supermercado"]]

# Obtener el **precio máximo** por `id_producto`
df_peor_precio = df_total.loc[df_total.groupby("id_producto")["precio"].idxmax(), ["id_producto", "precio"]]
df_peor_precio.rename(columns={"precio": "precio_max"}, inplace=True)  # Renombrar la columna

# Unir los datos con el mejor y peor precio
df_final = pd.merge(df_pivot, df_mejor_precio, on="id_producto", how="left")
df_final = pd.merge(df_final, df_peor_precio, on="id_producto", how="left")  # Agregar `precio_max`

# Mostrar resultados
print("📌 Productos con al menos dos supermercados:")
print(df_final)

# Crear el engine de SQLAlchemy para conectar a MySQL
usuario = 'root'
contraseña = '12345'
host = 'localhost:3306'
base_de_datos = 'supermercados'

# Cadena de conexión para MySQL
engine = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@{host}/{base_de_datos}')

df_final = df_final.drop(columns=["precio_supermercado"])

df_final.to_sql('productos_comparados', con=engine, if_exists='append', index=False)

