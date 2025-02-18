import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re

def productos_split2(productos):

    lineas = productos.split('\n')

    # Lista para almacenar los productos
    productos = []

    # Variable temporal para almacenar el bloque de texto de un producto
    producto_temporal = ""
    for i, linea in enumerate(lineas):
        if linea.startswith('('):
            continue
        # Si encontramos la palabra "Añadir", consideramos que es el final del producto
        if "Añadir" in linea:

            # Buscar precios en el bloque acumulado
            precios = re.findall(r"(\d+,\d{2}) €", producto_temporal)  # Buscar precios en formato "X,XX €"

            # Filtrar los precios para evitar los que están entre paréntesis (como "por 100 ml")
            precios_validos = [p for p in precios if "(" not in p and "por" not in p and "Unidad" not in p]

            # Si hay precios válidos, seleccionamos el más bajo
            if precios_validos:
                precio_minimo = min([float(p.replace(",", ".")) for p in precios_validos])
            else:
                precio_minimo = None

            # Buscar el nombre del producto (última línea que termina con un ".")
            nombre = None
            for line in reversed(producto_temporal.split("\n")):
                if line.endswith("."):
                    nombre = line.strip()
                    break

            # Agregar el nombre y precio del producto a la lista si es válido
            if nombre and precio_minimo is not None:
                productos.append((nombre, precio_minimo))

            # Limpiar el bloque temporal para el siguiente producto
            producto_temporal = ""

        else:
            # Acumular líneas para el producto actual
            producto_temporal += linea + "\n"

    return productos


def list_todf(lista):
    df_productos = pd.DataFrame(lista, columns=["Nombre", "Precio"])

    return df_productos

def clean_df(df):
    df = df.dropna(subset=["Nombre", "Precio"])

    return df



chrome_driver_path = 'chromedriver.exe'
options = webdriver.ChromeOptions()
# Ruta al ejecutable de Chrome
# options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--disable-extensions")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=2000,160000")
options.add_argument('--force-device-scale-factor=0.01')
# options.add_argument("--window-size=2000,16000")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36")
driver = webdriver.Chrome(options=options)


df = pd.read_csv("../../Downloads/urldia.csv")  # Asegúrate de que el archivo esté en el mismo directorio que el script

# Iterar sobre cada fila del DataFrame
with open("../../Downloads/urlalcampo.csv", "r", encoding="utf-8") as f:
    for line in f:
        url = line.strip()  # Elimina espacios y saltos de línea
        print(url)
        driver.get(url)
        time.sleep(20)
        productos = driver.find_elements(By.XPATH, '/html/body/div[1]/div/div[1]/main/div[2]')

        for producto in productos:

            lista_total = productos_split2(producto.text)

            df = list_todf(lista_total)
            print(df.head())
            df_final = clean_df(df)
            df_final.to_csv("datos/alcampo.csv", sep=';', encoding='utf-8', mode='a', index=False, header=False)