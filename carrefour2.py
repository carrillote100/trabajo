import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd
import undetected_chromedriver as uc
import re

# Configurar las opciones de Selenium para ocultar el navegador
chrome_driver_path = 'chromedriver.exe'

def productos_split(productos):
    print(productos)
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


with open("../../Downloads/urlcarrefour.csv", "r", encoding="utf-8") as f:
    for line in f:
        url = line.strip()  # Elimina espacios y saltos de línea

        numeros = list(range(0, 7000 , 24))
        # numeros = list(range(0, 1001, 24))
        for i in numeros:
            #driver = uc.Chrome(headless=True)  # Cambia a True si no quieres ver la ventana
            url_total = url+'?offset={}'.format(str(i))
            print(url_total)
            # Abrir Carrefour
            with uc.Chrome(headless=False) as driver:
                driver.get(url_total)

            time.sleep(2)

            try:
                driver.find_element(By.XPATH, '/html/body/div[5]/div[2]/div/div/div[2]/div/div/button[2]').click()
            except:
                print('No apacere este boton cookies')
            time.sleep(2)
            try:
                driver.find_element(By.XPATH, '/html/body/div[2]/div/nav/div[2]/div/div/span').click()
            except:
                print('No apacere este boton mail')

            for _ in range(3):  # Ajusta el número de scrolls si es necesario
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
                time.sleep(1)  # Esperar para que carguen los datos

            time.sleep(1)

            productos = driver.find_elements(By.CLASS_NAME, 'product-card-list')
            print('Hay productos')
            print(len(productos))
            if len(productos) > 0:
                for producto in productos:
                    lista_total = productos_split(producto.text)

                    df = list_todf(lista_total)
                    print(df.head())
                    print('limpiando el df')
                    df_final = clean_df(df)
                    df_final.to_csv("datos/carrefour.csv", sep=';', encoding='utf-8', mode='a', index=False, header=False)
                    driver.quit()
                driver.quit()
            else:
                driver.quit()
                break
            driver.quit()
        driver.quit()
    driver.quit()


print("FINAL")