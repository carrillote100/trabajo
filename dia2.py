import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd

def productos_split(productos):
    print("Entra")
    productos_s = productos.split('\n')
    lista_total = ['Palabra_inicial']
    lista_aux_añadir = ['','']
    nombre_fill = 'False'
    for i in productos_s:
        añadir = ''
        if i =='Oferta exclusiva CLUB Dia':
            pass
        else:
            if lista_total[-1] == 'Palabra_inicial' or lista_aux_añadir[-1] == 'Añadir':

                    nombre_producto = i
                    lista_total.append(nombre_producto)
                    lista_aux_añadir.append(nombre_producto)
                    nombre_fill = 'True'
            if nombre_fill == 'True':
                if i[-1] == '€':
                    precio = i
                    lista_total.append(precio)
                    nombre_fill = 'False'
            if i[-1] == ')':
                volumen = i
                lista_total.append(volumen)
                lista_aux_añadir.append(volumen)
            else:
                lista_aux_añadir.append(i)

    del lista_total[0]

    return lista_total


def list_todf(lista):
    conjuntos_de_tres = [lista[i:i + 3] for i in range(0, len(lista), 3)]

    # Crear un DataFrame de pandas
    df = pd.DataFrame(conjuntos_de_tres, columns=['Nombre', 'Precio', 'Volumen'])

    return df


def clean_df(df):
    df["Precio"] = df["Precio"].str.replace(" €", "", regex=False)
    df = df.dropna(subset=["Nombre", "Precio", "Volumen"])

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
with open("../../Downloads/urldia.csv", "r", encoding="utf-8") as f:
    for line in f:
        url = line.strip()  # Elimina espacios y saltos de línea

        driver.get(url)
        time.sleep(20)
        print(url)

        productos = driver.find_elements(By.XPATH, '/html/body/div[1]/div/div/div[2]/div[2]/div[1]/div[3]/div[1]/div/ul')

        for producto in productos:
            lista_total = productos_split(producto.text)

            df = list_todf(lista_total)
            print(df.head())
            df_final = clean_df(df)
            df_final.to_csv("datos/dia.csv", sep=';', encoding='utf-8', mode='a', index=False, header=False)
        time.sleep(1)