# from tabula.io import read_pdf
# import pandas as pd

# import mysql.connector

# cnx = mysql.connector.connect(user='cmsystem_electricidad_user', password='zTWXQMh;28EQ', host='3.130.94.157', database='cmsystem_electricidad')
# cursor = cnx.cursor()
# nombre_tabla = "tarifas"


# # Ruta al archivo PDF que contiene la tabla
# #archivo_pdf = "https://github.com/chezou/tabula-py/raw/master/tests/resources/data.pdf"  # Reemplaza con la ruta correcta a tu archivo PDF
# archivo_pdf = "html.pdf"  # Reemplaza con la ruta correcta a tu archivo PDF
# archivo_pdf = "tarifas.pdf"
# # Lee la tabla desde el archivo PDF
# df = read_pdf(archivo_pdf, pages="1")

# # Imprime la tabla
# # df1 = df[0]
# primera_columna =  df[0].iloc[:, :6]
# print(primera_columna)
# # print(df[1])
# # print(df[2])

# # Guarda la tabla en un archivo CSV
# #df.to_csv("tabla_extraida.csv", index=False)



import pandas as pd
from tabula.io import read_pdf
import mysql.connector
import sys
from conexion import cnx, cursor  # Importar conexión y cursor desde conexion.py


archivo_pdf = "tarifas.pdf" 

lista_df = read_pdf(archivo_pdf, pages="1", multiple_tables=True)  # Lee múltiples tablas de la página 1
for idx, df in enumerate(lista_df):
    # Asigna valores a las columnas por índice
    # if idx == 0:
    #     df.columns = ['cargo', 'red', 'etr', 'tarifario', 'unidad', 'cerrillos']
    # elif idx == 1:
    #     df.columns = ['cargo', 'red', 'etr', 'tarifario', 'unidad', 'maipu']
    df.iloc[:, 0:6] = df.apply(lambda row: pd.Series([
        row.iloc[0],  
        row.iloc[1],  
        row.iloc[2],  
        row.iloc[3],  
        row.iloc[4],  
        row.iloc[5], 
    ]), axis=1)

    for index, row in df.iterrows():
        
        if isinstance(row[5], str):  # Accede por índice en lugar de nombre de columna
            valores_columna6 = row[5].split()
            neto = valores_columna6[0] if len(valores_columna6) > 0 else None
            iva = valores_columna6[1] if len(valores_columna6) > 1 else None
        else:
            iva, neto = None, None

        consulta = f"INSERT INTO tarifas (cargo, red, etr, tarifario, unidad, comuna, neto, iva) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        valores = (row[0], row[1], row[2], row[3], row[4], 'cerrillos', neto, iva)  # Accede por índice en lugar de nombre de columna
        cursor.execute(consulta, valores)

cnx.commit()

# Cierra la conexión
cursor.close()
cnx.close()






# import pandas as pd
# from tabula.io import read_pdf
# import mysql.connector
# import sys

# cnx = mysql.connector.connect(user='cmsystem_electricidad_user', password='zTWXQMh;28EQ', host='3.130.94.157', database='cmsystem_electricidad')
# cursor = cnx.cursor()


# archivo_pdf = "tarifas.pdf" 

# lista_df = read_pdf(archivo_pdf, pages="1")
# df = lista_df[0] 
# # print(df.columns)
# # sys.exit()
# df[['columna1', 'columna2', 'columna3', 'columna4', 'columna5', 'columna6']] = df.apply(lambda row: pd.Series([
#     row['Unnamed: 0'],  
#     row['Unnamed: 1'],  
#     row['Unnamed: 2'],  
#     row['Unnamed: 3'],  
#     row['Unnamed: 4'],  
#     row['Cerrillos'], 
# ]), axis=1)


# for index, row in df.iterrows():
    
#     if isinstance(row['columna6'], str):
#         valores_columna6 = row['columna6'].split()
#         neto = valores_columna6[0] if len(valores_columna6) > 0 else None
#         iva = valores_columna6[1] if len(valores_columna6) > 1 else None
#     else:
#         iva, neto = None, None

#     consulta = f"INSERT INTO tarifas (cargo, red,etr, tarifario, unidad ,comuna,neto,iva) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
#     valores = (row['columna1'], row['columna2'], row['columna3'], row['columna4'],row['columna5'],'cerrillos',neto,iva)
#     cursor.execute(consulta, valores)

# cnx.commit()

# # Cierra la conexión
# cursor.close()
# cnx.close()


# cursor.execute("""
# INSERT INTO comprador (cargo, red,etr,tarifario,unidad
# ,comuna,neto,iva)VALUES (%s, %s, %s, %s, %s ,%s, %s, %s);
# """,
# (NombreColumna1, NombreColumna2))

# if cursor.lastrowid is not None:
#     print("comprador-->ID generado:", cursor.lastrowid)
# else:
#     print("comprador-->No se generó ningún ID")

# cnx.commit()
# # Filtra las primeras 6 columnas
# primeras_seis_columnas = df[0].iloc[:, :6]

# # Convierte las columnas a una lista de diccionarios
# datos_para_mysql = primeras_seis_columnas.to_dict(orient='records')

# # Conexión a la base de datos MySQL
# conexion = mysql.connector.connect(
#     host="3.130.94.157",
#     user="cmsystem_electricidad_user",
#     password="zTWXQMh;28EQ",
#     database="cmsystem_electricidad"
# )

# # Cursor para ejecutar consultas SQL
# cursor = conexion.cursor()

# # Nombre de la tabla en MySQL
# nombre_tabla = "tarifas"

# # Inserta los datos en la tabla MySQL
# for fila in datos_para_mysql:
#     consulta = f"INSERT INTO {nombre_tabla} ({', '.join(fila.keys())}) VALUES ({', '.join(['%s']*len(fila))})"
#     valores = tuple(fila.values())
#     cursor.execute(consulta, valores)

# # Realiza el commit para guardar los cambios
# conexion.commit()

# # Cierra la conexión
# cursor.close()
# conexion.close()

