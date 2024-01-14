import pandas as pd
from tabula.io import read_pdf
import mysql.connector
import sys
from conexion import cnx, cursor

archivo_pdf = "tarifas.pdf" 

lista_df = read_pdf(archivo_pdf, pages="1", multiple_tables=True)  
for idx, df in enumerate(lista_df):
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


cursor.close()
cnx.close()




