"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd

def ingest_data():

    #Carga de datos como dataframe
    df_init = pd.read_fwf("clusters_report.txt", colspecs=[(3,5),(9,14),(25,29),(40,119)], header=None)
    df_init.head(10)
    #Eliminando filas basura y nulos
    df_init.drop(df_init.index[:3], inplace=True)
    df_init.reset_index(drop=True, inplace=True)
    # Creación dataframe de salida 
    df_out = pd.DataFrame()
    df_out['cluster'] = df_init[0]
    df_out['cantidad_de_palabras_clave'] = df_init[1]
    df_out['porcentaje_de_palabras_clave'] = df_init[2].str.replace(',', '.')
    df_out.dropna(inplace=True)
    #Ordenando las 3 primeras columnas
    df_out.reset_index(drop=True,inplace=True)
    df_out['cluster'] = df_out['cluster'].str.strip().astype(int)
    df_out['cantidad_de_palabras_clave'] = df_out['cantidad_de_palabras_clave'].str.strip().astype(int)
    df_out['porcentaje_de_palabras_clave'] = df_out['porcentaje_de_palabras_clave'].str.strip().astype(float)
    #Preparando palabras clave
    words_1 = []
    [words_1.append(i) for i in df_init[3]]
    key_word = ' '.join(words_1).replace('control multi', 'control.multi')
    Words_2 = []
    [Words_2.append(i.strip()) for i in key_word[:-1].split('.')]
    #Ordenando columna 4 
    df_out['principales_palabras_clave'] = pd.concat([pd.Series(i) for i in Words_2]).reset_index(drop=True)
    df_out['principales_palabras_clave'] = df_out['principales_palabras_clave'].str.replace(' ,', ',').replace(',',', ').str.replace('   ',' ').str.replace('  ',' ').str.strip('\n').str.replace('  ', ' ')

    return df_out
