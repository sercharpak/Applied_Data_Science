
# coding: utf-8

# # Preparacion datos
# 
# ## Información proveniente del Portal de Datos Abiertos del Estado Colombiano

# **Integrantes**
# 
# - Claudia Viviana Arévalo Bernal
# - Sergio Daniel Hernández Charpak
# - Hugo Sin Triana

USAGE = "python PreparacionDatos.py input_csv_file"
# #### Librerias
import pandas as pd
import pandas_profiling
import numpy as np
import datetime
import urllib
import platform
import ntpath
import os
import subprocess
import sh
import sys

#---------------------------------------------------------

if(len(sys.argv)!=2):
    print ("Please use correctly")
    print (USAGE)
    sys.exit()

secop_file = sys.argv[1]
name_file = ntpath.basename(secop_file)
directory = ntpath.dirname(secop_file) + ntpath.altsep
output_csv = os.path.splitext(secop_file)[0] + "_clean.csv"

secop = pd.read_csv(secop_file, encoding='utf-8')


secop.head(3)


shape = secop.shape
dtypes = secop.dtypes
nulls = secop.isnull().sum()


print ("FORMA DE LOS DATOS:")
print (shape)
print ("\nTIPOS DE DATOS:")
print (dtypes)
print ("\nDATOS NULOS:")
print (nulls)

# ## Se eliminan duplicados, columnas innecesarias y registros con valores nulos

#Se eliminan columnas innecesarias y duplicados
secop = secop.drop_duplicates()
secop = secop.drop(['Fecha Ini Ejec Contrato',
                   'Fecha de Firma del Contrato',
                   'Fecha Fin Ejec Contrato',
		   'Municipios Ejecucion'], axis=1)
secop = secop.dropna()                   


shape = secop.shape
dtypes = secop.dtypes
nulls = secop.isnull().sum()

print ("FORMA DE LOS DATOS:")
print (shape)
print ("\nTIPOS DE DATOS:")
print (dtypes)
print ("\nDATOS NULOS:")
print (nulls)

secop.head(3)

# ## NIT de la Entidad Contratante - Se eliminan caracteres que no sean números


secop['NIT de la Entidad'].head(3)

secop[['NIT de la Entidad']] =secop[['NIT de la Entidad']].replace({'[^0-9]':''}, regex=True)

secop['NIT de la Entidad'].head(3)

secop = secop.replace('', np.nan, regex=True)

secop['NIT de la Entidad'].head(3)

shape = secop.shape
dtypes = secop.dtypes
nulls = secop.isnull().sum()

print ("FORMA DE LOS DATOS:")
print (shape)
print ("\nTIPOS DE DATOS:")
print (dtypes)
print ("\nDATOS NULOS:")
print (nulls)

# ## Identificacion del Contratista  - Se eliminan caracteres que no sean números

secop['Identificacion del Contratista'].head(3)

secop[['Identificacion del Contratista']] =secop[['Identificacion del Contratista']].replace({'[^0-9]':''}, regex=True)

secop['Identificacion del Contratista'].head(20)

#Numero de datos vacios
#len(np.where(secop.applymap(lambda x: x == ''))[0])

secop = secop.replace('', np.nan, regex=True)

nulls = secop.isnull().sum()
print ("\nDATOS NULOS:")
print (nulls)


# ## Cuantia a Contratar  - Se elimina caracter $

#Quitar símbolo pesos
secop[['Cuantia Contrato']] = secop[['Cuantia Contrato']].replace({'[\$,]':''}, regex=True)
#secop[['Cuantia a Contratar']] = secop[['Cuantia a Contratar']].replace({'[^0-9,]':''}, regex=True)

#Numero de datos vacios
#len(np.where(secop.applymap(lambda x: x == ''))[0])


# In[31]:

#Convertir a float 
secop[['Cuantia Contrato']] = secop[['Cuantia Contrato']].astype(float)

secop[['Cuantia Contrato']].dtypes

secop[['Cuantia Contrato']].head(20)

# ## Valor Contrato con Adiciones - Se elimina caracter $

#Quitar símbolo pesos
secop[['Valor Contrato con Adiciones']] = secop[['Valor Contrato con Adiciones']].replace({'[\$,]':''}, regex=True)

#Convertir a float 
secop[['Valor Contrato con Adiciones']] = secop[['Valor Contrato con Adiciones']].astype(float)

#Numero de datos vacios
#len(np.where(secop.applymap(lambda x: x == ''))[0])

secop[['Valor Contrato con Adiciones']].dtypes

secop[['Valor Contrato con Adiciones']].head(3)

secop.head(3)

# ## Eliminar los nuevos datos nulos
# #### En otras palabras: Eliminar los datos invalidos
secop = secop.dropna()                   
shape = secop.shape
dtypes = secop.dtypes
nulls = secop.isnull().sum()

print ("FORMA DE LOS DATOS:")
print (shape)
print ("\nTIPOS DE DATOS:")
print (dtypes)
print ("\nDATOS NULOS:")
print (nulls)

# ## Fecha de Cargue en SECOP - Se da formato de fecha

#Extraer llave de la columna
key_fecha_cargue_secop = secop.keys()[0]
key_fecha_cargue_secop

secop[key_fecha_cargue_secop] = secop[key_fecha_cargue_secop].apply(pd.to_datetime,errors='coerce')

secop.head(3)

# ### Verificacion que el nombre sea unico para ID o NIT

group_contratante = secop.groupby('NIT de la Entidad')['NIT de la Entidad'].unique()
VariosNombres_NIT = group_contratante[group_contratante.apply(lambda x: len(x)>1)] 

VariosNombres_NIT

VariosNombres_NIT.shape
#Un ejemplo:
#print (VariosNombres_NIT.index[123])
#print (VariosNombres_NIT[123])

group_Contratista = secop.groupby('Identificacion del Contratista')['Nom Raz Social Contratista'].unique()
VariosNombres_IDContratista = group_Contratista[group_Contratista.apply(lambda x: len(x)>1)] 


VariosNombres_IDContratista

#El primero parece ser un caso especial
print (VariosNombres_IDContratista.index[0])
print (VariosNombres_IDContratista[0])
print (len(VariosNombres_IDContratista[0]))

#El resto si parecen ser nombres repetidos para un ID, un ejemplo:
#print (VariosNombres_IDContratista.index[5])
#print (VariosNombres_IDContratista[5])

#Obtenemos la lista con solo uno de los nombres para estos
VariosNombres_IDContratista_new_values = [item_array[0] for item_array in VariosNombres_IDContratista]

VariosNombres_IDContratista[:] = VariosNombres_IDContratista_new_values

#print (VariosNombres_IDContratista.index[8])
#print (VariosNombres_IDContratista[8])

VariosNombres_IDContratista.shape


# #### Acciones a tomar
# <p>Podemos ver que hay 322 Entidades Contratantes cono NIT identico. </p>
# <p>A veces tiene sentido, al ser del mismo municipio, a veces no, al ser de municipios distintos.</p>
# <p>Del lado del contratatistas, parecen haber 347 que estan entre invalidos o con Identificacion 0 (lo cual es sospechoso)</p>
# <p>Hay, 136341 otros casos de identificacion repetida para el contratista, estos parecen ser errores de ortografia entre una o dos entradas.</p>
# 
# Vamos a:
# 1. Eliminar las entradas que comprendan los 347 contratistas con ID 0 y eliminar las 322 entidades contratantes en donde se repite nombre por NIT.
# 2. Reemplazar para los otros 136341 casos con idetificacion de contratista repetido por el primer nombre del subconjunto obtenido.

#Numero actual de entradas
secop.shape

#Nits y IDs a eliminar
NIT_to_drop = VariosNombres_NIT.index
ID_to_drop = VariosNombres_IDContratista.index[0] #Para el ID solo se borraria el primero
print (ID_to_drop)
print (len(NIT_to_drop))

#Indices a eliminar 
if(len(NIT_to_drop)>0):
    Indexes_NIT_to_drop = secop[secop['NIT de la Entidad Contratante'].isin(NIT_to_drop)].index.tolist()
    Indexes_ID_to_drop_values = secop[secop['Identificacion del Contratista'] == ID_to_drop].index.tolist()
    print (len(Indexes_NIT_to_drop))
    print (len(Indexes_ID_to_drop_values))
    print (len(Indexes_NIT_to_drop)+len(Indexes_ID_to_drop_values))

#Quitamos el primer elemento, que corresponde al ID 0
VariosNombres_IDContratista = VariosNombres_IDContratista.drop(VariosNombres_IDContratista.index[0])

VariosNombres_IDContratista_copy = VariosNombres_IDContratista.copy()

v_frame = VariosNombres_IDContratista_copy.to_frame()

v_frame.head(2)

v_frame['Identificacion del Contratista'] = v_frame.index

v_frame.head(2)

x1 = secop.set_index(['Identificacion del Contratista'])['Nom Raz Social Contratista']
x2 = v_frame.set_index(['Identificacion del Contratista'])['Nom Raz Social Contratista']
# call update function, this is inplace
x1.update(x2)

# replace the values in original df1
secop['Nom Raz Social Contratista'] = x1.values

secop.head(3)['Nom Raz Social Contratista']

group_Contratista = secop.groupby('Identificacion del Contratista')['Nom Raz Social Contratista'].unique()
VariosNombres_IDContratista = group_Contratista[group_Contratista.apply(lambda x: len(x)>1)] 

VariosNombres_IDContratista

#Eliminar las otras entradas
secop = secop.drop(secop[secop['Identificacion del Contratista'] == ID_to_drop].index)

secop = secop.drop(secop[secop['NIT de la Entidad'].isin(NIT_to_drop)].index)

secop.shape

# ## Crear nuevo archivo

secop.to_csv(output_csv, encoding='utf-8')

