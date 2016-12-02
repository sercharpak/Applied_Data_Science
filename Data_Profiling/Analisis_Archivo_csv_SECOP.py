
# coding: utf-8

# # Analisis de la Evolucion de la Calidad de los Datos
# ## Información proveniente del Portal de Datos Abiertos del Estado Colombiano

# **Integrantes**
# 
# - Claudia Viviana Arévalo Bernal
# - Sergio Daniel Hernández Charpak
# - Hugo Sin Triana

# #### Librerias

USAGE = "python Analisis_Archivo_csv_SECOP.py input_csv_file"


import pandas as pd
import pandas_profiling
import numpy as np
import datetime
import urllib
from bokeh.plotting import *
from bokeh.models import HoverTool
from collections import OrderedDict
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

inputfile_1 = sys.argv[1]



def process_Secop_by_year_file_to_text_file(secop_file):
    name_file = ntpath.basename(secop_file)
    directory = ntpath.dirname(secop_file) + ntpath.altsep
    output_html = os.path.splitext(secop_file)[0] + ".html"
    Secop_year = pd.read_csv(secop_file, encoding='utf_8')
    profile_year = pandas_profiling.ProfileReport(Secop_year)
    profile_year.to_file(outputfile=output_html)
    shape_year = Secop_year.shape
    dtypes_year = Secop_year.dtypes
    nulls_year = Secop_year.isnull().sum()
    nulls_year_all = np.sum(nulls_year)
    Secop_year = Secop_year.duplicated()
    duplicates_year = 0 
    for i in range(0,len(Secop_year)):
        if(Secop_year[i]==True):
            duplicates_year+=1
    output_name = os.path.splitext(secop_file)[0] + ".txt"
    print ("Writing in file " + output_name)
    fileout = open(output_name, 'w')
    fileout.write("%s \n %s\n %s\n %s \n %s\n"%(format(shape_year), format(dtypes_year.to_string()), nulls_year, duplicates_year, nulls_year_all))


print ("Processing file " + inputfile_1)
process_Secop_by_year_file_to_text_file(inputfile_1)

