# -*- coding: utf-8 -*-
"""
Created on Thu May 12 00:15:50 2022

@author: mxka1r54
"""

#Importar paqueterias
import os
import shutil
os.chdir(r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG\Pyhton') # relative path: scripts dir is under Lab

import pandas as pd
import numpy as np

#Set paths  
import_path = r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG'

#Leer archivo platforms TENEMOS 3 FILES QUE IMPORTAR ANTES QUE ES LO QUE LLEVAMOS DE INVOLVES
#Esto viene de la vista en ATHENA: SELECT * FROM dev_raw.assorment_lh4_join_keystone_weekly_sellout_and_calendar 
data_sellout = pd.read_csv(import_path + r'\view_asso_3.csv') #Año mayor o igual a 2022 y Mexico FILTRO QUERY

#Cambiamos el nombre de las categorias que no estan en cereals, salty y whole a REST
data_sellout.loc[~data_sellout['catg_nm'].isin(['CEREALS', 'SALTY SNACKS', 'WHOLESOME SNACKS']), 'catg_nm'] = 'REST'

#data_sellout['catg_nm'].unique()

#ELEGIMOS LA CATEGORIA CON LA QUE QUEREMOS CORRER EL SCRIPT
categorias = ['CEREALS', 'SALTY SNACKS', 'WHOLESOME SNACKS', 'REST'] # CEREALS / SALTY SNACKS / WHOLESOME SNACKS / REST
len(categorias)

#PRIMERO ARMAMOS EL DATA SER MAIN (SEG, RETAIL ENV, CHAIN, NIELSEN AREA, STORE UNIVERSE)

# Usamos una copia del original sellout ya con el filtro de la categoria aplicado
data_sellout_c = data_sellout.copy()

#data_sellout_c['catg_nm'].unique()

data_sellout_c_head = data_sellout_c.head(20)
#list(data_sellout_c_head.columns) 

#AGRUPAMOS EL DATASET A NIVEL 
##TRY_CAST(null AS varchar) "Final SKU Desc Israel", ESTA COLUMNA LA AGREGAMOS AL FINAL 

data_sellout_c_grouped = data_sellout_c.groupby([ 
    "seg",
    "retlr_env_nm", 
    "chain_name", 
    "Nielsen_Geography"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Aqui Haremos un ciclo for que vaya por todas las categorias??  A LO MEJOR ES MEJOR MAS ABAJO 


#----------------------------------- CALCULAMOS LAS SKU MONTHLY RANGE POR CADA CATEGORIA ---------------------------------------------


#Agrupamos el dataset 
data_sellout_c_grouped_catg = data_sellout_c.groupby([ 
    "catg_nm",
    "yr",
    "fisc_pd",
    "seg",
    "retlr_env_nm", 
    "chain_name", 
    "Nielsen_Geography"]).agg({
    'ean_nbr':'nunique'
}).reset_index()
        

#Volvemos a agrupar el dataset
data_sellout_c_grouped_catg_avg = data_sellout_c_grouped_catg.groupby([ 
    "catg_nm",
    "seg",
    "retlr_env_nm", 
    "chain_name", 
    "Nielsen_Geography"]).agg({
    'ean_nbr':'mean'
}).reset_index()

        
#Hacer un ciclo for y filtrar por categoria para que vaya haciendo el merge 
for i in range(len(categorias)):
    
    categ = categorias[i]
    
    # Filtramos por la Categoria que nos interesa
    if categ == 'REST':
        df_mask = (~data_sellout_c_grouped_catg_avg['catg_nm'].isin(['CEREALS', 'WHOLESOME SNACKS', 'SALTY SNACKS'])) #!!!! filtramos la categoria 
    else:    
        df_mask = (data_sellout_c_grouped_catg_avg['catg_nm'] == '%s'%categ)
        
    data_sellout_c_grouped_catg_avg_with_CATEGORY_filter = data_sellout_c_grouped_catg_avg[df_mask]
    
    
    data_sellout_c_grouped_catg_avg_with_CATEGORY_filter.rename(columns = {'ean_nbr':'%s_Monthly_sku_range'%categ}, inplace = True)        
    data_sellout_c_grouped_catg_avg_with_CATEGORY_filter.drop('catg_nm', inplace=True, axis=1)
    data_sellout_c_grouped_catg_avg_with_CATEGORY_filter.reset_index(drop=True, inplace=True)
    
    # HACEMOS EL MERGE A LA BASE MAIN
    data_sellout_c_grouped = pd.merge(data_sellout_c_grouped, data_sellout_c_grouped_catg_avg_with_CATEGORY_filter, how='left', left_on=["seg","retlr_env_nm", "chain_name", "Nielsen_Geography"], right_on=["seg","retlr_env_nm", "chain_name", "Nielsen_Geography"])
    




# -------------------------------------------  CALCULAMOS EL MONTHLY RANGE POR ALL NIELSEN AREAS COMO TOTAL MEXICO --------------------------------------------------------------

#Agrupamos un nuevo dataset que sera el main de nielsen que sera la base para hacer el merge con todas las categorias y despues el concat con la base main
#data_sellout_c_grouped_catg_nielsen_base = data_sellout_c_grouped_catg_nielsen[['seg','retlr_env_nm','chain_name']].drop_duplicates().reset_index(drop=True)

data_sellout_c_grouped_catg_nielsen_base = data_sellout_c.groupby([ 
    "seg",
    "retlr_env_nm", 
    "chain_name"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

        
#Agrupamos el dataset que separara meses y años
data_sellout_c_grouped_catg_nielsen = data_sellout_c.groupby([ 
    "catg_nm",
    "yr",
    "fisc_pd",
    "seg",
    "retlr_env_nm", 
    "chain_name"]).agg({
    'ean_nbr':'nunique'
}).reset_index()
        

#Volvemos a agrupar el dataset que saca el promedio por agrupacion de esos meses y años 
data_sellout_c_grouped_catg_nielsen_avg = data_sellout_c_grouped_catg_nielsen.groupby([ 
    "catg_nm", #tenemos que ajustar desde el inicio que todas las otras categorias sean REST
    "seg",
    "retlr_env_nm", 
    "chain_name"]).agg({
    'ean_nbr':'mean'
}).reset_index()
        

#Hacer un ciclo for y filtrar por categoria para que vaya haciendo el merge 
for i in range(len(categorias)):
    
    #i = 3
    categ = categorias[i]
    
    # Filtramos por la Categoria que nos interesa
    if categ == 'REST':
        df_mask = (~data_sellout_c_grouped_catg_nielsen_avg['catg_nm'].isin(['CEREALS', 'WHOLESOME SNACKS', 'SALTY SNACKS'])) #!!!! filtramos la categoria 
    else:    
        df_mask = (data_sellout_c_grouped_catg_nielsen_avg['catg_nm'] == '%s'%categ)
        
    data_sellout_c_grouped_catg_avg_nielsen_with_CATEGORY_filter = data_sellout_c_grouped_catg_nielsen_avg[df_mask]
    
    data_sellout_c_grouped_catg_avg_nielsen_with_CATEGORY_filter.rename(columns = {'ean_nbr':'%s_Monthly_sku_range'%categ}, inplace = True)        
    data_sellout_c_grouped_catg_avg_nielsen_with_CATEGORY_filter.drop('catg_nm', inplace=True, axis=1)
    data_sellout_c_grouped_catg_avg_nielsen_with_CATEGORY_filter.reset_index(drop=True, inplace=True)
    
    #Vamos haciendo el merge con cada categoria en el dataset de main de nielsen agrupado
    data_sellout_c_grouped_catg_nielsen_base = pd.merge(data_sellout_c_grouped_catg_nielsen_base, data_sellout_c_grouped_catg_avg_nielsen_with_CATEGORY_filter, how='left', left_on=["seg","retlr_env_nm", "chain_name"], right_on=["seg","retlr_env_nm", "chain_name"])


#Agregar la columna en Nielsen geog de Total Mexico
data_sellout_c_grouped_catg_nielsen_base.insert(3,'Nielsen_Geography','Total Mexico')


# HACEMOS EL CONCAT HACIA ABAJO (AGREGAREMOS ROWS) A LA BASE SKU RANGE MAIN
#Unir el df1 y el df2
data_sellout_c_grouped = pd.concat([data_sellout_c_grouped, data_sellout_c_grouped_catg_nielsen_base], ignore_index=True)

    
    


# --------------------------------------------------- CALCULAMOS EL MONTHLY RANGE POR ALL RE AREAS ----------------------------------------------------------------------------------------------

#Agrupamos un nuevo dataset que sera el main de ALL RE que sera la base para hacer el merge con todas las categorias y despues el concat con la base main

data_sellout_c_grouped_catg_all_re_base = data_sellout_c.groupby([ 
    "seg",
    "chain_name",
    "Nielsen_Geography"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

        
#Agrupamos el dataset que separara meses y años
data_sellout_c_grouped_catg_all_re = data_sellout_c.groupby([ 
    "catg_nm",
    "yr",
    "fisc_pd",
    "seg",
    "chain_name",
    "Nielsen_Geography"]).agg({
    'ean_nbr':'nunique'
}).reset_index()
        

#Volvemos a agrupar el dataset que saca el promedio por agrupacion de esos meses y años 
data_sellout_c_grouped_catg_all_re_avg = data_sellout_c_grouped_catg_all_re.groupby([ 
    "catg_nm", #tenemos que ajustar desde el inicio que todas las otras categorias sean REST
    "seg",
    "chain_name",
    "Nielsen_Geography"]).agg({
    'ean_nbr':'mean'
}).reset_index()
        

#Hacer un ciclo for y filtrar por categoria para que vaya haciendo el merge 
for i in range(len(categorias)):
    
    #i = 3
    categ = categorias[i]
    
    # Filtramos por la Categoria que nos interesa
    if categ == 'REST':
        df_mask = (~data_sellout_c_grouped_catg_all_re_avg['catg_nm'].isin(['CEREALS', 'WHOLESOME SNACKS', 'SALTY SNACKS'])) #!!!! filtramos la categoria 
    else:    
        df_mask = (data_sellout_c_grouped_catg_all_re_avg['catg_nm'] == '%s'%categ)
        
    data_sellout_c_grouped_catg_avg_all_re_with_CATEGORY_filter = data_sellout_c_grouped_catg_all_re_avg[df_mask]
    
    data_sellout_c_grouped_catg_avg_all_re_with_CATEGORY_filter.rename(columns = {'ean_nbr':'%s_Monthly_sku_range'%categ}, inplace = True)        
    data_sellout_c_grouped_catg_avg_all_re_with_CATEGORY_filter.drop('catg_nm', inplace=True, axis=1)
    data_sellout_c_grouped_catg_avg_all_re_with_CATEGORY_filter.reset_index(drop=True, inplace=True)
    
    #Vamos haciendo el merge con cada categoria en el dataset de main de nielsen agrupado
    data_sellout_c_grouped_catg_all_re_base = pd.merge(data_sellout_c_grouped_catg_all_re_base, data_sellout_c_grouped_catg_avg_all_re_with_CATEGORY_filter, how='left', left_on=["seg","Nielsen_Geography", "chain_name"], right_on=["seg","Nielsen_Geography", "chain_name"])


#Agregar la columna en RET ENV de ALL RE 
data_sellout_c_grouped_catg_all_re_base.insert(1,'retlr_env_nm','All RE')


# HACEMOS EL CONCAT HACIA ABAJO (AGREGAREMOS ROWS) A LA BASE SKU RANGE MAIN
#Unir el df1 y el df2
data_sellout_c_grouped = pd.concat([data_sellout_c_grouped, data_sellout_c_grouped_catg_all_re_base], ignore_index=True)




# ------------------------------------- CALCULAMOS EL MONTHLY RANGE POR TOTAL MEXICO (NIELSEN AREAS) Y ALL RE -----------------------------------------------------------------------------------

#Agrupamos un nuevo dataset que sera el main de ALL RE y Nielsen total Mexico que sera la base para hacer el merge con todas las categorias y despues el concat con la base main

data_sellout_c_grouped_catg_all_re_nielsen_base = data_sellout_c.groupby([ 
    "seg",
    "chain_name"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

        
#Agrupamos el dataset que separara meses y años
data_sellout_c_grouped_catg_all_re_nielsen = data_sellout_c.groupby([ 
    "catg_nm",
    "yr",
    "fisc_pd",
    "seg",
    "chain_name"]).agg({
    'ean_nbr':'nunique'
}).reset_index()
        

#Volvemos a agrupar el dataset que saca el promedio por agrupacion de esos meses y años 
data_sellout_c_grouped_catg_all_re_nielsen_avg = data_sellout_c_grouped_catg_all_re_nielsen.groupby([ 
    "catg_nm", #tenemos que ajustar desde el inicio que todas las otras categorias sean REST
    "seg",
    "chain_name"]).agg({
    'ean_nbr':'mean'
}).reset_index()
        

#Hacer un ciclo for y filtrar por categoria para que vaya haciendo el merge 
for i in range(len(categorias)):
    
    #i = 3
    categ = categorias[i]
    
    # Filtramos por la Categoria que nos interesa
    if categ == 'REST':
        df_mask = (~data_sellout_c_grouped_catg_all_re_nielsen_avg['catg_nm'].isin(['CEREALS', 'WHOLESOME SNACKS', 'SALTY SNACKS'])) #!!!! filtramos la categoria 
    else:    
        df_mask = (data_sellout_c_grouped_catg_all_re_nielsen_avg['catg_nm'] == '%s'%categ)
        
    data_sellout_c_grouped_catg_avg_all_re_nielsen_with_CATEGORY_filter = data_sellout_c_grouped_catg_all_re_nielsen_avg[df_mask]
    
    data_sellout_c_grouped_catg_avg_all_re_nielsen_with_CATEGORY_filter.rename(columns = {'ean_nbr':'%s_Monthly_sku_range'%categ}, inplace = True)        
    data_sellout_c_grouped_catg_avg_all_re_nielsen_with_CATEGORY_filter.drop('catg_nm', inplace=True, axis=1)
    data_sellout_c_grouped_catg_avg_all_re_nielsen_with_CATEGORY_filter.reset_index(drop=True, inplace=True)
    
    #Vamos haciendo el merge con cada categoria en el dataset de main de nielsen agrupado
    data_sellout_c_grouped_catg_all_re_nielsen_base = pd.merge(data_sellout_c_grouped_catg_all_re_nielsen_base, data_sellout_c_grouped_catg_avg_all_re_nielsen_with_CATEGORY_filter, how='left', left_on=["seg", "chain_name"], right_on=["seg", "chain_name"])


#Agregar la columna en Nielsen geog y ALL RE de Total Mexico
data_sellout_c_grouped_catg_all_re_nielsen_base.insert(1,'retlr_env_nm','All RE')
data_sellout_c_grouped_catg_all_re_nielsen_base.insert(3,'Nielsen_Geography','Total Mexico')


# HACEMOS EL CONCAT HACIA ABAJO (AGREGAREMOS ROWS) A LA BASE SKU RANGE MAIN
#Unir el df1 y el df2
data_sellout_c_grouped = pd.concat([data_sellout_c_grouped, data_sellout_c_grouped_catg_all_re_nielsen_base], ignore_index=True)


# -------------------------------------------------------------- AJUSTES FINALES ---------------------------------------------------
#Rellenamos los nan con 0 ESTO LO HACEMOS AL FINAL MEJOR DESPUES DE LOS TOTALES
data_sellout_c_grouped.fillna(0, inplace = True)

# ESTO VA DESPUES DE LOS TOTALES CREAMOS UNA NUEVA COLUMNA "Monthly SKU RANGE 2" QUE SERA LA SUMA DE LAS COLUMNAS DE MONTHLY RANGE ESTO VA A SER AL FINAL YA QUE TENGAMOS TODOS LOS TOTALES 
data_sellout_c_grouped.insert(5,'Monthly_sku_range_2',None)
data_sellout_c_grouped['Monthly_sku_range_2'] = data_sellout_c_grouped['CEREALS_Monthly_sku_range'] + data_sellout_c_grouped['SALTY SNACKS_Monthly_sku_range'] + data_sellout_c_grouped['WHOLESOME SNACKS_Monthly_sku_range'] + data_sellout_c_grouped['REST_Monthly_sku_range']


# Cambiamos el nombre de la columna del Store count 
data_sellout_c_grouped.rename(columns = {'k_store_nbr':'Store_Universe'}, inplace = True)        
#list(data_sellout_c_grouped.columns)  


#Exportamos
data_sellout_c_grouped.to_csv(r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG\LH4\ASSORMENT\MT MX\FIXED DATASETS\SKU_RANGE_MX_MT.csv', index = False, encoding = 'utf-8')

