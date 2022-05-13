# -*- coding: utf-8 -*-
"""
Created on Tue May 10 21:20:53 2022

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

#ELEGIMOS LA CATEGORIA CON LA QUE QUEREMOS CORRER EL SCRIPT
categoria = 'REST' # CEREALS / SALTY SNACKS / WHOLESOME SNACKS / REST

# Filtramos por la Categoria que nos interesa
if categoria == 'REST':
    df_mask = (~data_sellout['catg_nm'].isin(['CEREALS', 'WHOLESOME SNACKS', 'SALTY SNACKS'])) #!!!! filtramos la categoria 
else:    
    df_mask = (data_sellout['catg_nm'] == '%s'%categoria)
    
data_sellout_with_CATEGORY_filter = data_sellout[df_mask]
    

# Usamos una copia del original sellout ya con el filtro de la categoria aplicado
data_sellout_c = data_sellout_with_CATEGORY_filter.copy()

#Convertir todos los NA´s a Null o vacios o pones - YA QUE SI NO LOS CONVERTIMOS ELIMINA LOS ROWS CON NAs Y Salty no tiene datos en sub brand ni sub segment
data_sellout_c.fillna('', inplace = True)
#data_sellout_c['sub_seg_nm'].unique()
#data_sellout_c['catg_nm'].unique()

data_sellout_c_head = data_sellout_c.head(20)
#list(data_sellout_c_head.columns) 

#AGRUPAMOS EL DATASET A NIVEL 
##TRY_CAST(null AS varchar) "Final SKU Desc Israel", ESTA COLUMNA LA AGREGAMOS AL FINAL 

data_sellout_c_grouped = data_sellout_c.groupby([
    "catg_nm", 	
    "ean_nbr", 
    "prod_desc",
    "Final_SKU_Desc_Israel",
    "flvr", 
    "brand_nm", 
    "sub_brand_nm", 
    "sub_seg_nm", 
    "seg_nm",
    "k_sub_seg_nm", 
    "typ_pk_nm", 
    "wgt", 
    "seg",
    "retlr_env_nm", 
    "chain_name", 
    "Nielsen_Geography"]).agg({
    'sold_unit':'sum',
    'sold_wgt_prod':'sum',
    'sale_val':'sum',
}).reset_index()
    

# DESPUES CALCULAMOS AL MISMO NIVEL DE AGRUPACION EL NUMERO DE TIENDAS Y EL STORE UNIVERSE (VER QUERY DEL VIEW PARA VER COMO LO HACEMOS EN PYTHON)
#STORE UNIVERSE
data_sellout_c_grouped_suniverse = data_sellout_c.groupby([
    "catg_nm", 
    "chain_name", 
    "retlr_env_nm", 
    "seg", 
    "Nielsen_Geography"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_suniverse.rename(columns = {'k_store_nbr':'Store_universe'}, inplace = True)        

#STORE NUMBER
data_sellout_c_grouped_snumber = data_sellout_c.groupby([
    "ean_nbr", 
    "catg_nm", 
    "chain_name", 
    "retlr_env_nm", 
    "seg", 
    "Nielsen_Geography"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_snumber.rename(columns = {'k_store_nbr':'Store_number'}, inplace = True)        

# LOS UNIMOS CON EL DATASET PREVIO POR EL NIVEL DE AGRUPACION DE CADA UNO 
data_sellout_c_snumber = pd.merge(data_sellout_c_grouped, data_sellout_c_grouped_snumber, how='left', left_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"], right_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"])
#data_sellout_c_snumber_head = data_sellout_c_snumber.head()  

data_sellout_c_suniverse_snumber = pd.merge(data_sellout_c_snumber, data_sellout_c_grouped_suniverse, how='left', left_on=["catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"], right_on=["catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"])

# ------------------------------------- COMENZAMOS A CALCULAR EL TOTAL MEXICO QUE ABARCA TODAS LAS NIELSEN AREAS ------------------------------------------

# AGRUPAMOS Y HACEMOS LAS AGREGACIONES

data_sellout_c_nielsen_total_mexico = data_sellout_c.groupby([
    "catg_nm", 	
    "ean_nbr", 
    "prod_desc", 
    "Final_SKU_Desc_Israel",
    "flvr", 
    "brand_nm", 
    "sub_brand_nm", 
    "sub_seg_nm", 
    "seg_nm",
    "k_sub_seg_nm", 
    "typ_pk_nm", 
    "wgt", 
    "seg",
    "retlr_env_nm", 
    "chain_name"]).agg({
    'sold_unit':'sum',
    'sold_wgt_prod':'sum',
    'sale_val':'sum',
}).reset_index()
        

# CALCULAMOS DE NUEVO EL STORES UNIVERSE Y STORE NUMBER PARA EL TOTAL MEXICO NIELSEN AREA
#STORE UNIVERSE
data_sellout_c_grouped_nielsen_suniverse = data_sellout_c.groupby([
    "catg_nm", 
    "chain_name", 
    "retlr_env_nm", 
    "seg"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_nielsen_suniverse.rename(columns = {'k_store_nbr':'Store_universe'}, inplace = True)        

#STORE NUMBER
data_sellout_c_grouped_nielsen_snumber = data_sellout_c.groupby([
    "ean_nbr", 
    "catg_nm", 
    "chain_name", 
    "retlr_env_nm", 
    "seg"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_nielsen_snumber.rename(columns = {'k_store_nbr':'Store_number'}, inplace = True)        

# LOS UNIMOS CON EL DATASET PREVIO POR EL NIVEL DE AGRUPACION DE CADA UNO 
data_sellout_c_nielsen_total_mexico = pd.merge(data_sellout_c_nielsen_total_mexico, data_sellout_c_grouped_nielsen_snumber, how='left', left_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg"], right_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg"])
#data_sellout_c_snumber_head = data_sellout_c_snumber.head()  

data_sellout_c_nielsen_total_mexico = pd.merge(data_sellout_c_nielsen_total_mexico, data_sellout_c_grouped_nielsen_suniverse, how='left', left_on=["catg_nm", "chain_name", "retlr_env_nm", "seg"], right_on=["catg_nm", "chain_name", "retlr_env_nm", "seg"])


# AGREGAMOS UNA NUEVA COLUMNA AL DATAFRAME RESULTANTE EN AREA NIELSEN TOTAL MEXICO
data_sellout_c_nielsen_total_mexico.insert(14,'Nielsen_Geography','Total Mexico')
#list(data_sellout_c_nielsen_total_mexico.columns) 
#list(data_sellout_c_suniverse_snumber.columns)

# HACEMOS EL CONCAT (PERO DE FILAS NO DE COLUMNAS) CON LA BASE MAIN 
#Unir el df1 y el df2
data_sellout_c_suniverse_snumber = pd.concat([data_sellout_c_suniverse_snumber, data_sellout_c_nielsen_total_mexico], ignore_index=True)


# ------------------------------------- COMENZAMOS A CALCULAR EL ALL RE QUE ABARCA TODOS LOS RETAIL ENVIRONMENTS ------------------------------------------

# AGRUPAMOS Y HACEMOS LAS AGREGACIONES

data_sellout_c_all_re_total_mexico = data_sellout_c.groupby([
    "catg_nm", 	
    "ean_nbr", 
    "prod_desc", 
    "Final_SKU_Desc_Israel",
    "flvr", 
    "brand_nm", 
    "sub_brand_nm", 
    "sub_seg_nm", 
    "seg_nm",
    "k_sub_seg_nm", 
    "typ_pk_nm", 
    "wgt", 
    "seg",
    "chain_name",
    "Nielsen_Geography"]).agg({
    'sold_unit':'sum',
    'sold_wgt_prod':'sum',
    'sale_val':'sum',
}).reset_index()
        

# CALCULAMOS DE NUEVO EL STORES UNIVERSE Y STORE NUMBER PARA EL TOTAL MEXICO NIELSEN AREA
#STORE UNIVERSE
data_sellout_c_grouped_all_re_suniverse = data_sellout_c.groupby([
    "catg_nm", 
    "chain_name", 
    "seg",
    "Nielsen_Geography"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_all_re_suniverse.rename(columns = {'k_store_nbr':'Store_universe'}, inplace = True)        

#STORE NUMBER
data_sellout_c_grouped_all_re_snumber = data_sellout_c.groupby([
    "ean_nbr", 
    "catg_nm", 
    "chain_name", 
    "seg",
    "Nielsen_Geography"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_all_re_snumber.rename(columns = {'k_store_nbr':'Store_number'}, inplace = True)        

# LOS UNIMOS CON EL DATASET PREVIO POR EL NIVEL DE AGRUPACION DE CADA UNO 
data_sellout_c_all_re_total_mexico = pd.merge(data_sellout_c_all_re_total_mexico, data_sellout_c_grouped_all_re_snumber, how='left', left_on=["ean_nbr","catg_nm", "chain_name", "seg", "Nielsen_Geography"], right_on=["ean_nbr","catg_nm", "chain_name", "seg", "Nielsen_Geography"])
#data_sellout_c_snumber_head = data_sellout_c_snumber.head()  

data_sellout_c_all_re_total_mexico = pd.merge(data_sellout_c_all_re_total_mexico, data_sellout_c_grouped_all_re_suniverse, how='left', left_on=["catg_nm", "chain_name", "seg", "Nielsen_Geography"], right_on=["catg_nm", "chain_name", "seg", "Nielsen_Geography"])


# AGREGAMOS UNA NUEVA COLUMNA AL DATAFRAME RESULTANTE EN AREA NIELSEN TOTAL MEXICO
data_sellout_c_all_re_total_mexico.insert(12,'retlr_env_nm','All RE')
#list(data_sellout_c_nielsen_total_mexico.columns) 
#list(data_sellout_c_suniverse_snumber.columns)

# HACEMOS EL CONCAT (PERO DE FILAS NO DE COLUMNAS) CON LA BASE MAIN 
#Unir el df1 y el df2
data_sellout_c_suniverse_snumber = pd.concat([data_sellout_c_suniverse_snumber, data_sellout_c_all_re_total_mexico], ignore_index=True)


# ------------------------------------------------ TODOS LOS RETAIL ENVIRONMENTS Y TODAS LAS AREAS NIELSEN ----------------------------------------------------------------------

# AGRUPAMOS HACEMOS LAS AGREGACIONES

data_sellout_c_all_re_nielsen_total_mexico = data_sellout_c.groupby([
    "catg_nm", 	
    "ean_nbr", 
    "prod_desc", 
    "Final_SKU_Desc_Israel",
    "flvr", 
    "brand_nm", 
    "sub_brand_nm", 
    "sub_seg_nm", 
    "seg_nm",
    "k_sub_seg_nm", 
    "typ_pk_nm", 
    "wgt", 
    "seg",
    "chain_name"]).agg({
    'sold_unit':'sum',
    'sold_wgt_prod':'sum',
    'sale_val':'sum',
}).reset_index()
        

# CALCULAMOS DE NUEVO EL STORES UNIVERSE Y STORE NUMBER PARA EL TOTAL MEXICO NIELSEN AREA
#STORE UNIVERSE
data_sellout_c_grouped_all_re_nielsen_suniverse = data_sellout_c.groupby([
    "catg_nm", 
    "chain_name", 
    "seg"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_all_re_nielsen_suniverse.rename(columns = {'k_store_nbr':'Store_universe'}, inplace = True)        

#STORE NUMBER
data_sellout_c_grouped_all_re_nielsen_snumber = data_sellout_c.groupby([
    "ean_nbr", 
    "catg_nm", 
    "chain_name", 
    "seg"]).agg({
    'k_store_nbr':'nunique'
}).reset_index()

#Cambiar el nombre de la columna 
data_sellout_c_grouped_all_re_nielsen_snumber.rename(columns = {'k_store_nbr':'Store_number'}, inplace = True)        

# LOS UNIMOS CON EL DATASET PREVIO POR EL NIVEL DE AGRUPACION DE CADA UNO 
data_sellout_c_all_re_nielsen_total_mexico = pd.merge(data_sellout_c_all_re_nielsen_total_mexico, data_sellout_c_grouped_all_re_nielsen_snumber, how='left', left_on=["ean_nbr","catg_nm", "chain_name", "seg"], right_on=["ean_nbr","catg_nm", "chain_name", "seg"])
#data_sellout_c_snumber_head = data_sellout_c_snumber.head()  

data_sellout_c_all_re_nielsen_total_mexico = pd.merge(data_sellout_c_all_re_nielsen_total_mexico, data_sellout_c_grouped_all_re_nielsen_suniverse, how='left', left_on=["catg_nm", "chain_name", "seg"], right_on=["catg_nm", "chain_name", "seg"])


# AGREGAMOS UNA NUEVA COLUMNA AL DATAFRAME RESULTANTE EN AREA NIELSEN TOTAL MEXICO
data_sellout_c_all_re_nielsen_total_mexico.insert(12,'retlr_env_nm','All RE')
data_sellout_c_all_re_nielsen_total_mexico.insert(14,'Nielsen_Geography','Total Mexico')

#list(data_sellout_c_nielsen_total_mexico.columns) 
#list(data_sellout_c_suniverse_snumber.columns)

# HACEMOS EL CONCAT (PERO DE FILAS NO DE COLUMNAS) CON LA BASE MAIN 
#Unir el df1 y el df2
data_sellout_c_suniverse_snumber = pd.concat([data_sellout_c_suniverse_snumber, data_sellout_c_all_re_nielsen_total_mexico], ignore_index=True)




# ------------------------------- AQUI COMENZAMOS A HACER LOS CALCULOS DE LAS NUEVAS COLUMNAS DE OUTLET COUNT POR MES, Y SUM DE SALES VALUE Y SOLD WGT -------------------------------------------------------------
#OUTLET COUNT PER MONTH (COLUMNS MONTHLY)
#"Aqui ver si hacemos un diccionario que relacione los meses de cada año 2021 y sus meses y 2022 y sus meses" Poner una lista y de dos valores año y mes y ver si este mes esta en 2021 y darle sino pasar al otro y asi 
#O hacer una columna de juntar meses con año y en base a esa hacer el for 

#data_sellout_cc = data_sellout_c
#data_sellout_cc['year_month'] = data_sellout_cc['yr'].astype(str) + data_sellout_cc['fisc_pd'].astype(str)
#dscc_head = data_sellout_cc.head(20)

years = list(set(data_sellout_c['yr'].unique()))
number_of_years = len(years)

#Esto de month creo que no lo usamos
month_dict = {
    1:"Jan",
    2:"Feb",
    3:"Mar",
    4:"Apr",
    5:"May",
    6:"Jun",
    7:"Jul",
    8:"Ago",
    9:"Sep",
    10:"Oct",
    11:"Nov",
    12:"Dec"
        }


#Configurar los dataframes donde iremos guardando mes por mes los valores 

#Ciclo de llenado del conteo y las sumas mensuales 
for i in range(number_of_years):
    #i=0
    #print(i)
    months = list(set(data_sellout_c[data_sellout_c['yr'] == years[i]]['fisc_pd'].unique()))

    for j in range(len(months)):
        #j=1
        # Filtramos por el mes que nos interesa
        df_mask = (data_sellout_c['yr'] == years[i]) & (data_sellout_c['fisc_pd'] == months[j]) 
        data_so_monthly = data_sellout_c[df_mask]
        
        #-- CONTAR LAS TIENDAS (OUTLET COUNT)
        # Agrupamos el dataset 

        data_sellout_cc_grouped_monthly_count_outlet = data_so_monthly.groupby([
            "ean_nbr", 
            "catg_nm", 
            "chain_name", 
            "retlr_env_nm", 
            "seg", 
            "Nielsen_Geography"]).agg({
            'k_store_nbr':'nunique'
        }).reset_index()

        period =  month_dict[months[j]] + '_' + years[i].astype(str)
        data_sellout_cc_grouped_monthly_count_outlet.rename(columns = {'k_store_nbr':'Count_outlet_%s'%period}, inplace = True)
        data_sellout_c_suniverse_snumber = pd.merge(data_sellout_c_suniverse_snumber, data_sellout_cc_grouped_monthly_count_outlet, how='left', left_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"], right_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"])
        data_sellout_c_suniverse_snumber['Count_outlet_%s'%period].fillna(0, inplace = True)
        
#Ciclo de llenado del sum value 
for i in range(number_of_years):
    #i=0
    #print(i)
    months = list(set(data_sellout_c[data_sellout_c['yr'] == years[i]]['fisc_pd'].unique()))
    
    for j in range(len(months)):
        #j=1
        # Filtramos por el mes que nos interesa
        df_mask = (data_sellout_c['yr'] == years[i]) & (data_sellout_c['fisc_pd'] == months[j]) 
        data_so_monthly = data_sellout_c[df_mask]
        
        #-- #SUMAR LAS VENTAS (VALUE SALES)
        # Agrupamos el dataset 
        
        data_sellout_cc_grouped_monthly_sum_sales = data_so_monthly.groupby([
            "ean_nbr", 
            "catg_nm", 
            "chain_name", 
            "retlr_env_nm", 
            "seg", 
            "Nielsen_Geography"]).agg({
            'sale_val':'sum'
        }).reset_index()

        period =  month_dict[months[j]] + '_' + years[i].astype(str)
        data_sellout_cc_grouped_monthly_sum_sales.rename(columns = {'sale_val':'Sum_Sales_value_%s'%period}, inplace = True)
        data_sellout_c_suniverse_snumber = pd.merge(data_sellout_c_suniverse_snumber, data_sellout_cc_grouped_monthly_sum_sales, how='left', left_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"], right_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"])
        data_sellout_c_suniverse_snumber['Sum_Sales_value_%s'%period].fillna(0, inplace = True)


#Ciclo de llenado del sum sold_wgt 
for i in range(number_of_years):
    #i=0
    #print(i)
    months = list(set(data_sellout_c[data_sellout_c['yr'] == years[i]]['fisc_pd'].unique()))

    for j in range(len(months)):
        #j=1
        # Filtramos por el mes que nos interesa
        df_mask = (data_sellout_c['yr'] == years[i]) & (data_sellout_c['fisc_pd'] == months[j]) 
        data_so_monthly = data_sellout_c[df_mask]
        
        #-- #SUMAR LAS SOLD WGT 
        # Agrupamos el dataset 
        
        data_sellout_cc_grouped_monthly_sum_sold_wgt = data_so_monthly.groupby([
            "ean_nbr", 
            "catg_nm", 
            "chain_name", 
            "retlr_env_nm", 
            "seg", 
            "Nielsen_Geography"]).agg({
            'sold_wgt_prod':'sum'
        }).reset_index()

        period =  month_dict[months[j]] + '_' + years[i].astype(str)
        data_sellout_cc_grouped_monthly_sum_sold_wgt.rename(columns = {'sold_wgt_prod':'Sum_Sold_wgt_%s'%period}, inplace = True)
        data_sellout_c_suniverse_snumber = pd.merge(data_sellout_c_suniverse_snumber, data_sellout_cc_grouped_monthly_sum_sold_wgt, how='left', left_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"], right_on=["ean_nbr","catg_nm", "chain_name", "retlr_env_nm", "seg", "Nielsen_Geography"])
        data_sellout_c_suniverse_snumber['Sum_Sold_wgt_%s'%period].fillna(0, inplace = True)




head_final = data_sellout_c_suniverse_snumber.head(50)

data_sellout_c_suniverse_snumber.to_csv(r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG\LH4\ASSORMENT\MT MX\FIXED DATASETS\ASSORMENT_MT_MX_%s.csv'%categoria, index = False, encoding = 'utf-8')


# OJO CONVERTIR NAN A 0 EN LAS COLUMNAS DE CONTEOS MENSUALES??
# OJO QUEDA VER LO DEL DISTINCT O GROUP BY EN LA VISTA 

