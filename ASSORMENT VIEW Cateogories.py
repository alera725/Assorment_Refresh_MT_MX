# -*- coding: utf-8 -*-
"""
Created on Thu May 12 18:53:09 2022

@author: mxka1r54
"""

# CADA CORRIDA TENDREMOS QUE AJUSTAR LAS FECHAS 
# OJO SI HAY NUEVOS CAMBIOS EN LOS CHAINS (COMO WALMART SE CONVIRTIO EN SAMS EN CLUBS)
# ----------------------------------- VISTA KEYSTONE CATEGORIES (USADA PARA EL SCRIPT DE PYTHON) -----------------------------------------------------------------------

CREATE OR REPLACE VIEW dev_raw.assorment_lh4_join_keystone_weekly_sellout_and_calendar AS 
SELECT 
DISTINCT
"a"."catg_nm", 	
"a"."yr",
"b"."fisc_wk", 
"b"."fisc_pd",
"a"."ean_nbr", 
"a"."prod_desc", 
"c"."sku_desc" "Final_SKU_Desc_Israel",
"a"."flvr", 
"a"."brand_nm", 
"a"."sub_brand_nm", 
"a"."sub_seg_nm", 
"a"."seg_nm",
"a"."k_sub_seg_nm", 
"a"."typ_pk_nm", 
"a"."wgt", 
"a"."chnl_nm", 
"a"."retlr_env_nm", 
"a"."chn_nm", 
"a"."k_store_nbr",
(CASE WHEN ("a"."chn_nm" IN ('CHEDRAUI')) THEN 'CHEDRAUI' WHEN ("a"."chn_nm" IN ('WAL-MART')) THEN 'WAL-MART' WHEN ("a"."chn_nm" IN ('SORIANA')) THEN 'SORIANA' WHEN ("a"."chn_nm" IN ('SAMS')) THEN 'WAL-MART' ELSE 'Rest' END) "chain_name",
"regexp_replace"("a"."sub_zn_nm", '([A-Z])', '') "Nielsen_Geography", 
(CASE WHEN ("a"."chnl_nm" IN ('AFFLUENT')) THEN 'Seg1' WHEN ("a"."chnl_nm" IN ('ASPIRANT')) THEN 'Seg2' WHEN ("a"."chnl_nm" IN ('ECONOMY')) THEN 'Seg3' WHEN ("a"."chnl_nm" IN ('STOCK UP')) THEN 'Seg4' WHEN ("a"."chnl_nm" IN ('ECONOMY PROXIMITY')) THEN 'Seg5' WHEN ("a"."chnl_nm" IN ('CLUBS AFFLUENT', 'CLUBS ASPIRANT', 'CLUBS ECONOMY')) THEN 'Seg6' ELSE "a"."chnl_nm" END) "seg",
"a"."sold_unit", 
"a"."sold_wgt_prod", 
"a"."sale_val"
FROM dev_raw.keystone_weekly_sellout_v a
    INNER JOIN (
    SELECT 
        fisc_yr,
        fisc_pd,
        fisc_wk,
        fisc_wk_end_dt,
        fisc_yr_pd
    FROM dev_raw.keystone_md_calendar
    WHERE fisc_yr >= 2021
    GROUP BY fisc_yr, fisc_pd, fisc_wk, fisc_wk_end_dt, fisc_yr_pd
) b ON (b.fisc_yr = a.yr) AND (b.fisc_wk = a.wk_by_yr)
    LEFT JOIN (
    SELECT 
    ean_nbr,
    sku_desc
    FROM dev_cons.lh3_pvp_sap_md_sku_groups_dev
        ) c ON (a.ean_nbr = c.ean_nbr)
WHERE a.ctry_nm = 'MEXICO' AND ((a.yr = 2021 AND b.fisc_pd>=4) OR (a.yr = 2022 AND b.fisc_pd<=3)) AND a.chnl_nm IN ('AFFLUENT', 'ASPIRANT', 'ECONOMY', 'STOCK UP', 'ECONOMY PROXIMITY', 'CLUBS AFFLUENT', 'CLUBS ASPIRANT', 'CLUBS ECONOMY') AND a.sub_zn_nm IN ('A1', 'A2', 'A3', 'A4', 'A5', 'A6') AND retlr_env_nm IN ('SUPER','HYPER','SOFT DISCOUNTER','CLUBS','CEDIS')
