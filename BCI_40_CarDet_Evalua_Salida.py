# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_40_CarDet_Evalua_Salida
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_40_CarDet_Evalua_Salida.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011900276
# MAGIC * Autor: Gabriel MartÍnez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion criterios de salida de deterioro.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gagriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se agregan los Clientes que salen por LIR     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 09/04/2025 
# MAGIC * Descripción: Se eliminan las reglas de evaluacion para los Clientes que salen por Criterio LIR.     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 22/07/2025 
# MAGIC * Descripción: Para la evaluacion si el cliente u operacion sin refinanciamiento ni curse bajo mora, se remplaza la tabla tbl_cd_ope_condicion_ren por tbl_cd_ope_condicion_sal_ren.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_prin
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado
# MAGIC * {base_silver_x}.tbl_cd_segmentacion_cliente
# MAGIC * {base_silver_x}.tbl_cd_cliente_consolidado
# MAGIC * {base_silver_x}.tbl_cd_cliente_det_ssff 
# MAGIC * {base_silver_x}.tbl_cd_cliente_det_fact
# MAGIC * {base_silver_x}.tbl_cd_cliente_lir
# MAGIC * {base_silver_x}.tbl_cd_ope_dia_mora
# MAGIC * {base_silver_x}.tbl_cd_ope_pag_cons_ibm
# MAGIC * {base_silver_x}.tbl_cd_dat_ope_ini_cd
# MAGIC * {base_silver_x}.tbl_cd_ope_condicion_ren
# MAGIC * {base_silver_x}.tbl_cd_ope_curse_bajo_mora
# MAGIC * {base_silver_x}.tbl_cd_ope_ent_meses
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_ope_eval
# MAGIC ***************************************************************************
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Dependencias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga funciones comunes

# COMMAND ----------

# MAGIC %run "./Funciones_Comunes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setea Parámetros

# COMMAND ----------


dbutils.widgets.text("fecha_w","","01-Fecha:")
dbutils.widgets.text("bd_silver_w","","03-Nombre BD Silver:")

fecha_x = dbutils.widgets.get("fecha_w")
base_silver_x = dbutils.widgets.get("bd_silver_w")

spark.conf.set("bci.fecha", fecha_x)
spark.conf.set("bci.dbnamesilver", base_silver_x)

print(f"Fecha de Proceso actual: [fecha_x] {fecha_x}")
print(f"Nombre BD Silver: [base_silver_x] {base_silver_x}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Valida parámetros

# COMMAND ----------

# DBTITLE 1,Valida parámetro "FechaX"
valida_parametro(fecha_x)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "base_silverX"
valida_parametro(base_silver_x)

# COMMAND ----------

# MAGIC %md
# MAGIC ## INICIO Proceso extraccion y transformacion
# MAGIC --------------------------------------
# MAGIC - Por cada fuente que se utilice se debe:
# MAGIC      - Titulo: generar un titulo generico, con nombre fuente, y descripcion del proposito de la extraccion
# MAGIC      - Extraer: para el periodo, o rango de fecha que se necesita la iformacion. Debe tener el prefijo tmp_EXT_{nombrefuente}
# MAGIC      - Transformar: generar la informacion necesaria para la salida final. Se pueden generar mas de una tabla temporal para llegar al resultado final. Debe tener el prefijo tmp_RES_{nombre}_correlativo
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parametrizacion
# MAGIC ---
# MAGIC * define y asigna valores a los parametros
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parametria interna
#parametros internos notebook
p_cod_seg_ind='I'
p_evaluaciones = 'S01','S02','S03','S04','S05','S06','S07','S08','S09','S10','S11','S12','S13','S14'

print(f"p_cod_seg_ind: {p_cod_seg_ind}")
print(f"p_evaluaciones: {p_evaluaciones}")


# COMMAND ----------

# DBTITLE 1,Obtiene parametros tabla parametros
resultado = obtener_parametros_de_tbl(base_silver_x)
param_cal = resultado[0]
p_fec_cv = resultado[1]
p_crit_det_sal_tot_ifrs = resultado[2]
p1_valordvcn = resultado[3]
p1_valordvcx = resultado[4]
p1_valorcdir = resultado[5]
p1_valordmor = resultado[6]
p_cantminmes = resultado[7]
p1_diasmora = resultado[8]
p_pagcons = resultado[9]
p_cant_min_pag = resultado[10]
p3_montosbif = resultado[11]
p1_valor90180 = resultado[17]
p1_valor180 = resultado[18]
p1_valor3090 = resultado[19]


print('Valor param_cal :',param_cal )
print('Valor p_fec_cv :',p_fec_cv )
print('Valor p_crit_det_sal_tot_ifrs :',p_crit_det_sal_tot_ifrs)
print('Valor p1_valordvcn :',p1_valordvcn)
print('Valor p1_valordvcx :',p1_valordvcx)
print('Valor p1_valorcdir :',p1_valorcdir)
print('Valor p1_valordmor :',p1_valordmor)
print('Valor p_cantminmes :',p_cantminmes)
print('Valor p1_diasmora :',p1_diasmora)
print('Valor p_pagcons :',p_pagcons)
print('Valor p_cant_min_pag :',p_cant_min_pag)
print('Valor p3_montosbif :',p3_montosbif)
print('Valor p1_valor90180 :',p1_valor90180)
print('Valor p1_valor180 :',p1_valor180)
print('Valor p1_valor3090 :',p1_valor3090)


# COMMAND ----------

# DBTITLE 1,Fecha Tope Clientes LIR
#Para el cálculo de clientes LIR se debe tomar los ultimos 12 meses a partir de la fecha de proceso
p_fec_12_meses_atras=fecha_x_meses_atras(fecha_x,12)
print(f"p_fec_12_meses_atras: {p_fec_12_meses_atras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Deteriorados
# MAGIC --------------------------------------
# MAGIC - Extrae todos las operaciones deterioradas del proceso actual
# MAGIC - Una operacion puede estar deteriorada por varios motivos, aca solo interesa la operacion deteriorada, no el motivo. 

# COMMAND ----------

paso_query1 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_crit AS
SELECT  
  A.periodo_cierre             AS     periodo_cierre,
  A.fecha_cierre               AS     fecha_cierre,
  A.tipo_proceso               AS     tipo_proceso,
  A.rut_cliente                AS     rut_cliente,
  A.dv_rut_cliente             AS     dv_rut_cliente,
  A.tipo_operacion             AS     tipo_operacion,
  A.operacion                  AS     operacion,
  A.sistema                    AS     sistema,
  A.segmento                   AS     segmento,
  A.grupo                      AS     grupo,
  A.periodo_evaluacion         AS     periodo_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_cartdet_crit_ent_prin A
WHERE
    A.fecha_cierre =   {fecha_x} 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, sistema ORDER BY A.fecha_cierre DESC, A.periodo_evaluacion ASC) =1
"""

# COMMAND ----------

sql_safe(paso_query1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: cliente individual o grupal
# MAGIC ---
# MAGIC * Si cliente es individual marca todas las operaciones con flag =1 
# MAGIC

# COMMAND ----------

paso_query100 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_1 as
SELECT
 A.periodo_cierre                              AS periodo_cierre,
 A.fecha_cierre                                AS fecha_cierre,
 A.tipo_proceso                                AS tipo_proceso,
 A.segmento                                    AS segmento,
 A.operacion                                   AS operacion,
 A.tipo_operacion                              AS tipo_operacion,
 A.sistema                                     AS sistema,
 A.rut_cliente                                 AS rut_cliente,
 A.dv_rut_cliente                              AS dv_rut_cliente ,
 'cli_segmento_cliente'                            AS nombre_campo,
 cast(A.cli_segmento_cliente as string)            AS valor_campo,
 'Cliente con segmento individual'                 AS condicion_regla,     
 "[eq to {p_cod_seg_ind}]"                               AS valor_regla,
 CASE 
     WHEN IFNULL(trim(A.cli_segmento_cliente),'SD') = '{p_cod_seg_ind}' 
     THEN 1 
     ELSE 0 
 END                                          AS flag_resultado_regla,
 'S01'                                        AS cod_evaluacion
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro  A
"""


# COMMAND ----------

sql_safe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: cliente SIN clasificacion de deterioro (2)
# MAGIC ---
# MAGIC * Si cliente NO tiene clasificacion de deterioro entonces flag =1 
# MAGIC

# COMMAND ----------

paso_query105 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_2 as
SELECT
 A.periodo_cierre                                AS periodo_cierre,
 A.fecha_cierre                                  AS fecha_cierre,
 A.tipo_proceso                                  AS tipo_proceso,
 A.segmento                                      AS segmento,
 A.operacion                                     AS operacion,
 A.tipo_operacion                                AS tipo_operacion,
 A.sistema                                       AS sistema,
 A.rut_cliente                                   AS rut_cliente,
 A.dv_rut_cliente                                AS dv_rut_cliente ,
'cli_calificacion_bci'                               AS nombre_campo,
 cast(A.cli_calificacion_bci as string)              AS valor_campo,
 'Cliente sin calificacion de deterioro'         AS condicion_regla,     
 "[NOT IN {param_cal}]"                            AS valor_regla,
 CASE 
    WHEN trim(IFNULL(A.cli_calificacion_bci,'0')) NOT IN {param_cal} 
    THEN 1 
    ELSE 0 
  END                                            AS flag_resultado_regla,
 'S02'                                           AS cod_evaluacion  
FROM 
     {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A 
"""


# COMMAND ----------

sql_safe(paso_query105)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: operaciones con saldo cero (27)
# MAGIC ---
# MAGIC * Si operacion tiene saldo total ifrs cero entonces  flag =1 
# MAGIC

# COMMAND ----------

paso_query110 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_3 as
SELECT    
    A.periodo_cierre                                AS periodo_cierre,
    A.fecha_cierre                                  AS fecha_cierre,
    A.tipo_proceso                                  AS tipo_proceso,
    A.segmento                                      AS segmento,
    A.operacion                                     AS operacion,
    A.tipo_operacion                                AS tipo_operacion,
    A.sistema                                       AS sistema,
    A.rut_cliente                                   AS rut_cliente,
    A.dv_rut_cliente                                AS dv_rut_cliente ,
    'saldo_total_ifrs'                                              AS nombre_campo,
    cast(IFNULL(A.saldo_total_ifrs,0) as string)                     AS valor_campo,
    'Operacion con saldo igual o menor a  {p_crit_det_sal_tot_ifrs}'     AS condicion_regla,     
    '[<= {p_crit_det_sal_tot_ifrs}]'                       AS valor_regla,            
    CASE 
       WHEN IFNULL(A.saldo_total_ifrs,0) <= {p_crit_det_sal_tot_ifrs} 
       THEN 1 
       ELSE 0 
     END                                             AS flag_resultado_regla,
     'S03'                                           AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A
"""

# COMMAND ----------

sql_safe(paso_query110)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Mora SBIF cliente (62)
# MAGIC ---
# MAGIC * Si cliente tiene mora menor o igual al parametro entonces  flag =1 
# MAGIC

# COMMAND ----------

paso_query115 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_4 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    "cli_mora_sbif_f"                                           AS nombre_campo,
    cast(A.cli_mora_sbif_f as string)                             AS valor_campo,
    "Cliente con deuda morosa sbif menor o igual a: {p3_montosbif}"         AS condicion_regla,
    "[<= {p3_montosbif}]" AS valor_regla,           
    CASE 
         WHEN IFNULL(A.cli_mora_sbif_f,0) <= {p3_montosbif} 
         THEN 1 
         ELSE 0 
    END                                                         AS flag_resultado_regla,    
   'S04'                                                        AS cod_evaluacion  
FROM
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A 
"""

# COMMAND ----------

sql_safe(paso_query115)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Clientes deteriorados SSFF (40)
# MAGIC ---
# MAGIC * Si cliente NO esta informado deteriorado en SSFF entonces  flag =1 
# MAGIC

# COMMAND ----------

paso_query120 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_5 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    "flag_existe_cliente_ssff "                                 AS nombre_campo,
    cast(A.flag_existe_cliente_ssff as string)                  AS valor_campo,
    "Cliente sin deterioro SSFF"                                AS condicion_regla,
    "[eq to 0 (NOEXISTE)]"                                        AS valor_regla,           
    CASE 
         WHEN IFNULL(A.flag_existe_cliente_ssff,0) = 0 
         THEN 1 
         ELSE 0 
    END                                                         AS flag_resultado_regla,    
   'S05'                                                        AS cod_evaluacion  
FROM
     {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A 
"""

# COMMAND ----------

sql_safe(paso_query120)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Clientes deteriorados Factoring (41)
# MAGIC ---
# MAGIC * Si cliente NO esta informado deteriorado en factoring entonces  flag =1 
# MAGIC

# COMMAND ----------

paso_query125 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_6 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    "flag_existe_cliente_fact "                                 AS nombre_campo,
    cast(A.flag_existe_cliente_fact as string)                  AS valor_campo,
    "Cliente sin deterioro FACTORING"                           AS condicion_regla,
    "[eq to 0 (NOEXISTE)]"                                       AS valor_regla,           
    CASE 
         WHEN IFNULL(A.flag_existe_cliente_fact,0) = 0 
         THEN 1 
         ELSE 0 
    END                                                         AS flag_resultado_regla,    
   'S06'                                                        AS cod_evaluacion  
FROM
     {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A 
"""

# COMMAND ----------

sql_safe(paso_query125)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: clientes LIR (67)
# MAGIC ---
# MAGIC * Si cliente NO esta informado en archivo LIR entonces  flag =1 
# MAGIC

# COMMAND ----------

paso_query135 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_7 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    "flag_existe_cliente_lir "                                  AS nombre_campo,
    cast(A.flag_existe_cliente_lir as string)                   AS valor_campo,
    "Cliente no informado como LIR"                             AS condicion_regla,
    "[eq to 0 (NOEXISTE)]"                                      AS valor_regla,           
    CASE 
         WHEN IFNULL(A.flag_existe_cliente_lir,0) = 0 
         THEN 1 
         ELSE 0 
    END                                                         AS flag_resultado_regla,    
    'S07'                                                       AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro  A
"""


# COMMAND ----------

sql_safe(paso_query135)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Antiguedad clientes LIR (67)
# MAGIC ---
# MAGIC * Si cliente tiene mas de 12 meses de antiguedad desde la ultima vez que se informo como lir entonces  flag =1 
# MAGIC

# COMMAND ----------

paso_query140 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_15 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    'cli_fecha_informada_lir'                                   AS nombre_campo,
    cast(IFNULL(A.cli_fecha_informada_lir,00000000) as string)  AS valor_campo,
    "Cliente informado como lir con antiguedad mayor a 12 meses"     AS condicion_regla,     
    "[ <= {p_fec_12_meses_atras}]"                                AS valor_regla,           
    CASE 
       WHEN IFNULL(A.cli_fecha_informada_lir,19000101) <= {p_fec_12_meses_atras}
       THEN 1 
       ELSE 0 
    END AS flag_resultado_regla,
    'S15'                                                       AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro   A
"""


# COMMAND ----------

sql_safe(paso_query140)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Clientes con morosidad bci (60)
# MAGIC ---
# MAGIC - Si cliente tiene mora menor a una cantidad parametrica ({p1_diasmora}) en periodo actual, entonces flag =1 
# MAGIC

# COMMAND ----------

paso_query150 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_8 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    'max_dia_mora_pact'                                          AS nombre_campo,
    cast(IFNULL(A.max_dia_mora_pact,0) as string)               AS valor_campo,
    "Cliente con maximo dias de mora menor o igual a"               AS condicion_regla,     
    "[<= {p1_diasmora}]"                                             AS valor_regla,           
    CASE 
       WHEN IFNULL(A.max_dia_mora_pact,0) <= {p1_diasmora}
       THEN 1 
       ELSE 0 
    END                                                         AS flag_resultado_regla,
    'S08'                                                       AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro  A
"""

# COMMAND ----------

sql_safe(paso_query150)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Operacion con 4 o mas pagos consecutivos (61)
# MAGIC ---
# MAGIC - Si operacion tiene mas de n o mas pagos consecutivos entonces   flag =1 
# MAGIC

# COMMAND ----------

paso_query160 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_9 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    'cont_pag_cons'                                             AS nombre_campo,
    cast(IFNULL(A.cont_pag_cons,0) as string)                   AS valor_campo,
    "Operacion con pagos consecutivos mayor o igual a"          AS condicion_regla,     
    "[>={p_pagcons}]"                                           AS valor_regla,           
    CASE 
       WHEN IFNULL(A.cont_pag_cons,0) >= {p_pagcons}
       THEN 1 
       ELSE 0 
    END                                                         AS flag_resultado_regla,
    'S09'                                                       AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro   A
"""

# COMMAND ----------

sql_safe(paso_query160)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Operacion con pagos parciales (63)
# MAGIC ---
# MAGIC - Si operacion tiene mas de n o mas pagos parciales entonces   flag =1 

# COMMAND ----------

paso_query170 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_10 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    'ope_pag_parciales_ini_cd'                                  AS nombre_campo,
    cast(IFNULL(A.ope_pag_parciales_ini_cd,0) as string)        AS valor_campo,
    "Operacion con pagos parciales mayor o igual a"             AS condicion_regla,     
    "[>= {p_cant_min_pag}]"                                     AS valor_regla,           
    CASE 
       WHEN IFNULL(A.ope_pag_parciales_ini_cd,0) >= {p_cant_min_pag}
       THEN 1 
       ELSE 0 
    END                                                         AS flag_resultado_regla,
    'S10'                                                       AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro   A
"""

# COMMAND ----------

sql_safe(paso_query170)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Cliente operacion sin refinanciamiento ni curse bajo mora (64)
# MAGIC ---
# MAGIC - Si cliente o operacion no tiene refinanciamiento o curse bajo mora entonces   flag =1 

# COMMAND ----------

paso_query190 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_11 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    "flag_ope_ren_pact-flag_existe_operacion_rfz"                                  AS nombre_campo,
    concat('[',cast(A.flag_ope_ren_pact as string),'-', cast(A.flag_existe_operacion_rfz as string),']')                  AS valor_campo,
    "Cliente sin renegociados ni reestructuracio forzosa"                             AS condicion_regla,
    concat('[',"eq to 0 (NOREN)",' AND ',"eq to 0 (NORFZS)",']') AS valor_regla,           
    CASE 
         WHEN IFNULL(A.flag_ope_ren_pact,0) = 0 AND IFNULL(A.flag_existe_operacion_rfz,0) = 0
         THEN 1 
         ELSE 0 
    END                                                         AS flag_resultado_regla,    
    'S11'                                                       AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro   A

"""

# COMMAND ----------

sql_safe(paso_query190)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Operaciones con pago de capital (65)
# MAGIC ---
# MAGIC - Si operacion tiene pago de capital en mes actual entonces   flag =1 
# MAGIC - evalua si ha pagado capital desde que entro a deterioro la operacion
# MAGIC - el calculo para el campo ope_pag_capital_ifrs_ini_cd corresponde a la diferencia de capital desde la fecha de entrada a deterioro a la fecha actual de proceso
# MAGIC

# COMMAND ----------

paso_query200 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_12 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    'ope_pag_capital_ifrs_ini_cd'                                        AS nombre_campo,
    IFNULL(A.ope_pag_capital_ifrs_ini_cd,0)                              AS valor_campo,
    "Operacion con pago de saldo de capital ifrs"                    AS condicion_regla,     
    "[ >= {p_crit_det_sal_tot_ifrs } ]"                           AS valor_regla,           
    CASE 
       WHEN  IFNULL(A.ope_pag_capital_ifrs_ini_cd,0) > {p_crit_det_sal_tot_ifrs }
       THEN 1 
       ELSE 0 
    END                                                         AS flag_resultado_regla,
    'S12'                                                       AS cod_evaluacion  
FROM 
     {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro   A
"""

# COMMAND ----------

sql_safe(paso_query200)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Operacion Minimo de meses en cartdet (66)
# MAGIC ---
# MAGIC - Si cliente tiene un minimo de meses, {p_cantminmes},  en cartera deteriorada,  entonces   flag =1 

# COMMAND ----------

paso_query220 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_13 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    'ope_num_meses_en_cd'                                                 AS nombre_campo,
    IFNULL(A.ope_num_meses_en_cd,0)                                       AS valor_campo,
    "Operacion con minimo de meses en cartera deteriorada mayor o igual a"      AS condicion_regla,     
    "[ >= {p_cantminmes} ]"                                              AS valor_regla,           
    CASE 
       WHEN IFNULL(A.ope_num_meses_en_cd,0) >= {p_cantminmes}
       THEN 1 
       ELSE 0 
    END                                                         AS flag_resultado_regla,
    'S13'                                                       AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro   A
"""

# COMMAND ----------

sql_safe(paso_query220)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Operaciones deterioradas periodo anterior no informadas en periodo actual (30)
# MAGIC ---
# MAGIC - Si operacion está deteriorada en periodo anterior y no existe en periodo actual,  entonces   flag =1 

# COMMAND ----------

paso_query225 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_14 as
SELECT
    A.periodo_cierre                                            AS periodo_cierre,
    A.fecha_cierre                                              AS fecha_cierre,
    A.tipo_proceso                                              AS tipo_proceso,
    A.segmento                                                  AS segmento,
    A.operacion                                                 AS operacion,
    A.tipo_operacion                                            AS tipo_operacion,
    A.sistema                                                   AS sistema,
    A.rut_cliente                                               AS rut_cliente,
    A.dv_rut_cliente                                            AS dv_rut_cliente ,
    'operacion - sistema'                                       AS nombre_campo,
    concat('[',CAST(IFNULL(B.operacion,'NOEXISTE-PACT') AS string),'-',CAST(IFNULL(B.sistema,'') as string),']')          AS valor_campo,
    "Operacion deteriorada periodo anterior no existe en periodo actual"    AS condicion_regla,     
    "[NOEXISTE-PACT]"          AS valor_regla,           
    CASE 
       WHEN trim(B.operacion) is null
       THEN 1 
       ELSE 0 
    END                                                         AS flag_resultado_regla,
    'S14'                                                       AS cod_evaluacion  
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_crit A
LEFT JOIN
    dsr_gld_bciwork_db.tbl_cd_ope_condicion_salida_deterioro   B
ON  trim(A.operacion) = trim(B.operacion)
    AND trim(A.sistema) = trim(B.sistema)
"""

# COMMAND ----------

sql_safe(paso_query225)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaludado (tmp_tbl_cartdet_crit_ent_ope_campo)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones evaluadas
# MAGIC

# COMMAND ----------


paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_ope_eval AS
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_1 
UNION 
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_2 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_3 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_4
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_5
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_6
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_7
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_8
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_9
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_10
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_11
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_12
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_13
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_14
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_15
"""  

# COMMAND ----------

sql_safe(paso_query250)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Evaluacion 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query300 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_crit_sal_ope_eval """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------


paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_sal_ope_eval
SELECT 
    IFNULL(periodo_cierre,19000101),
    IFNULL(fecha_cierre,190001),
    IFNULL(tipo_proceso,' '),
    IFNULL(segmento,' '),
    IFNULL(operacion,' '),
    IFNULL(tipo_operacion,' '),
    IFNULL(sistema,' '),
    IFNULL(rut_cliente,0),
    IFNULL(dv_rut_cliente,' '),
    IFNULL(nombre_campo,' '),
    IFNULL(valor_campo,' '),
    IFNULL(condicion_regla,' '),
    IFNULL(valor_regla,' '),
    IFNULL(flag_resultado_regla,0),
    IFNULL(cod_evaluacion,' ')
FROM
  tmp_tbl_cartdet_crit_sal_ope_eval
"""  


# COMMAND ----------

sql_safe(paso_query310)

# COMMAND ----------

# MAGIC %md
# MAGIC ##ESTADISTICAS

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC fecha_cierre, 
# MAGIC cod_evaluacion, 
# MAGIC count(1) 
# MAGIC from  ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_ope_eval 
# MAGIC group by 1,2 order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")