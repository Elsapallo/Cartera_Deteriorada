# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_41_CarDet_Evalua_Sal_Exc
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_41_CarDet_Evalua_Sal_Exc.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011897637
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion criterior de excepciones salida deterioro
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gagriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se agrega salida por InterSegmento     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_prin
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_ope_eval
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado
# MAGIC * {base_silver_x}.tbl_cd_cliente_lir
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
# MAGIC ### Carga funciones comunes

# COMMAND ----------

# MAGIC %run "./Funciones_Comunes"

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

#PARAMETROS INTERNOS NOTEBOOK
p_filiales = '12','13'
p_evaluaciones = 'N01','N02','N03','N04'
p_segcliente = 'S01'
p_califbci = 'S02'
p_opedetant = 'S14'
p_flagres1 = '1'
p_flagres2 = '0'

print(f"p_filiales: {p_filiales}")
print(f"p_evaluaciones: {p_evaluaciones}")
print(f"p_segcliente: {p_segcliente}")
print(f"p_califbci: {p_califbci}")
print(f"p_opedetant: {p_opedetant}")
print(f"p_flagres1: {p_flagres1}")
print(f"p_flagres2: {p_flagres2}")


# COMMAND ----------

#PARAMETRIA PROCESO
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


# COMMAND ----------

# DBTITLE 1,Fecha Tope Clientes LIR
#Para el cálculo de clientes LIR se debe tomar los ultimos 12 meses a partir de la fecha de proceso
p_fec_12_meses_atras=fecha_x_meses_atras(fecha_x,12)
print(f"p_fec_12_meses_atras: {p_fec_12_meses_atras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Excepcion operaciones con monto deuda ifrs cero
# MAGIC ---
# MAGIC * Operaciones con deuda total ifrs cero, pero es del tipo Nova CCN455, COLMN CON731 Y COLMN COM732
# MAGIC * flag_resultado_regla =1: significa que es una excepcion a la regla de salida por saldo total ifrs cero

# COMMAND ----------

paso_query30 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_1 as
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
    'saldo_total_ifrs - (sistema - tipo_operacion)'               AS nombre_campo,
    concat('[',cast(trim(A.saldo_total_ifrs) as string),'-',cast(trim(A.sistema) as string),'-',cast(trim(A.tipo_operacion) as string),']')        AS valor_campo,
    'Excepcion saldo capital ifrs cero para'   AS condicion_regla,     
    '[eq to 0 - AND  (20-CCN455, 01-CON731, 01-COM732)]'                AS valor_regla,           
    CASE 
       WHEN trim(A.sistema) = '20' AND trim(A.tipo_operacion)='CCN455' THEN 1
       WHEN trim(A.sistema) = '01' AND trim(A.tipo_operacion)='CON731' THEN 1
       WHEN trim(A.sistema) = '01' AND trim(A.tipo_operacion)='COM732' THEN 1
       ELSE 0
    END                                                          AS flag_resultado_regla,
    'N01'                                                        AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A
WHERE 
    IFNULL(A.saldo_total_ifrs,0) = {p_crit_det_sal_tot_ifrs}
"""


# COMMAND ----------

sql_safe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Excepcion operaciones con monto deuda ifrs cero, PERO CLIENTE LIR
# MAGIC ---
# MAGIC * Operaciones con deuda total ifrs cero, pero es cliente lir 
# MAGIC * flag_resultado_regla =1: significa que es una excepcion a la regla de salida por saldo total ifrs cero
# MAGIC

# COMMAND ----------

paso_query40 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_2 as
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
    'saldo_total_ifrs - criterio_entrada_cliente'               AS nombre_campo,
    concat('[',cast(trim(A.saldo_total_ifrs) as string),'-', cast(IFNULL(A.criterio_entrada_cliente,'') as string),']')      AS valor_campo,
    'Excepcion saldo capital ifrs cero para cliente deteriorados LIR'           AS condicion_regla,     
    "[0 - 11]"                AS valor_regla,        
    CASE 
       WHEN IFNULL(A.criterio_entrada_cliente,0) = 11 THEN 1
       ELSE 0
    END                                                          AS flag_resultado_regla,
    'N02'                                                        AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro  A
WHERE 
    IFNULL(A.saldo_total_ifrs,0) = {p_crit_det_sal_tot_ifrs}
"""


# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Excepcion operaciones con monto deuda ifrs cero, PERO operacion INDIVIDUAL
# MAGIC ---
# MAGIC * Operaciones con deuda total ifrs cero, pero operacion  es segmento individual
# MAGIC * flag_resultado_regla =1: significa que es una excepcion a la regla de salida por saldo total ifrs cero
# MAGIC

# COMMAND ----------

paso_query50 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_3 as
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
    'saldo_total_ifrs - criterio_entrada_cliente'               AS nombre_campo,
    concat('[',cast(trim(A.saldo_total_ifrs) as string),'-', cast(IFNULL(A.criterio_entrada_cliente,'') as string),']')      AS valor_campo,
    'Excepcion saldo capital ifrs cero para cliente deteriorados LIR'           AS condicion_regla,     
    "[0 - 1]"                AS valor_regla,        
    CASE 
       WHEN IFNULL(A.criterio_entrada_cliente,0) = 1 THEN 1
       ELSE 0
    END                                                          AS flag_resultado_regla,
    'N03'                                                        AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A
WHERE 
    IFNULL(A.saldo_total_ifrs,0) = {p_crit_det_sal_tot_ifrs}
"""


# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Excepcion operaciones con monto deuda ifrs cero, PERO operacion Filiales
# MAGIC ---
# MAGIC * Operaciones con deuda total ifrs cero, pero operacion  Filiales
# MAGIC * flag_resultado_regla =1: significa que es una excepcion a la regla de salida por saldo total ifrs cero
# MAGIC

# COMMAND ----------

paso_query60 =  f"""
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
    'saldo_total_ifrs - criterio_entrada_cliente'               AS nombre_campo,
    concat('[',cast(trim(A.saldo_total_ifrs) as string),'-', cast(IFNULL(A.criterio_entrada_cliente,'') as string),']')      AS valor_campo,
    'Excepcion saldo capital ifrs cero para cliente deteriorados LIR'           AS condicion_regla,     
    "[0 - 12]"                AS valor_regla,        
    CASE 
       WHEN IFNULL(A.criterio_entrada_cliente,0) = 12 THEN 1
       ELSE 0
    END                                                          AS flag_resultado_regla,
    'N04'                                                        AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A
WHERE 
    IFNULL(A.saldo_total_ifrs,0) = {p_crit_det_sal_tot_ifrs}
"""

# COMMAND ----------

sql_safe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Excepcion operaciones grupal que sale de deterioro por Irradiación Intersegmento.
# MAGIC ---
# MAGIC * Operaciones Grupal con deterioro, pero que tambien contiene el cliente operaciones  de segmento individual sin deterioro.
# MAGIC * flag_resultado_regla =1: significa que es una excepcion a la regla de salida 

# COMMAND ----------

paso_query70 =  f"""
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
    'cli_segmento_cliente - segmento - cli_calificacion_bci'               AS nombre_campo,
    concat('[',cast(A.cli_segmento_cliente as string),'-',cast(A.segmento as string),'-',cast(A.cli_calificacion_bci as string),']')      AS valor_campo,
    "Cliente individual no deteriorado con operaciones grupales deterioradas (intersegmento)"           AS condicion_regla,     
    "[I - G - {param_cal}]"                           AS valor_regla,           
    CASE 
       WHEN trim(IFNULL(A.cli_calificacion_bci,'0')) NOT IN {param_cal} THEN 1
       ELSE 0
    END                                                          AS flag_resultado_regla,
    'N05'                                                        AS cod_evaluacion  
FROM 
   {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro A
WHERE
    A.cli_segmento_cliente='I' and A.segmento='G'
"""


# COMMAND ----------

sql_safe(paso_query70)

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

paso_query300 = f"""DELETE FROM {base_silver_x}.tbl_cd_cartdet_crit_sal_ope_eval where cod_evaluacion in {p_evaluaciones} """


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
# MAGIC ## Estadisticas

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC fecha_cierre, 
# MAGIC cod_evaluacion, 
# MAGIC count(1)  
# MAGIC from ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_ope_eval 
# MAGIC group by 1,2 order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")