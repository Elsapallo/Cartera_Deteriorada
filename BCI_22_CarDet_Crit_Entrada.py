# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_22_CarDet_Crit_Entrada
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_22_CarDet_Crit_Entrada.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011901624
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion criterios de entrada del periodo actual
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gagriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se cambia el Delete por Truncate al momento de eliminar los datos de la tabla tbl_cd_cartdet_crit_ent_crit     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_ope_eval 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_crit
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

# Calcula periodo en base a la fecha
periodo_x=fecha_x[:6]

print(f"Periodo: [periodo_x] {periodo_x}")

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

p_cod_seg_gru='G'
p_crit_det = 1,7,8,9,10,11,12,13
p_periodo_evaluacion='p_actual'

print(f"p_cod_seg_gru: {p_cod_seg_gru}")
print(f"p_crit_det: {p_crit_det}")
print(f"p_periodo_evaluacion: {p_periodo_evaluacion}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Evaluaciones  (riesgobdu_silver_db.tbl_cartdet_crit_ent_ope_eval)
# MAGIC --------------------------------------
# MAGIC - Se extraen todas las evaluaciones positivas 

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_ope_eval AS
SELECT 
        b.periodo_cierre,
        b.fecha_cierre,
        b.tipo_proceso,
        b.segmento,
        b.operacion,
        b.tipo_operacion,
        b.sistema,
        b.rut_cliente,
        b.dv_rut_cliente,
        b.nombre_campo,
        b.valor_campo,
        b.condicion_regla,
        b.valor_regla,
        b.flag_resultado_regla,
        b.cod_evaluacion
FROM
  {base_silver_x}.tbl_cd_cartdet_crit_ent_ope_eval b
WHERE 
    b.fecha_cierre =   {fecha_x} 
AND b.periodo_cierre = {periodo_x}
AND b.flag_resultado_regla = 1
QUALIFY  ROW_NUMBER() OVER(PARTITION BY b.operacion, b.sistema, b.cod_evaluacion ORDER BY b.fecha_cierre DESC) =1
"""


# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Matriz con evaluaciones de entrada por operacion
# MAGIC --------------------------------------
# MAGIC - Genera matriz por operacion con indicador de evaluacion indicando si cumple o no cumple dicha evaluacion

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_matriz_cartdet_crit_ent_ope_eval AS
SELECT
     periodo_cierre
    ,fecha_cierre
    ,tipo_proceso
    ,operacion
    ,sistema
    ,rut_cliente
    ,dv_rut_cliente
    ,tipo_operacion
    ,segmento
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'E01' THEN 1 ELSE 0 END) AS IND_E01
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'E04' THEN 1 ELSE 0 END) AS IND_E04
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'E05' THEN 1 ELSE 0 END) AS IND_E05
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'E06' THEN 1 ELSE 0 END) AS IND_E06
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'E07' THEN 1 ELSE 0 END) AS IND_E07
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'E08' THEN 1 ELSE 0 END) AS IND_E08
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'E09' THEN 1 ELSE 0 END) AS IND_E09
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'N01' THEN 1 ELSE 0 END) AS IND_N01
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'N02' THEN 1 ELSE 0 END) AS IND_N02
FROM
    tmp_EXT_tbl_cartdet_crit_ent_ope_eval A
GROUP BY 
   1,2,3,4,5,6,7,8,9
"""   

# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones de clientes individuales
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones deterioradas individualmente
# MAGIC - Para todas las operaciones del cliente que cumplan E01=1

# COMMAND ----------

paso_query50 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_1 as
SELECT
        periodo_cierre          AS periodo_cierre,
        fecha_cierre            AS fecha_cierre,
        tipo_proceso            AS tipo_proceso,
        rut_cliente             AS rut_cliente,
        dv_rut_cliente          AS dv_rut_cliente,
        tipo_operacion          AS tipo_operacion,
        operacion               AS operacion,
        sistema                 AS sistema,
        segmento                AS segmento,
        1                       AS criterio_entrada,
        0                       AS origen_deterioro,
        fecha_cierre            AS fecha_entrada,
        'BCI_Individual'        AS grupo
FROM 
    tmp_RES_matriz_cartdet_crit_ent_ope_eval  A
WHERE
    IND_E01 = 1 AND IND_N01=0 AND IND_N02=0
"""


# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones con morosidad 
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones grupalmente
# MAGIC - Operaciones que cumplen con: Operacion Grupal and E04 =1
# MAGIC

# COMMAND ----------

paso_query55 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_2 as
SELECT
        A.periodo_cierre          AS periodo_cierre,
        A.fecha_cierre            AS fecha_cierre,
        A.tipo_proceso            AS tipo_proceso,
        A.rut_cliente             AS rut_cliente,
        A.dv_rut_cliente          AS dv_rut_cliente,
        A.tipo_operacion          AS tipo_operacion,
        A.operacion               AS operacion,
        A.sistema                 AS sistema,
        A.segmento                AS segmento,
        CASE 
          WHEN SUBSTRING(A.tipo_operacion,1,3) IN ('HIP','CAE') 
          THEN 8 
          ELSE 7
        END                     AS criterio_entrada,
        1                       AS origen_deterioro,
        A.fecha_cierre          AS fecha_entrada,
        'BCI_Grupal'            AS grupo
FROM 
    tmp_RES_matriz_cartdet_crit_ent_ope_eval  A
WHERE
    A.segmento= '{p_cod_seg_gru}'
AND A.IND_E04 = 1 AND IND_N01=0 AND IND_N02=0
"""


# COMMAND ----------

sql_safe(paso_query55)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones renegociadas bci
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones grupales
# MAGIC - Operaciones que cumplen con: Operacion Grupal and E05=1
# MAGIC

# COMMAND ----------

paso_query60 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_3 as
SELECT
        A.periodo_cierre          AS periodo_cierre,
        A.fecha_cierre            AS fecha_cierre,
        A.tipo_proceso            AS tipo_proceso,
        A.rut_cliente             AS rut_cliente,
        A.dv_rut_cliente          AS dv_rut_cliente,
        A.tipo_operacion          AS tipo_operacion,
        A.operacion               AS operacion,
        A.sistema                 AS sistema,
        A.segmento                AS segmento,
        9                         AS criterio_entrada,
        3                         AS origen_deterioro,
        A.fecha_cierre            AS fecha_entrada,
        'BCI_Grupal'              AS grupo
FROM 
    tmp_RES_matriz_cartdet_crit_ent_ope_eval  A
WHERE
    A.segmento= '{p_cod_seg_gru}'
AND A.IND_E05=1 AND IND_N01=0 AND IND_N02=0
"""


# COMMAND ----------

sql_safe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones con curse bajo mora bci
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones deterioradas grupalmente
# MAGIC - Operaciones que cumplen con: Operacion Grupal and E06=1
# MAGIC

# COMMAND ----------

paso_query65 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_4 as
SELECT
        A.periodo_cierre          AS periodo_cierre,
        A.fecha_cierre            AS fecha_cierre,
        A.tipo_proceso            AS tipo_proceso,
        A.rut_cliente             AS rut_cliente,
        A.dv_rut_cliente          AS dv_rut_cliente,
        A.tipo_operacion          AS tipo_operacion,
        A.operacion               AS operacion,
        A.sistema                 AS sistema,
        A.segmento                AS segmento,
        10                         AS criterio_entrada,
        4                         AS origen_deterioro,
        A.fecha_cierre            AS fecha_entrada,
        'BCI_Grupal'              AS grupo
FROM 
    tmp_RES_matriz_cartdet_crit_ent_ope_eval  A
WHERE
    A.segmento= '{p_cod_seg_gru}'
AND A.IND_E06=1 AND IND_N01=0 AND IND_N02=0
"""


# COMMAND ----------

sql_safe(paso_query65)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones clientes LIR
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones deterioradas grupalmente
# MAGIC - Operaciones que cumplen con: Operacion Grupal and E07=1
# MAGIC

# COMMAND ----------

paso_query70 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_5 as
SELECT
        A.periodo_cierre          AS periodo_cierre,
        A.fecha_cierre            AS fecha_cierre,
        A.tipo_proceso            AS tipo_proceso,
        A.rut_cliente             AS rut_cliente,
        A.dv_rut_cliente          AS dv_rut_cliente,
        A.tipo_operacion          AS tipo_operacion,
        A.operacion               AS operacion,
        A.sistema                 AS sistema,
        A.segmento                AS segmento,
        11                        AS criterio_entrada,
        2                         AS origen_deterioro,
        A.fecha_cierre            AS fecha_entrada,
        'BCI_Grupal'              AS grupo
FROM 
    tmp_RES_matriz_cartdet_crit_ent_ope_eval  A
WHERE
    A.segmento= '{p_cod_seg_gru}'
AND A.IND_E07=1 AND IND_N01=0 AND IND_N02=0
"""


# COMMAND ----------

sql_safe(paso_query70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones de clientes Informados x SSFF
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones deterioradas grupalmente
# MAGIC - Operaciones que cumplen con: Operacion Grupal and E08=1
# MAGIC

# COMMAND ----------

paso_query75 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_6 as
SELECT
        A.periodo_cierre          AS periodo_cierre,
        A.fecha_cierre            AS fecha_cierre,
        A.tipo_proceso            AS tipo_proceso,
        A.rut_cliente             AS rut_cliente,
        A.dv_rut_cliente          AS dv_rut_cliente,
        A.tipo_operacion          AS tipo_operacion,
        A.operacion               AS operacion,
        A.sistema                 AS sistema,
        A.segmento                AS segmento,
        12                        AS criterio_entrada,
        6                         AS origen_deterioro,
        A.fecha_cierre            AS fecha_entrada,
        'SSFF'                    AS grupo
FROM 
    tmp_RES_matriz_cartdet_crit_ent_ope_eval  A
WHERE
    A.segmento= '{p_cod_seg_gru}'
AND A.IND_E08=1 AND IND_N01=0 AND IND_N02=0
"""


# COMMAND ----------

sql_safe(paso_query75)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones de clientes Informados x Factoring
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones deterioradas grupalmente
# MAGIC - Operaciones que cumplen con: Operacion Grupal and E09=1
# MAGIC

# COMMAND ----------

paso_query80 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_7 as
SELECT
        A.periodo_cierre          AS periodo_cierre,
        A.fecha_cierre            AS fecha_cierre,
        A.tipo_proceso            AS tipo_proceso,
        A.rut_cliente             AS rut_cliente,
        A.dv_rut_cliente          AS dv_rut_cliente,
        A.tipo_operacion          AS tipo_operacion,
        A.operacion               AS operacion,
        A.sistema                 AS sistema,
        A.segmento                AS segmento,
        13                        AS criterio_entrada,
        6                         AS origen_deterioro,
        A.fecha_cierre           AS fecha_entrada,
        'Factoring'              AS grupo
FROM 
    tmp_RES_matriz_cartdet_crit_ent_ope_eval  A
WHERE
    A.segmento= '{p_cod_seg_gru}'
AND A.IND_E09=1 AND IND_N01=0 AND IND_N02=0
"""


# COMMAND ----------

sql_safe(paso_query80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaludado (tmp_tbl_cartdet_crit_ent_crit)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones evaluadas
# MAGIC

# COMMAND ----------


paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_ent_crit AS
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_1  
UNION 
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_2  
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_3  
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_4 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_5 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_6 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_7 
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
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos). Tabla no es historica.

# COMMAND ----------

paso_query300 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_crit_ent_crit """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_ent_crit
SELECT 
    IFNULL(periodo_cierre,190001),
    IFNULL(fecha_cierre,19000101),
    IFNULL(tipo_proceso,' '),
    IFNULL(rut_cliente,0),
    IFNULL(dv_rut_cliente,' '),
    IFNULL(tipo_operacion,' '),
    IFNULL(operacion,' '),
    IFNULL(sistema,' '),
    IFNULL(segmento,' '),
    IFNULL(criterio_entrada,0),
    IFNULL(origen_deterioro,0),
    IFNULL(fecha_entrada,19000101),
    IFNULL(grupo,' '),
    '{p_periodo_evaluacion}'
FROM
    tmp_tbl_cartdet_crit_ent_crit 
"""  


# COMMAND ----------

sql_safe(paso_query310)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Estadisticas tabla salida

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC fecha_cierre,
# MAGIC criterio_entrada,
# MAGIC CASE 
# MAGIC   WHEN criterio_entrada=1 THEN 'CLASIFICACION_DETERIORO'
# MAGIC   WHEN criterio_entrada=7 THEN 'MOROSIDAD_NO_HIPCAE'
# MAGIC   WHEN criterio_entrada=8 THEN 'MOROSIDAD_HIPCAE'
# MAGIC   WHEN criterio_entrada=9 THEN 'RENEGOCIADO'
# MAGIC   WHEN criterio_entrada=10 THEN 'REESTRUCTURACION_FORZOSA'
# MAGIC   WHEN criterio_entrada=11 THEN 'LIR'
# MAGIC   WHEN criterio_entrada=12 THEN 'SSFF'
# MAGIC   WHEN criterio_entrada=13 THEN 'FACTORING'
# MAGIC   ELSE 'NO_IDENTIFICADO'    
# MAGIC END                    AS des_criterio_entrada,
# MAGIC COUNT(1) AS CANT_REG
# MAGIC FROM ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_crit
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")