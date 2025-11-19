# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_42_CarDet_Criterios_Salida
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_42_CarDet_Criterios_Salida.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011901275
# MAGIC * Autor: Gabriel MartÍnez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion criterios de salida del periodo actual
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se agrega nuevos Salidas (44 - LIR; 26 - InterSegmento)     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/04/2025 
# MAGIC * Descripción: Se elimina Salida LIR (44 - LIR)     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_ope_eval
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_crit
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

#Parametria interna notebook
p_critdet = 2,27,62,40,41,67,60,61,63,64,65,66,30,26
print(f"p_critdet: {p_critdet}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Evaluaciones  
# MAGIC --------------------------------------
# MAGIC - Se extraen todas las evaluaciones positivas 

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_sal_ope_eval AS
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
  {base_silver_x}.tbl_cd_cartdet_crit_sal_ope_eval b
WHERE 
    b.fecha_cierre =   {fecha_x} 
AND b.flag_resultado_regla = 1
QUALIFY  ROW_NUMBER() OVER(PARTITION BY b.operacion, b.sistema, b.cod_evaluacion ORDER BY b.fecha_cierre DESC) =1
"""


# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Matriz con evaluciones por operacion
# MAGIC --------------------------------------
# MAGIC - Genera matriz por operacion con indicador de evaluacion indicando si cumple o no cumple dicha evaluacion

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_matriz_cartdet_crit_sal_ope_eval AS
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
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S01' THEN 1 ELSE 0 END) AS IND_S01
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S02' THEN 1 ELSE 0 END) AS IND_S02
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S03' THEN 1 ELSE 0 END) AS IND_S03
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S04' THEN 1 ELSE 0 END) AS IND_S04
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S05' THEN 1 ELSE 0 END) AS IND_S05
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S06' THEN 1 ELSE 0 END) AS IND_S06
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S07' THEN 1 ELSE 0 END) AS IND_S07
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S08' THEN 1 ELSE 0 END) AS IND_S08
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S09' THEN 1 ELSE 0 END) AS IND_S09
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S10' THEN 1 ELSE 0 END) AS IND_S10
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S11' THEN 1 ELSE 0 END) AS IND_S11
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S12' THEN 1 ELSE 0 END) AS IND_S12
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S13' THEN 1 ELSE 0 END) AS IND_S13	
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S14' THEN 1 ELSE 0 END) AS IND_S14
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'S15' THEN 1 ELSE 0 END) AS IND_S15    
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'N01' THEN 1 ELSE 0 END) AS IND_N01
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'N02' THEN 1 ELSE 0 END) AS IND_N02
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'N03' THEN 1 ELSE 0 END) AS IND_N03
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'N04' THEN 1 ELSE 0 END) AS IND_N04
    ,MAX(CASE WHEN IFNULL(A.cod_evaluacion,'XXX') = 'N05' THEN 1 ELSE 0 END) AS IND_N05
FROM
    tmp_EXT_tbl_cartdet_crit_sal_ope_eval A
GROUP BY 
   1,2,3,4,5,6,7,8,9
"""   

# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operaciones de clientes individuales (2)
# MAGIC --------------------------------------
# MAGIC - Genera registros para operaciones deterioradas individualmente
# MAGIC - Clientes INDIVIDUAL sin calificacion de deterioro (Evaluacion S01 and S02), ENTONCES 1
# MAGIC - Clientes GRUPAL ENTONCES 1
# MAGIC - S01: cliente individual 
# MAGIC - S02: cliente NO tiene una calificacion deteriorada 

# COMMAND ----------

paso_query30 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_1 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,2                        AS criterio_salida
,CASE 
  WHEN IFNULL(A.IND_S01,0)=1 AND IFNULL(A.IND_S02,0)=1  THEN 1
  WHEN IFNULL(A.IND_S01,0)=0 THEN 1 
  ELSE 0 
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""


# COMMAND ----------

sql_safe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operaciones con saldo ifrs total cero (27)
# MAGIC --------------------------------------
# MAGIC - Operaciones que cumplen saldo total ifrs cero y no es una excepcion
# MAGIC - S03: Operacion con saldo igual o menor a  0.0
# MAGIC - N01: operacion es excepcion
# MAGIC

# COMMAND ----------

paso_query35 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_2 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento     
,27                     AS criterio_salida
,CASE 
  WHEN IFNULL(A.IND_S03,0) = 1  AND IFNULL(A.IND_N01,0) = 0 AND IFNULL(A.IND_N02,0) = 0 AND IFNULL(A.IND_N03,0) = 0 AND IFNULL(A.IND_N04,0) = 0 AND IFNULL(A.IND_N05,0) = 0
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""


# COMMAND ----------

sql_safe(paso_query35)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operaciones deterioradas mes anterior y que no se informan en mes actual (30)
# MAGIC --------------------------------------
# MAGIC - Operaciones deteriorada mes anterior y no informadas en mes actual
# MAGIC - S14: Operacion deterioradas periodo anterior no existe en periodo actual 
# MAGIC

# COMMAND ----------

paso_query40 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_3 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,30                       AS criterio_salida
,CASE 
  WHEN IFNULL(A.IND_S14,0) = 1 
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operaciones no informadas por SSFF (40)
# MAGIC --------------------------------------
# MAGIC - Operaciones no deterioradas en SSFF
# MAGIC - S05: NO Exista en tabla de deterioro SSFF 
# MAGIC

# COMMAND ----------

paso_query45 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_4 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,40                     AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S05,0) = 1   
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query45)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operaciones no informadas por Factoring (41)
# MAGIC --------------------------------------
# MAGIC - Operaciones no deterioradas en Factoring
# MAGIC - S05: NO Exista en tabla de deterioro Factoring 
# MAGIC

# COMMAND ----------

paso_query50 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_5 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,41                     AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S06,0) = 1   
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operaciones con mora menor a (60)
# MAGIC --------------------------------------
# MAGIC - Operaciones con mora menor a
# MAGIC - S08: Maximo dias de mora cliente menor o igual a
# MAGIC

# COMMAND ----------

paso_query55 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_7 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,60                     AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S08,0) = 1   
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query55)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro pagos consecutivos (61)
# MAGIC --------------------------------------
# MAGIC - Operaciones con mas de x pagos consecutivos
# MAGIC - S09: Cantidad de pagos consecutivos mayor o igual a
# MAGIC

# COMMAND ----------

paso_query60 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_8 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,61                     AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S09,0) = 1   
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro sin mora sbif (62)
# MAGIC --------------------------------------
# MAGIC - Operaciones de clientes sin mora sbif
# MAGIC - S04:  Deuda morosa sbif menor o igual a 
# MAGIC

# COMMAND ----------

paso_query65 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_9 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,62                       AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S04,0) = 1  
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query65)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operacion con pagos parciales (63)
# MAGIC --------------------------------------
# MAGIC - Operaciones con pagos parciales
# MAGIC - S04:  Deuda morosa sbif menor o igual a 
# MAGIC

# COMMAND ----------

paso_query70 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_10 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,63                   AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S10,0) = 1  
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operacion sin refinanciamiento (64)
# MAGIC --------------------------------------
# MAGIC - Operaciones sin refinanciamiento
# MAGIC - S11:  No tenga renegociados ni curse bajo mora 
# MAGIC

# COMMAND ----------

paso_query75 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_11 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,64                     AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S11,0) = 1  
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query75)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro operacion con amortizacion de capital (65)
# MAGIC --------------------------------------
# MAGIC - Operaciones con amortizacion de capital o pagos parciales
# MAGIC - S12:  Saldo capital periodo actual sea menor al saldo periodo anterior 
# MAGIC

# COMMAND ----------

paso_query80 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_12 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,65                     AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S12,0) = 1  
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro minimo de meses en deterioro (66)
# MAGIC --------------------------------------
# MAGIC - Operaciones con un minimo de meses en deterioro
# MAGIC - S13:  Si numero de meses es mayor o igual a 
# MAGIC

# COMMAND ----------

paso_query85 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_13 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,66                     AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S13,0) = 1  
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query85)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro clientes no LIR (67)
# MAGIC --------------------------------------
# MAGIC - Operaciones de los clientes que no son LIR o cuya antiguedad sea mayor a 12 meses
# MAGIC - S07: Cliente no es LIR entonces cumple regla
# MAGIC - S15: Cliente es LIR pero tiene antiguedad lir mayor a 12 meses entonces cumple regla
# MAGIC

# COMMAND ----------

paso_query90 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_14 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento      
,67                       AS criterio_salida
,CASE 
  WHEN  IFNULL(A.IND_S07,0) = 1 THEN 1  
  WHEN  IFNULL(A.IND_S07,0) = 0 AND IFNULL(A.IND_S15,0) = 1 THEN 1  
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
"""

# COMMAND ----------

sql_safe(paso_query90)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Deterioro por Intersegmento (26)
# MAGIC --------------------------------------
# MAGIC - Cliente con operaciones Individual con condicion de salida y operacion Grupal con deterioro.
# MAGIC - Deber cumplir con los siguientes criterios de salida: S01, S02

# COMMAND ----------

paso_query95 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_15 as
SELECT
 A.periodo_cierre         AS periodo_cierre            
,A.fecha_cierre           AS fecha_cierre          
,A.tipo_proceso           AS tipo_proceso          
,A.rut_cliente            AS rut_cliente         
,A.dv_rut_cliente         AS dv_rut_cliente            
,A.tipo_operacion         AS tipo_operacion            
,A.operacion              AS operacion       
,A.sistema                AS sistema     
,A.segmento               AS segmento     
,26                     AS criterio_salida
,CASE 
  WHEN IFNULL(A.IND_S01,0) = 1 AND IFNULL(A.IND_S02,0) = 1 AND IFNULL(A.IND_S14,0) = 0 AND IFNULL(A.IND_N01,0) = 0 AND IFNULL(A.IND_N02,0) = 0 AND IFNULL(A.IND_N03,0) = 0 AND IFNULL(A.IND_N04,0) = 0
  THEN 1
  ELSE 0
 END                    AS flag_resultado_regla
FROM 
    tmp_RES_matriz_cartdet_crit_sal_ope_eval  A
    
"""

# COMMAND ----------

sql_safe(paso_query95)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaludado (tmp_tbl_cartdet_crit_ent_crit)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones evaluadas
# MAGIC

# COMMAND ----------


paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_crit AS
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
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_7 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_8 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_9 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_10 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_11
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_12 
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_13
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_14
UNION
SELECT * FROM   tmp_RES_D00_OPE_CRIT_EVAL_15
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

paso_query300 = f"""DELETE FROM {base_silver_x}.tbl_cd_cartdet_crit_sal_crit where criterio_salida in {p_critdet} """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_sal_crit
SELECT 
    IFNULL(periodo_cierre,19000101),
    IFNULL(fecha_cierre,190001),
    IFNULL(tipo_proceso,' '),
    IFNULL(rut_cliente,0),
    IFNULL(dv_rut_cliente,' '),
    IFNULL(tipo_operacion,' '),
    IFNULL(operacion,' '),
    IFNULL(sistema,' '),
    IFNULL(segmento,' '),
    IFNULL(criterio_salida,0),
    IFNULL(flag_resultado_regla,0)
FROM
    tmp_tbl_cartdet_crit_sal_crit  
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
# MAGIC criterio_salida, 
# MAGIC count(1) 
# MAGIC from ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_crit  
# MAGIC group by 1,2 order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")