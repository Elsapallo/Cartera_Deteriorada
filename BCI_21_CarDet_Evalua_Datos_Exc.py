# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_21_CarDet_Evalua_Datos_Exc
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_21_CarDet_Evalua_Datos_Exc.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011899624
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion criterior de excepciones a entrada a deterioro
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gagriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se modifica el criterio 27 no considere las filiales.    
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_ope_condicion_deterioro
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_ope_eval
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
dbutils.widgets.text("bd_silver_w","","02-Nombre BD Silver:")

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

# DBTITLE 1,Parametros internos
#parametria interna notebook
p_cod_evaluacion='N01','N02'
print(f"p_cod_evaluacion: {p_cod_evaluacion}")

# COMMAND ----------

# DBTITLE 1,Fecha Tope Clientes LIR
#Para el cálculo de clientes LIR se debe tomar los ultimos 12 meses a partir de la fecha de proceso
p_fec_12_meses_atras=fecha_x_meses_atras(fecha_x,12)
print(f"p_fec_12_meses_atras: {p_fec_12_meses_atras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: operaciones con monto deuda ifrs cero
# MAGIC ---
# MAGIC * operaciones con monto dueda total ifrs cero
# MAGIC * Excepto para operaciones Nova CCN455, COLMN CON731 Y COLMN COM732
# MAGIC

# COMMAND ----------

paso_query20 =  f"""
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
    'saldo_total_ifrs-sistema-tipo_operacion-flag_existe_cliente_lir-cli_fecha_informada_lir-flag_existe_cliente_ssff'               AS nombre_campo,
    concat('[',cast(trim(A.saldo_total_ifrs) as string),'-',cast(trim(A.sistema) as string),'-',cast(trim(A.tipo_operacion) as string),'-', cast(A.flag_existe_cliente_lir as string),'-', cast(A.cli_fecha_informada_lir as string),'-', cast(A.flag_existe_cliente_ssff as string),']')        AS valor_campo,
    "Excepcion saldo ifrs = 0, para operaciones del sitema y tioaux a 20-CCN455, 01-CON731, 01-COM732, y cliente no lir y cliente no ssff"                     AS condicion_regla,     
    concat('[','0','-','(20-CCN455, 01-CON731, 01-COM732)','-','NO_LIR','-','NO_SSFF',']')               AS valor_regla,           
    CASE 
       WHEN trim(A.sistema) = '20' AND trim(A.tipo_operacion)='CCN455' THEN 0
       WHEN trim(A.sistema) = '01' AND trim(A.tipo_operacion)='CON731' THEN 0
       WHEN trim(A.sistema) = '01' AND trim(A.tipo_operacion)='COM732' THEN 0
       WHEN A.flag_existe_cliente_lir = 1 and A.cli_fecha_informada_lir > {p_fec_12_meses_atras}  THEN 0
       WHEN A.flag_existe_cliente_ssff =1  THEN 0
       ELSE 1 
    END                                                          AS flag_resultado_regla,
    'N01'                                                        AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_deterioro   A
WHERE 
    IFNULL(A.saldo_total_ifrs,0) = 0    
"""

# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: operaciones cae excepcion
# MAGIC ---
# MAGIC * operaciones CAE excepcion informadas en archivo
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
    'operacion-tipo_operacion'                                   AS nombre_campo,
    concat('[',cast(A.operacion as string),'-',cast(A.tipo_operacion as string),']')  AS valor_campo,
    "Excepcion CAE operacion y tipo de operacion mismo agno de licitacion"        AS condicion_regla,      
    Case when A.flag_cae_excepcion = 1 THEN 'EXISTE (detalle_incumplimiento_cierre_cae)' ELSE 'NO_EXISTE (detalle_incumplimiento_cierre_cae)'  END AS valor_regla,
    CASE 
         WHEN A.flag_cae_excepcion = 1
         THEN 1 
         ELSE 0 
     END                                                         AS flag_resultado_regla,
    'N02'                                                        AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_deterioro   A
WHERE
     A.flag_cae_excepcion = 1    
"""

# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaludado (tmp_tbl_cartdet_crit_ent_ope_campo)
# MAGIC ------------------
# MAGIC * generar salida temporal a nivel de campo evaluado. 
# MAGIC * se registran todas las operaciones evaluadas
# MAGIC

# COMMAND ----------


paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_ent_ope_eval AS
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_1 
UNION 
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_2 
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
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos). Tabla no es historica

# COMMAND ----------

paso_query300 = f"""DELETE FROM {base_silver_x}.tbl_cd_cartdet_crit_ent_ope_eval where cod_evaluacion in {p_cod_evaluacion} """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------


paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_ent_ope_eval
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
  tmp_tbl_cartdet_crit_ent_ope_eval
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
# MAGIC cod_evaluacion,
# MAGIC CASE 
# MAGIC   WHEN cod_evaluacion='E01' THEN 'CLIENTE_INDIVIDUAL_DETERIORADO'
# MAGIC   WHEN cod_evaluacion='E04' THEN 'CLIENTE_CON_MOROSIDAD'  
# MAGIC   WHEN cod_evaluacion='E05' THEN 'CLIENTE_CON_RENEGOCIADO'    
# MAGIC   WHEN cod_evaluacion='E06' THEN 'CLIENTE_REESTRUCTURCION_FORZOSA'    
# MAGIC   WHEN cod_evaluacion='E07' THEN 'CLIENTE_LIR'    
# MAGIC   WHEN cod_evaluacion='E08' THEN 'CLIENTE_DETERIORADO_SSFF'    
# MAGIC   WHEN cod_evaluacion='E09' THEN 'CLIENTE_DETERIORADO_FACT'
# MAGIC   WHEN cod_evaluacion='N01' THEN 'EXCEPCION_OPERACION_SALDO_IFRS_CERO'
# MAGIC   WHEN cod_evaluacion='N02' THEN 'EXCEPCION_OPERACION_CAE'
# MAGIC   ELSE 'NO_IDENTIFICADO'    
# MAGIC END                    AS des_cod_evaluacion,
# MAGIC COUNT(1) AS CANT_REG
# MAGIC FROM ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_ope_eval
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC fecha_cierre,
# MAGIC cod_evaluacion,
# MAGIC flag_resultado_regla,
# MAGIC CASE 
# MAGIC   WHEN cod_evaluacion='E01' THEN 'CLIENTE_INDIVIDUAL_DETERIORADO'
# MAGIC   WHEN cod_evaluacion='E04' THEN 'CLIENTE_CON_MOROSIDAD'  
# MAGIC   WHEN cod_evaluacion='E05' THEN 'CLIENTE_CON_RENEGOCIADO'    
# MAGIC   WHEN cod_evaluacion='E06' THEN 'CLIENTE_REESTRUCTURCION_FORZOSA'    
# MAGIC   WHEN cod_evaluacion='E07' THEN 'CLIENTE_LIR'    
# MAGIC   WHEN cod_evaluacion='E08' THEN 'CLIENTE_DETERIORADO_SSFF'    
# MAGIC   WHEN cod_evaluacion='E09' THEN 'CLIENTE_DETERIORADO_FACT'
# MAGIC   WHEN cod_evaluacion='N01' THEN 'EXCEPCION_OPERACION_SALDO_IFRS_CERO'
# MAGIC   WHEN cod_evaluacion='N02' THEN 'EXCEPCION_OPERACION_CAE'
# MAGIC   ELSE 'NO_IDENTIFICADO'    
# MAGIC END                    AS des_cod_evaluacion,
# MAGIC COUNT(1) AS CANT_REG
# MAGIC FROM ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_ope_eval
# MAGIC GROUP BY 1,2,3,4
# MAGIC ORDER BY 1,2,3,4
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dsr_gld_bciwork_db.tbl_cd_cartdet_crit_ent_ope_eval where rut_cliente=17944363