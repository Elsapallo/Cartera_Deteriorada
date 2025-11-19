# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_23_CarDet_Crit_Entrada_Ant
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_23_CarDet_Crit_Entrada_Ant.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011902103
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: obtiene todos las operaciones deterioradas del periodo anterior
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se agrega nueva regla LIR - No se condirera los clientes que no vienen informado en el archivo en el periodo actual.      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 09/04/2025 
# MAGIC * Descripción: Se elimina la nueva regla LIR - Se mantiene la logica de que en caso de entrar a LIR el cliente no sale.      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado_pant
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

p_ind_cartdet='D'
p_crit_det = 1,7,8,9,10,11,12,13
p_periodo_evaluacion='p_anterior'

print(f"p_ind_cartdet: {p_ind_cartdet}")
print(f"p_crit_det: {p_crit_det}")
print(f"p_periodo_evaluacion: {p_periodo_evaluacion}")


# COMMAND ----------

# Calcula periodo en base a la fecha
periodo_x=fecha_x[:6]

print(f"Periodo: [periodo_x] {periodo_x}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterioro operaciones periodo anterior
# MAGIC --------------------------------------
# MAGIC - obtiene todas los deterioros del cliente informados en periodo anterior
# MAGIC

# COMMAND ----------

paso_query20 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CRIT_EVAL_1 as
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
        A.criterio_entrada        AS criterio_entrada,
        A.origen_deterioro        AS origen_deterioro,
        A.fecha_entrada           AS fecha_entrada,
        CASE 
            WHEN A.criterio_entrada = 1  THEN 'BCI_Individual' 
            WHEN A.criterio_entrada = 12 THEN 'SSFF'
            WHEN A.criterio_entrada = 13 THEN 'Factoring'
            ELSE 'BCI_Grupal'
        END                       AS grupo
FROM 
    {base_silver_x}.tbl_cd_d00_segmentado_pant   A
WHERE
    A.ind_cartdet='{p_ind_cartdet}' 
AND IFNULL(A.criterio_entrada,0)>0
"""


# COMMAND ----------

sql_safe(paso_query20)

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

paso_query300 = f"""DELETE FROM {base_silver_x}.tbl_cd_cartdet_crit_ent_crit where criterio_entrada in {p_crit_det} and periodo_evaluacion = '{p_periodo_evaluacion}' """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_ent_crit
SELECT 
    IFNULL({periodo_x},190001),
    IFNULL({fecha_x},19000101),
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
    IFNULL('{p_periodo_evaluacion}',' ')
FROM
    tmp_tbl_cartdet_crit_ent_crit  
"""  


# COMMAND ----------

sql_safe(paso_query310)

# COMMAND ----------

##Estadisticas tabla salida


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