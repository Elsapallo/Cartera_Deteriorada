# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_44_CarDet_Crit_Salida_Pri
# MAGIC *********************************************************************************
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_44_CarDet_Crit_Salida_Pri.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011897935
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Se obtiene el criterio principal por el que sale de deterioro.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se agrega Criterio 26 y 44 como Excepcion     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 14/04/2025 
# MAGIC * Descripción: Se elimina Criterio 44 como Excepcion (LIR).     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_fam
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_prin
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

#Parametros internos notebook
p_crit_sal_exp=26,27,30
print(f"p_crit_sal_exp: {p_crit_sal_exp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Resultado Evaluacion Salida Deterioro Por Operacion
# MAGIC --------------------------------------
# MAGIC - Extrae todas las operaciones deterioradas y el resultado de su evalucion de salida a nivel de operacion
# MAGIC

# COMMAND ----------

paso_query1 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_cartdet_crit_sal_crit_fam AS
SELECT  
     A.periodo_cierre 
    ,A.fecha_cierre 
    ,A.tipo_proceso 
    ,A.rut_cliente 
    ,A.dv_rut_cliente 
    ,A.tipo_operacion 
    ,A.operacion 
    ,A.sistema 
    ,A.segmento 
    ,A.familia_ope 
    ,A.criterio_salida 
    ,A.flag_resultado_regla 
FROM 
  {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_fam A
WHERE
    A.fecha_cierre =   {fecha_x} 
"""

# COMMAND ----------

sql_safe(paso_query1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtiene indicadores de totales por cliente
# MAGIC --------------------------------------
# MAGIC - Calcula total de operaciones deterioradas, total operaciones con condiciones de salida y total operacion sin condiciones de salida

# COMMAND ----------

paso_query5 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cartdet_crit_sal_crit_cli AS
SELECT 
   a.periodo_cierre
  ,a.fecha_cierre
  ,a.tipo_proceso
  ,a.rut_cliente
  ,COUNT(a.operacion) AS cant_tot_ope
  ,SUM(CASE WHEN a.flag_resultado_regla=1 THEN 1 ELSE 0 END) AS cant_ope_ok
  ,SUM(CASE WHEN a.flag_resultado_regla=0 THEN 1 ELSE 0 END) AS cant_ope_nok

FROM
  tmp_EXT_tbl_cd_cartdet_crit_sal_crit_fam a
WHERE
  A.criterio_salida NOT IN {p_crit_sal_exp}
GROUP BY 1,2,3,4
"""


# COMMAND ----------

sql_safe(paso_query5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida de deterioro para clientes donde todas sus operaciones deterioradas cumplen condicion de salida
# MAGIC ------------------
# MAGIC - Cliente sale de deterioro si todas sus operaciones deterioradas tienen condicion de salida

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_ope_sal_EVAL_1 AS
SELECT 
     U.periodo_cierre 
    ,U.fecha_cierre 
    ,U.tipo_proceso 
    ,U.rut_cliente 
    ,U.dv_rut_cliente 
    ,U.tipo_operacion 
    ,U.operacion 
    ,U.sistema 
    ,U.segmento 
    ,U.familia_ope 
    ,U.criterio_salida 
    ,U.flag_resultado_regla 
FROM   
    tmp_EXT_tbl_cd_cartdet_crit_sal_crit_fam   U,
    tmp_RES_tbl_cd_cartdet_crit_sal_crit_cli A
WHERE
    U.rut_cliente = A.rut_cliente
AND U.criterio_salida NOT IN {p_crit_sal_exp}
AND A.cant_ope_nok = 0 /*todas las operaciones del cliente tienen condiciones de salida*/    
"""  

# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida de deterioro para operaciones con criterios de excepcion
# MAGIC ------------------
# MAGIC - las operaciones con criterio de excepcion salen directamente

# COMMAND ----------

paso_query15 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_ope_sal_EVAL_2 AS
SELECT 
     U.periodo_cierre 
    ,U.fecha_cierre 
    ,U.tipo_proceso 
    ,U.rut_cliente 
    ,U.dv_rut_cliente 
    ,U.tipo_operacion 
    ,U.operacion 
    ,U.sistema 
    ,U.segmento 
    ,U.familia_ope 
    ,U.criterio_salida 
    ,U.flag_resultado_regla 
FROM   
    tmp_EXT_tbl_cd_cartdet_crit_sal_crit_fam   U
WHERE
    U.criterio_salida IN {p_crit_sal_exp}
AND U.flag_resultado_regla = 1
"""  

# COMMAND ----------

sql_safe(paso_query15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Operacion (tmp_tbl_cartdet_crit_sal_crit_fam)
# MAGIC ------------------
# MAGIC - Tabla con operaciones de clientes que cumplen todas las condiciones de salida

# COMMAND ----------


paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_crit_sal_prin AS
SELECT * FROM tmp_RES_cartdet_ope_sal_EVAL_1
UNION 
SELECT * FROM tmp_RES_cartdet_ope_sal_EVAL_2
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

paso_query300 = f"""TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_crit_sal_prin """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_sal_prin
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
    IFNULL(familia_ope,' '),
    IFNULL(criterio_salida,0)
FROM
    tmp_tbl_cd_cartdet_crit_sal_prin   
QUALIFY  ROW_NUMBER() OVER(PARTITION BY operacion, sistema ORDER BY criterio_salida ASC) =1    
"""  


# COMMAND ----------

sql_safe(paso_query310)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Estadisticas

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC fecha_cierre, 
# MAGIC criterio_salida, 
# MAGIC count(1) 
# MAGIC from ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_prin  
# MAGIC group by 1,2 order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")