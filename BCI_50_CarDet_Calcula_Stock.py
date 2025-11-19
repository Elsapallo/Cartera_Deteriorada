# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_50_CarDet_Calcula_Stock
# MAGIC *********************************************************************************
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_50_CarDet_Calcula_Stock.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011897767
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Se obiene el stocks de operaciones deterioradas.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 
# MAGIC * Autor: <Nombre Autor> (<Empresa del Autor (Bci/Otra)>) - Ing. SW BCI: <Nombre Ing. SW BCI>
# MAGIC * Fecha: <dd/mm/yyyy> 
# MAGIC * Descripción: <Descripción de la mantención>      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada:
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_prin
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_prin
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_stock
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

# MAGIC %md
# MAGIC ### Extrae Operaciones Deteriorados
# MAGIC --------------------------------------
# MAGIC - Extrae todos las operaciones deterioradas del proceso actual y sus motivos de deterioro
# MAGIC

# COMMAND ----------

paso_query1 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_cartdet_crit_ent_prin AS
SELECT  
     A.periodo_cierre                   AS  periodo_cierre          
    ,A.fecha_cierre                     AS  fecha_cierre            
    ,A.tipo_proceso                     AS  tipo_proceso            
    ,A.rut_cliente                      AS  rut_cliente             
    ,A.dv_rut_cliente                   AS  dv_rut_cliente          
    ,A.tipo_operacion                   AS  tipo_operacion          
    ,A.operacion                        AS  operacion               
    ,A.sistema                          AS  sistema                 
    ,A.segmento                         AS  segmento                
    ,A.criterio_entrada                 AS  criterio_entrada        
    ,A.origen_deterioro                 AS  origen_deterioro        
    ,A.fecha_entrada                    AS  fecha_entrada           
    ,A.grupo                            AS  grupo                   
    ,A.periodo_evaluacion               AS  periodo_evaluacion      
    ,A.criterio_entrada_cliente         AS  criterio_entrada_cliente
FROM 
  {base_silver_x}.tbl_cd_cartdet_crit_ent_prin A
WHERE
    A.fecha_cierre =   {fecha_x} 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC, A.periodo_evaluacion ASC) =1
"""

# COMMAND ----------

sql_safe(paso_query1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Que Salen de Deterioro
# MAGIC --------------------------------------
# MAGIC - Se extraen todas operaciones que cumplen todos los requisitos de salida de deterioro

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_cartdet_crit_sal_prin AS
SELECT 
   a.periodo_cierre
  ,a.fecha_cierre
  ,a.tipo_proceso
  ,a.rut_cliente
  ,a.dv_rut_cliente
  ,a.tipo_operacion
  ,a.operacion
  ,a.sistema
  ,a.segmento
  ,a.familia_ope
  ,a.criterio_salida
FROM
  {base_silver_x}.tbl_cd_cartdet_crit_sal_prin a
WHERE 
    a.fecha_cierre =   {fecha_x} 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC, A.criterio_salida ASC) =1
"""


# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cruza Entradas y Salida de Deterioro
# MAGIC --------------------------------------
# MAGIC - cruza datos de entrada deterioro con el resultado del analisis de salida de deterioro
# MAGIC

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cartdet_entrada_salida AS
SELECT
     A.periodo_cierre                   AS  periodo_cierre          
    ,A.fecha_cierre                     AS  fecha_cierre            
    ,A.tipo_proceso                     AS  tipo_proceso            
    ,A.rut_cliente                      AS  rut_cliente             
    ,A.dv_rut_cliente                   AS  dv_rut_cliente          
    ,A.tipo_operacion                   AS  tipo_operacion          
    ,A.operacion                        AS  operacion               
    ,A.sistema                          AS  sistema                 
    ,A.segmento                         AS  segmento                
    ,A.criterio_entrada                 AS  criterio_entrada        
    ,A.origen_deterioro                 AS  origen_deterioro        
    ,A.fecha_entrada                    AS  fecha_entrada           
    ,A.grupo                            AS  grupo                   
    ,A.periodo_evaluacion               AS  periodo_evaluacion      
    ,A.criterio_entrada_cliente         AS  criterio_entrada_cliente
    ,B.familia_ope                      AS  familia_ope  
    ,B.criterio_salida                  AS  criterio_salida           
FROM
    tmp_EXT_tbl_cd_cartdet_crit_ent_prin A
LEFT JOIN
    tmp_EXT_tbl_cd_cartdet_crit_sal_prin  B
ON A.operacion = B.operacion AND A.sistema = B.sistema
"""


# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtiene STOCK de operaciones deterioradas
# MAGIC --------------------------------------
# MAGIC - determina todas las operaciones deterioradas que NO tiene condicion de salida
# MAGIC

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cartdet_stock AS
SELECT
     A.periodo_cierre                   AS  periodo_cierre          
    ,A.fecha_cierre                     AS  fecha_cierre            
    ,A.tipo_proceso                     AS  tipo_proceso            
    ,A.rut_cliente                      AS  rut_cliente             
    ,A.dv_rut_cliente                   AS  dv_rut_cliente          
    ,A.tipo_operacion                   AS  tipo_operacion          
    ,A.operacion                        AS  operacion               
    ,A.sistema                          AS  sistema                 
    ,A.segmento                         AS  segmento                
    ,A.criterio_entrada                 AS  criterio_entrada        
    ,A.origen_deterioro                 AS  origen_deterioro        
    ,A.fecha_entrada                    AS  fecha_entrada           
    ,A.grupo                            AS  grupo                   
    ,A.periodo_evaluacion               AS  periodo_evaluacion      
    ,A.criterio_entrada_cliente         AS  criterio_entrada_cliente
FROM
    tmp_RES_tbl_cd_cartdet_entrada_salida A
WHERE
     A.criterio_salida is null 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC, A.criterio_salida DESC) =1
"""

# COMMAND ----------

sql_safe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Campo Evaludado (tmp_tbl_cd_cartdet_stock)
# MAGIC ------------------
# MAGIC - genera stock deterioro salida con formato para insertar en tabla salida

# COMMAND ----------

paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_stock  AS
SELECT * FROM   tmp_RES_tbl_cd_cartdet_stock   
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

paso_query300 = f"""TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_stock """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_stock
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
     IFNULL(criterio_entrada,0),
     IFNULL(origen_deterioro,0),
     IFNULL(fecha_entrada,19000101),
     IFNULL(grupo,' '),
     IFNULL(periodo_evaluacion,' '),
     IFNULL(criterio_entrada_cliente,0)
FROM
    tmp_tbl_cd_cartdet_stock     
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
# MAGIC criterio_entrada_cliente, 
# MAGIC count(1) 
# MAGIC from ${bci.dbnamesilver}.tbl_cd_cartdet_stock  
# MAGIC group by 1,2 order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")