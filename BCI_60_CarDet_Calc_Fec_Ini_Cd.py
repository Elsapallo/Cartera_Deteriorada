# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_60_CarDet_Calc_Fec_Ini_Cd
# MAGIC *********************************************************************************
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_60_CarDet_Calc_Fec_Ini_Cd.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011900796
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 23/05/2023
# MAGIC * Descripcion: Se obtiene la Fecha de Inicio de Cartera Deteriorada.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 08/07/2025 
# MAGIC * Descripción: Se modifica el proceso para incorporar una tabla de trabajo (work) que contiene el universo de clientes, incluyendo sus respectivas fechas de entrada a deterioro a nivel de cliente.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 22/07/2025  
# MAGIC * Descripción: Se modificó completamente la lógica para calcular las operaciones y clientes que inician en deterioro, con el objetivo de mejorar la precisión del ingreso a cartera y ajustar el comportamiento histórico conforme a los criterios de negocio.     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 13/08/2025  
# MAGIC * Descripción: Se modifico el campo rut del cliente que se esta seleccionando para la tabla temporal tmp_RES_tbl_dat_cli_ini_cd.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_stock
# MAGIC * {base_silver_x}.tbl_cd_cartdet_ope_ini_cd
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado
# MAGIC * {base_silver_x}.tbl_cd_cartdet_cli_ini_cd_pant
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_ope_ini_cd
# MAGIC * {base_silver_x}.tbl_cd_cartdet_cli_ini_cd
# MAGIC ***************************************************************************

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

# DBTITLE 1,parametria local
#Parametria local notebook
p_cod_seg_ind='I'
p_cod_seg_gru='G'

print(f"p_cod_seg_ind: {p_cod_seg_ind}")
print(f"p_cod_seg_gru: {p_cod_seg_gru}")


# COMMAND ----------

# DBTITLE 1,parametros tabla parametros
resultado = obtener_parametros_tb_fecha(base_silver_x)
fecha_ant_y = resultado[1]  
print(f"[fecha_ant_y] {fecha_ant_y}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae stock de operaciones deterioradas
# MAGIC --------------------------------------
# MAGIC - Extrae todos las operaciones deterioradas del proceso actual y sus motivos de deterioro
# MAGIC

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_cartdet_stock AS
SELECT
	 periodo_cierre	
	,fecha_cierre	
	,tipo_proceso	
	,rut_cliente		
	,dv_rut_cliente	
	,operacion	
	,sistema		
	,segmento	
	,criterio_entrada
	,origen_deterioro
	,fecha_entrada	
FROM
	{base_silver_x}.tbl_cd_cartdet_stock
WHERE 
    fecha_cierre =   {fecha_x} 
"""

# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtenemos los datos de la tabla ope_ini_cd del periodo anterior.
# MAGIC --------------------------------------

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_dat_ope_ini_cd_PANT AS
SELECT  
  fecha_cierre,
  sistema,
  operacion,
  segmento,
  rut_cliente,
  fecha_entrada,
  cuotas_amort_por_vencer,
  saldo_capital_ifrs,
  origen_deterioro,
  fecha_informada
FROM
  {base_silver_x}.tbl_cd_cartdet_ope_ini_cd_pant
where
  fecha_informada = {fecha_ant_y}
"""

# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones D00 Periodo Actual
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para periodo actual

# COMMAND ----------

paso_query25 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_d00_segmentado AS
SELECT
 A.periodo_cierre                                AS periodo_cierre,
 A.fecha_cierre                                  AS fecha_cierre,
 A.segmento                                      AS segmento,
 A.operacion                                     AS operacion,
 A.sistema                                       AS sistema,
 A.rut_cliente                                   AS rut_cliente,
 A.saldo_capital_ifrs                            AS saldo_capital_ifrs,
 A.cuotas_amort_por_vencer                       AS cuotas_amort_por_vencer
FROM
	{base_silver_x}.tbl_cd_d00_segmentado A 
WHERE 
    A.fecha_cierre = {fecha_x} 
"""

# COMMAND ----------

sql_safe(paso_query25)

# COMMAND ----------

# MAGIC %md
# MAGIC mantener
# MAGIC ### Obtenemos los datos de la tabla ope_ini_cd del periodo actual.
# MAGIC --------------------------------------

# COMMAND ----------

paso_query27 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_dat_ope_ini_cd_PACT AS
SELECT 
  a.periodo_cierre                          AS periodo_cierre,
  a.fecha_cierre                            AS fecha_cierre,
  a.operacion                               AS operacion ,
  a.sistema                                 AS sistema,
  a.segmento                                AS segmento,
  a.rut_cliente                             AS rut_cliente ,
  a.fecha_entrada                           AS fecha_entrada,
  a.criterio_entrada                        AS criterio_entrada,
  a.origen_deterioro                        AS origen_deterioro,
  COALESCE(c.saldo_capital_ifrs, 0)         AS saldo_capital_ifrs,
  COALESCE(c.cuotas_amort_por_vencer,0)     AS cuotas_amort_por_vencer,
  {fecha_x}                                 AS fecha_informada
FROM 
  tmp_EXT_tbl_cd_cartdet_stock a
LEFT JOIN 
  tmp_EXT_tbl_cd_d00_segmentado c
ON a.operacion = c.operacion   AND a.sistema = c.sistema
"""   

# COMMAND ----------

sql_safe(paso_query27)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generando los datos de la tabla ope_ini_cd.
# MAGIC --------------------------------------
# MAGIC * Si la operacion existe en el periodo anterior, se mantiene los datos del periodo anterior.
# MAGIC * Si la operacion NO existe en el periodo anterior 'es nueva', se ingresan los nuevos datos del periodo actual.

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_dat_ope_ini_cd AS
SELECT 
  a.periodo_cierre                                                   AS periodo_cierre,
  a.fecha_cierre                                                     AS fecha_cierre,
  a.operacion                                                        AS operacion ,
  a.sistema                                                          AS sistema,
  COALESCE(c.segmento,a.segmento)                                    AS segmento,
  COALESCE(c.rut_cliente,a.rut_cliente)                              AS rut_cliente ,
  COALESCE(c.fecha_entrada,a.fecha_entrada )                         AS fecha_entrada,
  COALESCE(c.origen_deterioro, a.origen_deterioro  )                 AS origen_deterioro,
  COALESCE(c.saldo_capital_ifrs, a.saldo_capital_ifrs)               AS saldo_capital_ifrs,
  COALESCE(c.cuotas_amort_por_vencer, a.cuotas_amort_por_vencer)     AS cuotas_amort_por_vencer,
  CASE WHEN c.operacion IS NULL THEN 1 ELSE 0 END                    AS ind_periodo, /* valor 1 significa que es nuevo */
  a.fecha_informada                                                  AS fecha_informada 
FROM 
  tmp_EXT_tbl_dat_ope_ini_cd_PACT a
LEFT JOIN 
  tmp_EXT_tbl_dat_ope_ini_cd_PANT c
ON  a.operacion = c.operacion AND a.sistema = c.sistema
"""   

# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes deteriorados del periodo anterior 
# MAGIC --------------------------------------
# MAGIC - Extrae los clientes deteriorados del periodo anterior y la fecha de entrada a deterioro.
# MAGIC

# COMMAND ----------

paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_dat_cli_ini_cd_PANT AS
SELECT
   rut_cliente       AS rut_cliente
  ,MIN(fec_ini_cd)   AS fecha_entrada
  ,fecha_informada   AS fecha_informada
FROM
	{base_silver_x}.tbl_cd_cartdet_cli_ini_cd_pant
WHERE 
    fecha_informada =   {fecha_x}
GROUP BY 1,3    
"""

# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calcula clientes y la fecha de Inicio de Deterioro.
# MAGIC --------------------------------------
# MAGIC - Se extraen los clientes y su minima fecha de inicio de cartera deteriorada desde la tabla dsr_gld_prodservicios_db.tbl_hcd_cartdet_cli_ini_cd

# COMMAND ----------

paso_query90 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_dat_cli_ini_cd_PACT AS
SELECT DISTINCT
    a.rut_cliente        AS rut_cliente,
    a.fecha_informada    AS fecha_informada,
    MIN(a.fecha_entrada) AS fecha_entrada  
FROM
    tmp_RES_tbl_dat_ope_ini_cd  a 
GROUP BY
    1,2
"""

# COMMAND ----------

sql_safe(paso_query90)

# COMMAND ----------

paso_query100 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_dat_cli_ini_cd AS
SELECT 
  a.rut_cliente                                  AS rut_cliente,
  COALESCE(c.fecha_entrada, a.fecha_entrada)     AS fecha_entrada,
  CASE WHEN c.rut_cliente IS NULL 
       THEN 1 
       ELSE 0 
  END AS ind_periodo, /* valor 1 significa que es nuevo */
  a.fecha_informada                              AS fecha_informada 
FROM 
  tmp_EXT_tbl_dat_cli_ini_cd_PACT a
LEFT JOIN 
  tmp_EXT_tbl_dat_cli_ini_cd_PANT c
ON 
  a.rut_cliente = c.rut_cliente
"""   

# COMMAND ----------

sql_safe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Tablas de Salidas 
# MAGIC --------------------------------------
# MAGIC * carga resultados a tablas de salidas del notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cartdet_cli_ini_cd

# COMMAND ----------

paso_query200 = f"""TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_cli_ini_cd """

# COMMAND ----------

sql_safe(paso_query200)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cartdet_ope_ini_cd

# COMMAND ----------


paso_query210 = f"""TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_ope_ini_cd """

# COMMAND ----------

sql_safe(paso_query210)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cartdet_cli_ini_cd

# COMMAND ----------

paso_query220 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_cli_ini_cd
SELECT 
    {fecha_x}          AS fecha_cierre,  
    A.rut_cliente        AS rut_cliente,
    A.fecha_entrada      AS fecha_entrada,  
    A.fecha_informada    AS fecha_informada
FROM
    tmp_RES_tbl_dat_cli_ini_cd A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.fecha_entrada DESC) =1    
"""


# COMMAND ----------

sql_safe(paso_query220)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cartdet_ope_ini_cd

# COMMAND ----------

paso_query230 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_ope_ini_cd
SELECT 
   A.fecha_cierre
  ,A.sistema
  ,A.operacion
  ,A.segmento
  ,A.rut_cliente
  ,A.fecha_entrada
  ,A.cuotas_amort_por_vencer
  ,A.saldo_capital_ifrs
  ,A.origen_deterioro
  ,A.fecha_informada
FROM 
  tmp_RES_tbl_dat_ope_ini_cd A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_entrada DESC) =1     
"""


# COMMAND ----------

sql_safe(paso_query230)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")