# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_19_CarDet_Prepara_Entrada
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_19_CarDet_Prepara_Entrada.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011897208
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/05/2022
# MAGIC * Descripcion: Prepara y carga informacion en tablas que son utilizadas como criterio de entrada, las cuales que en un inicio se cargaban con archivos productivos. Objetivo es dejar de depender de archivos productivos de otros sistemas.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Prepara datos para nueva regla LIR      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 09/04/2025 
# MAGIC * Descripción: Elimina logica de regla de negocio, salida de Lir.      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Gonzalo Arias (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 08/09/2025 
# MAGIC * Descripción: Se agrega lógica de 60 días de mora para determinar las operaciones renegociadas a deteriorar      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado_pant
# MAGIC * {base_silver_x}.tbl_cd_curses
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_ope_condicion_ren 
# MAGIC * {base_silver_x}.tbl_cd_ope_condicion_mora
# MAGIC * {base_silver_x}.tbl_cd_ope_condicion_mora_pant
# MAGIC * {base_silver_x}.tbl_cd_ope_curse_bajo_mora
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
dbutils.widgets.text("bd_silver_w","","02-Nombre BD Silver:")

fecha_x = dbutils.widgets.get("fecha_w") 
base_silver_x = dbutils.widgets.get("bd_silver_w")

spark.conf.set("bci.Fecha", fecha_x)
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

#parametria interna notebook
p_tip_prod='ACT'
p_tip_cart='CON','COM'
p_cod_ren='S','C'

print(f"p_tip_prod: {p_tip_prod}")
print(f"p_tip_cart: {p_tip_cart}")
print(f"p_cod_ren: {p_cod_ren}")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Genera tabla pivote con la informacion necesaria para el calculo de deterioro
# MAGIC ---------------------------
# MAGIC * La tabla pivote es el d00segmentado del periodo actual

# COMMAND ----------

# MAGIC %md
# MAGIC ####Extrae datos de tablas nativas

# COMMAND ----------

paso_query100 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_ope_d00_pact as
SELECT 
/*DATOS DE: tbl_cd_d00_segmentado*/
 A.periodo_cierre
,A.fecha_cierre
,A.tipo_proceso
,A.macro_sistema
,A.tipo_cartera
,A.tipo_producto
,A.segmento
,A.cod_proceso
,A.sistema
,A.tipo_credito
,A.operacion
,A.tipo_operacion
,A.cuenta_contable
,A.saldo_cartera_venc
,A.saldo_total_ifrs
,A.cod_renegociado
,A.fecha_otorgamiento
,A.fecha_inicio_mora
,A.rut_cliente
,A.dv_rut_cliente
,A.cod_calificacion
,A.dias_mora
,A.fecha_cart_venc
,CASE
    WHEN A.fecha_otorgamiento = 19000101 THEN 0
    WHEN A.fecha_otorgamiento = 0 THEN 0
    WHEN to_date(cast(A.fecha_otorgamiento AS string), 'yyyyMMdd') <= (to_date(SUBSTRING(cast(A.fecha_cierre AS string),1,6)||'01', 'yyyyMMdd')) THEN 0
    ELSE datediff(to_date(cast(A.fecha_otorgamiento AS string), 'yyyyMMdd'),  (to_date(SUBSTRING(cast(A.fecha_cierre AS string),1,6)||'01', 'yyyyMMdd'))) + 1
 END AS ope_dias_curse_pact

/*DATOS DE: tbl_cd_d00_segmentado_pant*/
,CASE WHEN B.operacion is not null THEN 1 ELSE 0 END AS flag_existe_operacion_pant
,IFNULL(B.ind_cartdet,'N')  AS ope_ind_cartdet_pant
,IFNULL(B.fecha_cart_venc,0) AS ope_fecha_cart_venc_pant

/*DATOS DE: tbl_cd_curses*/
,CASE WHEN C.operacion is not null THEN 1 ELSE 0 END AS flag_existe_operacion_rfz
,IFNULL(C.fec_proc,0) AS ope_fec_proc_rfz

/*DATOS DE: tbl_cd_segmentacion_cliente*/
,CASE WHEN D.rut_cliente is not null THEN 1 ELSE 0 END AS flag_existe_cliente_seg_cli
,IFNULL(D.segmento_cliente,'SD') AS cli_segmento_cliente

/*DATOS DE: tbl_cd_cliente_consolidado*/
,CASE WHEN E.rut_cliente is not null THEN 1 ELSE 0 END AS flag_existe_cliente_cli_con
,IFNULL(E.calificacion_bci,'SD') AS cli_calificacion_bci

/*DATOS DE: tbl_cd_cliente_lir*/
,CASE WHEN F.rut_cliente is not null THEN 1 ELSE 0 END AS flag_existe_cliente_lir
,IFNULL(F.fecha_informada,0) AS cli_fecha_informada_lir

/*DATOS DE: tbl_cd_cliente_det_ssff*/
,CASE WHEN G.rut_cliente is not null THEN 1 ELSE 0 END AS flag_existe_cliente_ssff

/*DATOS DE: tbl_cd_cliente_det_fact*/
,CASE WHEN H.rut_cliente is not null THEN 1 ELSE 0 END AS flag_existe_cliente_fact

FROM
  {base_silver_x}.tbl_cd_d00_segmentado A
LEFT JOIN
  {base_silver_x}.tbl_cd_d00_segmentado_pant B
ON   A.operacion = B.operacion  AND A.sistema = B.sistema  
LEFT JOIN
   {base_silver_x}.tbl_cd_curses C
ON   trim(A.operacion) = trim(C.operacion) AND trim(A.sistema) = trim(C.cod_sistema)
LEFT JOIN
    {base_silver_x}.tbl_cd_segmentacion_cliente D
ON A.rut_cliente = D.rut_cliente 
LEFT JOIN
    {base_silver_x}.tbl_cd_cliente_consolidado E
ON A.rut_cliente = E.rut_cliente
LEFT JOIN
    {base_silver_x}.tbl_cd_cliente_lir F
ON A.rut_cliente = F.rut_cliente
LEFT JOIN
    {base_silver_x}.tbl_cd_cliente_det_ssff G
ON A.rut_cliente = G.rut_cliente
LEFT JOIN
    {base_silver_x}.tbl_cd_cliente_det_fact H
ON A.rut_cliente = H.rut_cliente
"""


# COMMAND ----------

sql_safe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cae Excepciones Imcumplimiento: solo operaciones

# COMMAND ----------

paso_query110 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_ope_cae_incl AS
SELECT 
  trim(b.operacion)                   AS operacion,
  trim(b.tipo_operacion)              AS tipo_operacion,
  trim(b.anio_lic)                    AS anio_lic,
  count(b.operacion)                  AS cant
FROM
   {base_silver_x}.tbl_cd_cae_ope_det_incumplimiento b
group by 1,2,3
"""  

# COMMAND ----------

sql_safe(paso_query110)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calcula maximo dias de mora para renegociados
# MAGIC ---------------------------
# MAGIC * Calculo exclusivo para el deterioro de operaciones renegociadas

# COMMAND ----------

# MAGIC %md
# MAGIC #####Calcula maximo dias de curse del cliente en periodo actual 
# MAGIC ---------------------------
# MAGIC * los dias de curse del periodo actual, para operaciones renegociadas, se consideran como mora, para el calculo de deterioro renegociado
# MAGIC * estos dias de curse (mora) se deben sumar a los dias de mora del periodo anterior para obtener los clientes renegociados con 60 o mas dias de mora.

# COMMAND ----------

paso_query131 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cli_mor_ren_pact as
select 
  a.fecha_cierre,
  a.rut_cliente,
  max(a.ope_dias_curse_pact) as max_dias_curse_pact
from 
   tmp_EXT_tbl_cd_ope_d00_pact  a
where
   flag_existe_operacion_pant=0 
  AND TRIM(A.tipo_producto) = '{p_tip_prod}' 
  AND TRIM(A.tipo_cartera) IN {p_tip_cart} 
  AND TRIM(A.cod_renegociado) IN {p_cod_ren}
group by 1,2
"""


# COMMAND ----------

sql_safe(paso_query131)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculo maximo dias de mora del cliente en periodo anterior
# MAGIC ---------------------------
# MAGIC * Estos dias de mora, en conjunto con dias de curse (mora) de las operaciones renegociadas del periodo actual, se suman para el calculo final
# MAGIC
# MAGIC

# COMMAND ----------

paso_query132 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cli_mor_ren_pant as
select 
  a.fecha_cierre,
  a.rut_cliente,
  max(a.dias_mora) as max_dia_mora
from 
   {base_silver_x}.tbl_cd_d00_segmentado_pant  a
group by 1,2
"""


# COMMAND ----------

sql_safe(paso_query132)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculo maximo dias mora cliente periodo actual 
# MAGIC ---------------------------
# MAGIC * Calculo maximo dias de mora en periodo actual considerando los dias de curse de las operaciones renegociadas nuevas del periodo actual mas
# MAGIC * el maximo dias de mora del periodo anterior de todas las operaciones del cliente.
# MAGIC
# MAGIC

# COMMAND ----------

paso_query133 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cli_mor_ren as
select
  a.fecha_cierre, 
  a.rut_cliente, 
  a.max_dias_curse_pact                              as max_dias_curse_pact,
  b.max_dia_mora                                     as max_dia_mor_pant, 
  (a.max_dias_curse_pact + ifnull(b.max_dia_mora,0)) as max_dia_mora
from
 tmp_RES_tbl_cd_cli_mor_ren_pact a
left join
 tmp_RES_tbl_cd_cli_mor_ren_pant b
on a.rut_cliente = b.rut_cliente
"""

# COMMAND ----------

sql_safe(paso_query133)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Genera tablon de salida con todos los datos necesarios para evaluar la entrada a cartera deteriorada

# COMMAND ----------

paso_query200 = f"""
CREATE OR REPLACE TEMPORARY VIEW tbl_cd_ope_condicion_deterioro as
SELECT
   A.periodo_cierre
  ,A.fecha_cierre
  ,A.tipo_proceso
  ,A.macro_sistema
  ,A.tipo_cartera
  ,A.tipo_producto
  ,A.segmento
  ,A.cod_proceso
  ,A.sistema
  ,A.tipo_credito
  ,A.operacion
  ,A.tipo_operacion
  ,A.cuenta_contable
  ,A.saldo_cartera_venc
  ,A.saldo_total_ifrs
  ,A.cod_renegociado
  ,A.fecha_otorgamiento
  ,A.fecha_inicio_mora
  ,A.rut_cliente
  ,A.dv_rut_cliente
  ,A.cod_calificacion
  ,A.dias_mora
  ,A.fecha_cart_venc
  ,A.ope_dias_curse_pact
  ,A.flag_existe_operacion_pant
  ,A.ope_ind_cartdet_pant
  ,A.ope_fecha_cart_venc_pant
  ,A.flag_existe_operacion_rfz
  ,A.ope_fec_proc_rfz
  ,A.flag_existe_cliente_seg_cli
  ,A.cli_segmento_cliente
  ,A.flag_existe_cliente_cli_con
  ,A.cli_calificacion_bci
  ,A.flag_existe_cliente_lir
  ,A.cli_fecha_informada_lir
  ,A.flag_existe_cliente_ssff
  ,A.flag_existe_cliente_fact
  ,IFNULL(B.max_dias_curse_pact,0) AS max_dias_curse_pact
  ,IFNULL(B.max_dia_mor_pant,0) AS max_dia_mor_pant
  ,IFNULL(B.max_dia_mora,0) AS max_dia_mora_ren
  ,CASE WHEN C.operacion IS NOT NULL AND substring(cast(A.fecha_otorgamiento as string),1,4) = trim(C.anio_lic)   THEN 1 ELSE 0 END AS flag_cae_excepcion

FROM
  tmp_EXT_tbl_cd_ope_d00_pact A
LEFT JOIN
  tmp_RES_tbl_cd_cli_mor_ren B
ON A.rut_cliente=B.rut_cliente
LEFT JOIN
  tmp_RES_tbl_cd_ope_cae_incl C
ON
    trim(A.operacion)  = trim(C.operacion) AND TRIM(A.tipo_operacion) = TRIM(C.tipo_operacion)    
"""


# COMMAND ----------

sql_safe(paso_query200)

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
# MAGIC #####TRUNCATE tbl_cd_ope_condicion_deterioro

# COMMAND ----------

paso_query300 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_ope_condicion_deterioro """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta registros tabla de salida

# COMMAND ----------

# MAGIC %md
# MAGIC #####INSERT INTO tbl_cd_ope_condicion_deterioro

# COMMAND ----------

paso_query400 = f"""
INSERT INTO {base_silver_x}.tbl_cd_ope_condicion_deterioro
SELECT 
   A.periodo_cierre
  ,A.fecha_cierre
  ,A.tipo_proceso
  ,A.macro_sistema
  ,A.tipo_cartera
  ,A.tipo_producto
  ,A.segmento
  ,A.cod_proceso
  ,A.sistema
  ,A.tipo_credito
  ,A.operacion
  ,A.tipo_operacion
  ,A.cuenta_contable
  ,A.saldo_cartera_venc
  ,A.saldo_total_ifrs
  ,A.cod_renegociado
  ,A.fecha_otorgamiento
  ,A.fecha_inicio_mora
  ,A.rut_cliente
  ,A.dv_rut_cliente
  ,A.cod_calificacion
  ,A.dias_mora
  ,A.fecha_cart_venc
  ,A.ope_dias_curse_pact
  ,A.flag_existe_operacion_pant
  ,A.ope_ind_cartdet_pant
  ,A.ope_fecha_cart_venc_pant
  ,A.flag_existe_operacion_rfz
  ,A.ope_fec_proc_rfz
  ,A.flag_existe_cliente_seg_cli
  ,A.cli_segmento_cliente
  ,A.flag_existe_cliente_cli_con
  ,A.cli_calificacion_bci
  ,A.flag_existe_cliente_lir
  ,A.cli_fecha_informada_lir
  ,A.flag_existe_cliente_ssff
  ,A.flag_existe_cliente_fact
  ,A.max_dias_curse_pact
  ,A.max_dia_mor_pant
  ,A.max_dia_mora_ren
  ,A.flag_cae_excepcion
FROM
  tbl_cd_ope_condicion_deterioro  A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC) =1  
""" 

# COMMAND ----------

sql_safe(paso_query400)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")