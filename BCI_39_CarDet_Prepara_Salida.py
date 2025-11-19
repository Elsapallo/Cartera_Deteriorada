# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_39_CarDet_Prepara_Salida
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_39_CarDet_Prepara_Salida.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011901092
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/05/2022
# MAGIC * Descripcion: Prepara y carga informacion en tablas que son utilizadas como criterio de salida, las cuales que en un inicio se cargaban con archivos productivos. Objetivo es dejar de depender de archivos productivos de otros sistemas.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 22/07/2025 
# MAGIC * Descripción: Se tabla work que contiene las operaciones Renegociadas con fecha de otorgamiento del periodo actual. Sera utilizada en el criterio de salida Sin Refinanciamiento.     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_prin
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado 
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado_pant
# MAGIC * {base_silver_x}.tbl_cd_cartdet_ope_ini_cd_pant
# MAGIC * {base_silver_x}.tbl_cd_cliente_consolidado
# MAGIC * {base_silver_x}.tbl_cd_segmentacion_cliente
# MAGIC * {base_silver_x}.tbl_cd_cliente_det_ssff
# MAGIC * {base_silver_x}.tbl_cd_cliente_det_fact
# MAGIC * {base_silver_x}.tbl_cd_cliente_lir
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro
# MAGIC ***************************************************************************

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

print(f"[fecha_x] {fecha_x}")
print(f"[base_silver_x] {base_silver_x}")


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

# DBTITLE 1,setea parametros internos

#Parametria interna notebook
periodo_x=fecha_x[:6]
p_num_cuotas_orig = 2
p_tip_ope = 'CAE'
p_macro_sist = 'BCI'
p_tip_prod = 'ACT' 
p_tip_ope_n = ('CCN008','TDC001','TDC002')
p_sal_mor = 0
p_cod_sis = '05'
p_cod_segmento = 'G'
p_per_ini = 2
p_per_fin = 5
p_cod_ren='S','C'


print(f"periodo_x: {periodo_x}")
print(f"p_num_cuotas_orig: {p_num_cuotas_orig}")
print(f"p_tip_ope: {p_tip_ope}")
print(f"p_macro_sist: {p_macro_sist}")
print(f"p_tip_prod: {p_tip_prod}")
print(f"p_tip_ope_n: {p_tip_ope_n}")
print(f"p_sal_mor: {p_sal_mor}")
print(f"p_cod_sis: {p_cod_sis}")
print(f"p_cod_segmento: {p_cod_segmento}")
print(f"p_per_ini: {p_per_ini}")
print(f"p_per_fin: {p_per_fin}")
print(f"p_cod_ren: {p_cod_ren}")



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

# MAGIC %md
# MAGIC ### Extrae Operaciones Deteriorados
# MAGIC --------------------------------------
# MAGIC - Extrae todos las operaciones deterioradas del proceso actual
# MAGIC

# COMMAND ----------

paso_query5 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_crit_ent_crit AS
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
  A.criterio_entrada	         AS     criterio_entrada,
  A.origen_deterioro	         AS     origen_deterioro,
  A.fecha_entrada	             AS     fecha_entrada,
  A.grupo	                     AS     grupo,
  A.periodo_evaluacion	       AS     periodo_evaluacion,
  A.criterio_entrada_cliente   AS     criterio_entrada_cliente

FROM 
    {base_silver_x}.tbl_cd_cartdet_crit_ent_prin A
WHERE
    A.fecha_cierre =   {fecha_x} 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC, A.periodo_evaluacion ASC) =1
"""

# COMMAND ----------

sql_safe(paso_query5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes Deteriorados
# MAGIC --------------------------------------
# MAGIC - Extrae todos los clientes con condiciones de deterioro del proceso actual

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_cli_ent_crit AS
SELECT  
  A.periodo_cierre              AS periodo_cierre,
  A.fecha_cierre                AS fecha_cierre,
  A.tipo_proceso                AS tipo_proceso,
  A.rut_cliente                 AS rut_cliente,
  A.dv_rut_cliente              AS dv_rut_cliente,
  A.criterio_entrada_cliente    AS criterio_entrada_cliente
FROM 
  tmp_EXT_tbl_cd_crit_ent_crit   A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.fecha_cierre DESC) =1
"""

# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae D00SEG periodo actual, para clientes deteriorados
# MAGIC --------------------------------------
# MAGIC - Se extrae todas las operaciones del periodo actual de los clientes deteriorados
# MAGIC

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_d00seg_pact as
SELECT
  A.periodo_cierre,
  A.fecha_cierre,
  A.tipo_proceso,
  A.macro_sistema,
  A.segmento,
  A.operacion,
  A.tipo_operacion,
  A.sistema,
  A.rut_cliente,
  A.dv_rut_cliente,
  A.cuenta_contable,
  A.ind_castigo,
  A.saldo_cartera_venc,
  A.saldo_total_ifrs,
  A.saldo_capital_ifrs,
  A.cod_renegociado,
  A.fecha_otorgamiento,
  A.fecha_inicio_mora,
  A.dias_mora,
  A.cuotas_amort_por_vencer,
  A.tipo_credito,
  A.num_cuotas_orig,
  A.fecha_extincion,
  A.tipo_producto,
  A.saldo_moroso1,
  A.saldo_moroso2,
  A.num_cuotas_pend,
  B.criterio_entrada_cliente

FROM
  {base_silver_x}.tbl_cd_d00_segmentado A,
  tmp_EXT_tbl_cd_cli_ent_crit  B
WHERE
  A.rut_cliente = B.rut_cliente
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC, A.fecha_otorgamiento ASC) =1  
"""  

# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae D00SEG periodo anterior, para clientes deteriorados
# MAGIC --------------------------------------
# MAGIC - Se extrae todas las operaciones del periodo anterior de los clientes deteriorados
# MAGIC

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cd_d00seg_pant as
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
  ,A.origen_deterioro
  ,A.criterio_entrada
  ,A.fecha_entrada
  ,A.ind_cartdet
  ,A.tipo_producto
  ,A.tipo_cartera
  ,A.cod_renegociado
  ,A.tipo_credito
  ,A.saldo_moroso2
  ,A.saldo_cartera_venc
  ,A.num_cuotas_pend
  ,A.cuotas_amort_por_vencer
  ,A.cuenta_contable
  ,A.saldo_capital_ifrs
  ,A.saldo_int_dev_ifrs
  ,A.saldo_reajuste_ifrs
  ,A.monto_no_facturado
  ,A.fecha_inicio_mora
  ,A.dias_mora
  ,A.fecha_cart_venc
  ,A.cont_pag_cons
FROM
  {base_silver_x}.tbl_cd_d00_segmentado_pant A,
  tmp_EXT_tbl_cd_cli_ent_crit  B
WHERE
  A.rut_cliente = B.rut_cliente
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC) =1  
"""  

# COMMAND ----------

sql_safe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculo de cantidad de meses en cartera deteriorada
# MAGIC --------------------------------------
# MAGIC - Se calculan los meses en catera deteriorada a la fecha de cierre.
# MAGIC - Se utiliza la fecha de entrada a cartera deteriorada de la operacion informada en periodoa anterior versus la fecha de proceso. 
# MAGIC - El calculo utiliza periodos (meses) completos, no importa la el dia en la fecha.

# COMMAND ----------

paso_query35 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_ope_num_meses_cd as
SELECT
   A.fecha_cierre
  ,A.operacion
  ,A.sistema  
  ,A.rut_cliente
  ,A.segmento
  ,A.tipo_operacion
  ,CASE WHEN IFNULL(B.fecha_entrada,19000101) > 19000101 
       THEN date(substring(B.fecha_entrada,1,4) ||'-'||  substring(B.fecha_entrada,5,2) ||'-01') 
       ELSE '1900-01-01'
   END AS ope_per_ini_cd
  ,date(substring(A.fecha_cierre,1,4) ||'-'||  substring(A.fecha_cierre,5,2) ||'-01')  AS ope_per_fin_cd
  ,CASE WHEN ope_per_ini_cd > '1900-01-01'
        THEN datediff(MONTH,ope_per_ini_cd, ope_per_fin_cd)
        ELSE 0
  END  AS ope_num_meses_en_cd
FROM
  tmp_EXT_tbl_cd_d00seg_pact A
LEFT JOIN  
  tmp_EXT_tbl_cd_d00seg_pant B
ON A.operacion = B.operacion AND A.sistema = B.sistema    
"""

# COMMAND ----------

sql_safe(paso_query35)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculo de pagos parciales
# MAGIC --------------------------------------
# MAGIC - el calculo utiliza la fecha de otorgamiento y fecha de extinsion de la operacion  para obtener los dias de duracion del credito
# MAGIC - luego, los dias de duracion del credito se divide por las cuotas originales, obteniendo la frecuencia de pago de las cuotas
# MAGIC - luego se divide por 30 para obtener un factor (periodicidad)
# MAGIC - si la periodicidad esta entre 2 y 5 (ambos incluidos) entonces es operacion con pagos parciales
# MAGIC - Se realiza el calculo del campo periodo, de la siguiente forma ((fecha_extincion - fecha_otorgamiento) / num_cuotas_orig) / 30
# MAGIC - flag_ope_cta_par =1 cumple

# COMMAND ----------

  paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_ope_ctas_par AS
SELECT 
     A.fecha_cierre                               AS fecha_cierre
    ,A.sistema                                    AS sistema
    ,A.operacion                                  AS operacion
    ,A.rut_cliente                                AS rut_cliente
    ,A.segmento                                   AS segmento
    ,A.tipo_operacion                             AS tipo_operacion
    ,A.macro_sistema                              AS macro_sistema
    ,A.num_cuotas_orig                            AS num_cuotas_orig
    ,A.fecha_otorgamiento                         AS fecha_otorgamiento
    ,A.fecha_extincion                            AS fecha_extincion
    ,date(substring(A.fecha_otorgamiento,1,4) ||'-'|| substring(A.fecha_otorgamiento,5,2)  ||'-'|| substring(A.fecha_otorgamiento,7,2) ) as fech_oto
    ,date(substring(A.fecha_extincion,1,4) ||'-'|| substring(A.fecha_extincion,5,2)  ||'-'|| substring(A.fecha_extincion,7,2) ) as fech_ext
    ,ifnull(cast(datediff(fech_ext,fech_oto) as int),0) as ope_num_dias
    ,cast(((ope_num_dias / A.num_cuotas_orig) / 30) as int)  as periodicidad
    ,case when periodicidad BETWEEN {p_per_ini} AND {p_per_fin} THEN 1 ELSE 0 END as flag_ope_cta_par
FROM
    tmp_EXT_tbl_cd_d00seg_pact A
WHERE 
      UPPER(A.segmento) = UPPER('{p_cod_segmento}')
  AND A.num_cuotas_orig >= {p_num_cuotas_orig}
  AND substring(trim(A.tipo_operacion),1,3) <> '{p_tip_ope}' 
  AND trim(A.macro_sistema) = '{p_macro_sist}' 
"""


# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculo de pagos consecutivos
# MAGIC --------------------------------------
# MAGIC - el calculo se realiza utilizando los pagos consecutivos del mes anterior como base
# MAGIC - para los pagos consecutivos del mes actual se consideran 3 precalculos, con las cuotas originales, pendientes y vencidas
# MAGIC

# COMMAND ----------

paso_query45 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_ope_pag_cons AS
SELECT
      A.fecha_cierre                              
    ,A.sistema                                
    ,A.operacion                             
    ,A.rut_cliente                          
    ,A.segmento                            
    ,A.tipo_operacion                       
    ,A.tipo_producto                    
    ,A.saldo_moroso1 
    ,A.saldo_moroso2
    ,A.saldo_cartera_venc
    ,IFNULL(A.num_cuotas_orig,0)              AS num_cuotas_orig_pact
    ,IFNULL(A.num_cuotas_pend,0)              AS num_cuotas_pend_pact
    ,IFNULL(A.cuotas_amort_por_vencer,0)      AS cuotas_amort_por_vencer_pact
    ,IFNULL(B.num_cuotas_pend,0)			        AS num_cuotas_pend_pant
    ,IFNULL(B.cuotas_amort_por_vencer,0)      AS cuotas_amort_por_vencer_pant
    ,IFNULL(B.cont_pag_cons,0)                AS cont_pag_cons_pant

    ,(num_cuotas_orig_pact - num_cuotas_pend_pact)                    AS cont_pag_cons_orig
    ,(num_cuotas_pend_pant - num_cuotas_pend_pact)                    AS cont_pag_cons_pend
    ,(cuotas_amort_por_vencer_pant - cuotas_amort_por_vencer_pact)    AS cont_pag_cons_venc

    ,case when cont_pag_cons_pend >= 0 then cont_pag_cons_pend + cont_pag_cons_pant else cont_pag_cons_pant end  as cont_pag_cons_1
    ,case when cont_pag_cons_venc >= 0 then cont_pag_cons_venc + cont_pag_cons_pant else cont_pag_cons_pant end  as cont_pag_cons_2
    ,case when cont_pag_cons_orig >= 0 then cont_pag_cons_orig + cont_pag_cons_pant else cont_pag_cons_pant end  as cont_pag_cons_3

    ,CASE 
      WHEN B.cont_pag_cons IS NULL AND cont_pag_cons_3 > 0 THEN cont_pag_cons_3
      WHEN B.cont_pag_cons IS NULL AND cont_pag_cons_2 > 0 THEN cont_pag_cons_2
      ELSE cont_pag_cons_1 
    END                       AS cont_pag_cons

FROM
  tmp_EXT_tbl_cd_d00seg_pact A
LEFT JOIN
  tmp_EXT_tbl_cd_d00seg_pant B
ON A.operacion = B.operacion AND A.sistema = B.sistema  
WHERE
      A.tipo_producto = '{p_tip_prod}'
  AND A.tipo_operacion NOT IN {p_tip_ope_n}
  AND (A.saldo_moroso1 + A.saldo_moroso2 + A.saldo_cartera_venc) <= {p_sal_mor}
  AND a.sistema <> '{p_cod_sis}'
"""

# COMMAND ----------

sql_safe(paso_query45)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtiene datos de la operacion al inicio del evento cartera deteriorada (ope ini cd)
# MAGIC --------------------------------------
# MAGIC - Esta tabla contiene datos de cuotas, capital y motivo de entrada, de la operacion deteriorada cuando entró por primera vez, del ultimo evento de cartera deteriorada.
# MAGIC - Esta informacion sirve para determinar si desde que entró a cartera deteriorada ha realizado pagos de cuotas y pagos de capital
# MAGIC - se calcula si la operacion ha tenido pagos de capital y cuotas de capital desde que entro a deterioro hasta fecha actual
# MAGIC
# MAGIC

# COMMAND ----------

paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_ope_ini_cd AS
SELECT
     A.fecha_cierre                              
    ,A.sistema                                
    ,A.operacion                             
    ,A.rut_cliente                          
    ,A.segmento                            
    ,A.tipo_operacion                       
    ,A.tipo_producto
    ,IFNULL(B.fecha_entrada,19000101)      AS fecha_entrada_ini_cd
    ,IFNULL(B.cuotas_amort_por_vencer,0)   AS cuotas_amort_por_vencer_ini_cd
    ,IFNULL(B.saldo_capital_ifrs,0)        AS saldo_capital_ifrs_ini_cd
    
    ,CASE 
         WHEN IFNULL(B.cuotas_amort_por_vencer,0) > IFNULL(A.cuotas_amort_por_vencer,0)
         THEN IFNULL(B.cuotas_amort_por_vencer,0) - IFNULL(A.cuotas_amort_por_vencer,0)
         ELSE 0
    END AS ope_pag_parciales_ini_cd
   ,CASE 
         WHEN IFNULL(B.saldo_capital_ifrs,0) > IFNULL(A.saldo_capital_ifrs,0) 
         THEN IFNULL(B.saldo_capital_ifrs,0) - IFNULL(A.saldo_capital_ifrs,0) 
         ELSE 0
    END AS ope_pag_capital_ifrs_ini_cd
FROM
  tmp_EXT_tbl_cd_d00seg_pact A
LEFT JOIN
  {base_silver_x}.tbl_cd_cartdet_ope_ini_cd_pant B
ON A.operacion = B.operacion AND A.sistema = B.sistema  
"""

# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtiene indicador operacion con reestructuracion forzosa
# MAGIC --------------------------------------
# MAGIC - Estas operaciones son informadas por riesgo
# MAGIC

# COMMAND ----------

paso_query53 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_ope_curses_rfz AS
SELECT
     A.fecha_cierre                              
    ,A.sistema                                
    ,A.operacion                             
    ,A.rut_cliente                          
    ,A.segmento                            
    ,A.tipo_operacion                       
    ,A.tipo_producto
    ,CASE WHEN B.operacion is not null THEN 1 ELSE 0 END AS flag_existe_operacion_rfz
FROM
  tmp_EXT_tbl_cd_d00seg_pact A
LEFT JOIN
  {base_silver_x}.tbl_cd_curses B
ON A.operacion = B.operacion  AND A.sistema = B.cod_sistema
"""

# COMMAND ----------

sql_safe(paso_query53)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculo maxima mora del cliente en periodo actual
# MAGIC --------------------------------------
# MAGIC - Se consideran todas las operaciones del cliente del periodo actual
# MAGIC
# MAGIC

# COMMAND ----------

paso_query55 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cli_max_mora AS
SELECT
   A.fecha_cierre
  ,A.tipo_proceso
  ,A.rut_cliente
  ,MAX(A.dias_mora) AS max_dia_mora_pact
FROM
  tmp_EXT_tbl_cd_d00seg_pact A
group by 1,2,3
"""

# COMMAND ----------

sql_safe(paso_query55)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calcula mora sistema financiero SBIF 
# MAGIC --------------------------------------
# MAGIC - Se consideran las deudas dsf_dvcn + dsf_dvcx + dsf_cdir + bdsf_dmor + deuda_estud_90_180 + deuda_estud_180 + deuda_estud_30_90 multiplicadas por un factor que es parametrico
# MAGIC
# MAGIC

# COMMAND ----------

paso_query60 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cli_mor_dsf AS
SELECT         
	 A.fecha_cierre                                 AS fecha_cierre
	,A.tipo_proceso                                 AS tipo_proceso
	,A.rut_cliente                                  AS rut_cliente
	,IFNULL(B.dsf_dvcn,0)                           AS dsf_dvcn_1
	,IFNULL(B.dsf_dvcx,0)                           AS dsf_dvcx_1
	,IFNULL(B.dsf_cdir,0)                           AS dsf_cdir_1
	,IFNULL(B.dsf_dmor,0)                           AS dsf_dmor_1
	,IFNULL(B.deuda_estud_90_180,0)                 AS deuda_estud_90_180_1
	,IFNULL(B.deuda_estud_180,0)                    AS deuda_estud_180_1
	,IFNULL(B.deuda_estud_30_90,0)                  AS deuda_estud_30_90_1
	,(dsf_dvcn_1 * {p1_valordvcn})                  AS dsf_dvcn_f
	,(dsf_dvcx_1 * {p1_valordvcx})                  AS dsf_dvcx_f
	,(dsf_cdir_1 * {p1_valorcdir})                  AS dsf_cdir_f
	,(dsf_dmor_1 * {p1_valordmor})                  AS dsf_dmor_f
	,(deuda_estud_90_180_1 * {p1_valor90180})       AS deuda_estud_90_180_f
	,(deuda_estud_180_1 * {p1_valor180} )           AS deuda_estud_180_f
	,(deuda_estud_30_90_1 * {p1_valor3090})         AS deuda_estud_30_90_f
  ,(dsf_dvcn_f + dsf_dvcx_f + dsf_cdir_f + dsf_dmor_f + deuda_estud_90_180_f + deuda_estud_180_f + deuda_estud_30_90_f) AS cli_mora_sbif_f
FROM
   tmp_EXT_tbl_cd_cli_ent_crit A
LEFT JOIN
   {base_silver_x}.tbl_cd_cliente_consolidado  B ON A.rut_cliente = B.rut_cliente
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.fecha_cierre DESC) =1   
"""


# COMMAND ----------

sql_safe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calcula indicadores a nivel de cliente 
# MAGIC --------------------------------------
# MAGIC - Se obtiene indicadores de pertenencia.
# MAGIC - cliente informado con deterioro en ssff
# MAGIC - cliente informado con deterioro en factoring
# MAGIC - cliente informado en fuente lir y fecha de ultima vez que fue informado
# MAGIC - segmento del cliente (I,G)
# MAGIC - calificacion bci del cliente (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

paso_query65 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cd_cli_indicadores AS
SELECT         
	 A.fecha_cierre                                          AS fecha_cierre
	,A.tipo_proceso                                          AS tipo_proceso
	,A.rut_cliente                                           AS rut_cliente
  ,IFNULL(B.calificacion_bci,'SD')                         AS calificacion_bci
  ,IFNULL(C.segmento_cliente,'SD')                         AS segmento_cliente
  ,CASE WHEN D.rut_cliente is not null THEN 1 ELSE 0 END   AS flag_existe_cliente_ssff
  ,CASE WHEN E.rut_cliente is not null THEN 1 ELSE 0 END   AS flag_existe_cliente_fact
  ,CASE WHEN F.rut_cliente is not null THEN 1 ELSE 0 END   AS flag_existe_cliente_lir
  ,IFNULL(F.fecha_informada,19000101)                      AS cli_fecha_informada_lir

FROM
   tmp_EXT_tbl_cd_cli_ent_crit A
LEFT JOIN
   {base_silver_x}.tbl_cd_cliente_consolidado  B ON A.rut_cliente = B.rut_cliente
LEFT JOIN
   {base_silver_x}.tbl_cd_segmentacion_cliente C ON A.rut_cliente = C.rut_cliente
LEFT JOIN
   {base_silver_x}.tbl_cd_cliente_det_ssff D ON A.rut_cliente = D.rut_cliente
LEFT JOIN
   {base_silver_x}.tbl_cd_cliente_det_fact E ON A.rut_cliente = E.rut_cliente   
LEFT JOIN
   {base_silver_x}.tbl_cd_cliente_lir F ON A.rut_cliente = F.rut_cliente

QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.fecha_cierre DESC) =1   
"""


# COMMAND ----------

sql_safe(paso_query65)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Genera tabla temporal de salida
# MAGIC ---
# MAGIC * contiene toda la informacion recopilada y calculada que se utiliza para el analisis de salida de cartera deteriorada

# COMMAND ----------

paso_query100 = f"""
CREATE OR REPLACE TEMPORARY VIEW tbl_cd_ope_condicion_salida_deterioro AS
SELECT
 A.periodo_cierre                                        AS periodo_cierre
,A.fecha_cierre                                          AS fecha_cierre
,A.tipo_proceso                                          AS tipo_proceso
,A.macro_sistema                                         AS macro_sistema
,A.segmento                                              AS segmento
,A.operacion                                             AS operacion
,A.tipo_operacion                                        AS tipo_operacion
,A.sistema                                               AS sistema
,A.rut_cliente                                           AS rut_cliente
,A.dv_rut_cliente                                        AS dv_rut_cliente
,A.cuenta_contable                                       AS cuenta_contable
,A.ind_castigo                                           AS ind_castigo
,A.saldo_cartera_venc                                    AS saldo_cartera_venc
,A.saldo_total_ifrs                                      AS saldo_total_ifrs
,A.saldo_capital_ifrs                                    AS saldo_capital_ifrs
,A.cod_renegociado                                       AS cod_renegociado
,A.fecha_otorgamiento                                    AS fecha_otorgamiento
,A.fecha_inicio_mora                                     AS fecha_inicio_mora
,A.dias_mora                                             AS dias_mora
,A.cuotas_amort_por_vencer                               AS cuotas_amort_por_vencer
,A.tipo_credito                                          AS tipo_credito
,A.num_cuotas_orig                                       AS num_cuotas_orig
,A.fecha_extincion                                       AS fecha_extincion
,A.tipo_producto                                         AS tipo_producto
,A.saldo_moroso1                                         AS saldo_moroso1
,A.saldo_moroso2                                         AS saldo_moroso2
,A.num_cuotas_pend                                       AS num_cuotas_pend
,A.criterio_entrada_cliente                              AS criterio_entrada_cliente
,CASE WHEN E.operacion is null THEN 0 ELSE 1 END         AS flag_ope_new_pact
,CASE 
    WHEN TRIM(A.cod_renegociado) IN {p_cod_ren} AND 
	     CAST(substring(A.fecha_otorgamiento,1,6) as integer) = {periodo_x} 
    THEN 1 
    ELSE 0 
 END                                                     AS flag_ope_ren_pact
,IFNULL(B.ope_num_meses_en_cd,0)                         AS ope_num_meses_en_cd
,IFNULL(C.flag_ope_cta_par,0)                            AS flag_ope_cta_par
,IFNULL(D.cont_pag_cons,0)                               AS cont_pag_cons
,IFNULL(E.fecha_entrada,19000101)                        AS ope_fecha_entrada_cd_pant
,IFNULL(E.num_cuotas_pend,0)                             AS num_cuotas_pend_pant
,IFNULL(E.cuotas_amort_por_vencer,0)                     AS cuotas_amort_por_vencer_pant
,IFNULL(E.cont_pag_cons,0)                               AS cont_pag_cons_pant
,IFNULL(F.max_dia_mora_pact,0)                           AS max_dia_mora_pact
,IFNULL(G.cli_mora_sbif_f,0)                             AS cli_mora_sbif_f
,IFNULL(H.fecha_entrada_ini_cd,19000101)                 AS fecha_entrada_ope_ini_cd
,IFNULL(H.cuotas_amort_por_vencer_ini_cd,0)              AS cuotas_amort_por_vencer_ope_ini_cd
,IFNULL(H.saldo_capital_ifrs_ini_cd,0)                   AS saldo_capital_ifrs_ope_ini_cd
,IFNULL(J.calificacion_bci,'SD')                         AS cli_calificacion_bci
,IFNULL(J.segmento_cliente,'SD')                         AS cli_segmento_cliente
,IFNULL(J.flag_existe_cliente_ssff,0)                    AS flag_existe_cliente_ssff
,IFNULL(J.flag_existe_cliente_fact,0)                    AS flag_existe_cliente_fact
,IFNULL(J.flag_existe_cliente_lir,0)                     AS flag_existe_cliente_lir
,IFNULL(J.cli_fecha_informada_lir,19000101)              AS cli_fecha_informada_lir
,IFNULL(H.ope_pag_parciales_ini_cd,0)                    AS ope_pag_parciales_ini_cd
,IFNULL(H.ope_pag_capital_ifrs_ini_cd,0)                 AS ope_pag_capital_ifrs_ini_cd
,IFNULL(K.flag_existe_operacion_rfz,0)                   AS flag_existe_operacion_rfz

FROM 
  tmp_EXT_tbl_cd_d00seg_pact A
LEFT JOIN
  tmp_RES_tbl_cd_ope_num_meses_cd B ON A.operacion = B.operacion AND A.sistema = B.sistema
LEFT JOIN
  tmp_RES_tbl_cd_ope_ctas_par C ON A.operacion = C.operacion AND A.sistema = C.sistema
LEFT JOIN
  tmp_RES_tbl_cd_ope_pag_cons D ON A.operacion = D.operacion AND A.sistema = D.sistema
LEFT JOIN
  tmp_EXT_tbl_cd_d00seg_pant E ON A.operacion = E.operacion AND A.sistema = E.sistema
LEFT JOIN
  tmp_RES_tbl_cd_cli_max_mora F ON A.rut_cliente = F.rut_cliente  
LEFT JOIN
  tmp_RES_tbl_cd_cli_mor_dsf G ON A.rut_cliente = G.rut_cliente
LEFT JOIN
  tmp_RES_tbl_cd_ope_ini_cd H ON A.operacion = H.operacion AND A.sistema = H.sistema
LEFT JOIN
  tmp_RES_tbl_cd_cli_indicadores J ON A.rut_cliente = J.rut_cliente
LEFT JOIN
  tmp_RES_tbl_cd_ope_curses_rfz K ON A.operacion = K.operacion AND A.sistema = K.sistema
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
# MAGIC ##### TRUNCATE tbl_cd_ope_condicion_salida_deterioro

# COMMAND ----------

paso_query120 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro """

# COMMAND ----------

sql_safe(paso_query120)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_ope_condicion_salida_deterioro 

# COMMAND ----------

paso_query125 = f"""
INSERT INTO {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro
SELECT 
   periodo_cierre
  ,fecha_cierre
  ,tipo_proceso
  ,macro_sistema
  ,segmento
  ,operacion
  ,tipo_operacion
  ,sistema
  ,rut_cliente
  ,dv_rut_cliente
  ,cuenta_contable
  ,ind_castigo
  ,saldo_cartera_venc
  ,saldo_total_ifrs
  ,saldo_capital_ifrs
  ,cod_renegociado
  ,fecha_otorgamiento
  ,fecha_inicio_mora
  ,dias_mora
  ,cuotas_amort_por_vencer
  ,tipo_credito
  ,num_cuotas_orig
  ,fecha_extincion
  ,tipo_producto
  ,saldo_moroso1
  ,saldo_moroso2
  ,num_cuotas_pend
  ,criterio_entrada_cliente
  ,flag_ope_new_pact
  ,flag_ope_ren_pact
  ,ope_num_meses_en_cd
  ,flag_ope_cta_par
  ,cont_pag_cons
  ,ope_fecha_entrada_cd_pant
  ,num_cuotas_pend_pant
  ,cuotas_amort_por_vencer_pant
  ,cont_pag_cons_pant
  ,max_dia_mora_pact
  ,cli_mora_sbif_f
  ,fecha_entrada_ope_ini_cd
  ,cuotas_amort_por_vencer_ope_ini_cd
  ,saldo_capital_ifrs_ope_ini_cd
  ,cli_calificacion_bci
  ,cli_segmento_cliente
  ,flag_existe_cliente_ssff
  ,flag_existe_cliente_fact
  ,flag_existe_cliente_lir
  ,cli_fecha_informada_lir
  ,ope_pag_parciales_ini_cd
  ,ope_pag_capital_ifrs_ini_cd
  ,flag_existe_operacion_rfz

FROM
  tbl_cd_ope_condicion_salida_deterioro
QUALIFY  ROW_NUMBER() OVER(PARTITION BY operacion, sistema ORDER BY fecha_cierre DESC) =1  
"""  
 

# COMMAND ----------

sql_safe(paso_query125)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")