# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_05_CarDet_Datos_Entrada
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_05_CarDet_Datos_Entrada.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011901902
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Obtener todos los datos de las tablas historicas a tablas del periodo. Objetivo es estandarizar la lectura de los datos, mantener un estandar en los nombres de campos, centralizar la informacion utilizada, mantener el rendimiento del proceso de negocio.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Extrae clientes LIR , cuando es una ejecucion PC, se va a buscar la informacion mayor e igual a la fecha de ejecucion
# MAGIC Para filiales (ssff y fact) cuando se ejecuta un C, se va a buscar el ultimo dia calendario, sea habil o no     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 08/04/2025 
# MAGIC * Descripción: Se modifica el proceso para incorporar la logica de la version anterior de Lir, donde si el cliente entra en esta condicion, no vuelve a salir de deterioro.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 08/07/2025 
# MAGIC * Descripción: Se modifica el proceso para incorporar una tabla de trabajo (work) que contiene el universo de clientes, incluyendo sus respectivas fechas de entrada a deterioro a nivel de cliente.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 4
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 22/07/2025 
# MAGIC * Descripción: Se realizaron dos modificaciones. 
# MAGIC *  1.- De la tabla {base_golden_x}.tbl_segmentacion_d00_segmentado_ext se cambiaron algunos campos a seleccionar, por ejemplo en vez de tipo_cartera a sl1_tipo_cartera.
# MAGIC * 2.- Se modifico el QUALIFY a la tabla tmp_RES_dat_ope_ini_cd_pant.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_seg_segmentacion_d00_segmentado_ext
# MAGIC * {base_silver_x}.tbl_seg_segmentacion_cliente_consolidado
# MAGIC * {base_silver_x}.tbl_seg_segmentacion_clientes
# MAGIC * {base_golden_x}.tbl_segmentacion_d00_segmentado_ext
# MAGIC * {base_golden_x}.tbl_hcd_cartdet_ope_ini_cd 
# MAGIC * {bd_golden_seg_x}.tbl_segmentacion_cliente_consolidado
# MAGIC * slv_Clientes_Personas_db.fn
# MAGIC * slv_FuncionesInt_FilialesNormativos_db.ssff_cardet
# MAGIC * slv_ProdServicios_Factoring_db.CARDET_FAC
# MAGIC * slv_ProdServicios_ProdPersona_db.detalle_incumplimiento_cierre_cae
# MAGIC * prd_slv_RiesgoCred_RiesgoCredPer_db.reestrucforzosa_cur
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado
# MAGIC * {base_silver_x}.tbl_cd_cliente_consolidado
# MAGIC * {base_silver_x}.tbl_cd_cliente_consolidado_pant
# MAGIC * {base_silver_x}.tbl_cd_segmentacion_cliente
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado_pant 
# MAGIC * {base_silver_x}.tbl_cd_cliente_lir
# MAGIC * {base_silver_x}.tbl_cd_cliente_det_ssff
# MAGIC * {base_silver_x}.tbl_cd_cliente_det_fact
# MAGIC * {base_silver_x}.tbl_cd_cae_ope_det_incumplimiento
# MAGIC * {base_silver_x}.tbl_cd_curses
# MAGIC * {base_silver_x}.tbl_cd_cartdet_ope_ini_cd
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

dbutils.widgets.removeAll()
dbutils.widgets.text("fecha_w","20250930","01-Fecha:")
dbutils.widgets.text("bd_silver_w","dsr_gld_bciwork_db","02-Nombre BD Silver:")
dbutils.widgets.text("bd_golden_w","dsr_gld_prodservicios_db","03-Nombre BD Golden Cartdet:")
dbutils.widgets.text("lir_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_Clientes_Personas_db/fn/","04-LIR:"    )
dbutils.widgets.text("ssff_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_FuncionesInt_FilialesNormativos_db/ssff_cardet","05-SSFF:"  )
dbutils.widgets.text("fact_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_ProdServicios_Factoring_db/CARDET_FAC","06-FACT:"  )
dbutils.widgets.text("curse_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_RiesgoCred_RiesgoCredPer_db/reestrucforzosa_cur/","07-CURSE:")
dbutils.widgets.text("detcae_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_ProdServicios_ProdPersona_db/detalle_incumplimiento_cierre_cae","08-DETCAE:")
dbutils.widgets.text("tipo_proceso_w","C","09-Tipo de Proceso (C o PC):")
dbutils.widgets.text("bd_golden_seg_w","dsr_gld_clientes_db","10-Nombre BD Golden Segmentacion:")

# COMMAND ----------

fecha_x = dbutils.widgets.get("fecha_w") 
base_silver_x = dbutils.widgets.get("bd_silver_w")
base_golden_x = dbutils.widgets.get("bd_golden_w")
lir_x = dbutils.widgets.get("lir_w")
ssff_x = dbutils.widgets.get("ssff_w")
fact_x = dbutils.widgets.get("fact_w")
curse_x = dbutils.widgets.get("curse_w")
detcae_x = dbutils.widgets.get("detcae_w")
tipo_proceso_x = dbutils.widgets.get("tipo_proceso_w")
bd_golden_seg_x = dbutils.widgets.get("bd_golden_seg_w")

spark.conf.set("bci.fecha", fecha_x)
spark.conf.set("bci.dbnamesilver", base_silver_x)
spark.conf.set("bci.dbnamegolden", base_golden_x)
spark.conf.set("bci.lir", lir_x)
spark.conf.set("bci.ssff", ssff_x)
spark.conf.set("bci.fact", fact_x)
spark.conf.set("bci.curse", curse_x)
spark.conf.set("bci.detcae", detcae_x)
spark.conf.set("bci.tipo_proceso", tipo_proceso_x)
spark.conf.set("bci.bd_golden_seg", bd_golden_seg_x)

print(f"fecha_x : {fecha_x}")
print(f"base_silver_x : {base_silver_x}")
print(f"base_golden_x : {base_golden_x}")
print(f"lir_x : {lir_x}")
print(f"ssff_x : {ssff_x}")
print(f"fact_x : {fact_x}")
print(f"curse_x : {curse_x}")
print(f"detcae_x : {detcae_x}")
print(f"tipo_proceso_x : {tipo_proceso_x}")
print(f"bd_golden_seg_x : {bd_golden_seg_x}")


# COMMAND ----------

# Calcula periodo en base a la fecha
periodo_x=fecha_x[:6]
guardar_log_x="S"

spark.conf.set("bci.periodo", periodo_x)
spark.conf.set("bci.guarda_log", guardar_log_x)

print(f"periodo_x: {periodo_x}")      
print(f"guardar_log_x: {guardar_log_x}")

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

# DBTITLE 1,Valida parámetro "base_goldX"
valida_parametro(base_golden_x)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "tipo_procesoX"
valida_parametro(tipo_proceso_x)

# COMMAND ----------

 resultado = obtener_parametros_tb_fecha(base_silver_x)
 fecha_ant_y = resultado[1]
 periodo_ant_y = resultado[2]

print(f"Fecha cierre anterior: [fecha_ant_y] {fecha_ant_y}")
print(f"Periodo cierre anterior: [periodo_ant_y] {periodo_ant_y}")



# COMMAND ----------

# DBTITLE 1,Valida fecha anterior "FechaAntY"
valida_parametro(fecha_ant_y)

# COMMAND ----------

# DBTITLE 1,Valida periodo anterior "PeriodoAntY"
valida_parametro(periodo_ant_y)

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
# MAGIC ### Extrae Operaciones D00 Periodo Actual
# MAGIC --------------------------------------
# MAGIC - Se extrae operaciones para periodo actual
# MAGIC - el proceso de segmentacion no esta confirmado, por eso se usa BD work

# COMMAND ----------

paso_query10 = f"""

CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado AS
SELECT 
 A.periodo_cierre                             AS periodo_cierre
,A.fecha_cierre                               AS fecha_cierre
,trim(A.tipo_proceso)                         AS tipo_proceso
,trim(A.macro_sistema)                        AS macro_sistema
,trim(A.sl1_tipo_cartera)                     AS tipo_cartera
,trim(A.sl1_tipo_producto)                    AS tipo_producto
,trim(A.segm_codigo_segmento)                 AS cod_segmento
,trim(A.sl1_codigo_proceso)                   AS cod_proceso
,trim(A.sl1_codigo_sistema)                   AS cod_sistema
,trim(A.sl1_tipo_credito)                     AS tipo_credito
,trim(A.operacion)                            AS operacion
,trim(A.sl1_tipo_operacion)                   AS tipo_operacion
,A.situacion_operacion                        AS situacion_operacion
,A.oficina_credito                            AS oficina_credito
,A.estado_credito                             AS estado_credito
,A.act_dest_credito                           AS act_dest_credito
,A.monto_original                             AS monto_original
,A.cuenta_contable                            AS cuenta_contable
,A.sl1_ind_castigo                            AS ind_castigo
,A.sl1_saldo_contable                         AS saldo_contable
,A.saldo_moroso1                              AS saldo_moroso1
,A.saldo_moroso2                              AS saldo_moroso2
,A.saldo_cartera_venc                         AS saldo_cartera_venc
,A.sl1_int_por_cobrar                         AS int_por_cobrar
,A.sl1_int_por_cobrar_venc                    AS int_por_cobrar_venc
,A.sl1_reaj_devengados                        AS reaj_devengados
,A.saldo_reprogramado                         AS monto_no_facturado
,A.sl1_saldo_total_ifrs                       AS saldo_total_ifrs
,A.saldo_capital_ifrs                         AS saldo_capital_ifrs
,A.saldo_int_dev_ifrs						              AS saldo_int_dev_ifrs
,A.saldo_reajuste_ifrs						            AS saldo_reajuste_ifrs
,A.num_cuotas_orig                            AS num_cuotas_orig
,A.excl_operacion                             AS excl_operacion
,trim(A.cod_renegociado)                      AS cod_renegociado
,A.fecha_otorgamiento                         AS fecha_otorgamiento
,A.fecha_inicio_mora                          AS fecha_inicio_mora
,A.fecha_data                                 AS fecha_data
,A.rut_cliente                                AS rut_cliente
,upper(trim(A.dv_cliente))                    AS dv_cliente
,A.ind_tipo_deuda                             AS ind_tipo_deuda
,A.tasa_interes_efectiva                      AS tasa_interes_efectiva
,A.tasa_interes                               AS tasa_interes
,trim(A.codigo_moneda)                        AS codigo_moneda
,A.num_cuotas_pend                            AS num_cuotas_pend
,trim(A.sl1_codigo_banca)                     AS codigo_banca
,trim(A.ind_cartdet)                          AS ind_cartdet
,trim(A.sl1_cod_calificacion)                 AS cod_calificacion
,A.sl1_cic_cliente                            AS cic_cliente
,A.sl1_dias_mora                              AS dias_mora
,A.fecha_extincion                            AS fecha_extincion
,A.fecha_cart_venc                            AS fecha_cart_venc
,A.venc_impagos                               AS venc_impagos
,A.cuotas_amort_por_vencer                    AS cuotas_amort_por_venc
,A.sl1_monto_total_mora                       AS monto_total_mora
,A.monto_venc_mes                             AS monto_venc_mes
,trim(A.sl1_operacion_original)               AS operacion_original
,trim(A.cdet_cod_cartdet_cliente)             AS cod_cartdet_cliente
,trim(A.cdet_cod_motivo_cartdet)              AS cod_motivo_cartdet
,A.cdet_fecha_cartdet_ope                     AS fecha_cartdet_ope
,A.sl1_ind_trazabilidad                       AS ind_trazabilidad
,A.cdet_cod_salida_cartdet                    AS cod_salida_cartdet
,A.cdet_fecha_salida_cartdet                  AS fecha_salida_cartdet
,trim(A.segm_codigo_matriz_prov)              AS codigo_matriz_prov
,A.cdet_fecha_cartdet_cliente                 AS fecha_cartdet_cliente
FROM
  {base_silver_x}.tbl_seg_segmentacion_d00_segmentado_ext A
WHERE 
    A.fecha_cierre =   {fecha_x} 
AND A.periodo_cierre = {periodo_x}
AND A.tipo_proceso = '{tipo_proceso_x}'
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sl1_codigo_sistema ORDER BY A.fecha_cierre DESC) =1
"""


# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes Consolidado Periodo Actual
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes para el periodo actual con la clasificacion bci desde la tabla tbl_segmentacion_cliente_consolidado 

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_cliente_consolidado as
SELECT 
   A.periodo_cierre                    AS periodo_cierre
  ,A.fecha_cierre                      AS fecha_cierre
  ,trim(A.tipo_proceso)                AS tipo_proceso
  ,A.cic_cliente                       AS cic_cliente
  ,A.rut_cliente                       AS rut_cliente
  ,upper(trim(A.dv_cliente))           AS dv_cliente
  ,trim(A.banca)                       AS banca
  ,trim(A.calificacion_bci)            AS calificacion_bci
  ,trim(A.calificacion_regulador)      AS calificacion_regulador
  ,A.fecha_calificacion                AS fecha_calificacion
  ,A.maximo_dias_mora                  AS maximo_dias_mora
  ,A.deuda_factoring                   AS deuda_factoring
  ,A.deuda_ssff                        AS deuda_ssff
  ,A.saldo_mora_1                      AS saldo_mora_1
  ,A.saldo_mora_2                      AS saldo_mora_2
  ,A.saldo_cartera_vencida             AS saldo_cartera_vencida
  ,A.saldo_castigado                   AS saldo_castigado
  ,A.dsf_dvgn                          AS dsf_dvgn
  ,A.dsf_dvcn                          AS dsf_dvcn
  ,A.dsf_dvgx                          AS dsf_dvgx
  ,A.dsf_dvcx                          AS dsf_dvcx
  ,A.dsf_cdir                          AS dsf_cdir
  ,A.dsf_dmor                          AS dsf_dmor
  ,A.deuda_estud_90_180                AS deuda_estud_90_180
  ,A.deuda_estud_180                   AS deuda_estud_180
  ,A.deuda_estud_30_90                 AS deuda_estud_30_90
  ,A.sgc_sdbc_fec_fcal                 AS sgc_sdbc_fec_fcal
  ,trim(A.sgc_sdbc_cod_banc)           AS sgc_sdbc_cod_banc
  ,trim(A.sgc_sdbc_cod_sbifc)          AS sgc_sdbc_cod_sbifc
  ,A.flag_pertenencia                  AS flag_pertenencia
FROM
  {base_silver_x}.tbl_seg_segmentacion_cliente_consolidado A
WHERE 
    A.fecha_cierre =   {fecha_x} 
AND A.periodo_cierre = {periodo_x}
AND A.tipo_proceso = '{tipo_proceso_x}'  
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.fecha_cierre DESC) =1
"""


# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes Consolidado Periodo Anterior
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes para el periodo anterior con la clasificacion bci desde la tabla gld_riesgobdu_golden_db.tbl_segmentacion_cliente_consolidado 

# COMMAND ----------

paso_query25 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_cliente_consolidado_pant as
SELECT 
   A.periodo_cierre                    AS periodo_cierre
  ,A.fecha_cierre                      AS fecha_cierre
  ,trim(A.tipo_proceso)                AS tipo_proceso
  ,A.cic_cliente                       AS cic_cliente
  ,A.rut_cliente                       AS rut_cliente
  ,upper(trim(A.dv_cliente))           AS dv_cliente
  ,trim(A.banca)                       AS banca
  ,trim(A.calificacion_bci)            AS calificacion_bci
  ,trim(A.calificacion_regulador)      AS calificacion_regulador
  ,A.fecha_calificacion                AS fecha_calificacion
  ,A.maximo_dias_mora                  AS maximo_dias_mora
  ,A.deuda_factoring                   AS deuda_factoring
  ,A.deuda_ssff                        AS deuda_ssff
  ,A.saldo_mora_1                      AS saldo_mora_1
  ,A.saldo_mora_2                      AS saldo_mora_2
  ,A.saldo_cartera_vencida             AS saldo_cartera_vencida
  ,A.saldo_castigado                   AS saldo_castigado
  ,A.dsf_dvgn                          AS dsf_dvgn
  ,A.dsf_dvcn                          AS dsf_dvcn
  ,A.dsf_dvgx                          AS dsf_dvgx
  ,A.dsf_dvcx                          AS dsf_dvcx
  ,A.dsf_cdir                          AS dsf_cdir
  ,A.dsf_dmor                          AS dsf_dmor
  ,A.deuda_estud_90_180                AS deuda_estud_90_180
  ,A.deuda_estud_180                   AS deuda_estud_180
  ,A.deuda_estud_30_90                 AS deuda_estud_30_90
  ,A.sgc_sdbc_fec_fcal                 AS sgc_sdbc_fec_fcal
  ,trim(A.sgc_sdbc_cod_banc)           AS sgc_sdbc_cod_banc
  ,trim(A.sgc_sdbc_cod_sbifc)          AS sgc_sdbc_cod_sbifc
  ,A.flag_pertenencia                  AS flag_pertenencia
FROM
   {bd_golden_seg_x}.tbl_segmentacion_cliente_consolidado A
WHERE 
    A.fecha_cierre =   {fecha_ant_y} 
AND A.periodo_cierre = {periodo_ant_y}
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.fecha_cierre DESC) =1
"""

# COMMAND ----------

sql_safe(paso_query25)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Clientes Segmentacion Periodo Actual
# MAGIC --------------------------------------
# MAGIC - Se extrae clientes para el periodo actual con su segmentacion (la segmentacion del cliente y segmentacion de la operacion pueden ser distintas)

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_clientes as
SELECT 
    A.periodo_cierre                  AS periodo_cierre
   ,A.fecha_cierre                    AS fecha_cierre
   ,trim(A.tipo_proceso)              AS tipo_proceso
   ,A.rut_cliente                     AS rut_cliente
   ,upper(trim(A.dv_rut_cliente))     AS dv_rut_cliente
   ,A.cic_cliente                     AS cic_cliente
   ,trim(A.segmento_cliente)          AS segmento_cliente
   ,trim(A.nuevo_segmento)            AS nuevo_segmento
   ,trim(A.tipo_cliente)              AS tipo_cliente
   ,trim(A.motivo_segmentacion)       AS motivo_segmentacion
   ,A.flag_pertenencia                AS flag_pertenencia
FROM
    {base_silver_x}.tbl_seg_segmentacion_clientes A
WHERE 
    A.fecha_cierre =   {fecha_x} 
AND A.periodo_cierre = {periodo_x}
AND A.tipo_proceso = '{tipo_proceso_x}'  
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.fecha_cierre DESC) =1
"""

# COMMAND ----------

sql_safe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones D00 Periodo Anterior
# MAGIC --------------------------------------
# MAGIC - Se extrae todas las operaciones del cierre anterior
# MAGIC

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_segmentacion_d00_segmentado_pant as
SELECT
   cast(a.periodo_cierre as int)                       AS periodo_cierre
  ,cast(a.fecha_cierre   as int)                       AS fecha_cierre
  ,upper(a.tipo_proceso)                               AS tipo_proceso
  ,CAST(substring(a.rut_cliente,1,9) as integer)       AS rut_cliente
  ,upper(a.dv_cliente)                                 AS dv_cliente
  ,a.sl1_tipo_operacion                                AS tipo_operacion
  ,trim(a.operacion)                                   AS operacion
  ,trim(a.sl1_codigo_sistema)                          AS cod_sistema
  ,trim(a.segm_codigo_segmento)                        AS cod_segmento
  ,cast(a.cdet_cod_cartdet_cliente as integer)         AS origen_deterioro
  ,cast(a.cdet_cod_motivo_cartdet as integer)          AS criterio_entrada
  ,cast(a.cdet_fecha_cartdet_ope as int)               AS fecha_entrada
  ,trim(a.cdet_ind_cartdet)                            AS ind_cartdet
  ,trim(a.sl1_tipo_producto)                           AS tipo_producto    
  ,trim(a.sl1_tipo_cartera)                            AS tipo_cartera     
  ,trim(a.cod_renegociado)                             AS cod_renegociado  
  ,cast(a.sl1_tipo_credito  as int)                    AS tipo_credito
  ,cast(a.saldo_moroso2 as decimal(18,0))              AS saldo_moroso2
  ,cast(a.saldo_cartera_venc as decimal(18,0))         AS saldo_cartera_venc
  ,cast(a.num_cuotas_pend as int)                      AS num_cuotas_pend
  ,cast(a.cuotas_amort_por_vencer  as int)             AS cuotas_amort_por_vencer
  ,trim(a.cuenta_contable)                             AS cuenta_contable  
  ,cast(a.sl1_saldo_contable as decimal(18,0))         AS saldo_capital_ifrs   
  ,cast(a.sl1_int_por_cobrar as decimal(18,0))         AS saldo_int_dev_ifrs
  ,cast(a.sl1_reaj_devengados as decimal(18,0))         AS saldo_reajuste_ifrs
  ,cast(a.saldo_reprogramado as decimal(18,0))         AS monto_no_facturado  
  ,cast(a.fecha_inicio_mora  as int)                   AS fecha_inicio_mora
  ,cast(a.sl1_dias_mora  as int)                       AS dias_mora
  ,cast(a.fecha_cart_venc  as int)                     AS fecha_cart_venc  
  ,trim(a.cont_pag_cons)                               AS cont_pag_cons 
FROM 
  {base_golden_x}.tbl_segmentacion_d00_segmentado_ext a
WHERE
    a.fecha_cierre =   {fecha_ant_y} 
AND a.periodo_cierre = {periodo_ant_y}
QUALIFY  ROW_NUMBER() OVER(PARTITION BY a.operacion, a.sl1_codigo_sistema ORDER BY a.fecha_cierre DESC) =1     
"""

# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae clientes LIR
# MAGIC --------------------------------------
# MAGIC - Extrae clientes acogidos a LIR

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae ultimo mes cargado en las ingestas sin Pre-Cierre

# COMMAND ----------

# Esto deberia cubrir el caso de cierre (C)
p_ant_act_lir_fec_x = fecha_x

if tipo_proceso_x == 'PC':
  p_ant_act_lir_fec_x = obtiene_ult_fec(lir_x,fecha_x)
print(p_ant_act_lir_fec_x)

# COMMAND ----------

paso_query80 = f"""
CREATE OR REPLACE TEMPORARY VIEW tbl_EXT_c4_tmp_cli_cond_lir AS
SELECT
  {fecha_x}        AS fecha_cierre,
  b.rut            AS rut_cliente,
  b.fecha_informada AS fecha_informada
FROM
   delta.`{lir_x}` b
WHERE
  b.fecha_informada <= {p_ant_act_lir_fec_x}
QUALIFY  ROW_NUMBER() OVER(PARTITION BY b.rut ORDER BY b.fecha_informada DESC) =1
"""

# COMMAND ----------

sql_safe(paso_query80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae clientes deteriorados informados por SSFF
# MAGIC --------------------------------------
# MAGIC - Extrae clientes deteriorados por SSFF

# COMMAND ----------

# MAGIC %md
# MAGIC #### Busca ultimo dia del mes sin importar si es habil o no

# COMMAND ----------

# Condicion por defecto
# Esto deberia cubrir el caso de Precierre (PC)
if tipo_proceso_x == 'PC':
  condicion_ssff_ult_dia_mes = obtiene_ult_fec(ssff_x,fecha_x)

# Si se esta ejecutando el cierre, no necesariamente la fecha del dato va a coincidir con la fecha ingresada por parametro
# y en este caso lo que hacemos es ir a buscar el ultimo dia del mes en ejecución
if tipo_proceso_x == 'C':
  condicion_ssff_ult_dia_mes = ultimo_dia_mes(fecha_x)

print(condicion_ssff_ult_dia_mes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extrae clientes deteriorados SSFF

# COMMAND ----------

paso_query90 = f"""
CREATE OR REPLACE TEMPORARY VIEW tbl_EXT_tbl_c4_filiales_entrada_SSFF AS
SELECT
   {fecha_x}          AS fecha_cierre
  ,a.Rut              AS rut_cliente
FROM
   delta.`{ssff_x}` a
WHERE 
    a.Fecha_informada = {condicion_ssff_ult_dia_mes} 
 QUALIFY  ROW_NUMBER() OVER(PARTITION BY a.Rut ORDER BY a.Fecha_informada DESC) =1
"""


# COMMAND ----------

sql_safe(paso_query90)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae clientes deteriorados informados por Factoring
# MAGIC --------------------------------------
# MAGIC - Extrae clientes deteriorados por Factoring

# COMMAND ----------

# MAGIC %md
# MAGIC #### Busca ultimo dia del mes sin importar si es habil o no

# COMMAND ----------

# Condicion por defecto
# Esto deberia cubrir el caso de Precierre (PC)
if tipo_proceso_x == 'PC':
  condicion_fact_ult_dia_mes = obtiene_ult_fec(fact_x,fecha_x)

# Si se esta ejecutando el cierre, no necesariamente la fecha del dato va a coincidir con la fecha ingresada por parametro
# y en este caso lo que hacemos es ir a buscar el ultimo dia del mes en ejecución
if tipo_proceso_x == 'C':
  condicion_fact_ult_dia_mes = ultimo_dia_mes(fecha_x)

print(condicion_fact_ult_dia_mes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extrae clientes deteriorados factoring

# COMMAND ----------

paso_query100 = f"""
CREATE OR REPLACE TEMPORARY VIEW tbl_EXT_tbl_c4_filiales_entrada_Fact AS
SELECT
   {fecha_x}    AS fecha_cierre
  ,a.rut        AS rut_cliente
FROM
    delta.`{fact_x}` a
WHERE 
    a.Fecha_informada = {condicion_fact_ult_dia_mes}
 QUALIFY  ROW_NUMBER() OVER(PARTITION BY a.Rut ORDER BY a.FINIDET DESC) =1
"""


# COMMAND ----------

sql_safe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae operaciones CAE que no se deterioran
# MAGIC --------------------------------------
# MAGIC - Extrae operaciones CAE excepcion deterioro

# COMMAND ----------

paso_query120 = f"""
CREATE OR REPLACE TEMPORARY VIEW tbl_EXT_tbl_CAE_detalleIncumplimiento AS
SELECT 
  b.fecha_informada                              AS fecha_cierre,
  trim(b.numero_operacion_vigente)               AS operacion,
  b.rut_cliente                                  AS rut_cliente,
  trim(b.producto)                               AS tipo_operacion,
  trim(b.numero_operacion_codeudor)              AS operacion_ori,
  b.anio_licitacion                              AS anio_lic
FROM
  delta.`{detcae_x}` b
WHERE
  b.fecha_informada = {fecha_x}
QUALIFY  ROW_NUMBER() OVER(PARTITION BY b.numero_operacion_vigente, b.producto, b.rut_cliente, b.anio_licitacion, b.numero_operacion_codeudor ORDER BY b.fecha_informada DESC) =1
"""

# COMMAND ----------

sql_safe(paso_query120)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae operaciones con reestructuracion forsoza
# MAGIC --------------------------------------
# MAGIC - Extrae operaciones (c4_curses). estas operaciones son informadas por riesgo

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extrae ultimo mes cargado en las ingestas sin Pre-Cierre

# COMMAND ----------

# Esto deberia cubrir el caso de cierre (C)
p_ant_act_curse_fec_x = fecha_x

if tipo_proceso_x == 'PC':
  p_ant_act_curse_fec_x = obtiene_ult_fec(curse_x,fecha_x)
print(p_ant_act_curse_fec_x)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extrae operaciones con reestructuracion forsoza

# COMMAND ----------

paso_query180= f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_c4_curses AS
SELECT
    {fecha_x} AS fecha_proceso
   ,{periodo_x} AS fecha_dato           
   ,num_operacion
   ,codigo_sistema
   ,{fecha_x} AS fecha_informada    
FROM
  delta.`{curse_x}` 
WHERE
  fecha_proceso = {p_ant_act_curse_fec_x}
QUALIFY  ROW_NUMBER() OVER(PARTITION BY fecha_proceso, fecha_dato, num_operacion, codigo_sistema ORDER BY fecha_informada DESC) =1  
"""

# COMMAND ----------

sql_safe(paso_query180)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae CLIENTES informacion OPE INI CD periodo anterior
# MAGIC --------------------------------------
# MAGIC - Extrae ultimos evento de deterioro de los clientes

# COMMAND ----------

paso_query130 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_dat_cli_ini_cd_pant AS 
SELECT 
   fecha_cierre               as fecha_cierre
  ,rut_cliente                as rut_cliente
  ,fec_ini_cd                 as fec_ini_cd
  ,fecha_informada            as fecha_informada_ant
  ,{fecha_x}                  as fecha_informada
FROM
  {base_golden_x}.tbl_hcd_cartdet_cli_ini_cd 
where 
  fecha_informada = '{fecha_ant_y}'
"""

# COMMAND ----------

sql_safe(paso_query130)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae OPERACIONES informacion OPE INI CD periodo anterior
# MAGIC --------------------------------------
# MAGIC - Extrae ultimos evento de deterioro de las operaciones

# COMMAND ----------

paso_query185 = f"""
CREATE OR REPLACE TEMPORARY VIEW  tmp_RES_dat_ope_ini_cd_pant AS 
SELECT 
   fecha_cierre               as fecha_cierre
  ,CASE WHEN LENGTH(sistema) = 1 THEN '0' || sistema ELSE sistema END AS sistema   
  ,operacion                  as operacion 
  ,segmento                   as segmento
  ,rut_cliente                as rut_cliente
  ,fecha_entrada              as fecha_entrada
  ,cuotas_amort_por_vencer    as cuotas_amort_por_vencer
  ,saldo_capital_ifrs         as saldo_capital_ifrs
  ,origen_deterioro           as origen_deterioro
  ,fecha_informada            as fecha_informada
FROM
  {base_golden_x}.tbl_hcd_cartdet_ope_ini_cd 
where 
  fecha_informada = '{fecha_ant_y}'
 QUALIFY  ROW_NUMBER() OVER(PARTITION BY operacion,  sistema ORDER BY fecha_entrada DESC) =1  
"""


# COMMAND ----------

sql_safe(paso_query185)

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
# MAGIC ##### TRUNCATE tbl_cd_d00_segmentado

# COMMAND ----------

paso_query300 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_d00_segmentado """


# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cliente_consolidado

# COMMAND ----------

paso_query305 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cliente_consolidado """

# COMMAND ----------

sql_safe(paso_query305)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cliente_consolidado_pant

# COMMAND ----------

paso_query307 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cliente_consolidado_pant """

# COMMAND ----------

sql_safe(paso_query307)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_segmentacion_cliente

# COMMAND ----------

paso_query310 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_segmentacion_cliente """

# COMMAND ----------

sql_safe(paso_query310)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_d00_segmentado_pant

# COMMAND ----------

paso_query315 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_d00_segmentado_pant """

# COMMAND ----------

sql_safe(paso_query315)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cliente_lir

# COMMAND ----------

paso_query335 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cliente_lir """

# COMMAND ----------

sql_safe(paso_query335)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cliente_det_ssff

# COMMAND ----------

paso_query340 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cliente_det_ssff """

# COMMAND ----------

sql_safe(paso_query340)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cliente_det_fact

# COMMAND ----------

paso_query345 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cliente_det_fact """

# COMMAND ----------

sql_safe(paso_query345)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cae_ope_det_incumplimiento

# COMMAND ----------

paso_query355 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cae_ope_det_incumplimiento """

# COMMAND ----------

sql_safe(paso_query355)

# COMMAND ----------

# MAGIC %md
# MAGIC #####TRUNCATE tbl_cd_curses

# COMMAND ----------

paso_query384 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_curses """

# COMMAND ----------

sql_safe(paso_query384)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cartdet_ope_ini_cd_pant

# COMMAND ----------

paso_query386 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_ope_ini_cd_pant """

# COMMAND ----------

sql_safe(paso_query386)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TRUNCATE tbl_cd_cartdet_cli_ini_cd_pant

# COMMAND ----------

paso_query390 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_cli_ini_cd_pant """

# COMMAND ----------

sql_safe(paso_query390)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_d00_segmentado

# COMMAND ----------


paso_query400 = f"""
INSERT INTO {base_silver_x}.tbl_cd_d00_segmentado
SELECT *
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado
"""  


# COMMAND ----------

sql_safe(paso_query400)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cliente_consolidado

# COMMAND ----------


paso_query405 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cliente_consolidado
SELECT *
FROM
  tmp_EXT_tbl_segmentacion_cliente_consolidado
"""  


# COMMAND ----------

sql_safe(paso_query405)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cliente_consolidado_pant

# COMMAND ----------

paso_query407 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cliente_consolidado_pant
SELECT *
FROM
  tmp_EXT_tbl_segmentacion_cliente_consolidado_pant
"""  

# COMMAND ----------

sql_safe(paso_query407)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_segmentacion_cliente

# COMMAND ----------


paso_query410 = f"""
INSERT INTO {base_silver_x}.tbl_cd_segmentacion_cliente
SELECT *
FROM
  tmp_EXT_tbl_segmentacion_clientes
"""  


# COMMAND ----------

sql_safe(paso_query410)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_d00_segmentado_pant

# COMMAND ----------


paso_query415 = f"""
INSERT INTO {base_silver_x}.tbl_cd_d00_segmentado_pant
SELECT *
FROM
  tmp_EXT_tbl_segmentacion_d00_segmentado_pant
"""  


# COMMAND ----------

sql_safe(paso_query415)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cliente_lir

# COMMAND ----------

paso_query435 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cliente_lir
SELECT 
  a.fecha_cierre,
  a.rut_cliente,
  a.fecha_informada
FROM
  tbl_EXT_c4_tmp_cli_cond_lir a
"""  

# COMMAND ----------

sql_safe(paso_query435)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cliente_det_ssff

# COMMAND ----------

paso_query440 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cliente_det_ssff
SELECT *
FROM
  tbl_EXT_tbl_c4_filiales_entrada_SSFF
"""  


# COMMAND ----------

sql_safe(paso_query440)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cliente_det_fact

# COMMAND ----------

paso_query445 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cliente_det_fact
SELECT *
FROM
  tbl_EXT_tbl_c4_filiales_entrada_Fact
"""  


# COMMAND ----------

sql_safe(paso_query445)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cae_ope_det_incumplimiento

# COMMAND ----------

paso_query455 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cae_ope_det_incumplimiento
SELECT *
FROM
  tbl_EXT_tbl_CAE_detalleIncumplimiento
"""  


# COMMAND ----------

sql_safe(paso_query455)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_curses

# COMMAND ----------

paso_query500 = f"""
INSERT INTO {base_silver_x}.tbl_cd_curses
SELECT *
FROM
  tmp_EXT_tbl_c4_curses
"""  

# COMMAND ----------

sql_safe(paso_query500)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cartdet_ope_ini_cd_pant

# COMMAND ----------

paso_query505 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_ope_ini_cd_pant
SELECT *
FROM
  tmp_RES_dat_ope_ini_cd_pant
"""  

# COMMAND ----------

sql_safe(paso_query505)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSERT INTO tbl_cd_cartdet_cli_ini_cd_pant

# COMMAND ----------

paso_query510 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_cli_ini_cd_pant
SELECT 
  fecha_cierre
  ,rut_cliente
  ,fec_ini_cd
  ,fecha_informada
FROM
  tmp_RES_dat_cli_ini_cd_pant
"""  

# COMMAND ----------

sql_safe(paso_query510)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")