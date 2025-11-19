# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_20_CarDet_Evalua_Datos_Ent
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_20_CarDet_Evalua_Datos_Ent.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011900936
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion criterios de entrada a deterioro 
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gagriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se cambia el Delete por Truncate al momento de eliminar los datos de la tabla tbl_cd_cartdet_crit_ent_ope_eval   
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

spark.conf.set("bci.Fecha", fecha_x)
spark.conf.set("bci.dbnamesilver", base_silver_x)

print(f"Fecha de Proceso actual [fecha_x]: {fecha_x}")
print(f"Nombre BD Silver [base_silver_x]: {base_silver_x}")


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

# DBTITLE 1,Codigos evaluacion
#parametria interna proceso
p_cod_seg_ind='I'
p_cod_evaluacion='E01','E02','E03','E04','E05','E06','E07','E08','E09','E10','E11','E12'
p_dias_mora_ren=60
p_cod_seg_gru='G'
p_ind_cartdet='D'
p_num_cuotas_orig=2
p_tipope='CAE'
p_macro_sist='BCI'
p_tip_prod='ACT'
p_tip_cart='CON','COM'
p_cod_ren='S','C'
p_ccontable='14'
p_dias_mora_ent=90
p_sal_cart_venc=0
p_tipope_n='CCN008','TDC001','TDC002'
p_sal_mor=0
p_cod_sis='05'
p_tio_esp='HIP','CAE'


print(f"p_cod_seg_ind: {p_cod_seg_ind}")
print(f"p_cod_evaluacion: {p_cod_evaluacion}")
print(f"p_cod_seg_gru: {p_cod_seg_gru}")
print(f"p_dias_mora_ren: {p_dias_mora_ren}")
print(f"p_ind_cartdet: {p_ind_cartdet}")
print(f"p_num_cuotas_orig: {p_num_cuotas_orig}")
print(f"p_tipope: {p_tipope}")
print(f"p_macro_sist: {p_macro_sist}")
print(f"p_tip_prod: {p_tip_prod}")
print(f"p_tip_cart: {p_tip_cart}")
print(f"p_cod_ren: {p_cod_ren}")
print(f"p_ccontable: {p_ccontable}")
print(f"p_dias_mora_ent: {p_dias_mora_ent}")
print(f"p_sal_cart_venc: {p_sal_cart_venc}")
print(f"p_tipope_n: {p_tipope_n}")
print(f"p_sal_mor: {p_sal_mor}")
print(f"p_cod_sis: {p_cod_sis}")
print(f"p_tio_esp: {p_tio_esp}")


# COMMAND ----------

# DBTITLE 1,Obtiene parametros tabla
#Obtiene parametros de tabla de parametros
resultado = obtener_parametros_de_tbl(base_silver_x)
tp_param_cal = resultado[0]
tp_fec_cv = resultado[1]
tp_ccontable = resultado[12]
tp_sal_cart_venc = resultado[13]
tp_dias_mora_ent = resultado[14]
tp_cod_ren = resultado[15]
tp_tio_esp = resultado[16]

print('tp_param_cal :',tp_param_cal )
print('tp_fec_cv :',tp_fec_cv )
print('tp_ccontable :', tp_ccontable)
print('tp_sal_cart_venc :', tp_sal_cart_venc)
print('tp_dias_mora_ent :', tp_dias_mora_ent)
print('tp_cod_ren :', tp_cod_ren)
print('tp_tio_esp :', tp_tio_esp)


# COMMAND ----------

# DBTITLE 1,Fecha Limite Renegociados
p_fecha_limite_xmeses = calcula_fecha_otorgamiento(fecha_x)
print(f"p_fecha_limite_xmeses: {p_fecha_limite_xmeses}")

# COMMAND ----------

# DBTITLE 1,Fecha Tope Clientes LIR
#Para el cálculo de clientes LIR se debe tomar los ultimos 12 meses a partir de la fecha de proceso
p_fec_12_meses_atras=fecha_x_meses_atras(fecha_x,12)
print(f"p_fec_12_meses_atras: {p_fec_12_meses_atras}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### TABLA de Evaluacion: 
# MAGIC ---
# MAGIC * El objetivo de las tablas de evaluacion es validar todas las condiciones de entrada para todas las operaciones del d00
# MAGIC * de tal manera que cuando se necesite revisar algun cliente se tengan todos sus datos evaludados
# MAGIC * los campos:
# MAGIC -  **nombre_campo**: deben ir todos los campos utilizados en la evaluacion (flag_resultado_regla)
# MAGIC -  **valor_campo**: corresponde los valores de la parte izquierda de la evaluacion (flag_resultado_regla)
# MAGIC -  **condicion_regla**: corresponde a la parte del centro de la ecuacion, la logica que se aplica, en palabras  (flag_resultado_regla)
# MAGIC -  **valor_regla**: corresponde a los valores de la parte derecha de la evaluacion (flag_resultado_regla)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: cliente individual deteriorado
# MAGIC ---
# MAGIC * Si cliente es individual y tiene calificacion de deterioro entonces flag=1
# MAGIC

# COMMAND ----------

paso_query50 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_1 as
SELECT
 A.periodo_cierre                              AS periodo_cierre,
 A.fecha_cierre                                AS fecha_cierre,
 A.tipo_proceso                                AS tipo_proceso,
 A.segmento                                    AS segmento,
 A.operacion                                   AS operacion,
 A.tipo_operacion                              AS tipo_operacion,
 A.sistema                                     AS sistema,
 A.rut_cliente                                 AS rut_cliente,
 A.dv_rut_cliente                              AS dv_rut_cliente ,
'cli_segmento_cliente-cli_calificacion_bci'    AS nombre_campo,
 concat('[',A.cli_segmento_cliente,'-',A.cli_calificacion_bci,']')        AS valor_campo,
 'Cliente con segmento individual y calificacion de deterioro'             AS condicion_regla,     
 "[{p_cod_seg_ind}-{tp_param_cal}]"                           AS valor_regla,
 CASE 
     WHEN IFNULL(trim(A.cli_segmento_cliente),'SD') = '{p_cod_seg_ind}' AND trim(IFNULL(A.cli_calificacion_bci,'0'))  IN {tp_param_cal} 
     THEN 1 
     ELSE 0 
 END                                          AS flag_resultado_regla,
 'E01'                                        AS cod_evaluacion
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_deterioro  A
"""


# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: operaciones con condiciones de morosidad
# MAGIC ---
# MAGIC * Si operacion tiene dias de mora mayor o igual a 90 {p_dias_mora_ent}, entonces  flag =1 
# MAGIC * Si operacion tiene saldo en cartera vencida mayor o igual a 0 {p_sal_cart_venc}, entonces flag =1 
# MAGIC * Si operacion tiene cuenta contable que comienza con 14 {p_ccontable}, entonces flag =1 
# MAGIC * Si operacion tiene fecha de cartera vencida mayor o igual a fecha limite {p_fec_cv} y es mayor a la fecha de cv del periodo anterior, entonces flag =1 

# COMMAND ----------

paso_query80 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_4 as
SELECT    
    A.periodo_cierre                                   AS periodo_cierre,
    A.fecha_cierre                                     AS fecha_cierre,
    A.tipo_proceso                                     AS tipo_proceso,
    A.segmento                                         AS segmento,
    A.operacion                                        AS operacion,
    A.tipo_operacion                                   AS tipo_operacion,
    A.sistema                                          AS sistema,
    A.rut_cliente                                      AS rut_cliente,
    A.dv_rut_cliente                                   AS dv_rut_cliente ,
    '[dias_mora-saldo_cartera_venc-cuenta_contable-fecha_cart_venc-ope_fecha_cart_venc_pant]'                  AS nombre_campo,
    concat('[',cast(A.dias_mora as string),'-',cast(A.saldo_cartera_venc as string),'-',cast(A.cuenta_contable as string),'-',cast(A.fecha_cart_venc as string),'-',cast(A.ope_fecha_cart_venc_pant as string),']')     AS valor_campo,
    'Operacion con condicion de morosidad. dias_mora>={tp_dias_mora_ent} o saldo_cartera_venc>{tp_sal_cart_venc} o cuenta_contable like {tp_ccontable} o (fecha_cart_venc>{tp_fec_cv} y fecha_cart_venc>ope_fecha_cart_venc_pant'   AS condicion_regla,     
    concat('[',{tp_dias_mora_ent},'-',{tp_sal_cart_venc},'-','{tp_ccontable}','-',{tp_fec_cv},']') AS valor_regla,  
    CASE
      WHEN A.dias_mora >= {tp_dias_mora_ent} THEN 1 	
      WHEN A.saldo_cartera_venc  > {tp_sal_cart_venc} THEN 1 
      WHEN A.cuenta_contable like '{tp_ccontable}' THEN 1 
      WHEN (A.fecha_cart_venc >= {tp_fec_cv} AND A.fecha_cart_venc > A.ope_fecha_cart_venc_pant) THEN 1		
      ELSE 0 
    END                                                 AS flag_resultado_regla,
    'E04'                                               AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_deterioro A
"""

# COMMAND ----------

sql_safe(paso_query80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: operaciones renegociadas
# MAGIC ---
# MAGIC - Si operacion tiene codigo de renegociado S o C {p_cod_ren} y
# MAGIC - Si operacion tiene fecha de otorgamiento mayor o igual a un limite (fecha otorgamiento - 6 meses) y
# MAGIC - Si operacion tiene fecha de otorgamiento menor o igual a la fecha de proceso y
# MAGIC - Si operacion renegociada es nueva (no existe en periodo anterior). y 
# MAGIC - Si cliente tiene mora mayor a 60 (p_dias_mora_ren), entonces Flag=1
# MAGIC ---
# MAGIC * nota: max_dia_mora (cliente) =  max_dia_mora periodo anterior (considerando todas sus operaciones del periodo anterior) + max_dia_curse_ren periodo actual (considera todas las operaciones renegociadas del periodo actual).

# COMMAND ----------

paso_query90 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_5 as
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
    '[cod_renegociado-fecha_otorgamiento-max_dia_mora_ren-flag_existe_operacion_pant-tipo_cartera-tipo_producto]' AS nombre_campo,
    concat('[',cast(A.cod_renegociado as string),'-',cast(A.fecha_otorgamiento as string),'-',cast(A.max_dia_mora_ren as string),'-',cast(A.flag_existe_operacion_pant as string),'-',cast(A.tipo_cartera as string),'-',cast(A.tipo_producto as string),']') AS valor_campo,
    "Operacion renegociada nueva,activa,cartera consumo o comercial. cod_renegociado in {p_cod_ren} y fecha_otorgamiento <= {fecha_x} y fecha_otorgamiento  > {p_fecha_limite_xmeses} y max_dia_mora_ren>= {p_dias_mora_ren}" AS condicion_regla,     
    concat('[',"{p_cod_ren}-{fecha_x}-{p_fecha_limite_xmeses}-{p_dias_mora_ren}-{p_tip_cart}-{p_tip_prod}",']') AS valor_regla,   
    CASE 
         WHEN 
              A.cod_renegociado in {p_cod_ren} AND 
              A.fecha_otorgamiento <= {fecha_x} AND 
              A.fecha_otorgamiento > {p_fecha_limite_xmeses} AND
              A.max_dia_mora_ren >= {p_dias_mora_ren} AND
              A.flag_existe_operacion_pant = 0 AND
              A.tipo_cartera IN {p_tip_cart} AND
              A.tipo_producto = '{p_tip_prod}'
         THEN 1 
         ELSE 0 
    END                                                          AS flag_resultado_regla,
   'E05'                                                         AS cod_evaluacion  
FROM
    {base_silver_x}.tbl_cd_ope_condicion_deterioro A
"""

# COMMAND ----------

sql_safe(paso_query90)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: curse bajo mora
# MAGIC ---
# MAGIC * operaciones que exista en tabla de curse bajo mora
# MAGIC * 'abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_RiesgoCred_RiesgoCredPer_db/reestrucforzosa_cur/'
# MAGIC

# COMMAND ----------

paso_query100 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_6 as
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
    'operacion'                                                 AS nombre_campo,
    concat('[',cast(A.operacion as string),'-',cast(A.sistema as string),']')     AS valor_campo,
    "Operacion informada en location reestructuracion forsoza. flag_existe_operacion_rfz=1"                    AS condicion_regla,     
    CASE WHEN A.flag_existe_operacion_rfz = 1 THEN 'EXISTE' ELSE 'NO_EXISTE' END  AS valor_regla,           
    CASE 
       WHEN A.flag_existe_operacion_rfz = 1 
       THEN 1 
       ELSE 0 
    END AS flag_resultado_regla,
    'E06'                                                        AS cod_evaluacion  
FROM 
   {base_silver_x}.tbl_cd_ope_condicion_deterioro A
"""


# COMMAND ----------

sql_safe(paso_query100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: clientes LIR
# MAGIC ---
# MAGIC * clientes acogidos a la Ley de Insolvencia y Reemprendimiento
# MAGIC * clientes informados en location de LIR
# MAGIC * Se consideran aquellos clientes cuya fecha de mas reciente en ser informado como lir mayor a 12 meses
# MAGIC * 'abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_Clientes_Personas_db/fn/''
# MAGIC

# COMMAND ----------

paso_query110 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_7 as
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
    'rut_cliente - cli_fecha_informada_lir'                             AS nombre_campo,
    concat('[', cast(A.rut_cliente as string),'-', cast(A.cli_fecha_informada_lir as string),']') AS valor_campo,
    "Cliente informado en location lir con fecha informada mayor a {p_fec_12_meses_atras}"              AS condicion_regla,     
    Case when A.flag_existe_cliente_lir = 1 THEN CONCAT('EXISTE',' and cli_fecha_informada_lir >',{p_fec_12_meses_atras}) ELSE 'NO_EXISTE'  END AS valor_regla,
    CASE 
       WHEN A.flag_existe_cliente_lir = 1 and A.cli_fecha_informada_lir > {p_fec_12_meses_atras}
       THEN 1 
       ELSE 0 
    END AS flag_resultado_regla,
    'E07'                                                        AS cod_evaluacion  
FROM 
   {base_silver_x}.tbl_cd_ope_condicion_deterioro A
"""


# COMMAND ----------

sql_safe(paso_query110)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Filiales SSFF
# MAGIC ---
# MAGIC - Operaciones deterioradas SSFF
# MAGIC - No deteriora operaciones HIP ni CAE

# COMMAND ----------

paso_query120 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_8 as
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
    "rut_cliente - tipo_operacion"                              AS nombre_campo,
    CONCAT(CAST(A.rut_cliente AS STRING), '-', SUBSTRING(A.tipo_operacion, 1,3))    AS valor_campo,
    "Cliente deteriorado en SSFF y operacion tipo_operacion NO es {p_tio_esp}"      AS condicion_regla,     
    Case when A.flag_existe_cliente_ssff = 1 THEN CONCAT('EXISTE','-',"{p_tio_esp}") ELSE 'NO_EXISTE'  END AS valor_regla,
    CASE 
       WHEN A.flag_existe_cliente_ssff = 1 AND SUBSTRING(A.tipo_operacion,1,3) NOT IN {p_tio_esp}
       THEN 1 
       ELSE 0 
     END                                                         AS flag_resultado_regla,
    'E08'                                                        AS cod_evaluacion  
FROM 
    {base_silver_x}.tbl_cd_ope_condicion_deterioro A
"""


# COMMAND ----------

sql_safe(paso_query120)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion: Filiales Factoring
# MAGIC ---
# MAGIC - operaciones deterioradas FACTORING
# MAGIC - No deteriora operaciones HIP ni CAE
# MAGIC

# COMMAND ----------

paso_query130 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_D00_OPE_CAMPO_EVAL_9 as
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
    "rut_cliente - tipo_operacion"                              AS nombre_campo,
    CONCAT(CAST(A.rut_cliente AS STRING), '-', SUBSTRING(A.tipo_operacion, 1,3))    AS valor_campo,
    "Cliente deteriorado en factoring y operacion tipo_operacion NO es {p_tio_esp}"      AS condicion_regla,     
    Case when A.flag_existe_cliente_fact = 1 THEN CONCAT('EXISTE','-',"{p_tio_esp}") ELSE 'NO_EXISTE'  END AS valor_regla,
    CASE 
       WHEN A.flag_existe_cliente_fact = 1 AND SUBSTRING(A.tipo_operacion,1,3) NOT IN {p_tio_esp}
       THEN 1 
       ELSE 0 
     END                                                         AS flag_resultado_regla,
    'E09'                                                        AS cod_evaluacion
FROM 
   {base_silver_x}.tbl_cd_ope_condicion_deterioro A
"""


# COMMAND ----------

sql_safe(paso_query130)

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
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_4
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_5
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_6
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_7
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_8
UNION
SELECT * FROM   tmp_RES_D00_OPE_CAMPO_EVAL_9
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
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos) No es una tabla con historia

# COMMAND ----------

paso_query300 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_crit_ent_ope_eval """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------


paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_ent_ope_eval
SELECT 
    IFNULL(periodo_cierre,190001),
    IFNULL(fecha_cierre,19000101),
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
# MAGIC   ELSE 'NO_IDENTIFICADO'    
# MAGIC END                    AS des_cod_evaluacion,
# MAGIC COUNT(1) AS CANT_REG
# MAGIC FROM ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_ope_eval
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")