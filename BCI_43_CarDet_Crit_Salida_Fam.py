# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_43_CarDet_Crit_Salida_Fam
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_43_CarDet_Crit_Salida_Fam.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011897448
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: Evaluacion de salida por familia de creditos
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
# MAGIC * Descripción: Se elimina reglas de Salidas (44 - LIR)     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Gonzalo Arias (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 23/07/2025 
# MAGIC * Descripción: se realiza cambios en la asignacion de la familia de la operacion (tmp_RES_cartdet_ope_det_cod_fam)
# MAGIC *       estos cambios solo afectan a las operaciones MACH (comienzan con M)     
# MAGIC *       El tioaux de MACH --> MCH001 [CON580] deuda activa asigna familia TCR_Act
# MAGIC *       El tioaux de MACH --> TDC501 [CON581] deuda activa asigna familia TCR_Act
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_prin
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_crit
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado
# MAGIC * {base_silver_x}.tbl_cd_tmp_ope_ctas_par
# MAGIC * {base_silver_x}.tbl_cierreriesgo_parametro
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_fam
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_matriz
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
p_cod_seg_ind='I'
p_crit_sal_fam = (66,60,62,65,61,63,64,67,41,40,2)
p_crit_sal_exp=26,27,30

print(f"p_crit_sal_exp: {p_crit_sal_exp}")
print(f"p_crit_sal_fam: {p_crit_sal_fam}")
print(f"p_cod_seg_ind: {p_cod_seg_ind}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Operaciones Deteriorados
# MAGIC --------------------------------------
# MAGIC - Extrae todos las operaciones deterioradas del proceso actual
# MAGIC - Una operacion puede estar deteriorada por varios motivos, aca solo interesa la operacion deteriorada, no el motivo. 

# COMMAND ----------

paso_query5 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_crit AS
SELECT  
     A.periodo_cierre                     AS    periodo_cierre           
    ,A.fecha_cierre                       AS    fecha_cierre             
    ,A.tipo_proceso                       AS    tipo_proceso             
    ,A.rut_cliente                        AS    rut_cliente              
    ,A.dv_rut_cliente                     AS    dv_rut_cliente           
    ,A.tipo_operacion                     AS    tipo_operacion           
    ,A.operacion                          AS    operacion                
    ,A.sistema                            AS    sistema                  
    ,A.segmento                           AS    segmento                 
    ,A.criterio_entrada                   AS    criterio_entrada         
    ,A.origen_deterioro                   AS    origen_deterioro         
    ,A.fecha_entrada                      AS    fecha_entrada            
    ,A.grupo                              AS    grupo                    
    ,A.periodo_evaluacion                 AS    periodo_evaluacion       
    ,A.criterio_entrada_cliente           AS    criterio_entrada_cliente 
FROM 
  {base_silver_x}.tbl_cd_cartdet_crit_ent_prin  A
WHERE
    A.fecha_cierre =   {fecha_x} 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre DESC, A.periodo_evaluacion ASC) =1
"""

# COMMAND ----------

sql_safe(paso_query5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrae Criterios Salida por Operacion
# MAGIC --------------------------------------
# MAGIC - Se extraen todas las evaluaciones 

# COMMAND ----------

paso_query10 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_sal_crit AS
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
    ,a.criterio_salida
    ,a.flag_resultado_regla
FROM
  {base_silver_x}.tbl_cd_cartdet_crit_sal_crit a
WHERE 
    a.fecha_cierre =   {fecha_x} 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY a.operacion, a.sistema, a.criterio_salida ORDER BY a.fecha_cierre DESC) =1
"""


# COMMAND ----------

sql_safe(paso_query10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clasifica operacion segun familia
# MAGIC --------------------------------------
# MAGIC - Genera el codigo de familia por operacion, para todas las operaciones deterioradas
# MAGIC - Se modifica asignacion de la familia de evaluacion de salida para operaciones MACH
# MAGIC - Los  tioaux [MCH001,TDC501], para deuda activa, de las operaciones de MACH, deben evaluarse con famila [TCR_Act]

# COMMAND ----------

paso_query15 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_ope_det_cod_fam AS
SELECT 
   a.periodo_cierre                 AS periodo_cierre           
  ,a.fecha_cierre                   AS fecha_cierre       
  ,a.tipo_proceso                   AS tipo_proceso       
  ,a.operacion                      AS operacion    
  ,a.sistema                        AS sistema  
  ,a.rut_cliente                    AS rut_cliente      
  ,a.dv_rut_cliente                 AS dv_rut_cliente         
  ,a.tipo_operacion                 AS tipo_operacion         
  ,a.segmento                       AS segmento
  ,a.grupo                          AS grupo
  ,a.periodo_evaluacion             AS periodo_evaluacion  
  ,IFNULL(b.tipo_credito,'0')       AS tipo_credito
  ,case 
     when c.operacion is not null 
     then 1 
     else 0 
   end                              AS ind_ope_ctas_par
  ,case 
      WHEN IFNULL(b.tipo_credito,'0') IN ('02','09')                    THEN 'Contingente' 
      WHEN TRIM(a.tipo_operacion) LIKE 'CAE%'                           THEN 'CAE'
      WHEN TRIM(a.operacion) LIKE 'V%'                                  THEN 'LC_Act'
      WHEN TRIM(a.operacion) LIKE 'A%'                                  THEN 'LC_Act'
      WHEN TRIM(a.operacion) LIKE 'C%'                                  THEN 'LC_Act'
      WHEN TRIM(a.tipo_operacion) = 'CCN008' and TRIM(a.sistema) = '20' THEN 'LC_Act'
      WHEN TRIM(a.operacion) LIKE 'E0%'                                 THEN 'TCR_Act'
      WHEN TRIM(a.operacion) LIKE 'E1%'                                 THEN 'TCR_Act'
      WHEN TRIM(a.operacion) LIKE 'W%' and TRIM(a.sistema) = '05'       THEN 'TCR_Act'
      WHEN TRIM(a.tipo_operacion) = 'MCH001' and TRIM(a.sistema) = '30' THEN 'TCR_Act'
	    WHEN TRIM(a.tipo_operacion) = 'TDC501' and TRIM(a.sistema) = '05' THEN 'TCR_Act'
      WHEN IFNULL(c.flag_ope_cta_par,0) = 1                                      THEN 'Pago_Parcial'
      ELSE 'Pago_Estructurado'
   end                                AS familia_ope
FROM
  tmp_EXT_tbl_cartdet_crit_ent_crit  a
LEFT JOIN
  {base_silver_x}.tbl_cd_d00_segmentado b
ON  a.operacion = b.operacion AND a.sistema = b.sistema
LEFT JOIN
  {base_silver_x}.tbl_cd_ope_condicion_salida_deterioro c
ON  a.operacion = c.operacion AND a.sistema = c.sistema 
"""

# COMMAND ----------

sql_safe(paso_query15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla con criterios de salida y familia de la operacion
# MAGIC --------------------------------------
# MAGIC - Genera tabla de criterios de salida mas el codigo de familia

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_crit_fam AS
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
    ,U.criterio_salida
    ,U.flag_resultado_regla
    ,A.familia_ope
    ,A.grupo                     
    ,A.periodo_evaluacion          
FROM 
     tmp_EXT_tbl_cartdet_crit_sal_crit U,
     tmp_RES_cartdet_ope_det_cod_fam A
WHERE
    U.operacion=A.operacion AND U.sistema = A.sistema
"""

# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla operaciones que salen por criterios de excepciones (directos)
# MAGIC --------------------------------------
# MAGIC - operaciones que salen por saldo ifrs cero
# MAGIC - operaciones que salen por no estar informadas en mes actual y estaban deterioradas mes anterior

# COMMAND ----------

paso_query25 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_excepcion AS
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
    ,A.criterio_salida
    ,A.flag_resultado_regla
    ,A.familia_ope
    ,A.grupo                     
    ,A.periodo_evaluacion   
    ,1 AS prioridad
FROM 
   tmp_RES_cartdet_crit_sal_crit_fam A
WHERE
    A.criterio_salida in {p_crit_sal_exp}
AND A.flag_resultado_regla =1    
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.criterio_salida ASC) =1
"""

# COMMAND ----------

sql_safe(paso_query25)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla operaciones que salen por Factoring
# MAGIC --------------------------------------
# MAGIC - Si la operacion esta deteriorada por Factoring, estas operaciones salen solo con la condicion de no ser informadas por Factoring
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene clientes deteriorados por Factoring

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_cli_det_fact AS
SELECT
     A.periodo_cierre
    ,A.fecha_cierre
    ,A.tipo_proceso
    ,A.rut_cliente
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_crit A
WHERE
    A.criterio_entrada_cliente = 13
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente  ORDER BY A.fecha_cierre ASC) =1    
"""

# COMMAND ----------

sql_safe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene clientes con condicion de salida para Factoring

# COMMAND ----------

paso_query32 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_cli_sal_fact AS
SELECT
     A.periodo_cierre
    ,A.fecha_cierre
    ,A.tipo_proceso
    ,A.rut_cliente
FROM 
    tmp_EXT_tbl_cartdet_crit_sal_crit A
WHERE
    A.criterio_salida = 41
AND A.flag_resultado_regla = 1
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente  ORDER BY A.fecha_cierre ASC) =1    
"""

# COMMAND ----------

sql_safe(paso_query32)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene clientes deteriorados por Factoring y que tiene condicion de salida Factoring

# COMMAND ----------

paso_query34 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_cli_nodet_fact AS
SELECT
     A.periodo_cierre
    ,A.fecha_cierre
    ,A.tipo_proceso
    ,A.rut_cliente
FROM
    tmp_RES_cartdet_cli_det_fact a,
    tmp_RES_cartdet_cli_sal_fact b
WHERE
    a.rut_cliente = b.rut_cliente
"""    

# COMMAND ----------

sql_safe(paso_query34)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Genera tabla con operaciones salida de deterioro Factoring

# COMMAND ----------

paso_query36 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_factoring AS
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
    ,A.criterio_salida
    ,A.flag_resultado_regla
    ,A.familia_ope
    ,A.grupo                     
    ,A.periodo_evaluacion     
    ,2 AS prioridad
FROM 
   tmp_RES_cartdet_crit_sal_crit_fam A
JOIN
   tmp_RES_cartdet_cli_nodet_fact B
ON A.rut_cliente = B.rut_cliente   
WHERE     
     A.criterio_salida = 41     
AND  A.flag_resultado_regla = 1     
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.criterio_salida ASC) =1
""" 

# COMMAND ----------

sql_safe(paso_query36)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla operaciones que salen por SSFF
# MAGIC --------------------------------------
# MAGIC - Si la operacion esta deteriorada por SSFF, estas operaciones salen solo con la condicion de no ser informadas por SSFF
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene clientes deteriorados por SSFF

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_cli_det_ssff AS
SELECT
     A.periodo_cierre
    ,A.fecha_cierre
    ,A.tipo_proceso
    ,A.rut_cliente
FROM 
    tmp_EXT_tbl_cartdet_crit_ent_crit A
WHERE
    A.criterio_entrada_cliente = 12
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente  ORDER BY A.fecha_cierre ASC) =1    
"""

# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene clientes con condicion de salida para SSFF

# COMMAND ----------

paso_query42 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_cli_sal_ssff AS
SELECT
     A.periodo_cierre
    ,A.fecha_cierre
    ,A.tipo_proceso
    ,A.rut_cliente
FROM 
    tmp_EXT_tbl_cartdet_crit_sal_crit A
WHERE
    A.criterio_salida = 40
AND A.flag_resultado_regla = 1
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente  ORDER BY A.fecha_cierre ASC) =1    
"""

# COMMAND ----------

sql_safe(paso_query42)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene clientes deteriorados por SSFF y que tiene condicion de salida SSFF

# COMMAND ----------

paso_query44 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_cli_nodet_ssff AS
SELECT
     A.periodo_cierre
    ,A.fecha_cierre
    ,A.tipo_proceso
    ,A.rut_cliente
FROM
    tmp_RES_cartdet_cli_det_ssff a,
    tmp_RES_cartdet_cli_sal_ssff b
WHERE
    a.rut_cliente = b.rut_cliente
"""    

# COMMAND ----------

sql_safe(paso_query44)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Genera tabla con operaciones salida de deterioro SSFF
# MAGIC ------------------------------------------------------------------------
# MAGIC * clientes que su deterioro principal es SSFF (12)
# MAGIC * clientes que tiene condicion de salida SSFF, es decir, en periodo actual no se informan deteriorados (40)
# MAGIC * operaciones que no hayan salida por excepcion de saldo o no informada en periodo actual

# COMMAND ----------

paso_query46 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_ssff AS
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
    ,A.criterio_salida
    ,A.flag_resultado_regla
    ,A.familia_ope
    ,A.grupo                     
    ,A.periodo_evaluacion
    ,3 AS prioridad 
FROM 
   tmp_RES_cartdet_crit_sal_crit_fam A
JOIN   
   tmp_RES_cartdet_cli_nodet_ssff B
ON A.rut_cliente = B.rut_cliente   
WHERE
     A.criterio_salida = 40     
AND  A.flag_resultado_regla = 1
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.criterio_salida ASC) =1
"""
      

# COMMAND ----------

sql_safe(paso_query46)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla operaciones individuales que salen por que cliente no tiene clasificacion de deterioro
# MAGIC --------------------------------------
# MAGIC - Si la operacion es individual, deteriorada del mes anterior, y este mes no tiene clasificacion de deterioro el cliente, entonces, sale de deterioro.
# MAGIC

# COMMAND ----------

paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_ind AS
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
    ,A.criterio_salida
    ,A.flag_resultado_regla
    ,A.familia_ope
    ,A.grupo                     
    ,A.periodo_evaluacion     
    ,4 AS prioridad 
FROM 
   tmp_RES_cartdet_crit_sal_crit_fam A
WHERE
     A.segmento = '{p_cod_seg_ind}'    
AND  A.criterio_salida = 2
AND  A.flag_resultado_regla = 1
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.criterio_salida ASC) =1
"""
      

# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla con operaciones ya evaluadas en condicion de salida
# MAGIC --------------------------------------
# MAGIC - Operaciones con salida por excepcion (saldo total ifrs cero)
# MAGIC - Operaciones con salida por excepcion (no informadas en periodo actual)
# MAGIC - Operaciones con salida por SSFF (deterioradas SSFF y no informada en periodo actual con deterioro)
# MAGIC - Operaciones con salida por FACT (deterioradas FACT y no informada en periodo actual con deterioro)
# MAGIC - Operaciones con salida indidivual (cliente sin clasificacion de deterioro periodo actual)

# COMMAND ----------

paso_query52 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_NO_familia AS
SELECT * FROM tmp_RES_cartdet_crit_sal_ope_excepcion
UNION
SELECT * FROM tmp_RES_cartdet_crit_sal_ope_factoring
UNION
SELECT * FROM tmp_RES_cartdet_crit_sal_ope_ssff
UNION
SELECT * FROM tmp_RES_cartdet_crit_sal_ope_ind
"""


# COMMAND ----------

sql_safe(paso_query52)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla con criterio de salida versus matriz de salida por familia
# MAGIC --------------------------------------
# MAGIC - Genera tabla de criterios de salida versus matriz de salida por familia
# MAGIC - Genera respuesta entre los criterios que cumple la operacion y lo que debe cumplir la operacion
# MAGIC - Si operacion cumple todas las condiciones de salida pero en periodo anterior era un Cliente LIR entonces NO sale

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene parametros por codigo de familia. 
# MAGIC  - Esta matriz indica los requisitos, por familia, que deben cumplir las operaciones para salir de deterioro

# COMMAND ----------

paso_query55 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tbl_EXT_cartdet_matriz_salida as
SELECT
  tipo_parametro   AS familia,
  codigo           AS evaluacion,
  parametro1       AS flag_esperado,
  parametro2       AS criterio,
CASE
  WHEN upper(trim(tipo_parametro)) IN ('CONTINGENTE', 'CAE', 'LC_ACT', 'PAGO_ESTRUCTURADO', 'PAGO_PARCIAL', 'TCR_ACT') 
       AND (parametro1 = 0 OR parametro2 IN (27, 30)) THEN 'No'
  ELSE 'Si'
END AS requerido,
  descripcion      AS descripcion
FROM
  {base_silver_x}.tbl_cierreriesgo_parametro
WHERE 
     upper(proceso) = 'CARTERA_DETERIORADA'
AND  upper(vigencia) = 'V'      
AND upper(trim(tipo_parametro)) IN ('CONTINGENTE', 'CAE', 'LC_ACT', 'PAGO_ESTRUCTURADO', 'PAGO_PARCIAL', 'TCR_ACT') 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY proceso, tipo_parametro, codigo ORDER BY fecha_informada DESC) =1
"""




# COMMAND ----------

sql_safe(paso_query55)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cruza matriz familia con operaciones deterioradas y sus evaluaciones
# MAGIC  - Marca las condiciones que cumplen y no cumple para la evaluacion salida

# COMMAND ----------

paso_query60 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_crit_fam_matriz AS
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
    ,U.criterio_salida
    ,U.flag_resultado_regla
    ,U.familia_ope
    ,U.grupo
    ,U.periodo_evaluacion
    ,A.evaluacion
    ,A.flag_esperado
    ,A.requerido
    ,CASE WHEN A.requerido='Si' THEN
                  CASE 
                     WHEN U.flag_resultado_regla = A.flag_esperado THEN 'OK' 
                     ELSE 'NOK' 
                  END
          ELSE 
                  CASE WHEN U.flag_resultado_regla = A.flag_esperado THEN 'OK' ELSE 'OK' END
      END AS Respuesta    
    ,CASE WHEN A.requerido='Si' THEN
                  CASE 
                     WHEN U.flag_resultado_regla = A.flag_esperado THEN ' ' 
                     ELSE 'Error: No cumple criterio obligatorio' 
                  END
          ELSE 
                  CASE WHEN U.flag_resultado_regla = A.flag_esperado THEN ' ' ELSE ' ' END
      END AS Desc_Respuesta
FROM 
     tmp_RES_cartdet_crit_sal_crit_fam U
LEFT JOIN     
     tbl_EXT_cartdet_matriz_salida  A
ON   U.familia_ope = A.familia and U.criterio_salida=A.criterio
LEFT JOIN
     tmp_RES_cartdet_crit_sal_ope_NO_familia B
ON U.operacion=B.operacion AND U.sistema = B.sistema
LEFT JOIN
      tmp_EXT_tbl_cartdet_crit_ent_crit E
ON U.operacion=E.operacion AND U.sistema = E.sistema 
WHERE
     U.criterio_salida in {p_crit_sal_fam}
AND  B.operacion is null /*Operacion no este en salida de operaciones NO FAMILIA*/
"""

# COMMAND ----------

sql_safe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla por Operacion con indicadores para evalucion criterio salida familia
# MAGIC --------------------------------------
# MAGIC - Genera totales por operaciones de los criterios que totales, que cumplen y que no cumplen

# COMMAND ----------

paso_query62 =  f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_resp_fam as
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
,A.familia_ope            AS familia_ope
,A.grupo                  AS grupo
,A.periodo_evaluacion     AS periodo_evaluacion
,COUNT(operacion)         AS TOT_RESP
,SUM(CASE WHEN Respuesta='OK' THEN 1 ELSE 0 END) AS TOT_RESP_OK
,SUM(CASE WHEN Respuesta='NOK' THEN 1 ELSE 0 END) AS TOT_RESP_NOK

FROM 
    tmp_RES_cartdet_crit_sal_crit_fam_matriz  A
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12    
"""


# COMMAND ----------

sql_safe(paso_query62)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genera tabla operaciones que salen por criterios de famila 
# MAGIC --------------------------------------
# MAGIC - se genera indicador si cumple o no cumple criterio salida por operacion
# MAGIC - se genera codigo de salida segun familia

# COMMAND ----------

paso_query64 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cartdet_crit_sal_ope_familia AS
SELECT
  A.periodo_cierre                          AS periodo_cierre,
  A.fecha_cierre                            AS fecha_cierre,
  A.tipo_proceso                            AS tipo_proceso,
  A.rut_cliente                             AS rut_cliente,
  A.dv_rut_cliente                          AS dv_rut_cliente,
  A.tipo_operacion                          AS tipo_operacion,
  A.operacion                               AS operacion,
  A.sistema                                 AS sistema,
  A.segmento                                AS segmento,
  CASE 
       WHEN trim(A.familia_ope) = 'Contingente' THEN  34
       WHEN trim(A.familia_ope) = 'CAE' THEN  35
       WHEN trim(A.familia_ope) = 'TCR_Act' THEN  36
       WHEN trim(A.familia_ope) = 'LC_Act' THEN  37
       WHEN trim(A.familia_ope) = 'Pago_Parcial' THEN  38
       WHEN trim(A.familia_ope) = 'Pago_Estructurado' THEN  39
       ELSE '99'
  END                                        AS criterio_salida,
  CASE 
      WHEN A.TOT_RESP_NOK=0 
      THEN 1 
      ELSE 0 
  END                                        AS flag_resultado_regla,
  A.familia_ope                              AS familia_ope,
  A.grupo                                    AS grupo,
  A.periodo_evaluacion                       AS periodo_evaluacion
  ,5 AS prioridad 
FROM 
  tmp_RES_cartdet_crit_sal_ope_resp_fam  A
"""

# COMMAND ----------

sql_safe(paso_query64)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salida Temporal a Nivel de Operacion Evaludado (tmp_tbl_cartdet_crit_sal_crit_fam)
# MAGIC ------------------
# MAGIC

# COMMAND ----------

paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cartdet_crit_sal_crit_fam AS
SELECT * FROM   tmp_RES_cartdet_crit_sal_ope_NO_familia   
UNION 
SELECT * FROM   tmp_RES_cartdet_crit_sal_ope_familia  
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
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query300 = f"""TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_fam """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_fam
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
    IFNULL(criterio_salida,0),
    IFNULL(flag_resultado_regla,0)
FROM
    tmp_tbl_cartdet_crit_sal_crit_fam   
QUALIFY  ROW_NUMBER() OVER(PARTITION BY operacion, sistema ORDER BY prioridad ASC) =1        
"""  


# COMMAND ----------

sql_safe(paso_query310)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Tabla Informativa
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reproceso (Elimina registros en caso de reprocesos)

# COMMAND ----------

paso_query400 = f"""TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_matriz """

# COMMAND ----------

sql_safe(paso_query400)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query410 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_sal_crit_matriz
SELECT 
     periodo_cierre	           
    ,fecha_cierre	           
    ,tipo_proceso	           
    ,rut_cliente	           
    ,dv_rut_cliente	           
    ,tipo_operacion	           
    ,operacion	               
    ,sistema	               
    ,segmento	               
    ,criterio_salida	       
    ,flag_resultado_regla	   
    ,familia_ope	           
    ,evaluacion	               
    ,flag_esperado	           
    ,requerido	               
    ,Respuesta	               
    ,Desc_Respuesta	           
FROM
    tmp_RES_cartdet_crit_sal_crit_fam_matriz  
"""  

# COMMAND ----------

sql_safe(paso_query410)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estadisticas

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC fecha_cierre, 
# MAGIC criterio_salida, 
# MAGIC count(1) 
# MAGIC from ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_crit_fam  
# MAGIC group by 1,2 order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")