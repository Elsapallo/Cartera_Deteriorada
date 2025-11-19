# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_24_CarDet_Crit_Entrada_Pri
# MAGIC *********************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_24_CarDet_Crit_Entrada_Pri.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011898507
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 12/08/2022
# MAGIC * Descripcion: selecciona el criterio de entrada principal segun jerarquia
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 25/04/2025 
# MAGIC * Descripción: Se modifica condicion para dejar la minima fecha de entrada de la operacion a cartera deteriorada.     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gonzalo Arias (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 30/06/2025 
# MAGIC * Descripción: Se modifica flujo para asignar deterioro principal. Se utiliza
# MAGIC *              logica onpremise. Primero se deteriora considerando solo criterios Bci
# MAGIC *              segundo, se deteriora con criterios de filiales.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 22/07/2025 
# MAGIC * Descripción: Se modifica condicion para irradiar las operaciones Hipotecarias y Cae entre si.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_crit
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado 
# MAGIC * {base_silver_x}.tbl_cd_d00_segmentado_pant
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * {base_silver_x}.tbl_cd_cartdet_crit_ent_prin
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
# MAGIC ## Parametría

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

# Calcula periodo en base a la fecha
periodo_x=fecha_x[:6]

print(f"Periodo: [periodo_x] {periodo_x}")

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
p_crit_det = 1,7,8,9,10,11,12,13
p_periodo_evaluacion='p_anterior'
p_tio_esp='HIP','CAE'
p_ind_cartdet='D'

print(f"p_crit_det: {p_crit_det}")
print(f"p_periodo_evaluacion: {p_periodo_evaluacion}")
print(f"p_tio_esp: {p_tio_esp}")
print(f"p_ind_cartdet: {p_ind_cartdet}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### [BCI] CALCULO DETERIORO BCI
# MAGIC --------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extrae criterios de deterioro de las operaciones BCI
# MAGIC --------------------------------------
# MAGIC - obtiene todos los deterioros calculados en proceso previos
# MAGIC - genera jerarquia (esta jerarquia debe estar en la tabla de parametros)
# MAGIC - son 3 jerarquias. criterio deterioro, grupo, y periodo evaluacion
# MAGIC - Esta jerarquia es para determinar el calculo del deterioro (no es lo mismo que el criterio que se informa x cliente)

# COMMAND ----------

paso_query20 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_crit_BCI AS
SELECT
  A.periodo_cierre       AS periodo_cierre,
  A.fecha_cierre         AS fecha_cierre,
  A.tipo_proceso         AS tipo_proceso,
  A.rut_cliente          AS rut_cliente,
  A.dv_rut_cliente       AS dv_rut_cliente,
  A.tipo_operacion       AS tipo_operacion,
  A.operacion            AS operacion,
  A.sistema              AS sistema,
  A.segmento             AS segmento,
  A.criterio_entrada     AS criterio_entrada,
  A.origen_deterioro     AS origen_deterioro,
  MIN(A.fecha_entrada) OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre ASC) AS fecha_entrada,
  A.grupo                AS grupo,
  A.periodo_evaluacion   AS periodo_evaluacion,
  CASE 
      WHEN A.criterio_entrada = 1 THEN 1
      WHEN A.criterio_entrada = 11 THEN 2
      WHEN A.criterio_entrada = 8 THEN 3
      WHEN A.criterio_entrada = 7 THEN 4
      WHEN A.criterio_entrada = 9 THEN 5
      WHEN A.criterio_entrada = 10 THEN 6
      WHEN A.criterio_entrada = 12 THEN 7
      WHEN A.criterio_entrada = 13 THEN 8      
      ELSE 99
  END                    AS jer_criterio_entrada,  
  CASE 
      WHEN trim(A.grupo) = 'BCI_Individual' THEN 1
      WHEN trim(A.grupo) = 'BCI_Grupal' THEN 2
      WHEN trim(A.grupo) = 'Factoring' THEN 3
      WHEN trim(A.grupo) = 'SSFF' THEN 4
      ELSE 99
  END                    AS jer_grupo,
  CASE 
      WHEN trim(A.periodo_evaluacion) = 'p_actual' THEN 1
      WHEN trim(A.periodo_evaluacion) = 'p_anterior' THEN 2
      ELSE 99
  END                    AS jer_periodo_evaluacion  
FROM
  {base_silver_x}.tbl_cd_cartdet_crit_ent_crit A
WHERE
    fecha_cierre = {fecha_x}  
AND periodo_cierre = {periodo_x}  
AND trim(A.grupo) IN ('BCI_Individual','BCI_Grupal') /*SOLO CRITERIOS BCI*/ 
""" 


# COMMAND ----------

sql_safe(paso_query20)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene criterio principal por operacion BCI
# MAGIC --------------------------------------
# MAGIC - selecciona el criterio principal segun jerarquia de los 3 campos
# MAGIC

# COMMAND ----------

paso_query30 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cartdet_crit_ent_ope_prin_BCI AS
SELECT
  A.periodo_cierre           AS periodo_cierre,
  A.fecha_cierre             AS fecha_cierre,
  A.tipo_proceso             AS tipo_proceso,
  A.rut_cliente              AS rut_cliente,
  A.dv_rut_cliente           AS dv_rut_cliente,
  A.tipo_operacion           AS tipo_operacion,
  A.operacion                AS operacion,
  A.sistema                  AS sistema,
  A.segmento                 AS segmento,
  A.criterio_entrada         AS criterio_entrada,
  A.origen_deterioro         AS origen_deterioro,
  A.fecha_entrada            AS fecha_entrada,
  A.grupo                    AS grupo,
  A.periodo_evaluacion       AS periodo_evaluacion,
  A.jer_periodo_evaluacion   AS jer_periodo_evaluacion,
  A.jer_grupo                AS jer_grupo,
  A.jer_criterio_entrada     AS jer_criterio_entrada
  
FROM
  tmp_EXT_tbl_cartdet_crit_ent_crit_BCI A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY  A.jer_criterio_entrada ASC, A.jer_periodo_evaluacion ASC, A.jer_grupo ASC) =1  
""" 


# COMMAND ----------

sql_safe(paso_query30)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene criterio principal del cliente BCI
# MAGIC --------------------------------------
# MAGIC - Selecciona el criterio principal segun jerarquia, para el cliente 
# MAGIC - Este criterio principal se usa para la irradiacion de deterioro. 
# MAGIC - Este criterio principal del cliente no es lo mismo que el criterio de deterioro del cliente que se informara en los archivos finales

# COMMAND ----------

paso_query40 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cartdet_crit_ent_cli_prin_BCI AS
SELECT
  A.periodo_cierre       AS periodo_cierre,
  A.fecha_cierre         AS fecha_cierre,
  A.tipo_proceso         AS tipo_proceso,
  A.rut_cliente          AS rut_cliente,
  A.dv_rut_cliente       AS dv_rut_cliente,
  A.criterio_entrada     AS criterio_entrada

FROM
  tmp_RES_tbl_cartdet_crit_ent_ope_prin_BCI A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.jer_periodo_evaluacion ASC, A.jer_grupo ASC, A.jer_criterio_entrada ASC) =1  
""" 


# COMMAND ----------

sql_safe(paso_query40)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene operaciones deterioradas con criterio propio BCI
# MAGIC --------------------------------------
# MAGIC - genera salida de operaciones deterioradas con criterio propio o heredado del mes anterior
# MAGIC

# COMMAND ----------

paso_query50 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_det_prin_BCI AS
SELECT 
    U.periodo_cierre	                      AS periodo_cierre	                  
   ,U.fecha_cierre	                      AS fecha_cierre	                  
   ,U.tipo_proceso	                      AS tipo_proceso	                  
   ,U.rut_cliente	                         AS rut_cliente	                  
   ,U.dv_rut_cliente	                      AS dv_rut_cliente	                  
   ,U.tipo_operacion	                      AS tipo_operacion	                  
   ,U.operacion	                         AS operacion	                      
   ,U.sistema	                            AS sistema	                      
   ,U.segmento	                            AS segmento	                      
   ,U.criterio_entrada	                   AS criterio_entrada	              
   ,U.origen_deterioro	                   AS origen_deterioro	              
   ,U.fecha_entrada	                      AS fecha_entrada	                  
   ,U.grupo	                               AS grupo	                          
   ,U.periodo_evaluacion                   AS periodo_evaluacion               
   ,A.criterio_entrada                     AS criterio_entrada_cliente
FROM   
   tmp_RES_tbl_cartdet_crit_ent_ope_prin_BCI U,
   tmp_RES_tbl_cartdet_crit_ent_cli_prin_BCI A
WHERE
   U.rut_cliente = A.rut_cliente
"""  

# COMMAND ----------

sql_safe(paso_query50)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene operaciones de clientes deteriorados BCI
# MAGIC --------------------------------------
# MAGIC - Obtiene todas las operaciones de los clientes con deterioro propio calculados en pasos previos
# MAGIC
# MAGIC

# COMMAND ----------

paso_query60 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_cli_BCI AS
SELECT
    U.periodo_cierre	                    AS periodo_cierre	                  
   ,U.fecha_cierre	                         AS fecha_cierre	                  
   ,U.tipo_proceso	                         AS tipo_proceso	                  
   ,U.rut_cliente	                         AS rut_cliente	                  
   ,U.dv_rut_cliente	                    AS dv_rut_cliente	                  
   ,U.tipo_operacion	                    AS tipo_operacion	                  
   ,U.operacion	                         AS operacion	                      
   ,U.sistema	                              AS sistema	                      
   ,U.segmento	                              AS segmento	                      
FROM
     {base_silver_x}.tbl_cd_d00_segmentado U,
     tmp_RES_tbl_cartdet_crit_ent_cli_prin_BCI  A
WHERE
     U.rut_cliente = A.rut_cliente
"""  

# COMMAND ----------

sql_safe(paso_query60)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Operaciones irradiadas 1: operaciones que no son HIP ni CAE
# MAGIC --------------------------------------
# MAGIC - si cliente tiene deterioro, las operaciones sin deterioro propio son irradiadas segun el criterio del cliente
# MAGIC - excepciones: operaciones hipotecarias vivienda (HIP) y creditos con aval del estado (CAE)
# MAGIC

# COMMAND ----------

paso_query70 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_det_prin_irra_1_BCI AS
SELECT
    U.periodo_cierre	                    AS periodo_cierre	                  
   ,U.fecha_cierre	                         AS fecha_cierre	                  
   ,U.tipo_proceso	                         AS tipo_proceso	                  
   ,U.rut_cliente	                         AS rut_cliente	                  
   ,U.dv_rut_cliente	                    AS dv_rut_cliente	                  
   ,U.tipo_operacion	                    AS tipo_operacion	                  
   ,U.operacion	                         AS operacion	                      
   ,U.sistema	                              AS sistema	                      
   ,U.segmento	                              AS segmento	                      
   ,A.criterio_entrada	                    AS criterio_entrada	              
   ,5               	                    AS origen_deterioro	              
   ,U.fecha_cierre	                         AS fecha_entrada	                  
   ,'Bci_Irradiacion'	                    AS grupo	                          
   ,'p_actual'                               AS periodo_evaluacion               
   ,A.criterio_entrada                       AS criterio_entrada_cliente         
FROM
     tmp_RES_cd_cartdet_ope_cli_BCI U
INNER JOIN
     tmp_RES_tbl_cartdet_crit_ent_cli_prin_BCI  A
ON U.rut_cliente = A.rut_cliente     
LEFT JOIN 
      tmp_RES_cd_cartdet_ope_det_prin_BCI B
ON U.operacion = B.operacion AND U.sistema = B.sistema
WHERE
     B.operacion IS NULL /*que no tenga deterioro propio*/
AND  substring(U.tipo_operacion,1,3) NOT IN {p_tio_esp}  /*no irradie hipotecario vivienda ni cae*/
"""

# COMMAND ----------

sql_safe(paso_query70)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Operaciones irradiadas 2: Para operaciones CAE
# MAGIC --------------------------------------
# MAGIC - Los cae deteriorados, NO irradian hipotecarios
# MAGIC - Obtiene todos los clientes con criterio deterioro propio de mora paras las operaciones CAE

# COMMAND ----------

paso_query75 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_cli_cae_det_prp_BCI AS
select 
A.*
from tmp_RES_cd_cartdet_ope_det_prin_BCI A
where 
substring(trim(A.tipo_operacion),1,3)='CAE' and
A.origen_deterioro=1 and
A.criterio_entrada_cliente=8 
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.periodo_cierre asc) =1  
"""


# COMMAND ----------

sql_safe(paso_query75) 

# COMMAND ----------

paso_query80 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_det_prin_irra_2_BCI AS
SELECT
    U.periodo_cierre	                 AS periodo_cierre	                  
   ,U.fecha_cierre	                      AS fecha_cierre	                  
   ,U.tipo_proceso	                      AS tipo_proceso	                  
   ,U.rut_cliente	                      AS rut_cliente	                  
   ,U.dv_rut_cliente	                 AS dv_rut_cliente	                  
   ,U.tipo_operacion	                 AS tipo_operacion	                  
   ,U.operacion	                      AS operacion	                      
   ,U.sistema	                           AS sistema	                      
   ,U.segmento	                           AS segmento	                      
   ,A.criterio_entrada	                 AS criterio_entrada	              
   ,5               	                 AS origen_deterioro	              
   ,U.fecha_cierre	                      AS fecha_entrada	                  
   ,'Bci_Irradiacion'	                 AS grupo	                          
   ,'p_actual'                            AS periodo_evaluacion               
   ,A.criterio_entrada                    AS criterio_entrada_cliente         
FROM
     tmp_RES_cd_cartdet_ope_cli_BCI U
INNER JOIN
     tmp_RES_tbl_cartdet_crit_ent_cli_prin_BCI  A
ON U.rut_cliente = A.rut_cliente       
INNER JOIN
     tmp_RES_cd_cartdet_cli_cae_det_prp_BCI  B  /*SOLO CLIENTES DETERIORO PROPIO CAE*/
ON U.rut_cliente = B.rut_cliente     
LEFT JOIN
      tmp_RES_cd_cartdet_ope_det_prin_BCI C
ON U.operacion = C.operacion AND U.sistema = C.sistema
WHERE
    C.operacion IS NULL /*que no tenga deterioro propio*/
AND substring(trim(U.tipo_operacion),1,3) <> 'HIP'  /*NO IRRADIA A HIP VIV*/
AND coalesce(A.criterio_entrada,0) = 8 /*criterio deterioro 8 [hipotecario o cae]*/
"""

# COMMAND ----------

sql_safe(paso_query80) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Operaciones irradiadas 3: Para operaciones  HIP  
# MAGIC --------------------------------------
# MAGIC - Los hipotecarios deteriorados, NO irradian a los cae
# MAGIC - Obtiene todos los clientes con criterio deterioro propio de mora paras las operaciones hipotecaria vivienda

# COMMAND ----------

paso_query81 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_cli_hip_det_prp_BCI AS
select 
A.*
from tmp_RES_cd_cartdet_ope_det_prin_BCI A
where 
substring(trim(A.tipo_operacion),1,3)='HIP' and
A.origen_deterioro=1 and
A.criterio_entrada_cliente=8  
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.periodo_cierre asc) =1  
"""

# COMMAND ----------

sql_safe(paso_query81)

# COMMAND ----------

paso_query85 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_det_prin_irra_3_BCI AS
SELECT
    U.periodo_cierre	                 AS periodo_cierre	                  
   ,U.fecha_cierre	                   AS fecha_cierre	                  
   ,U.tipo_proceso	                   AS tipo_proceso	                  
   ,U.rut_cliente	                     AS rut_cliente	                  
   ,U.dv_rut_cliente	                 AS dv_rut_cliente	                  
   ,U.tipo_operacion	                 AS tipo_operacion	                  
   ,U.operacion	                       AS operacion	                      
   ,U.sistema	                         AS sistema	                      
   ,U.segmento	                       AS segmento	                      
   ,A.criterio_entrada	               AS criterio_entrada	              
   ,5               	                 AS origen_deterioro	              
   ,U.fecha_cierre	                   AS fecha_entrada	                  
   ,'Bci_Irradiacion'	                 AS grupo	                          
   ,'p_actual'                         AS periodo_evaluacion               
   ,A.criterio_entrada                 AS criterio_entrada_cliente         
FROM
     tmp_RES_cd_cartdet_ope_cli_BCI U
INNER JOIN
     tmp_RES_tbl_cartdet_crit_ent_cli_prin_BCI  A
ON U.rut_cliente = A.rut_cliente            
INNER JOIN
     tmp_RES_cd_cartdet_cli_hip_det_prp_BCI  B  /*SOLO CLIENTES DETERIORO PROPIO HIP VIV*/
ON U.rut_cliente = B.rut_cliente     
LEFT JOIN
      tmp_RES_cd_cartdet_ope_det_prin_BCI C
ON U.operacion = C.operacion AND U.sistema = C.sistema
WHERE
    C.operacion IS NULL /*que no tenga deterioro propio*/
AND substring(trim(U.tipo_operacion),1,3) <> 'CAE'  /*NO IRRADIA CAE*/
AND coalesce(A.criterio_entrada,0) = 8 /*criterio deterioro 8 [hipotecario o cae]*/
"""

# COMMAND ----------

sql_safe(paso_query85) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Operaciones irradiadas 4: Para clientes LIR
# MAGIC --------------------------------------
# MAGIC - Los lir deteriorados, irradian a todos

# COMMAND ----------

paso_query87 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_det_prin_irra_4_BCI AS
SELECT
    U.periodo_cierre	                    AS periodo_cierre	                  
   ,U.fecha_cierre	                      AS fecha_cierre	                  
   ,U.tipo_proceso	                      AS tipo_proceso	                  
   ,U.rut_cliente	                        AS rut_cliente	                  
   ,U.dv_rut_cliente	                    AS dv_rut_cliente	                  
   ,U.tipo_operacion	                    AS tipo_operacion	                  
   ,U.operacion	                          AS operacion	                      
   ,U.sistema	                            AS sistema	                      
   ,U.segmento	                          AS segmento	                      
   ,A.criterio_entrada	                  AS criterio_entrada	              
   ,5               	                    AS origen_deterioro	              
   ,U.fecha_cierre	                      AS fecha_entrada	                  
   ,'Bci_Irradiacion'	                    AS grupo	                          
   ,'p_actual'                            AS periodo_evaluacion               
   ,A.criterio_entrada                    AS criterio_entrada_cliente         
FROM
     tmp_RES_cd_cartdet_ope_cli_BCI U
INNER JOIN
     tmp_RES_tbl_cartdet_crit_ent_cli_prin_BCI  A  /*CLIENTES CON CRITERIO PROPIO*/
ON U.rut_cliente = A.rut_cliente     
LEFT JOIN
      tmp_RES_cd_cartdet_ope_det_prin_BCI B
ON U.operacion = B.operacion AND U.sistema = B.sistema
WHERE
    B.operacion IS NULL /*que no tenga deterioro propio*/
AND coalesce(A.criterio_entrada,0) = 11 /*criterio deterioro 11 [cliente Lir]*/
"""

# COMMAND ----------

sql_safe(paso_query87) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Genera salida de deterioro propios e irradiados BCI
# MAGIC ------------------
# MAGIC * Une las salida de deterioro propio + irradiados + irradiados hip, cae
# MAGIC

# COMMAND ----------

paso_query90 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_crit_ent_BCI AS
SELECT * FROM tmp_RES_cd_cartdet_ope_det_prin_BCI
UNION
SELECT * FROM tmp_RES_cd_cartdet_ope_det_prin_irra_1_BCI
UNION
SELECT * FROM tmp_RES_cd_cartdet_ope_det_prin_irra_2_BCI
UNION
SELECT * FROM tmp_RES_cd_cartdet_ope_det_prin_irra_3_BCI
UNION
SELECT * FROM tmp_RES_cd_cartdet_ope_det_prin_irra_4_BCI
"""  

# COMMAND ----------

sql_safe(paso_query90)

# COMMAND ----------

# MAGIC %md
# MAGIC ### [FILIALES] CALCULO DETERIORO FILIALES
# MAGIC --------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extrae criterios de deterioro de las operaciones FILIALES
# MAGIC --------------------------------------
# MAGIC - obtiene todos los deterioros calculados en proceso previos
# MAGIC - genera jerarquia (esta jerarquia debe estar en la tabla de parametros)
# MAGIC - son 3 jerarquias. criterio deterioro, grupo, y periodo evaluacion
# MAGIC - Esta jerarquia es para determinar el calculo del deterioro (no es lo mismo que el criterio que se informa x cliente)

# COMMAND ----------

paso_query110 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_EXT_tbl_cartdet_crit_ent_crit_FIL AS
SELECT
  A.periodo_cierre       AS periodo_cierre,
  A.fecha_cierre         AS fecha_cierre,
  A.tipo_proceso         AS tipo_proceso,
  A.rut_cliente          AS rut_cliente,
  A.dv_rut_cliente       AS dv_rut_cliente,
  A.tipo_operacion       AS tipo_operacion,
  A.operacion            AS operacion,
  A.sistema              AS sistema,
  A.segmento             AS segmento,
  A.criterio_entrada     AS criterio_entrada,
  A.origen_deterioro     AS origen_deterioro,
  MIN(A.fecha_entrada) OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre ASC) AS fecha_entrada,
  A.grupo                AS grupo,
  A.periodo_evaluacion   AS periodo_evaluacion,
  CASE 
      WHEN A.criterio_entrada = 1 THEN 1
      WHEN A.criterio_entrada = 11 THEN 2
      WHEN A.criterio_entrada = 8 THEN 3
      WHEN A.criterio_entrada = 7 THEN 4
      WHEN A.criterio_entrada = 9 THEN 5
      WHEN A.criterio_entrada = 10 THEN 6
      WHEN A.criterio_entrada = 12 THEN 7
      WHEN A.criterio_entrada = 13 THEN 8      
      ELSE 99
  END                    AS jer_criterio_entrada,  
  CASE 
      WHEN trim(A.grupo) = 'BCI_Individual' THEN 1
      WHEN trim(A.grupo) = 'BCI_Grupal' THEN 2
      WHEN trim(A.grupo) = 'Factoring' THEN 3
      WHEN trim(A.grupo) = 'SSFF' THEN 4
      ELSE 99
  END                    AS jer_grupo,
  CASE 
      WHEN trim(A.periodo_evaluacion) = 'p_actual' THEN 1
      WHEN trim(A.periodo_evaluacion) = 'p_anterior' THEN 2
      ELSE 99
  END                    AS jer_periodo_evaluacion  
FROM
  {base_silver_x}.tbl_cd_cartdet_crit_ent_crit A
WHERE
    fecha_cierre = {fecha_x}  
AND periodo_cierre = {periodo_x}  
AND trim(A.grupo) IN ('Factoring','SSFF') /*SOLO CRITERIOS FILIALES*/ 
""" 


# COMMAND ----------

sql_safe(paso_query110) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene criterio principal por operacion FILIALES
# MAGIC --------------------------------------
# MAGIC - selecciona el criterio principal segun jerarquia de los 3 campos
# MAGIC

# COMMAND ----------

paso_query115 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cartdet_crit_ent_ope_prin_FIL AS
SELECT
  A.periodo_cierre           AS periodo_cierre,
  A.fecha_cierre             AS fecha_cierre,
  A.tipo_proceso             AS tipo_proceso,
  A.rut_cliente              AS rut_cliente,
  A.dv_rut_cliente           AS dv_rut_cliente,
  A.tipo_operacion           AS tipo_operacion,
  A.operacion                AS operacion,
  A.sistema                  AS sistema,
  A.segmento                 AS segmento,
  A.criterio_entrada         AS criterio_entrada,
  A.origen_deterioro         AS origen_deterioro,
  A.fecha_entrada            AS fecha_entrada,
  A.grupo                    AS grupo,
  A.periodo_evaluacion       AS periodo_evaluacion,
  A.jer_periodo_evaluacion   AS jer_periodo_evaluacion,
  A.jer_grupo                AS jer_grupo,
  A.jer_criterio_entrada     AS jer_criterio_entrada
  
FROM
  tmp_EXT_tbl_cartdet_crit_ent_crit_FIL A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.jer_periodo_evaluacion ASC, A.jer_grupo ASC, A.jer_criterio_entrada ASC) =1  
""" 


# COMMAND ----------

sql_safe(paso_query115) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene criterio principal del cliente FILIALES
# MAGIC --------------------------------------
# MAGIC - Selecciona el criterio principal segun jerarquia, para el cliente 
# MAGIC - Este criterio principal se usa para la irradiacion de deterioro. 
# MAGIC - Este criterio principal del cliente no es lo mismo que el criterio de deterioro del cliente que se informara en los archivos finales

# COMMAND ----------

paso_query120 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_tbl_cartdet_crit_ent_cli_prin_FIL AS
SELECT
  A.periodo_cierre       AS periodo_cierre,
  A.fecha_cierre         AS fecha_cierre,
  A.tipo_proceso         AS tipo_proceso,
  A.rut_cliente          AS rut_cliente,
  A.dv_rut_cliente       AS dv_rut_cliente,
  A.criterio_entrada     AS criterio_entrada

FROM
  tmp_RES_tbl_cartdet_crit_ent_ope_prin_FIL A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.jer_periodo_evaluacion ASC, A.jer_grupo ASC, A.jer_criterio_entrada ASC) =1  
""" 



# COMMAND ----------

sql_safe(paso_query120) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene operaciones deterioradas con criterio propio FILIALES
# MAGIC --------------------------------------
# MAGIC - genera salida de operaciones deterioradas con criterio propio o heredado del mes anterior
# MAGIC

# COMMAND ----------

paso_query125 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_det_prin_FIL AS
SELECT 
    U.periodo_cierre	                    AS periodo_cierre	                  
   ,U.fecha_cierre	                      AS fecha_cierre	                  
   ,U.tipo_proceso	                      AS tipo_proceso	                  
   ,U.rut_cliente	                        AS rut_cliente	                  
   ,U.dv_rut_cliente	                    AS dv_rut_cliente	                  
   ,U.tipo_operacion	                    AS tipo_operacion	                  
   ,U.operacion	                          AS operacion	                      
   ,U.sistema	                            AS sistema	                      
   ,U.segmento	                          AS segmento	                      
   ,U.criterio_entrada	                  AS criterio_entrada	              
   ,U.origen_deterioro	                  AS origen_deterioro	              
   ,U.fecha_entrada	                      AS fecha_entrada	                  
   ,U.grupo	                              AS grupo	                          
   ,U.periodo_evaluacion                  AS periodo_evaluacion               
   ,A.criterio_entrada                    AS criterio_entrada_cliente
FROM   
   tmp_RES_tbl_cartdet_crit_ent_ope_prin_FIL U,
   tmp_RES_tbl_cartdet_crit_ent_cli_prin_FIL A
WHERE
   U.rut_cliente = A.rut_cliente
"""  

# COMMAND ----------

sql_safe(paso_query125) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtiene operaciones de clientes deteriorados FILIALES
# MAGIC --------------------------------------
# MAGIC - Obtiene todas las operaciones de los clientes con deterioro propio calculados en pasos previos
# MAGIC
# MAGIC

# COMMAND ----------

paso_query130 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_cli_FIL AS
SELECT
    U.periodo_cierre	                 AS periodo_cierre	                  
   ,U.fecha_cierre	                   AS fecha_cierre	                  
   ,U.tipo_proceso	                   AS tipo_proceso	                  
   ,U.rut_cliente	                     AS rut_cliente	                  
   ,U.dv_rut_cliente	                 AS dv_rut_cliente	                  
   ,U.tipo_operacion	                 AS tipo_operacion	                  
   ,U.operacion	                       AS operacion	                      
   ,U.sistema	                         AS sistema	                      
   ,U.segmento	                       AS segmento	                      
FROM
     {base_silver_x}.tbl_cd_d00_segmentado U,
     tmp_RES_tbl_cartdet_crit_ent_cli_prin_FIL  A
WHERE
     U.rut_cliente = A.rut_cliente
"""  

# COMMAND ----------

sql_safe(paso_query130) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Operaciones irradiadas 1: operaciones que no son HIP ni CAE
# MAGIC --------------------------------------
# MAGIC - si cliente tiene deterioro, las operaciones sin deterioro propio son irradiadas segun el criterio del cliente
# MAGIC - excepciones: operaciones hipotecarias vivienda (HIP) y creditos con aval del estado (CAE)
# MAGIC

# COMMAND ----------

paso_query135 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_RES_cd_cartdet_ope_det_prin_irra_1_FIL AS
SELECT
    U.periodo_cierre	                 AS periodo_cierre	                  
   ,U.fecha_cierre	                      AS fecha_cierre	                  
   ,U.tipo_proceso	                      AS tipo_proceso	                  
   ,U.rut_cliente	                      AS rut_cliente	                  
   ,U.dv_rut_cliente	                 AS dv_rut_cliente	                  
   ,U.tipo_operacion	                 AS tipo_operacion	                  
   ,U.operacion	                      AS operacion	                      
   ,U.sistema	                           AS sistema	                      
   ,U.segmento	                           AS segmento	                      
   ,A.criterio_entrada	                 AS criterio_entrada	              
   ,6               	                 AS origen_deterioro	              
   ,U.fecha_cierre	                      AS fecha_entrada	                  
   ,'Fil_Irradiacion'	                 AS grupo	                          
   ,'p_actual'                            AS periodo_evaluacion               
   ,A.criterio_entrada                    AS criterio_entrada_cliente         
FROM
     tmp_RES_cd_cartdet_ope_cli_FIL U
LEFT JOIN
     tmp_RES_tbl_cartdet_crit_ent_cli_prin_FIL  A
ON U.rut_cliente = A.rut_cliente     
LEFT JOIN
      tmp_RES_cd_cartdet_ope_det_prin_FIL B
ON U.operacion = B.operacion AND U.sistema = B.sistema
WHERE
     B.operacion IS NULL /*que no tenga deterioro propio*/
AND  substring(U.tipo_operacion,1,3) NOT IN {p_tio_esp}  /*no irradie hipotecario vivienda ni cae*/
"""

# COMMAND ----------

sql_safe(paso_query135) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Une registros de deterioro propios e irradiados FILIALES
# MAGIC ------------------
# MAGIC * Une las salida de deterioro propio FILIALES + irradiados FILIALES
# MAGIC

# COMMAND ----------

paso_query140 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_crit_ent_FIL AS
SELECT * FROM tmp_RES_cd_cartdet_ope_det_prin_FIL
UNION
SELECT * FROM tmp_RES_cd_cartdet_ope_det_prin_irra_1_FIL
"""  

# COMMAND ----------

sql_safe(paso_query140) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Salida Temporal]  Genera salida deterioro BCI y Filiales
# MAGIC ------------------
# MAGIC * une informacion de deterioro de BCI y Filiales
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Une informacion de deterioro de BCI y Filiales
# MAGIC

# COMMAND ----------

paso_query250 = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_crit_ent_prin_1 AS
SELECT A.*, 1 AS IND FROM  tmp_tbl_cd_cartdet_crit_ent_BCI A
UNION  
SELECT B.*, 2 AS IND FROM  tmp_tbl_cd_cartdet_crit_ent_FIL B
"""  

# COMMAND ----------

sql_safe(paso_query250)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Asigna Jerarquia al Criterio de Deterioro
# MAGIC ---
# MAGIC - considera todos los deterioros, bci y filiales, del mes actual y del mes anterior
# MAGIC - obtiene la minima fecha de entrada a deterioro
# MAGIC - es distinto al deterior por cliente que se usa para deteriorar
# MAGIC

# COMMAND ----------

paso_query255= f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_crit_ent_prin_2 AS
SELECT
  A.periodo_cierre,
  A.fecha_cierre,
  A.tipo_proceso,
  A.rut_cliente,
  A.dv_rut_cliente,
  A.tipo_operacion,
  A.operacion,
  A.sistema,
  A.segmento,
  A.criterio_entrada,
  A.origen_deterioro,
  MIN(A.fecha_entrada) OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.fecha_cierre ASC) AS fecha_entrada,
  A.grupo,
  A.periodo_evaluacion,
  A.criterio_entrada_cliente,
  A.IND,
  CASE 
      WHEN A.criterio_entrada = 1 THEN 1
      WHEN A.criterio_entrada = 11 THEN 2
      WHEN A.criterio_entrada = 8 THEN 3
      WHEN A.criterio_entrada = 7 THEN 4
      WHEN A.criterio_entrada = 9 THEN 5
      WHEN A.criterio_entrada = 10 THEN 6
      WHEN A.criterio_entrada = 12 THEN 7
      WHEN A.criterio_entrada = 13 THEN 8      
      ELSE 99
  END                                         AS jer_criterio_entrada_show
FROM
  tmp_tbl_cd_cartdet_crit_ent_prin_1 A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.operacion, A.sistema ORDER BY A.IND ASC) =1    
""" 


# COMMAND ----------

sql_safe(paso_query255)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calcula criterio deterioro por cliente
# MAGIC ---
# MAGIC - considera todos los deterioros, bci y filiales, del mes actual y del mes anterior
# MAGIC

# COMMAND ----------

paso_query257= f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_crit_ent_cli_show AS
select 
 A.periodo_cierre
,A.fecha_cierre
,A.tipo_proceso
,A.rut_cliente
,A.criterio_entrada AS criterio_entrada_show
from 
  tmp_tbl_cd_cartdet_crit_ent_prin_2 A
QUALIFY  ROW_NUMBER() OVER(PARTITION BY A.rut_cliente ORDER BY A.jer_criterio_entrada_show ASC) =1      
""" 

# COMMAND ----------

sql_safe(paso_query257)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Genera tabla temporal final con datos de salida
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

paso_query260= f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_tbl_cd_cartdet_crit_ent_prin AS
select 
 A.periodo_cierre
,A.fecha_cierre
,A.tipo_proceso
,A.rut_cliente
,A.dv_rut_cliente
,A.tipo_operacion
,A.operacion
,A.sistema
,A.segmento
,A.criterio_entrada
,A.origen_deterioro
,A.fecha_entrada
,A.grupo
,A.periodo_evaluacion
,A.criterio_entrada_cliente
,B.criterio_entrada_show
from 
  tmp_tbl_cd_cartdet_crit_ent_prin_2 A,
  tmp_tbl_cd_cartdet_crit_ent_cli_show B
where
	A.rut_cliente = B.rut_cliente
""" 

# COMMAND ----------

sql_safe(paso_query260)

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

paso_query300 = f""" TRUNCATE TABLE {base_silver_x}.tbl_cd_cartdet_crit_ent_prin  """

# COMMAND ----------

sql_safe(paso_query300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inserta Registros tabla salida

# COMMAND ----------

paso_query310 = f"""
INSERT INTO {base_silver_x}.tbl_cd_cartdet_crit_ent_prin
SELECT 
    IFNULL(periodo_cierre,190001),
    IFNULL(fecha_cierre,19000101),
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
    IFNULL(criterio_entrada_show,0)
FROM
    tmp_tbl_cd_cartdet_crit_ent_prin  
QUALIFY  ROW_NUMBER() OVER(PARTITION BY operacion, sistema ORDER BY  fecha_cierre ASC) =1 
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
# MAGIC FROM ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_prin
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")