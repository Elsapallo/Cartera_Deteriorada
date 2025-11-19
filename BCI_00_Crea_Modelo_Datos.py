# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_00_Crea_Modelo_Datos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_00_Crea_Modelo_Datos.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011900526
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 13/03/2023
# MAGIC * Descripción: Este notebook tiene por finalidad crear tablas que contienen la data del proceso en ejecución.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 15/03/2025 
# MAGIC * Descripción: Se agrega tabla de salida LIR (tbl_cd_cliente_lir) y tbl_cd_ajuste_calificaciones     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 08/04/2025 
# MAGIC * Descripción: Se elimina tabla de salida LIR (tbl_cd_cli_lir_fec_sal)     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Francisco Valenzuela (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 25/04/2025 
# MAGIC * Descripción: Se agrega tabla de salida para los cambios de calificacion "tbl_cd_cambio_ajuste_calificaciones", que se ocupa como entrada para segmentacion 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 4
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 08/07/2025 
# MAGIC * Descripción: Se modifica el proceso para incorporar una tabla de trabajo (work) que contiene el universo de clientes, incluyendo sus respectivas fechas de entrada a deterioro a nivel de cliente.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 5
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 22/07/2025 
# MAGIC * Descripción: Se modifica el proceso para incorporar una tabla de trabajo (work) que contendra las operaciones renegociadas que cumplen condiciones de salida.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 6
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 16/10/2025 
# MAGIC * Descripción: Se modifica el proceso para incorporar dos tablas (work) que se utilizarán en el proceso de Forzaje.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listado de tablas Work
# MAGIC -------------------------
# MAGIC * tbl_cd_cae_ope_det_incumplimiento
# MAGIC * tbl_cd_cartdet_c4_gr_op_eli
# MAGIC * tbl_cd_cartdet_c4_gr_op_ing
# MAGIC * tbl_cd_cartdet_cli_ini_cd
# MAGIC * tbl_cd_cartdet_cli_ini_cd_pant
# MAGIC * tbl_cd_cartdet_crit_ent_crit
# MAGIC * tbl_cd_cartdet_crit_ent_ope_eval
# MAGIC * tbl_cd_cartdet_crit_ent_prin
# MAGIC * tbl_cd_cartdet_crit_sal_crit
# MAGIC * tbl_cd_cartdet_crit_sal_crit_fam
# MAGIC * tbl_cd_cartdet_crit_sal_crit_matriz
# MAGIC * tbl_cd_cartdet_crit_sal_ope_eval
# MAGIC * tbl_cd_cartdet_crit_sal_prin
# MAGIC * tbl_cd_cartdet_ing_eli_fzj
# MAGIC * tbl_cd_cartdet_matriz_salida
# MAGIC * tbl_cd_cartdet_no_vig
# MAGIC * tbl_cd_cartdet_ope_ini_cd
# MAGIC * tbl_cd_cartdet_registros_fzj_log
# MAGIC * tbl_cd_cartdet_stock
# MAGIC * tbl_cd_cartdet_tablon_archivos
# MAGIC * tbl_cd_cartdet_vig
# MAGIC * tbl_cd_cliente_lir
# MAGIC * tbl_cd_cliente_consolidado
# MAGIC * tbl_cd_cliente_consolidado_pant
# MAGIC * tbl_cd_cliente_det_fact
# MAGIC * tbl_cd_cliente_det_ssff
# MAGIC * tbl_cd_curses
# MAGIC * tbl_cd_d00_extendido
# MAGIC * tbl_cd_d00_segmentado
# MAGIC * tbl_cd_d00_segmentado_pant
# MAGIC * tbl_cd_dat_ope_ini_cd
# MAGIC * tbl_cd_fec_proc
# MAGIC * tbl_cd_ope_condicion_mora
# MAGIC * tbl_cd_ope_condicion_mora_pant
# MAGIC * tbl_cd_ope_condicion_ren
# MAGIC * tbl_cd_ope_condicion_sal_ren
# MAGIC * tbl_cd_ope_curse_bajo_mora
# MAGIC * tbl_cd_ope_dia_mora
# MAGIC * tbl_cd_ope_ent_meses
# MAGIC * tbl_cd_ope_pag_cons_ibm
# MAGIC * tbl_cd_segmentacion_cliente
# MAGIC * tbl_cd_tmp_ope_ctas_par
# MAGIC * tbl_cd_ajuste_calificaciones
# MAGIC * tbl_cd_cambio_ajuste_calificaciones

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listado de tablas Gold
# MAGIC -------------------------
# MAGIC * tbl_hcd_cartdet_no_vig
# MAGIC * tbl_hcd_cartdet_stock
# MAGIC * tbl_hcd_cartdet_vig
# MAGIC * tbl_hcd_cartdet_ope_ini_cd
# MAGIC * tbl_hcd_cartdet_cli_ini_cd
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
dbutils.widgets.text("bd_golden_w","","03-Nombre BD Gold:")
dbutils.widgets.text("ruta_silver_w","","04-Ruta adss Silver:")
dbutils.widgets.text("ruta_gold_w","","05-Ruta adss Gold:")

fecha_x = dbutils.widgets.get("fecha_w") 
base_silver_x = dbutils.widgets.get("bd_silver_w")
base_gold_x = dbutils.widgets.get("bd_golden_w")
ruta_silver_x = dbutils.widgets.get("ruta_silver_w")
ruta_gold_x = dbutils.widgets.get("ruta_gold_w")

spark.conf.set("bci.fecha", fecha_x)
spark.conf.set("bci.dbnamesilver", base_silver_x)
spark.conf.set("bci.dbnamegold", base_gold_x)
spark.conf.set("bci.ruta_silver", ruta_silver_x)
spark.conf.set("bci.ruta_gold", ruta_gold_x)

print(f"fecha_x : {fecha_x}")
print(f"base_silver_x: {base_silver_x}")
print(f"base_gold_x: {base_gold_x}")
print(f"ruta_silver_x: {ruta_silver_x}")
print(f"ruta_gold_x: {ruta_gold_x}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Valida parámetros
# MAGIC

# COMMAND ----------

# DBTITLE 1,Valida parámetro "base_goldX"
valida_parametro(base_silver_x)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "ruta_goldX"
valida_parametro(ruta_silver_x)

# COMMAND ----------

# DBTITLE 1,Valida parámetro "bd_ruta_goldX"
valida_parametro(base_gold_x)

# COMMAND ----------

valida_parametro(ruta_gold_x)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables para tabla fecha de proceso

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREA BASE DATOS WORK 

# COMMAND ----------

# MAGIC %sql
# MAGIC     CREATE DATABASE IF NOT EXISTS ${bci.dbnamesilver}
# MAGIC     LOCATION '${bci.ruta_silver}';

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Tablas Work
# MAGIC ------

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_ent_ope_eval (resultado de evaluaciones de entrada a deterioro)
# MAGIC
# MAGIC  - Almacena todas las evaluaciones aplicadas a las operaciones, dejando un indicador si cumple o no cumple dicha regla.
# MAGIC  - Una operación puede tener muchas evaluaciones. Evaluciones por mora, renegociado, individual, factoring, ssff, etc.
# MAGIC  - En esta tabla no se asigna criterios de deterioros
# MAGIC  - Entonces la llave de esta tabla debe ser la operación y el codigo de evaluacion

# COMMAND ----------

# MAGIC %sql			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_ope_eval(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,nombre_campo	string	COMMENT	'Código que identifica el criterio evaluado. Datos posibles del campo (MOT_BANCA_IND, MOT_DEUDA_TOT, MOT_DEUDA_PROM, MOT_GRUPO_EMP)'
# MAGIC ,valor_campo	string	COMMENT	'Valor del criterio evaluado. Datos posibles del campo (G,C6, I,B4)'
# MAGIC ,condicion_regla	string	COMMENT	'Condición del criterio evaluado. Datos posibles del campo (like, IN, >)'
# MAGIC ,valor_regla	string	COMMENT	'Valor de la regla. Datos posibles del campo (I, C%, I2, I3, I4, 20000, tbl_grupos_empresariales)'
# MAGIC ,flag_resultado_regla	tinyint	COMMENT	'Flag resultado (0 ó 1), el cual va a depender de la regla indicada por el negocio, para cada criterio. Cuando cumple es 1, de lo contrario es 0'
# MAGIC ,cod_evaluacion	string	COMMENT	'codigo evaluacion'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene las operaciones con su criterio evaluado de entrada a cartera deteriorada'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_ent_ope_eval';			

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_ent_crit (criterios entrada deterioro)
# MAGIC
# MAGIC  - Almacena todos los criterios de deterioros del cliente, del mes actual y del mes anterior
# MAGIC  - Una operación puede ser deteriorada por varios criterios y aquí debe estar todos los criterios con que se evaluaron.
# MAGIC  - Entonces la llave de esta tabla debe ser la operación y el criterio de deterioro.

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_crit(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,criterio_entrada	int	COMMENT	'Criterio de entrada a deterioro por operación'
# MAGIC ,origen_deterioro	int	COMMENT	'Origen de deterioro'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,grupo	string	COMMENT	'Inidca la regla de de negocio por el cual la operacion se debe evaluar, esta se determina de la siguiente forma (SSFF, Factoring, BCI_Grupal, Bci_Irradiacion, BCI_Individual)'
# MAGIC ,periodo_evaluacion          string		COMMENT	'Numenclatura que indica cuando la operacion fue evaluada. Los datos que se encuentran en este campo es p_anterior o p_actual)'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene las operaciones que entran a cartera deteriorada con su criterio de entrada '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_ent_crit';			

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_ent_prin (criterios entrada principal de deterioro por operacion)
# MAGIC
# MAGIC  - Almacena el criterio principal de deterioro por operacion, aplicando jerarquia
# MAGIC  - Una operación puede ser deteriorada por varios criterios y aquí debe estar un solo movito de deterioro por operacion.
# MAGIC  - Entonces la llave de esta tabla debe ser la operación

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_ent_prin(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,criterio_entrada	int	COMMENT	'Criterio de entrada a deterioro por operación'
# MAGIC ,origen_deterioro	int	COMMENT	'Origen de deterioro'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,grupo	string	COMMENT	'Inidca la regla de de negocio por el cual la operacion se debe evaluar, esta se determina de la siguiente forma (SSFF, Factoring, BCI_Grupal, Bci_Irradiacion, BCI_Individual)'
# MAGIC ,periodo_evaluacion          	string	COMMENT	'Numenclatura que indica cuando la operacion fue evaluada. Los datos que se encuentran en este campo es p_anterior o p_actual)'
# MAGIC ,criterio_entrada_cliente    	int	COMMENT	'Indicador de entrada a deterioro definido a nivel de cliente, donde el indicador es un numero, por ejemplo (9 = renegociado, 8 Cartera Vencida; 12 = SSFF)'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT ' Tabla que contienelas operaciones en cardet con su criterio de entrada principal'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_ent_prin';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_sal_ope_eval (resultado de evaluaciones de salida de deterioro)
# MAGIC
# MAGIC  - Almacena todas las evaluaciones aplicadas a las operaciones, dejando un indicador si cumple o no cumple dicha regla.
# MAGIC  - Una operación puede tener muchas evaluaciones. Evaluciones sin mora, sin refinanciamiento, sin clasificacion, sin factorig, sin lir, etc.
# MAGIC  - En esta tabla no se asigna criterios de deterioros
# MAGIC  - Entonces la llave de esta tabla debe ser la operación y el codigo de evaluacion

# COMMAND ----------

# MAGIC %sql			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_ope_eval(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,nombre_campo	string	COMMENT	'nombre del campo'
# MAGIC ,valor_campo	string	COMMENT	'valor del campo'
# MAGIC ,condicion_regla	string	COMMENT	'condicion regla'
# MAGIC ,valor_regla	string	COMMENT	'valor de la regla'
# MAGIC ,flag_resultado_regla	tinyint	COMMENT	'flag indicador resultado de la regla'
# MAGIC ,cod_evaluacion	string	COMMENT	'codigo evaluacion'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT ' Tabla que contiene las operaciones de salida evaluadas en cardet'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_sal_ope_eval';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_sal_crit (criterios salida deterioro)
# MAGIC
# MAGIC  - Almacena todos los criterios de deterioros del cliente, del mes actual 
# MAGIC  - Una operación puede tener varios criteros de salida de deteioro. 
# MAGIC  - Entonces la llave de esta tabla debe ser la operación y el criterio de deterioro.

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_crit(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,criterio_salida	int	COMMENT	'Indicador que se le designa a la operacion del cliente, por el cual sale de cartera deteriorada'
# MAGIC ,flag_resultado_regla        	tinyint	COMMENT	'flag indicador resultado de la regla'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene las operaciones de cardet con su criterio de salida '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_sal_crit';			

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_sal_crit_fam (criterios salida deterioro segun familia)
# MAGIC  - Almacena todos los criterios de deterioros del cliente, del mes actual 
# MAGIC  - Una operación puede tener varios criteros de salida de deteioro. 
# MAGIC  - Entonces la llave de esta tabla debe ser la operación y el criterio de deterioro.

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_crit_fam(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,familia_ope                 	string	COMMENT	'Se agrupan las operaciones, de la siguiente forma ( cuando la operacion inicia con (V, A o C), se asigna LC_Act, cuando incia con (E0, E1) se asigna TCR_Act, cuando el tipo_credito es (02,09) se designa contingente, cuando  tipo_operacion inicia cae se asigna CAE, cuando la operacion pertenece a pagos parciales, se asigna Pago_Parcial. de lo contrario, se asigna Pago_Estructurado'
# MAGIC ,criterio_salida	int	COMMENT	'Codido que nos indica el por que motivo sale el cliente de deterioro. Ejemplo ( 2: Salida Deterioro operaciones de clientes individuales; 27: Salida Deterioro operaciones con saldo ifrs total cero; 30: Salida Deterioro operaciones deterioradas mes anterior y que no se informan en mes actual, etc ). '
# MAGIC ,flag_resultado_regla        	tinyint	COMMENT	'flag indicador resultado de la regla'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene la informacion de operaciones den cardet con su criterio de salida agrupados por familia de operacion'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_sal_crit_fam';			

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_sal_prin (operaciones que salen de deterioro)
# MAGIC  - Almacena todas las operaciones de los clientes donde todas sus operaciones salen de detrioro

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_prin(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,familia_ope                 	string	COMMENT	'Se agrupan las operaciones, de la siguiente forma ( cuando la operacion inicia con (V, A o C), se asigna LC_Act, cuando incia con (E0, E1) se asigna TCR_Act, cuando el tipo_credito es (02,09) se designa contingente, cuando  tipo_operacion inicia cae se asigna CAE, cuando la operacion pertenece a pagos parciales, se asigna Pago_Parcial. de lo contrario, se asigna Pago_Estructurado'
# MAGIC ,criterio_salida	int	COMMENT	'Indicador que se le designa a la operacion del cliente, por el cual sale de cartera deteriorada'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene informacion de operaciones en cardet con criterio de salida principal '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_sal_prin';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_stock 
# MAGIC  - Esta tabla almacena el resultado de las entradas menos las salidas de deterioro grupal e individual 
# MAGIC

# COMMAND ----------

# MAGIC  %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_stock (
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'Año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente con cartera deteriorada'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente con cartera deteriorada'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,criterio_entrada	int	COMMENT	'Indicador de entrada a deterioro por operacion, donde el indicador es un numero, por ejemplo (9 = renegociado, 8 Cartera Vencida; 12 = SSFF)'
# MAGIC ,origen_deterioro	int	COMMENT	'Indicador por el cual la operacion fue deteriorada, dodne el indicador es un numero, por ejemplo (1 = operaciond e origen, 5 = operacion irradiada)'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,grupo	string	COMMENT	'Inidca la regla de de negocio por el cual la operacion se debe evaluar, esta se determina de la siguiente forma (SSFF, Factoring, BCI_Grupal, Bci_Irradiacion, BCI_Individual)'
# MAGIC ,periodo_evaluacion          	string	COMMENT	'Numenclatura que indica cuando la operacion fue evaluada. Los datos que se encuentran en este campo es p_anterior o p_actual)'
# MAGIC ,criterio_entrada_cliente    	int	COMMENT	'Indicador de entrada a deterioro definido a nivel de cliente, donde el indicador es un numero, por ejemplo (9 = renegociado, 8 Cartera Vencida; 12 = SSFF)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_cierre)
# MAGIC COMMENT 'Tabla que contiene todo el stock de las operaciones que se encuentran deterioradas del periodo actual.'	
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_stock';

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_crit_sal_crit_matriz (resultado de evaluaciones de salida)
# MAGIC
# MAGIC  - Contiene el resultado de las evaluaciones de salida de una operacion
# MAGIC  - Contiene el cruce con los indicadores que debe cumplir segun familia
# MAGIC  - Contiene las operaciones que salen por excepciones 
# MAGIC

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_crit_sal_crit_matriz (			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,criterio_salida	int	COMMENT	'Indicador que se le designa a la operacion del cliente, por el cual sale de cartera deteriorada'
# MAGIC ,flag_resultado_regla	tinyint	COMMENT	'flag indicador resultado de la regla'
# MAGIC ,familia_ope	string	COMMENT	'Se agrupan las operaciones, de la siguiente forma ( cuando la operacion inicia con (V, A o C), se asigna LC_Act, cuando incia con (E0, E1) se asigna TCR_Act, cuando el tipo_credito es (02,09) se designa contingente, cuando  tipo_operacion inicia cae se asigna CAE, cuando la operacion pertenece a pagos parciales, se asigna Pago_Parcial. de lo contrario, se asigna Pago_Estructurado'
# MAGIC ,evaluacion	string	COMMENT	'Nomenclatura que indica el resultado de una evaluacion realizada a la operacion'
# MAGIC ,flag_esperado	tinyint	COMMENT	'Indicador numerico (1,0), que indica si es que cumple o no lo indicado en el campo evaluacion'
# MAGIC ,requerido	string	COMMENT	'Indicador (SI, NO) que hace referencia a si es que se utiliza el registro o no'
# MAGIC ,Respuesta	string	COMMENT	'la respuesta indica si es que se encuentra ok o no ok el registro evaluado'
# MAGIC ,Desc_Respuesta	string	COMMENT	'Descripcion del resultado de la evaluacion, cuando el el campo respuesta se encuentra NOK'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene informacion de matriz que muestra el criterio de salida de la operacion con su evaluacion y descripcion '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_crit_sal_crit_matriz';			

# COMMAND ----------

# MAGIC %md
# MAGIC ###tbl_cd_cartdet_cli_ini_cd
# MAGIC - Esta tabla almacena el resultado de las entradas menos las salidas de deterioro grupal e individual mas la fecha de inicio en cartera deteriorada por cliente.

# COMMAND ----------

# MAGIC  %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_cli_ini_cd (
# MAGIC  fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,fec_ini_cd                  	int	COMMENT	'Fecha conformada de año, mes y dia correspondiente al dia en que el crédito entra en mora'
# MAGIC ,fecha_informada	int	COMMENT	'año, mes y dia correspondiente a la fecha ingresada por parametro para la ejecucion de los procesos'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_cierre)
# MAGIC COMMENT 'Tabla a nivel de cliente. Contiene información de los clientes que se encuentran vigentes y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Incluye la fecha de inicio de deterioro más antigua por cliente.'
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_cli_ini_cd';

# COMMAND ----------

# MAGIC %md
# MAGIC ###tbl_cd_cartdet_cli_ini_cd_pant
# MAGIC - Esta tabla almacena el resultado de las entradas menos las salidas de deterioro grupal e individual mas la fecha de inicio en cartera deteriorada por cliente del periodo anterior.

# COMMAND ----------

# MAGIC  %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_cli_ini_cd_pant (
# MAGIC  fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,fec_ini_cd   int	COMMENT	'Fecha conformada de año, mes y dia correspondiente al dia en que entro a cartera deteriorada'
# MAGIC ,fecha_informada	int	COMMENT	'año, mes y dia correspondiente a la fecha ingresada por parametro para la ejecucion de los procesos'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_cierre)
# MAGIC COMMENT 'Tabla a nivel de cliente. Contiene información de los clientes que se encuentran vigentes y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Incluye la fecha de inicio de deterioro más antigua por cliente.'
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_cli_ini_cd_pant';

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_d00_segmentado

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_d00_segmentado (			
# MAGIC  periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,macro_sistema	string	COMMENT	'El Marco sistema corresponde al origen de la información, si este viene del D00 Bci o del D00 Leasing, identificado como (BCI, LEA)'
# MAGIC ,tipo_cartera	string	COMMENT	'Identifica la cartera a la que la operación pertenece, esta es una sigla compuesta de 6 caracteres, como por ejemplo (101061, BGT460, CAE961, COM001, CON992, LEA004, TDC500)'
# MAGIC ,tipo_producto	string	COMMENT	'Tipo de producto actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, el codigo de sistema, el tipo de crédito, la macro sistema y la cuenta contable de la operación obtenida del D00. se expresan valores de ejemplo (ACT, CTG)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,cod_proceso	string	COMMENT	'Código de proceso inicial, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el macro sistema, el código de sistema y el tipo de operacion de la operación obtenida del D00. Se expresan valores de ejemplo (GR_COM, GR_HIP, GR_CON)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,tipo_credito	string	COMMENT	'Tipo de crédito actualizado, se obtiene del proceso SL1 a travez del proceso de segmentacion, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786, C01361425478,D01010589707,E00000463489)'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,situacion_operacion	int	COMMENT	'Sitiacion de la operación'
# MAGIC ,oficina_credito	string	COMMENT	'Oficina en la cual fue otorgado el crédito, esta se compone de 3 dígitos, como por ejemplo (125, 451, 124)'
# MAGIC ,estado_credito	int	COMMENT	'Es un numero de un dígito, que identifica el estado en el que se encuentra el crédito, estos pueden ser (0, 1, 2, 3, 4), donde "1":Vigente "2":Moroso "3":Vencido "4".Extinguido'
# MAGIC ,act_dest_credito	string	COMMENT	'Corresponde al destino del crédito, donde se identifica a través de una sigla correspondiente a 2 dígitos, como por ejemplo (34, 01, 22, 52)'
# MAGIC ,monto_original	decimal(18,0)	COMMENT	'Monto original del credito'
# MAGIC ,cuenta_contable	string	COMMENT	'Número de la cuenta contable'
# MAGIC ,ind_castigo	string	COMMENT	'indicador de castigo'
# MAGIC ,saldo_contable	decimal(18,0)	COMMENT	'El Saldo Contable corresponde a lo que queda adeudado del crédito'
# MAGIC ,saldo_moroso1	decimal(18,0)	COMMENT	'Saldo Moroso 1, Saldo Moroso hasta 29 días'
# MAGIC ,saldo_moroso2	decimal(18,0)	COMMENT	'Saldo Moroso 2, Saldo moroso desde 30 días y no en cartera vencida'
# MAGIC ,saldo_cartera_venc	decimal(18,0)	COMMENT	'saldo cartera vencida'
# MAGIC ,int_por_cobrar	decimal(18,0)	COMMENT	'Intereses por cobrar relacionados al saldo adeudado del crédito'
# MAGIC ,int_por_cobrar_venc	decimal(18,0)	COMMENT	'Intereses por cobrar vencidos relacionados al saldo adeudado moroso del crédito'
# MAGIC ,reaj_devengados	decimal(18,0)	COMMENT	'Reajustes Devengados riesgosos relacionado a la operación crediticia'
# MAGIC ,monto_no_facturado	decimal(18,0)	COMMENT	'Monto no facturado'
# MAGIC ,saldo_total_ifrs	decimal(18,0)	COMMENT	'Saldo total ifrs actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos'
# MAGIC ,saldo_capital_ifrs	decimal(18,0)	COMMENT	'Monto correspondiente al saldo capital IFRS'
# MAGIC ,saldo_int_dev_ifrs	decimal(18,0)	COMMENT	'Monto correspondiente al Interés devengados IFRS'
# MAGIC ,saldo_reajuste_ifrs	decimal(18,0)	COMMENT	'Monto correspondiente al Reajustes devengados IFRS'
# MAGIC ,num_cuotas_orig	int	COMMENT	'Numero de cuotas originales del crédito, por el cual este fue otorgado'
# MAGIC ,excl_operacion	string	COMMENT	'Indicador Exclusion Operación, Este indicador es para excluir las operaciones que se encuentran marcadas como castigadas o excluibles. posibles datos en este campo (E, C)'
# MAGIC ,cod_renegociado	string	COMMENT	'Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada'
# MAGIC ,fecha_otorgamiento	int	COMMENT	'Es la fecha conformada de año, mes y dia en la que corresponde a la fecha de curse o última renovación'
# MAGIC ,fecha_inicio_mora	int	COMMENT	'Cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,fecha_data	int	COMMENT	'Fecha compuesta de año, mes, dia que corresponde a la fecha en la que pertenece el registro de origen'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente con cartera deteriorada'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente con cartera deteriorada'
# MAGIC ,ind_tipo_deuda	string	COMMENT	'indicador tipo deuda'
# MAGIC ,tasa_interes_efectiva	decimal(9,4)	COMMENT	'Tasa de Interes efectiva (con cuatro decimales)'
# MAGIC ,tasa_interes	decimal(9,4)	COMMENT	'Tasa de Interes (con cuatro decimales)'
# MAGIC ,codigo_moneda	string	COMMENT	'Codigo de "Monedas" de la SBIF, correspondiente al crédito relacionado al cliente, se componen de tres dígitos, los cuales son (072, 082, 102, 142, 994, 998, 999)'
# MAGIC ,num_cuotas_pend	int	COMMENT	'numero de cuotas pendientes'
# MAGIC ,codigo_banca	string	COMMENT	'Código de banca relacionada a la operación'
# MAGIC ,ind_cartdet	string	COMMENT	'Indicador de cartera deteriorada. cuando la operación no existe, se identifica con una N, de lo contrario es D'
# MAGIC ,cod_calificacion	string	COMMENT	'Código de calificación del cliente, el cual puede estar representado por alguna de estas siglas (B4, C1, C2, C3, C4, C5, C6)'
# MAGIC ,cic_cliente	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual esta representado por un número'
# MAGIC ,dias_mora	int	COMMENT	'Días mora se obtiene, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la fecha de inicio de mora, código de sistema y saldo contable de la operación obtenida del D00. Los valores se expresan en números'
# MAGIC ,fecha_extincion	int	COMMENT	'Fecha conformada de año, mes y día que corresponde al termino del crédito'
# MAGIC ,fecha_cart_venc 	int	COMMENT	'Fecha conformada de año, mes y dia, que identifica el paso a Cartera Vencida de la operación'
# MAGIC ,venc_impagos	int	COMMENT	'Numero de cuotas que están impagas relacionadas al credito'
# MAGIC ,cuotas_amort_por_vencer	int	COMMENT	'Número de cuotas amortiz. por vencer'
# MAGIC ,monto_total_mora	decimal(18,0)	COMMENT	'Monto total mora, se obtiene directo de tabla de salida del proceso SL1, donde se suman los montos morosos de la operación obtenida del D00. los valore se expresan en montos'
# MAGIC ,monto_venc_mes	decimal(18,0)	COMMENT	'Monto Vencimientos del Mes'
# MAGIC ,operacion_original	string	COMMENT	'Operación original, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la vigencia de la operación obtenida del D00. los valores expresados en este campo están compuestos de letras y numeros'
# MAGIC ,cod_cartdet_cliente	string	COMMENT	'Código cartera deteriorada del cliente. cuando el dato no exista, se deja un vacío'
# MAGIC ,cod_motivo_cartdet	string	COMMENT	'Código de motivo cartera deteriorada. cuando el dato no exista, se deja un vacío'
# MAGIC ,fecha_cartdet_ope	string	COMMENT	'Fecha de operación cartera deteriorada. cuando el dato no exista, se deja un 19000101'
# MAGIC ,ind_trazabilidad	string	COMMENT	'Indicador de trazabilidad de la operación obtenida del D00, el cual contiene un 1 o un 0'
# MAGIC ,cod_salida_cartdet	string	COMMENT	'Código de salida cartera deteriorada. cuando el dato no exista, se deja un cero'
# MAGIC ,fecha_salida_cartdet	int	COMMENT	'Fecha de salida cartera deteriorada. cuando el dato no exista, se deja un 19000101'
# MAGIC ,codigo_matriz_prov	string	COMMENT	'Código de matriz que es un identificador de cada clientes para evaluaciones posteriores. el cual se obtiene en base a una fórmula, la cual esta expresada en la pestaña ReglasValidaciones. Valores posibles de este campo (MAT_COM_IND_STD, MAT_LEA_STD, MOD_INT, MAT_HIP_STD, MAT_ESTU_STD, MAT_COM_GEN_STD'
# MAGIC ,fecha_cartdet_cliente	int		
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT ' Tabla que contiene la informacion de segmentacion del cliente'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_d00_segmentado';	

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cliente_consolidado

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cliente_consolidado (			
# MAGIC  periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,cic_cliente	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,banca	string	COMMENT	'Sigla correspondiente al código de banca d00 al que pertenece el cliente, datos de ejemplo (BC, EMQ, PME)'
# MAGIC ,calificacion_bci	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,calificacion_regulador	string	COMMENT	'Codigo de calificación de riesgo identificada por la CMF con respecto al cliente, la cual es identificada por parámetro. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,fecha_calificacion	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC ,maximo_dias_mora	int	COMMENT	'se resta la fecha inicio de mora (fecha_inicio_mora) menos la fecha de cierre del mes (fecha_cierre) + 1 de cada operación y luego se selecciona la mayor mora del cliente'
# MAGIC ,deuda_factoring	decimal(18,0)	COMMENT	'total deuda factoring. se obtiene con la suma del saldo neto (saldo_neto ) del cliente deudor con la suma del saldo neto (saldo_neto ) del cliente directo'
# MAGIC ,deuda_ssff	decimal(18,0)	COMMENT	'Monto total de la deuda informada por el sistema financiero al mes de cierre, la cual ya viene calculada de su origen'
# MAGIC ,saldo_mora_1	decimal(18,0)	COMMENT	'Saldo Moroso 1, Saldo Moroso hasta 29 días'
# MAGIC ,saldo_mora_2	decimal(18,0)	COMMENT	'Saldo Moroso 2, Saldo moroso desde 30 días y no en cartera vencida'
# MAGIC ,saldo_cartera_vencida	decimal(18,0)	COMMENT	'saldo cartera vencida'
# MAGIC ,saldo_castigado	decimal(18,0)	COMMENT	'saldo castigado. se obtiene con la suma del campo sl1_saldo_total_ifrs cuando el sl1_ind_castigo es igual a N, de lo contrario se deja un cero. todo esto agrupado por fecha de proceso, periodo de proceso, cic del cliente y rut del cliente'
# MAGIC ,dsf_dvgn	decimal(18,0)	COMMENT	'deuda directa vigente nacional (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dvcn	decimal(18,0)	COMMENT	'deuda directa vencida nacional (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dvgx	decimal(18,0)	COMMENT	'deuda directa vigente extrajera (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dvcx	decimal(18,0)	COMMENT	'deuda directa vencida extrajera (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_cdir	decimal(18,0)	COMMENT	'deuda en castigo (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dmor	decimal(18,0)	COMMENT	'deuda en mora entre 60 y 90 dìas (dsf)'
# MAGIC ,deuda_estud_90_180	decimal(18,0)	COMMENT	'deuda estudiantil directa vencida la se clasifica al estar entre 90 y 180 días'
# MAGIC ,deuda_estud_180	decimal(18,0)	COMMENT	'deuda estudiantil directa vencida mayor a 180 días'
# MAGIC ,deuda_estud_30_90	decimal(18,0)	COMMENT	'deuda estudiantil directa vencida entre 30 y 90 días'
# MAGIC ,sgc_sdbc_fec_fcal	int	COMMENT	'Año, mes, dia correspondiente a la fecha de calificación relacionada al cliente, la cual proviene de la SGC'
# MAGIC ,sgc_sdbc_cod_banc	string	COMMENT	'código banca (sgc) código numérico correspondiente al la sigla de la banca d00 al que pertenece el cliente, datos de ejemplo (07,13,16, 04 )'
# MAGIC ,sgc_sdbc_cod_sbifc	string	COMMENT	'Corresponde a códigos sbif informados en la SGC, los cuales se ven expresados de forma alfanuméricas, como por ejemplo (A5, B1, C3)'
# MAGIC ,flag_pertenencia	int	COMMENT	'Cuando d00.rut_cliente pertenece a Factoring (rut_cliente o rut_deudor) se pone un 1. Cuando el cliente ( Factoring (rut_cliente o rut_deudor)) no se encuentra en D00 se pone un 2'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla, a nivel de cliente, que contiene información de deudas externas al bci, como indicadores CMF, deuda estudiantil, deuda SSFF y Factoring. Contiene información demográfica del cliente, como calificaciones internas y externas. Esta tabla se utiliza para el cálculo de los promedios de deuda externa que se almacena en tabla tbl_segmentacion_deuda_cliente. '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cliente_consolidado';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_segmentacion_cliente

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_segmentacion_cliente (			
# MAGIC  periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente con cartera deteriorada'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente con cartera deteriorada'
# MAGIC ,cic_cliente	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,segmento_cliente	string	COMMENT	'Indicador del segmento actual del cliente (I, G)'
# MAGIC ,nuevo_segmento	string	COMMENT	'indicador del nuevo segmento del cliente (I, G)'
# MAGIC ,tipo_cliente	string	COMMENT	'Indicador del cliente, el cual puede ser nuevo o antiguo (N, A)'
# MAGIC ,motivo_segmentacion	string	COMMENT	'Breve descripcion de la banca a la que pertenece el cliente (Cliente es banca individual, Grupal)'
# MAGIC ,flag_pertenencia	int	COMMENT	'Cuando d00.rut_cliente pertenece a Factoring (rut_cliente o rut_deudor) se pone un 1. Cuando el cliente ( Factoring (rut_cliente o rut_deudor)) no se encuentra en D00 se pone un 2'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene la informacion del segmento antiguo y el segmento actual del cliente '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_segmentacion_cliente';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_d00_segmentado_pant

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_d00_segmentado_pant (			
# MAGIC  periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,origen_deterioro	int	COMMENT	'Origen de deterioro'
# MAGIC ,criterio_entrada	int	COMMENT	'Criterio de entrada a deterioro por operación'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,ind_cartdet	string	COMMENT	'Indicador de cartera deteriorada. cuando la operación no existe, se identifica con una N, de lo contrario es D'
# MAGIC ,tipo_producto	string	COMMENT	'Tipo de producto actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, el codigo de sistema, el tipo de crédito, la macro sistema y la cuenta contable de la operación obtenida del D00. se expresan valores de ejemplo (ACT, CTG)'
# MAGIC ,tipo_cartera	string	COMMENT	'Tipo de cartera actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (COM, HIP, CON)'
# MAGIC ,cod_renegociado	string	COMMENT	'Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada'
# MAGIC ,tipo_credito	integer	COMMENT	'Tipo de crédito actualizado, se obtiene del proceso SL1 a travez del proceso de segmentacion, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)'
# MAGIC ,saldo_moroso2	decimal(18,0)	COMMENT	'Saldo Moroso 2, Saldo moroso desde 30 días y no en cartera vencida'
# MAGIC ,saldo_cartera_venc	decimal(18,0)	COMMENT	'Saldo Moroso 2, Saldo moroso desde 30 días y no en cartera vencida'
# MAGIC ,num_cuotas_pend	integer	COMMENT	'saldo cartera vencida'
# MAGIC ,cuotas_amort_por_vencer	integer	COMMENT	'Número de cuotas amortiz. por vencer'
# MAGIC ,cuenta_contable	string	COMMENT	'Número de la cuenta contable'
# MAGIC ,saldo_capital_ifrs	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS'
# MAGIC ,saldo_int_dev_ifrs	decimal(18,0)	COMMENT	'Interés devengados IFRS'
# MAGIC ,saldo_reajuste_ifrs	decimal(18,0)	COMMENT	'Reajustes devengados IFRS'
# MAGIC ,monto_no_facturado	decimal(18,0)	COMMENT	'Monto no facturado'
# MAGIC ,fecha_inicio_mora	integer	COMMENT	'Cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,dias_mora	integer	COMMENT	'Días mora se obtiene, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la fecha de inicio de mora, código de sistema y saldo contable de la operación obtenida del D00. Los valores se expresan en números'
# MAGIC ,fecha_cart_venc	integer	COMMENT	'Fecha conformada de año, mes y dia, que identifica el paso a Cartera Vencida de la operación'
# MAGIC ,cont_pag_cons	integer	COMMENT	'como infomacion, se ingresa un 0, para indicar que no tiene pagos consistentes'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT ' Tabla que contiene la informacion de segmentacion del cliente'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_d00_segmentado_pant';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cliente_consolidado_pant

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cliente_consolidado_pant (			
# MAGIC  periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,cic_cliente	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,banca	string	COMMENT	'Sigla correspondiente al código de banca d00 al que pertenece el cliente, datos de ejemplo (BC, EMQ, PME)'
# MAGIC ,calificacion_bci	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,calificacion_regulador	string	COMMENT	'Codigo de calificación de riesgo identificada por la CMF con respecto al cliente, la cual es identificada por parámetro. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,fecha_calificacion	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC ,maximo_dias_mora	int	COMMENT	'se resta la fecha inicio de mora (fecha_inicio_mora) menos la fecha de cierre del mes (fecha_cierre) + 1 de cada operación y luego se selecciona la mayor mora del cliente'
# MAGIC ,deuda_factoring	decimal(18,0)	COMMENT	'total deuda factoring. se obtiene con la suma del saldo neto (saldo_neto ) del cliente deudor con la suma del saldo neto (saldo_neto ) del cliente directo'
# MAGIC ,deuda_ssff	decimal(18,0)	COMMENT	'Monto total de la deuda informada por el sistema financiero al mes de cierre, la cual ya viene calculada de su origen'
# MAGIC ,saldo_mora_1	decimal(18,0)	COMMENT	'Saldo Moroso 1, Saldo Moroso hasta 29 días'
# MAGIC ,saldo_mora_2	decimal(18,0)	COMMENT	'Saldo Moroso 2, Saldo moroso desde 30 días y no en cartera vencida'
# MAGIC ,saldo_cartera_vencida	decimal(18,0)	COMMENT	'saldo cartera vencida'
# MAGIC ,saldo_castigado	decimal(18,0)	COMMENT	'saldo castigado. se obtiene con la suma del campo sl1_saldo_total_ifrs cuando el sl1_ind_castigo es igual a N, de lo contrario se deja un cero. todo esto agrupado por fecha de proceso, periodo de proceso, cic del cliente y rut del cliente'
# MAGIC ,dsf_dvgn	decimal(18,0)	COMMENT	'deuda directa vigente nacional (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dvcn	decimal(18,0)	COMMENT	'deuda directa vencida nacional (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dvgx	decimal(18,0)	COMMENT	'deuda directa vigente extrajera (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dvcx	decimal(18,0)	COMMENT	'deuda directa vencida extrajera (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_cdir	decimal(18,0)	COMMENT	'deuda en castigo (dsf), la cual proviene directamente de sbif'
# MAGIC ,dsf_dmor	decimal(18,0)	COMMENT	'deuda en mora entre 60 y 90 dìas (dsf)'
# MAGIC ,deuda_estud_90_180	decimal(18,0)	COMMENT	'deuda estudiantil directa vencida la se clasifica al estar entre 90 y 180 días'
# MAGIC ,deuda_estud_180	decimal(18,0)	COMMENT	'deuda estudiantil directa vencida mayor a 180 días'
# MAGIC ,deuda_estud_30_90	decimal(18,0)	COMMENT	'deuda estudiantil directa vencida entre 30 y 90 días'
# MAGIC ,sgc_sdbc_fec_fcal	int	COMMENT	'Año, mes, dia correspondiente a la fecha de calificación relacionada al cliente, la cual proviene de la SGC'
# MAGIC ,sgc_sdbc_cod_banc	string	COMMENT	'código banca (sgc) código numérico correspondiente al la sigla de la banca d00 al que pertenece el cliente, datos de ejemplo (07,13,16, 04 )'
# MAGIC ,sgc_sdbc_cod_sbifc	string	COMMENT	'Corresponde a códigos sbif informados en la SGC, los cuales se ven expresados de forma alfanuméricas, como por ejemplo (A5, B1, C3)'
# MAGIC ,flag_pertenencia	int	COMMENT	'Cuando d00.rut_cliente pertenece a Factoring (rut_cliente o rut_deudor) se pone un 1. Cuando el cliente ( Factoring (rut_cliente o rut_deudor)) no se encuentra en D00 se pone un 2'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla, a nivel de cliente, del periodo anterior, que contiene información de deudas externas al bci, como indicadores CMF, deuda estudiantil, deuda SSFF y Factoring. Contiene información demográfica del cliente, como calificaciones internas y externas. Esta tabla se utiliza para el cálculo de los promedios de deuda externa que se almacena en tabla tbl_segmentacion_deuda_cliente. '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cliente_consolidado_pant';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cliente_lir

# COMMAND ----------

# MAGIC %sql			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cliente_lir (			
# MAGIC  fecha_cierre 	INT	COMMENT	'año, mes y dia correspondiente a la fecha ingresada por parametro para la ejecucion de los procesos'
# MAGIC ,rut_cliente 	  INT	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,fecha_informada INT COMMENT 'es la fecha de ultima publicacion como cliente lir'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT ' Tabla que contiene informacion de clientes que estan en cardet por la ley de quiebra'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cliente_lir';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cliente_det_ssff

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cliente_det_ssff (			
# MAGIC  fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,rut_cliente	int	COMMENT	'rut cliente deteriorado'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT ' Tabla que contiene informacion de clientes con respecto al sistema financiero'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cliente_det_ssff';			

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cliente_det_fact

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cliente_det_fact (			
# MAGIC  fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,rut_cliente	int	COMMENT	'rut cliente deteriorado'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene informacion de clientes pertenecientes a factoring '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cliente_det_fact';		

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cae_ope_det_incumplimiento

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cae_ope_det_incumplimiento (			
# MAGIC  fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786, C01361425478,D01010589707,E00000463489)'
# MAGIC ,rutdv_cliente	string	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion_ori	string	COMMENT	'Número operación original'
# MAGIC ,anio_lic	string	COMMENT	'Año correspondiente a la fecha de incumplimiento'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que contiene informacion de clientes con operaciones de incumplimiento '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cae_ope_det_incumplimiento';	

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_ope_ini_cd

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_ope_ini_cd (
# MAGIC fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cuotas_amort_por_vencer	int	COMMENT	'Número de cuotas amortizadas por vencer, las cuales son extraida por el campo fecha_informada del mes anterior'
# MAGIC ,saldo_capital_ifrs	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS, las cuales son extraida por el campo fecha_informada del mes anterior'
# MAGIC ,origen_deterioro	int	COMMENT	'Origen de deterioro'
# MAGIC ,fecha_informada	int	COMMENT	'año, mes y dia correspondiente a la fecha ingresada por parametro para la ejecuciond e los procesos'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_informada)
# MAGIC COMMENT 'Tabla histórica a nivel de operación. Contiene toda la información de clientes con sus operaciones vigentes y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Incluye la fecha de inicio de deterioro para cada una de las operaciones'
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_ope_ini_cd';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### tbl_cd_cartdet_tablon_archivos

# COMMAND ----------

# MAGIC %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_tablon_archivos(			
# MAGIC periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786, C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_cliente	String	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,cic_cliente	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,saldo_total_ifrs	decimal(18,0)	COMMENT	'Saldo total ifrs actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos'
# MAGIC ,codigo_moneda	string	COMMENT	'Codigo de "Monedas" de la SBIF, correspondiente al crédito relacionado al cliente, se componen de tres dígitos, los cuales son (072, 082, 102, 142, 994, 998, 999) '
# MAGIC ,fecha_inicio_mora	int	COMMENT	'Cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,criterio_entrada     	int	COMMENT	'Criterio de entrada a deterioro por operación'
# MAGIC ,segmento             	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,origen_deterioro     	int	COMMENT	'Indicador que se le asigna al cliente par apoder identificar el origen  por el cual ha ingresado a cartera deteriorada'
# MAGIC ,calificacion_bci     	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,calificacion_bci_pant	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6), para el periodo anterior al de proceso'
# MAGIC ,fecha_calificacion   	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC ,fec_susp_dev         	int	COMMENT	'Fecha suspencion de devengo, se ingresa 190000101'
# MAGIC ,fecha_entrada        	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cuenta_contable      	string	COMMENT	'Número de la cuenta contable'
# MAGIC ,fecha_proceso        	int	COMMENT	'En la fecha de proceso, se ingresa por parametro la fecha de sistema del dia de ejecucion, la cual se representa por yyyymmdd'
# MAGIC ,cont_pagos_cons      	int	COMMENT	'como infomacion, se ingresa un 0, para indicar que no tiene pagos consistentes'
# MAGIC ,tipo_credito         	string	COMMENT	'Tipo de crédito actualizado, se obtiene del proceso SL1 a travez del proceso de segmentacion, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)'
# MAGIC ,tipo_operacion       	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,criterio_salida      	int	COMMENT	'Indicador que se le designa a la operacion del cliente, por el cual sale de cartera deteriorada'
# MAGIC ,estado               	string	COMMENT	'Se ingresa la informacion de "Cartera Vencida", para identificar el estado de la operacion'
# MAGIC ,saldo_capital_ifrs   	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS'
# MAGIC ,saldo_int_dev_ifrs   	decimal(18,0)	COMMENT	'Interés devengados IFRS'
# MAGIC ,saldo_reajuste_ifrs  	decimal(18,0)	COMMENT	'Reajustes devengados IFRS'
# MAGIC ,saldo_reprogramado   	decimal(18,0)	COMMENT	'Monto del crédito que no es facturado'
# MAGIC ,cod_renegociado      	string	COMMENT	'Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada'
# MAGIC ,oficina_credito      	string	COMMENT	'Oficina en la cual fue otorgado el crédito, esta se compone de 3 dígitos, como por ejemplo (125, 451, 124)'
# MAGIC ,grupo                	string	COMMENT	'Grupo'
# MAGIC ,periodo_evaluacion   	string	COMMENT	'Periodo de evaluacion del cliente, al momento de estar en cartera deteriorada'
# MAGIC ,criterio_entrada_cliente	int	COMMENT	'Indicador que  se le asigna al cliente, para identifica el motivo de Criterio por el cual entra a cartera deteriorada'
# MAGIC ,fec_ini_cd 	int	COMMENT	'Fecha conformada de año, mes y dia correspondiente al dia en que el crédito entra en mora'
# MAGIC ,familia_ope	string	COMMENT	'Se agrupan las operaciones, de la siguiente forma ( cuando la operacion inicia con (V, A o C), se asigna LC_Act, cuando incia con (E0, E1) se asigna TCR_Act, cuando el tipo_credito es (02,09) se designa contingente, cuando  tipo_operacion inicia cae se asigna CAE, cuando la operacion pertenece a pagos parciales, se asigna Pago_Parcial. de lo contrario, se asigna Pago_Estructurado'
# MAGIC ,estado_cd  	string	COMMENT	'Estado cartera deteriorada'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla creada como base para la creacion de los archivos de salida, la cual contiene la mayoria de los datos procesados en Cardet '			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_tablon_archivos';	
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_vig

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_vig(
# MAGIC   periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_cliente	String	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,cic_cliente	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,saldo_total_ifrs	decimal(18,0)	COMMENT	'Saldo total ifrs actualizado, se obtiene del proceso SL1, a travez del procesos de segmentacion, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos'
# MAGIC ,codigo_moneda	string	COMMENT	'Codigo de "Monedas" de la SBIF, correspondiente al crédito relacionado al cliente, se componen de tres dígitos, los cuales son (072, 082, 102, 142, 994, 998, 999)'
# MAGIC ,fecha_inicio_mora	int	COMMENT	'Cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,criterio_entrada     	int	COMMENT	'Criterio de entrada a deterioro por operación'
# MAGIC ,segmento             	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,origen_deterioro     	int	COMMENT	'Indicador que se le asigna al cliente par apoder identificar el origen  por el cual ha ingresado a cartera deteriorada'
# MAGIC ,calificacion_bci     	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,calificacion_bci_pant	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6), para el periodo anterior al de proceso'
# MAGIC ,fecha_calificacion   	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC ,fec_susp_dev         	int	COMMENT	'Fecha suspencion de devengo, se ingresa 190000101'
# MAGIC ,fecha_entrada        	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cuenta_contable      	string	COMMENT	'Número de la cuenta contable'
# MAGIC ,fecha_proceso        	int	COMMENT	'En la fecha de proceso, se ingresa por parametro la fecha de sistema del dia de ejecucion, la cual se representa por yyyymmdd'
# MAGIC ,cont_pagos_cons      	int	COMMENT	'como infomacion, se ingresa un 0, para indicar que no tiene pagos consistentes'
# MAGIC ,tipo_credito         	string	COMMENT	'Tipo de crédito actualizado, se obtiene del proceso SL1 a travez del proceso de segmentacion, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)'
# MAGIC ,tipo_operacion       	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,criterio_salida      	int	COMMENT	'Indicador que se le designa a la operacion del cliente, por el cual sale de cartera deteriorada'
# MAGIC ,estado               	string	COMMENT	'Se ingresa la informacion de "Cartera Vencida", para identificar el estado de la operacion'
# MAGIC ,saldo_capital_ifrs   	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS'
# MAGIC ,saldo_int_dev_ifrs   	decimal(18,0)	COMMENT	'Interés devengados IFRS'
# MAGIC ,saldo_reajuste_ifrs  	decimal(18,0)	COMMENT	'Reajustes devengados IFRS'
# MAGIC ,saldo_reprogramado   	decimal(18,0)	COMMENT	'Monto del crédito que no es facturado'
# MAGIC ,cod_renegociado      	string	COMMENT	'Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada'
# MAGIC ,oficina_credito      	string	COMMENT	'Oficina en la cual fue otorgado el crédito, esta se compone de 3 dígitos, como por ejemplo (125, 451, 124)'
# MAGIC ,grupo                	string	COMMENT	'Grupo'
# MAGIC ,periodo_evaluacion   	string	COMMENT	'Periodo de evaluacion del cliente, al momento de estar en cartera deteriorada'
# MAGIC ,criterio_entrada_cliente	int	COMMENT	'Indicador que  se le asigna al cliente, para identifica el motivo de Criterio por el cual entra a cartera deteriorada'
# MAGIC ,fec_ini_cd           	int	COMMENT	'Fecha conformada de año, mes y dia correspondiente al dia en que el crédito entra en mora'
# MAGIC ,familia_ope          	string           	COMMENT	'Se agrupan las operaciones, de la siguiente forma ( cuando la operacion inicia con (V, A o C), se asigna LC_Act, cuando incia con (E0, E1) se asigna TCR_Act, cuando el tipo_credito es (02,09) se designa contingente, cuando  tipo_operacion inicia cae se asigna CAE, cuando la operacion pertenece a pagos parciales, se asigna Pago_Parcial. de lo contrario, se asigna Pago_Estructurado'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_cierre)
# MAGIC COMMENT 'Tabla a nivel de operación. Contiene toda la informacion de clientes con sus operaciones que se encuentran vigente y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Contiene informacion de riesgo como la clasificacion actual o del mes anterior, como tambien sus saldos IFRS, agrupaciones familiares de operaciones y criterios de entrada. La tabla contiene informacion de historia del cliente, la cual sirve parqa que la ejecucion actual pueda realizar una revision y verificar que la operacion que esta ingresando a cartera, se nueva o antigua.'	
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_vig';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_no_vig

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_no_vig(
# MAGIC  periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Código de sistema actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (01, 02, 03)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento inicial, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el macro sistema, tipo de cartera y código de banca de la operación obtenida del D00. se expresan valores de ejemplo (I, G)'
# MAGIC ,familia_ope           	string	COMMENT	'Campo que contiene la familia correspondiente al cliente cuando este sea BCI en su evaluación de salida. Pueden ser (Contingente; Pago_Parcial; LC_Act; Pago_Estructurado; TCR_Act y CAE).'
# MAGIC ,criterio_salida	int	COMMENT	'Codido que nos indica el por que motivo sale el cliente de deterioro. Ejemplo ( 2: Salida Deterioro operaciones de clientes individuales; 27: Salida Deterioro operaciones con saldo ifrs total cero; 30: Salida Deterioro operaciones deterioradas mes anterior y que no se informan en mes actual, etc ). '
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cic_cliente           	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,saldo_total_ifrs      	decimal(18,0)	COMMENT	'Saldo total ifrs actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos'
# MAGIC ,codigo_moneda         	string	COMMENT	'Codigo de "Monedas" de la SBIF, correspondiente al crédito relacionado al cliente, se componen de tres dígitos, los cuales son (072, 082, 102, 142, 994, 998, 999) '
# MAGIC ,calificacion_bci      	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,calificacion_bci_pant 	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6), para el periodo anterior al de proceso'
# MAGIC ,fecha_calificacion    	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC ,cuenta_contable       	string	COMMENT	'Número de la cuenta contable'
# MAGIC ,saldo_capital_ifrs    	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS'
# MAGIC ,saldo_int_dev_ifrs    	decimal(18,0)	COMMENT	'Interés devengados IFRS'
# MAGIC ,saldo_reajuste_ifrs   	decimal(18,0)	COMMENT	'Reajustes devengados IFRS'
# MAGIC ,saldo_reprogramado    	decimal(18,0)	COMMENT	'Monto del crédito que no es facturado'
# MAGIC ,cod_renegociado       	string	COMMENT	'Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada'
# MAGIC ,tipo_credito          	string	COMMENT	'Tipo de crédito actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)'
# MAGIC ,oficina_credito       	string	COMMENT	'Oficina en la cual fue otorgado el crédito, esta se compone de 3 dígitos, como por ejemplo (125, 451, 124)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_cierre)
# MAGIC COMMENT 'Tabla contiene todas las operaciones que salieron de deterioro en el periodo actual. La diferencia con la tabla tbl_cd_cartdet_crit_sal_prin es la cantidad de campos que contiene.'	
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_no_vig';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_fec_proc

# COMMAND ----------

# MAGIC %sql			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_fec_proc (			
# MAGIC nro_corr	INT	COMMENT	'Número correlativo'
# MAGIC ,fec_proc	INT	COMMENT	'Fecha proceso'
# MAGIC ,fec_ejec	INT	COMMENT	'Fecha ejecución'
# MAGIC ,ind_vig	STRING	COMMENT	'Indicador de vigencia'
# MAGIC ,fec_cie	INT	COMMENT	'Fecha cierre'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (nro_corr)			
# MAGIC COMMENT ' Tabla que contiene informacion de la fechas de ejecuciones del proceso, como tambien indicador de vigencia (N, E, S ,F ) que representa el tipo de ejecucion (N o S) = PreCierre, (E, F) = Cierre, (N, E) = No-Confirmado, (S o F) = Confirmado'			
# MAGIC LOCATION '${bci.rutaSilver}/tbl_cd_fec_proc';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_curses

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_curses (
# MAGIC   fec_proc            int COMMENT 'Fecha de realización de proceso',
# MAGIC   fecha_dato          int COMMENT 'Fecha Dato',
# MAGIC   operacion           string COMMENT 'Número de operación',
# MAGIC   cod_sistema         string COMMENT 'Código Sistema',
# MAGIC   fecha_informada     int COMMENT 'Fecha en que se produce la ingesta'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fec_proc)
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_curses';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_c4_gr_op_ing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_c4_gr_op_ing (
# MAGIC sistema           string  COMMENT 'codigo sistema si es colocacion, comex, etc',
# MAGIC num_interno_ident string  COMMENT 'codigo o numero de operación',
# MAGIC cod_criterio_det  int     COMMENT 'criterio a asignar en archivo',
# MAGIC usuario           string  COMMENT 'usuario encargado de cambio',
# MAGIC fecha_informada   int     COMMENT 'Fecha en que se produce la ingesta'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_informada)
# MAGIC COMMENT 'Tabla que contiene la informacion de operaciones que son forzadas a ingresar a cartera deteriorada'
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_c4_gr_op_ing';

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_c4_gr_op_eli

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_c4_gr_op_eli (
# MAGIC sistema           string  COMMENT 'codigo de sistema',
# MAGIC num_interno_ident string  COMMENT 'numero de operación',
# MAGIC fecha_informada   int     COMMENT 'Fecha en que se produce la ingesta'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_informada)
# MAGIC COMMENT 'Tabla que contiene la informacion de operaciones que son forzadas a salir de cartera deteriorada'
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_c4_gr_op_eli';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_ajuste_calificaciones

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_ajuste_calificaciones  (			
# MAGIC fecha_cierre	int	COMMENT	'Año, mes y dia que componen la fecha correspondiente a la información cargada',
# MAGIC rut_cliente	int	COMMENT	'Rut de cliente deteriorado',
# MAGIC sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)',
# MAGIC operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)',
# MAGIC segmento_cliente	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)',
# MAGIC segmento_operacion	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)',
# MAGIC monto	decimal(18,0)	COMMENT	'Saldo total ifrs actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos',
# MAGIC calificacion_anterior	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)',
# MAGIC calificacion_nueva	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)',
# MAGIC calificacion_regulador_nueva	string	COMMENT	'Codigo de calificación de riesgo identificada por la CMF con respecto al cliente, la cual es identificada por parámetro. Datos de ejemplo (3, 4, 5, 6)',
# MAGIC fecha_calif_bci_pre_ajuste	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo',
# MAGIC fecha_calif_bci_pos_ajuste	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo',
# MAGIC indicador_det_cal_act	string	COMMENT	'Indicador que se le asigna a la operacion para poder identificar si estaba deteriorado con la calificacion actual, ejemplo "N": No deteriorado "D": deteriorado',
# MAGIC indicador_det_cal_nueva	string	COMMENT	'Indicador que se le asigna a la operacion para poder identificar si estaba deteriorado con la calificacion nueva, ejemplo "N": No deteriorado "D": deteriorado',
# MAGIC estado	string	COMMENT	'Se ingresa la informacion de "Entra a CD" o "Sale de CD", para identificar el estado de la operacion',
# MAGIC existe_tbl_vig	string	COMMENT	'Se ingresa "SI" o "NO" dependiendo si se encuentra la operacion en la tabla work tbl_cd_cartdet_vig',
# MAGIC existe_tbl_no_vig	string	COMMENT	'Se ingresa "SI" o "NO" dependiendo si se encuentra la operacion en la tabla work tbl_cd_cartdet_no_vig'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que almacena información sobre operaciones cuya calificación ha cambiado tras un ajuste. Incluye detalles sobre la calificación anterior y nueva, así como si la operación entró o salió de deterioro.'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_ajuste_calificaciones';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cambio_ajuste_calificaciones

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cambio_ajuste_calificaciones  (			
# MAGIC    periodo_cierre					int COMMENT 'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC    ,fecha_cierre						int COMMENT 'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC    ,tipo_proceso						string  COMMENT 'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC    ,cic_cliente						string	COMMENT	'CIC cliente ( código identificador del cliente), el cual esta representado por un número'
# MAGIC    ,rut_cliente						int	COMMENT	'Rut de cliente relacionado a la calificacion'
# MAGIC    ,dv_cliente						string	COMMENT	'Digito verificador del rut del cliente'
# MAGIC    ,banca							string	COMMENT	'Banca del cliente'
# MAGIC    ,calif_bci_pre_ajuste				string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC    ,calif_regulador_pre_ajuste			string	COMMENT	'Codigo de calificación de riesgo identificada por la CMF con respecto al cliente, la cual es identificada por parámetro. Datos de ejemplo (B1, B2, A4, A6)'
# MAGIC    ,fecha_calif_bci_pre_ajuste		int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC    ,calif_bci_pos_ajuste				string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC    ,calif_regulador_pos_ajuste			string	COMMENT	'Codigo de calificación de riesgo identificada por la CMF con respecto al cliente, la cual es identificada por parámetro. Datos de ejemplo (B1, B2, A4, A6)'
# MAGIC    ,fecha_calif_bci_pos_ajuste		int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC    ,ind_calif_distinta				int	COMMENT	'Identificador de cambio de calificacion (0:sin cambio, 1: con cambio'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla que almacena información sobre clientes cuya calificación ha cambiado tras un ajuste. Incluye detalles sobre la calificación anterior y nueva.'			
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cambio_ajuste_calificaciones';

# COMMAND ----------

# MAGIC %md
# MAGIC ###tbl_cd_ope_condicion_deterioro

# COMMAND ----------

# MAGIC  %sql          
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_ope_condicion_deterioro (      
# MAGIC periodo_cierre   int   COMMENT '   año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada   '
# MAGIC ,fecha_cierre   int   COMMENT '   año, mes y dia que componen la fecha correspondiente a la información cargada   '
# MAGIC ,tipo_proceso   string   COMMENT '   El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla   '
# MAGIC ,macro_sistema   string   COMMENT '   El Marco sistema corresponde al origen de la información, si este viene del D00 Bci o del D00 Leasing, identificado como (BCI, LEA)   '
# MAGIC ,tipo_cartera   string   COMMENT '   Tipo de cartera indica a la cartera que pertenece la operacion, ejemplo (CON,COM,HIP)   '
# MAGIC ,tipo_producto   string   COMMENT '   Tipo producto indica si es una deuda activa o contingente (ACT,CTG)   '
# MAGIC ,segmento   string   COMMENT '   Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)   '
# MAGIC ,cod_proceso   string   COMMENT '   Código de proceso inicial, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el macro sistema, el código de sistema y el tipo de operacion de la operación obtenida del D00. Se expresan valores de ejemplo (GR_COM, GR_HIP, GR_CON)   '
# MAGIC ,sistema   string   COMMENT '   Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)   '
# MAGIC ,tipo_credito   string   COMMENT '   Tipo de crédito actualizado, se obtiene del proceso SL1 a travez del proceso de segmentacion, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)   '
# MAGIC ,operacion   string   COMMENT '   Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786, C01361425478,D01010589707,E00000463489)   '
# MAGIC ,tipo_operacion   string   COMMENT '   Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)   '
# MAGIC ,cuenta_contable	string	COMMENT '	Identifica el codigo de la cuenta contable asociada a la cuenta de capital del producto	'
# MAGIC ,saldo_cartera_venc   decimal(18,0)   COMMENT '   Muestra el monto en saldo cartera vencida   '
# MAGIC ,saldo_total_ifrs   decimal(18,0)   COMMENT '   Saldo total ifrs actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos   '
# MAGIC ,cod_renegociado   string   COMMENT '   Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada  "C": Renegociado prov castigo no activado   '
# MAGIC ,fecha_otorgamiento   int   COMMENT '   Es la fecha conformada de año, mes y dia en la que corresponde a la fecha de curse o última renovación   '
# MAGIC ,fecha_inicio_mora   int   COMMENT '   Cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora   '
# MAGIC ,rut_cliente   int   COMMENT '   Rut de cliente deudor directo principal   '
# MAGIC ,dv_rut_cliente   string   COMMENT '   Digito verificador cliente con cartera deteriorada   '
# MAGIC ,cod_calificacion   string   COMMENT '   Código de calificación del cliente, el cual puede estar representado por alguna de estas siglas (B4, C1, C2, C3, C4, C5, C6)   '
# MAGIC ,dias_mora   int   COMMENT '   Días mora se obtiene, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la fecha de inicio de mora, código de sistema y saldo contable de la operación obtenida del D00. Los valores se expresan en números   '
# MAGIC ,fecha_cart_venc   int   COMMENT '   Fecha conformada de año, mes y dia, que identifica el paso a Cartera Vencida de la operación   '
# MAGIC ,ope_dias_curse_pact   int   COMMENT '   Dias de curse desde la fecha proceso a la fecha de otorgamiento solo para operaciones nuevas. Se utiliza para calcular deterioro renegociado   '
# MAGIC ,flag_existe_operacion_pant   int   COMMENT '   Flag que indica si la operacion fue informada en d00 del periodo anterior   '
# MAGIC ,ope_ind_cartdet_pant   string   COMMENT '   Marca de deterioro del periodo anterior. D: deteriorada   '
# MAGIC ,ope_fecha_cart_venc_pant   int   COMMENT '   Fecha de cartera vencida informada en periodo anterior   '
# MAGIC ,flag_existe_operacion_rfz   int   COMMENT '   Flag que indica si la operacion es informada en location de restruccturacion forzosa, tbl_cd_curses   '
# MAGIC ,ope_fec_proc_rfz   int   COMMENT '   Fecha informada en location de restruccturacion forzosa, tbl_cd_curses   '
# MAGIC ,flag_existe_cliente_seg_cli   int   COMMENT '   Flag que indica si cliente esta informado en tbl_cd_segmentacion_cliente   '
# MAGIC ,cli_segmento_cliente   string   COMMENT '   Informa el segmento del cliente contenido en tbl_cd_segmentacion_cliente   '
# MAGIC ,flag_existe_cliente_cli_con   int   COMMENT '   Flag que indica si cliente esta informado en tbl_cd_cliente_consolidado   '
# MAGIC ,cli_calificacion_bci   string   COMMENT '   Informa la calificacion del cliente contenido en tbl_cd_cliente_consolidado   '
# MAGIC ,flag_existe_cliente_lir   int   COMMENT '   Flag que indica si cliente esta informado en tbl_cd_cliente_lir   '
# MAGIC ,cli_fecha_informada_lir   int   COMMENT '   Informa la ultima fecha del cliente informado como LIR en tbl_cd_cliente_lir   '
# MAGIC ,flag_existe_cliente_ssff   int   COMMENT '   Flag que indica si cliente esta informado en tbl_cd_cliente_det_ssff   '
# MAGIC ,flag_existe_cliente_fact   int   COMMENT '   Flag que indica si cliente esta informado en tbl_cd_cliente_det_fact   '
# MAGIC ,max_dias_curse_pact   int   COMMENT '   Contiene el maximo dias de curse, para operaciones renegociadas nuevas, del periodo actual   '
# MAGIC ,max_dia_mor_pant   int   COMMENT '   Contiene el maximo dias de mora, considerando todas las operaciones del cliente, del periodo anterior   '
# MAGIC ,max_dia_mora_ren   int   COMMENT '   Contien la suma de los campos max_dias_curse_pact, max_dia_mor_pant. Este dato es excluisivo para el calculo de las operaciones renegociadas nuevas.   '
# MAGIC ,flag_cae_excepcion   int   COMMENT '   Flag que indica si la operacion es informada en location de cae excepciones incumplimiento  '
# MAGIC )         
# MAGIC USING DELTA         
# MAGIC PARTITIONED BY (fecha_cierre)         
# MAGIC COMMENT ' Tabla que contiene toda la informacion utilizada para el analisis de entrada a deterioro'         
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_ope_condicion_deterioro';  

# COMMAND ----------

# MAGIC %md
# MAGIC ###tbl_cd_ope_condicion_salida_deterioro

# COMMAND ----------

# MAGIC  %sql          
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_ope_condicion_salida_deterioro (      
# MAGIC  periodo_cierre int COMMENT ' año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada '
# MAGIC ,fecha_cierre int COMMENT ' año, mes y dia que componen la fecha correspondiente a la información cargada '
# MAGIC ,tipo_proceso string COMMENT ' El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla '
# MAGIC ,macro_sistema string COMMENT ' El Marco sistema corresponde al origen de la información, si este viene del D00 Bci o del D00 Leasing, identificado como (BCI, LEA) '
# MAGIC ,segmento string COMMENT ' Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G) '
# MAGIC ,operacion string COMMENT ' Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786, C01361425478,D01010589707,E00000463489) '
# MAGIC ,tipo_operacion string COMMENT ' Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604) '
# MAGIC ,sistema string COMMENT ' Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05) '
# MAGIC ,rut_cliente int COMMENT ' Rut de cliente con cartera deteriorada '
# MAGIC ,dv_rut_cliente string COMMENT ' Digito verificador cliente con cartera deteriorada '
# MAGIC ,cuenta_contable string COMMENT ' Número de la cuenta contable '
# MAGIC ,ind_castigo string COMMENT ' indicador de castigo '
# MAGIC ,saldo_cartera_venc decimal(18,0) COMMENT ' saldo cartera vencida '
# MAGIC ,saldo_total_ifrs decimal(18,0) COMMENT ' Saldo total ifrs actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos '
# MAGIC ,saldo_capital_ifrs decimal(18,0) COMMENT ' Monto correspondiente al saldo capital IFRS '
# MAGIC ,cod_renegociado string COMMENT ' Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada '
# MAGIC ,fecha_otorgamiento int COMMENT ' Es la fecha conformada de año, mes y dia en la que corresponde a la fecha de curse o última renovación '
# MAGIC ,fecha_inicio_mora int COMMENT ' Cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora '
# MAGIC ,dias_mora int COMMENT ' Días mora se obtiene, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la fecha de inicio de mora, código de sistema y saldo contable de la operación obtenida del D00. Los valores se expresan en números '
# MAGIC ,cuotas_amort_por_vencer int COMMENT ' Número de cuotas amortiz. por vencer '
# MAGIC ,tipo_credito string COMMENT ' Tipo de crédito actualizado, se obtiene del proceso SL1 a travez del proceso de segmentacion, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05) '
# MAGIC ,num_cuotas_orig int COMMENT ' Numero de cuotas originales del crédito, por el cual este fue otorgado '
# MAGIC ,fecha_extincion int COMMENT ' Fecha conformada de año, mes y día que corresponde al termino del crédito '
# MAGIC ,tipo_producto string COMMENT ' Tipo de producto actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, el codigo de sistema, el tipo de crédito, la macro sistema y la cuenta contable de la operación obtenida del D00. se expresan valores de ejemplo (ACT, CTG) '
# MAGIC ,saldo_moroso1 decimal(18,0) COMMENT ' Saldo Moroso 1, Saldo Moroso hasta 29 días '
# MAGIC ,saldo_moroso2 decimal(18,0) COMMENT ' Saldo Moroso 2, Saldo moroso desde 30 días y no en cartera vencida '
# MAGIC ,num_cuotas_pend int COMMENT ' numero de cuotas pendientes de pago '
# MAGIC ,criterio_entrada_cliente int COMMENT 'Indicador de entrada a deterioro definido a nivel de cliente, donde el indicador es un numero, por ejemplo (9 = renegociado, 8 Cartera Vencida; 12 = SSFF)'
# MAGIC ,flag_ope_new_pact int COMMENT ' Es un flag que indica si la operacion es nueva en periodo actual. No fue informada en periodo anterior. '
# MAGIC ,flag_ope_ren_pact int COMMENT ' Es un flag que indica que es una operacion renegociada con fecha de otorgamiento del periodo actual '
# MAGIC ,ope_num_meses_en_cd bigint COMMENT ' Contiene la cantidad de meses que ha estado en cartera deteriorada '
# MAGIC ,flag_ope_cta_par int COMMENT ' Es un flag que indica si la operacion es clasificada con pagos parciales '
# MAGIC ,cont_pag_cons int COMMENT ' Contiene la cantidad de pagos consecutivos realizados a la operacion '
# MAGIC ,ope_fecha_entrada_cd_pant int COMMENT ' Contiene la fecha de entrada a cartera deteriorada informada en periodo anterior '
# MAGIC ,num_cuotas_pend_pant int COMMENT ' Contiene la cantidad de cuotas pendientes de pago informadas en periodo anterior '
# MAGIC ,cuotas_amort_por_vencer_pant int COMMENT ' Contiene la cantidad de cuotas de capital por vencer informadas en periodo anterior '
# MAGIC ,cont_pag_cons_pant int COMMENT ' Contiene la cantidad de pagos consecutivos realizados a la operacion informadas en periodo anterior '
# MAGIC ,max_dia_mora_pact int COMMENT ' Contiene el maximo dias de mora del cliente, de todas sus operaciones, informadas en periodo actual '
# MAGIC ,cli_mora_sbif_f decimal(26,0) COMMENT ' Contiene el monto moroso del cliente, informado en sistema financiero, para el periodo actual. '
# MAGIC ,fecha_entrada_ope_ini_cd int COMMENT ' Contiene la fecha de entrada a cartera deteriorada para el inicio del ultimo evento de deterioro de la operacion '
# MAGIC ,cuotas_amort_por_vencer_ope_ini_cd int COMMENT ' Contiene las cuotas de capital pendientes de pago de la operacion al inicio del ultimo evento de deterioro de la operacion '
# MAGIC ,saldo_capital_ifrs_ope_ini_cd decimal(18,0) COMMENT ' Contiene el saldo de capital ifrs de la operacion al inicio del ultimo evento de deterioro de la operacion '
# MAGIC ,cli_calificacion_bci string COMMENT ' Contiene la calificacion bci del cliente en periodo actual. Ej. 1,2,...15,16 '
# MAGIC ,cli_segmento_cliente string COMMENT ' Contiene el segmento del cliente en periodo actual (G o I) '
# MAGIC ,flag_existe_cliente_ssff int COMMENT ' Es un flag que indica que el cliente es informado con deterioro por SSFF en periodo actual '
# MAGIC ,flag_existe_cliente_fact int COMMENT ' Es un flag que indica que el cliente es informado con deterioro por Factoring en periodo actual '
# MAGIC ,flag_existe_cliente_lir int COMMENT ' Es un flag que indica que si el cliente esta informado en la fuente LIR '
# MAGIC ,cli_fecha_informada_lir int COMMENT ' Corresponde a la fecha de la ultima vez que el cliente fue informado en la fuente LIR '
# MAGIC ,ope_pag_parciales_ini_cd int COMMENT ' Contiene la cantidad de pagos de cuotas de capital desde que entro a cartera deteriorada hasta el periodo actual '
# MAGIC ,ope_pag_capital_ifrs_ini_cd decimal(19,0) COMMENT ' Contiene el saldo de capital ifrs desde que entro a cartera deteriorada hasta el periodo actual '
# MAGIC ,flag_existe_operacion_rfz int COMMENT ' Es un flag que indica que si la operacion esta informada en la fuente reestructuracion forzosa '
# MAGIC
# MAGIC )         
# MAGIC USING DELTA         
# MAGIC PARTITIONED BY (fecha_cierre)         
# MAGIC COMMENT ' Tabla que contiene toda la informacion utilizada para el analisis de salida de cartera deteriorada'         
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_ope_condicion_salida_deterioro';  

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cartdet_ope_ini_cd_pant

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cartdet_ope_ini_cd_pant (
# MAGIC fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cuotas_amort_por_vencer	int	COMMENT	'Número de cuotas amortizadas por vencer, las cuales son extraida por el campo fecha_informada del mes anterior'
# MAGIC ,saldo_capital_ifrs	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS, las cuales son extraida por el campo fecha_informada del mes anterior'
# MAGIC ,origen_deterioro	int	COMMENT	'Origen de deterioro'
# MAGIC ,fecha_informada	int	COMMENT	'año, mes y dia correspondiente a la fecha ingresada por parametro para la ejecuciond e los procesos'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha_informada)
# MAGIC COMMENT 'Tabla a nivel de operación. Contiene toda la información de clientes con sus operaciones vigentes y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Incluye la fecha de inicio de deterioro para cada una de las operaciones'
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cartdet_ope_ini_cd_pant';

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_cd_cierreriesgo_log

# COMMAND ----------

# MAGIC %sql      
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamesilver}.tbl_cd_cierreriesgo_log(     
# MAGIC periodo_cierre  int COMMENT 'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre int COMMENT 'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso string  COMMENT 'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,nombre_notebook  string  COMMENT 'Nombre del notebook correspondiente a la ejecución'
# MAGIC ,id_ejecutor  string  COMMENT 'Identificador del usuario que ha ejecutado el notebook, como por ejemplo xxxxx@bci.cl'
# MAGIC ,fecha_ejecucion  string  COMMENT 'dia, mes y año en formato "dd-mm-yyyy" correspondiente al dia que se a ejecutado el notebook'
# MAGIC ,hora_inicio  string  COMMENT 'Hora, minuto y segundo en formato "hh:mm:ss" correspondiente a la hora que inicia la ejecutado el notebook'
# MAGIC ,hora_termino string  COMMENT 'Hora, minuto y segundo en formato "hh:mm:ss" correspondiente a la hora que termina la ejecutado el notebook'
# MAGIC ,codigo_status  int COMMENT 'corresponde a un código que representa la ejecución correcta o errónea del notebook, ya sea por una regla programada o por alguna condición de sistema, como por ejemplo (0: ejecución correcta, 280444: tabla contiene 0 registros'
# MAGIC ,mensaje_status string  COMMENT 'Contiene el mensaje relacionado al campo codigo_status'
# MAGIC )     
# MAGIC USING DELTA     
# MAGIC PARTITIONED BY (fecha_cierre)     
# MAGIC COMMENT 'Tabla que contiene información correspondiente a la ejecución de los notebook, en el cual se puede identificar cuando se ha ejecutado, por quien se ha ejecutado y  si está a terminado de forma exitosa o de forma errónea.'      
# MAGIC LOCATION '${bci.ruta_silver}/tbl_cd_cierreriesgo_log';

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREA BASE DATOS GOLD

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas Gold
# MAGIC ------
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC     CREATE DATABASE IF NOT EXISTS ${bci.dbnamegold}
# MAGIC     LOCATION '${bci.ruta_gold}';

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_hcd_cartdet_no_vig

# COMMAND ----------

# MAGIC %sql			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamegold}.tbl_hcd_cartdet_no_vig(			
# MAGIC  periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Código de sistema actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (01, 02, 03)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento inicial, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el macro sistema, tipo de cartera y código de banca de la operación obtenida del D00. se expresan valores de ejemplo (I, G)'
# MAGIC ,familia_ope           	string	COMMENT	'Campo que contiene la familia correspondiente al cliente cuando este sea BCI en su evaluación de salida. Pueden ser (Contingente; Pago_Parcial; LC_Act; Pago_Estructurado; TCR_Act y CAE).'
# MAGIC ,criterio_salida	int	COMMENT	'Codido que nos indica el por que motivo sale el cliente de deterioro. Ejemplo ( 2: Salida Deterioro operaciones de clientes individuales; 27: Salida Deterioro operaciones con saldo ifrs total cero; 30: Salida Deterioro operaciones deterioradas mes anterior y que no se informan en mes actual, etc ). '
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cic_cliente           	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,saldo_total_ifrs      	decimal(18,0)	COMMENT	'Saldo total ifrs actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos'
# MAGIC ,codigo_moneda         	string	COMMENT	'Codigo de "Monedas" de la SBIF, correspondiente al crédito relacionado al cliente, se componen de tres dígitos, los cuales son (072, 082, 102, 142, 994, 998, 999) '
# MAGIC ,calificacion_bci      	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,calificacion_bci_pant 	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6), para el periodo anterior al de proceso'
# MAGIC ,fecha_calificacion    	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC ,cuenta_contable       	string	COMMENT	'Número de la cuenta contable'
# MAGIC ,saldo_capital_ifrs    	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS'
# MAGIC ,saldo_int_dev_ifrs    	decimal(18,0)	COMMENT	'Interés devengados IFRS'
# MAGIC ,saldo_reajuste_ifrs   	decimal(18,0)	COMMENT	'Reajustes devengados IFRS'
# MAGIC ,saldo_reprogramado    	decimal(18,0)	COMMENT	'Monto del crédito que no es facturado'
# MAGIC ,cod_renegociado       	string	COMMENT	'Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada'
# MAGIC ,tipo_credito          	string	COMMENT	'Tipo de crédito actualizado, se obtiene directo de tabla de salida del proceso SL1, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)'
# MAGIC ,oficina_credito       	string	COMMENT	'Oficina en la cual fue otorgado el crédito, esta se compone de 3 dígitos, como por ejemplo (125, 451, 124)'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla Historica contiene todas las operaciones que salieron de deterioro en el periodo actual. La diferencia con la tabla tbl_cd_cartdet_crit_sal_prin es la cantidad de campos que contiene.'			
# MAGIC LOCATION '${bci.ruta_gold}/tbl_hcd_cartdet_no_vig';	

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_hcd_cartdet_stock 
# MAGIC  - Esta tabla almacena el resultado de las entradas menos las salidas de deterioro grupal e individual 

# COMMAND ----------

# MAGIC %sql			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamegold}.tbl_hcd_cartdet_stock(			
# MAGIC  periodo_cierre	int	COMMENT	'Año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'Año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente con cartera deteriorada'
# MAGIC ,dv_rut_cliente	string	COMMENT	'Digito verificador cliente con cartera deteriorada'
# MAGIC ,tipo_operacion	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,criterio_entrada	int	COMMENT	'Indicador de entrada a deterioro por operacion, donde el indicador es un numero, por ejemplo (9 = renegociado, 8 Cartera Vencida; 12 = SSFF)'
# MAGIC ,origen_deterioro	int	COMMENT	'Indicador por el cual la operacion fue deteriorada, dodne el indicador es un numero, por ejemplo (1 = operaciond e origen, 5 = operacion irradiada)'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,grupo	string	COMMENT	'Inidca la regla de de negocio por el cual la operacion se debe evaluar, esta se determina de la siguiente forma (SSFF, Factoring, BCI_Grupal, Bci_Irradiacion, BCI_Individual)'
# MAGIC ,periodo_evaluacion          	string	COMMENT	'Numenclatura que indica cuando la operacion fue evaluada. Los datos que se encuentran en este campo es p_anterior o p_actual)'
# MAGIC ,criterio_entrada_cliente    	int	COMMENT	'Indicador de entrada a deterioro definido a nivel de cliente, donde el indicador es un numero, por ejemplo (9 = renegociado, 8 Cartera Vencida; 12 = SSFF)'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla Historica que contiene todo el stock de las operaciones que se encuentran deterioradas del periodo actual.'			
# MAGIC LOCATION '${bci.ruta_gold}/tbl_hcd_cartdet_stock';		

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_hcd_cartdet_vig

# COMMAND ----------

# MAGIC %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamegold}.tbl_hcd_cartdet_vig(			
# MAGIC   periodo_cierre	int	COMMENT	'año y mes del campo fecha_cierre, el cual identifica de cuando es la información cargada'
# MAGIC ,fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,tipo_proceso	string	COMMENT	'El tipo de proceso contiene la sigla C (Cierre) o PC (Precierre), que identifica de cuando es extraída la información cargada en esta tabla'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,dv_cliente	String	COMMENT	'Digito verificador cliente deteriorado'
# MAGIC ,cic_cliente	string	COMMENT	'CIC cliente ( código identificador del cliente), el cual está representado por un número'
# MAGIC ,saldo_total_ifrs	decimal(18,0)	COMMENT	'Saldo total ifrs actualizado, se obtiene del proceso SL1, a travez del procesos de segmentacion, donde se evaluó el tipo de credito y la cuenta contable de la operación obtenida del D00. sumando saldo capital ifrs, saldo ajuste ifrs, saldo reprogramado). Los valores se expresan en montos'
# MAGIC ,codigo_moneda	string	COMMENT	'Codigo de "Monedas" de la SBIF, correspondiente al crédito relacionado al cliente, se componen de tres dígitos, los cuales son (072, 082, 102, 142, 994, 998, 999)'
# MAGIC ,fecha_inicio_mora	int	COMMENT	'Cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,criterio_entrada     	int	COMMENT	'Criterio de entrada a deterioro por operación'
# MAGIC ,segmento             	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,origen_deterioro     	int	COMMENT	'Indicador que se le asigna al cliente par apoder identificar el origen  por el cual ha ingresado a cartera deteriorada'
# MAGIC ,calificacion_bci     	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6)'
# MAGIC ,calificacion_bci_pant	string	COMMENT	'Codigo de calificación de riesgo identificada por BCI relacionada al cliente, la cual es definida por el departamento de riesgo. Datos de ejemplo (3, 4, 5, 6), para el periodo anterior al de proceso'
# MAGIC ,fecha_calificacion   	int	COMMENT	'Año, mes, día correspondiente a la fecha de calificación relacionada al cliente, la cual es definida por el departamento de riesgo'
# MAGIC ,fec_susp_dev         	int	COMMENT	'Fecha suspencion de devengo, se ingresa 190000101'
# MAGIC ,fecha_entrada        	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cuenta_contable      	string	COMMENT	'Número de la cuenta contable'
# MAGIC ,fecha_proceso        	int	COMMENT	'En la fecha de proceso, se ingresa por parametro la fecha de sistema del dia de ejecucion, la cual se representa por yyyymmdd'
# MAGIC ,cont_pagos_cons      	int	COMMENT	'como infomacion, se ingresa un 0, para indicar que no tiene pagos consistentes'
# MAGIC ,tipo_credito         	string	COMMENT	'Tipo de crédito actualizado, se obtiene del proceso SL1 a travez del proceso de segmentacion, donde se evaluó la trazabilidad, la cartera y el tioaux de la operación obtenida del D00. se expresan valores de ejemplo (02, 03, 05)'
# MAGIC ,tipo_operacion       	string	COMMENT	'Identifica al tipo y sub-tipo (Tio-Aux) de operación relacionado al cliente, esta es una sigla compuesta de 6 caracteres, como por ejemplo (CON447, 110459, COM604)'
# MAGIC ,criterio_salida      	int	COMMENT	'Indicador que se le designa a la operacion del cliente, por el cual sale de cartera deteriorada'
# MAGIC ,estado               	string	COMMENT	'Se ingresa la informacion de "Cartera Vencida", para identificar el estado de la operacion'
# MAGIC ,saldo_capital_ifrs   	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS'
# MAGIC ,saldo_int_dev_ifrs   	decimal(18,0)	COMMENT	'Interés devengados IFRS'
# MAGIC ,saldo_reajuste_ifrs  	decimal(18,0)	COMMENT	'Reajustes devengados IFRS'
# MAGIC ,saldo_reprogramado   	decimal(18,0)	COMMENT	'Monto del crédito que no es facturado'
# MAGIC ,cod_renegociado      	string	COMMENT	'Es un Flag que indica en qué estado se encuentra la operación, si esta se encuentra Renegociada, si no se encuentra Renegociada, como por ejemplo "S": Renegociada "N": No Renegociada'
# MAGIC ,oficina_credito      	string	COMMENT	'Oficina en la cual fue otorgado el crédito, esta se compone de 3 dígitos, como por ejemplo (125, 451, 124)'
# MAGIC ,grupo                	string	COMMENT	'Grupo al que pertenece el cliente'
# MAGIC ,periodo_evaluacion   	string	COMMENT	'Periodo de evaluacion del cliente, al momento de estar en cartera deteriorada'
# MAGIC ,criterio_entrada_cliente	int	COMMENT	'Indicador que  se le asigna al cliente, para identifica el motivo de Criterio por el cual entra a cartera deteriorada'
# MAGIC ,fec_ini_cd           	int	COMMENT	'Fecha conformada de año, mes y dia correspondiente al dia en que el crédito entra en mora'
# MAGIC ,familia_ope          	string           	COMMENT	'Se agrupan las operaciones, de la siguiente forma ( cuando la operacion inicia con (V, A o C), se asigna LC_Act, cuando incia con (E0, E1) se asigna TCR_Act, cuando el tipo_credito es (02,09) se designa contingente, cuando  tipo_operacion inicia cae se asigna CAE, cuando la operacion pertenece a pagos parciales, se asigna Pago_Parcial. de lo contrario, se asigna Pago_Estructurado'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla a nivel de operación. Contiene toda la informacion de clientes con sus operaciones que se encuentran vigente y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Contiene informacion de riesgo como la clasificacion actual o del mes anterior, como tambien sus saldos IFRS, agrupaciones familiares de operaciones y criterios de entrada. La tabla contiene informacion de historia del cliente, la cual sirve parqa que la ejecucion actual pueda realizar una revision y verificar que la operacion que esta ingresando a cartera, se nueva o antigua.'			
# MAGIC LOCATION '${bci.ruta_gold}/tbl_hcd_cartdet_vig';			
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### tbl_hcd_cartdet_ope_ini_cd

# COMMAND ----------

# MAGIC %sql			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamegold}.tbl_hcd_cartdet_ope_ini_cd (			
# MAGIC fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,sistema	string	COMMENT	'Corresponde a un código que identifica de que sistema viene la operación relacionada al cliente, este código tiene dos dígitos, com por ejemplo (01, 02, 03, 04, 05)'
# MAGIC ,operacion	string	COMMENT	'Corresponde al número de la operacion relacionada al cliente, esta puede estar compuesta de letras y numeros, como por ejemplo (A01020073786,C01361425478,D01010589707,E00000463489)'
# MAGIC ,segmento	string	COMMENT	'Código de segmento el cual se obtiene directo de tabla de salida del proceso Segmentacion, donde se expresan valores como por ejemplo (I, G)'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,fecha_entrada	int	COMMENT	'Se calcula con el campo fecha_inicio_mora , cuando el dato es null o 0 se deja 19000101, de lo contrario se deja fecha_inicio_mora que es la fecha conformada de año, mes y dia en la que el crédito cae en mora'
# MAGIC ,cuotas_amort_por_vencer	int	COMMENT	'Número de cuotas amortizadas por vencer, las cuales son extraida por el campo fecha_informada del mes anterior'
# MAGIC ,saldo_capital_ifrs	decimal(18,0)	COMMENT	'Contiene el monto que representa el Saldo capital IFRS, las cuales son extraida por el campo fecha_informada del mes anterior'
# MAGIC ,origen_deterioro	int	COMMENT	'Origen de deterioro'
# MAGIC ,fecha_informada	int	COMMENT	'año, mes y dia correspondiente a la fecha ingresada por parametro para la ejecuciond e los procesos'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_informada)			
# MAGIC COMMENT 'Tabla histórica a nivel de operación. Contiene toda la información de clientes con sus operaciones vigentes y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Incluye la fecha de inicio de deterioro para cada una de las operaciones'			
# MAGIC LOCATION '${bci.ruta_gold}/tbl_hcd_cartdet_ope_ini_cd';	

# COMMAND ----------

# MAGIC %md
# MAGIC ###tbl_hcd_cartdet_cli_ini_cd

# COMMAND ----------

# MAGIC  %sql 			
# MAGIC CREATE TABLE IF NOT EXISTS ${bci.dbnamegold}.tbl_hcd_cartdet_cli_ini_cd (			
# MAGIC  fecha_cierre	int	COMMENT	'año, mes y dia que componen la fecha correspondiente a la información cargada'
# MAGIC ,rut_cliente	int	COMMENT	'Rut de cliente deteriorado'
# MAGIC ,fec_ini_cd                  	int	COMMENT	'Fecha conformada de año, mes y dia correspondiente al dia en que el crédito entra en mora'
# MAGIC ,fecha_informada	int	COMMENT	'año, mes y dia correspondiente a la fecha ingresada por parametro para la ejecucion de los procesos'
# MAGIC )			
# MAGIC USING DELTA			
# MAGIC PARTITIONED BY (fecha_cierre)			
# MAGIC COMMENT 'Tabla histórica a nivel de cliente. Contiene información de los clientes que se encuentran vigentes y en cartera deteriorada, ya sea por un nuevo ingreso o por arrastre del periodo anterior. Incluye la fecha de inicio de deterioro más antigua por cliente.'			
# MAGIC LOCATION '${bci.ruta_gold}/tbl_hcd_cartdet_cli_ini_cd';		

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")