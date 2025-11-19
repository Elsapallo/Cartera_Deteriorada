# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_01_Valida_Parametros
# MAGIC **************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_01_Valida_Parametros.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011899085
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 30/08/2022
# MAGIC * Descripcion: Valida todos los parámetros de entrada ingresados en el orquestador. Estos parámetros de entrada son enviados después en los demás notebooks de procesamiento.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gagriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se modifica cancelacion error agregando el comando (raise)     
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * 
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * riesgobdu_silver_db.tbl_cierreriesgo_log
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Dependencias

# COMMAND ----------

from datetime import datetime

diaIniX = datetime.now()
hora_inicio = diaIniX.strftime('%H:%M:%S')
print(hora_inicio)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametría

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setea Parámetros

# COMMAND ----------


dbutils.widgets.text("fecha_w","","01-Fecha:")
dbutils.widgets.text("bd_silver_w","","02-Nombre BD Silver:")
dbutils.widgets.text("bd_golden_w","","03-Nombre BD Gold:")
dbutils.widgets.text("tipo_proceso_w","C","04-Tipo de Proceso (C o PC):")
dbutils.widgets.text("guardar_log_w","S","05-Guarda Log en BD (S o N):")

fecha_x = dbutils.widgets.get("fecha_w") 
base_silver_x = dbutils.widgets.get("bd_silver_w")
base_golden_x = dbutils.widgets.get("bd_golden_w")
tipo_proceso_x = dbutils.widgets.get("tipo_proceso_w")
guardar_log_x = dbutils.widgets.get("guardar_log_w")

spark.conf.set("bci.fecha", fecha_x)
spark.conf.set("bci.dbnamesilver", base_silver_x)
spark.conf.set("bci.dbnamegold", base_golden_x)
spark.conf.set("bci.tipo_proceso", tipo_proceso_x)
spark.conf.set("bci.guarda_log", guardar_log_x)

print(f"fecha_x: {fecha_x}")
print(f"base_silver_x: {base_silver_x}")
print(f"base_golden_x: {base_golden_x}")
print(f"tipo_proceso_x: {tipo_proceso_x}")
print(f"guardar_log_x: {guardar_log_x}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga funciones comunes

# COMMAND ----------

# MAGIC %run "./Funciones_Comunes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valida Parámetros

# COMMAND ----------

# MAGIC %md
# MAGIC #### fecha_x

# COMMAND ----------

valida_parametro(fecha_x, 'Fecha')

# COMMAND ----------

# MAGIC %md
# MAGIC #### base_silver_x

# COMMAND ----------

valida_parametro(base_silver_x, 'Base de Datos Silver')

# COMMAND ----------

# MAGIC %md
# MAGIC #### base_golden_x

# COMMAND ----------

valida_parametro(base_golden_x, 'Base de Datos Gold')

# COMMAND ----------

# MAGIC %md
# MAGIC #### tipo_proceso_x

# COMMAND ----------

valida_parametro(tipo_proceso_x, 'Tipo de Proceso')

# COMMAND ----------

# MAGIC %md
# MAGIC #### guardar_log_x

# COMMAND ----------

valida_parametro(guardar_log_x, 'Flag Log')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida Fecha Válida

# COMMAND ----------

valida_fecha_entrada(fecha_x)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida "tipo_proceso_x" Válido

# COMMAND ----------

if tipo_proceso_x not in ["C","PC"]:
    raise ValueError("{\"coderror\":28016, \"msgerror\":\"Tipo de proceso inválido: "+str(tipo_proceso_x)+" .Valores permitidos: C o PC\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida "guardar_log_x" Válido

# COMMAND ----------

if guardar_log_x not in ["S","N"]:
    raise ValueError("{\"coderror\":28016, \"msgerror\":\"Flag de Log inválido: "+str(guardar_log_w)+" .Valores permitidos: S o N\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Valida Base de Datos

# COMMAND ----------

# MAGIC %md
# MAGIC ### base_silver_x

# COMMAND ----------

valida_bd(base_silver_x)

# COMMAND ----------

# MAGIC %md
# MAGIC ### base_golden_x

# COMMAND ----------

valida_bd(base_golden_x)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")