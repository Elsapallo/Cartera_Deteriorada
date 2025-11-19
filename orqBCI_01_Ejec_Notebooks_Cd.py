# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: orqBCI_01_Ejec_Notebooks_Cd
# MAGIC **************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: orqBCI_01_Ejec_Notebooks_Cd.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011900118
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 31/03/2023
# MAGIC * Descripcion: Orquestador Cartera Deteriorada 
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 21/10/2025 
# MAGIC * Descripción: Se modifica el proceso para incorporar dos nuevas ejecuciones (notebooks), que realizan el forzaje a Cartera Deterioda.
# MAGIC ************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC * 
# MAGIC
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC * 
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga Dependencias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga liberías

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tiempo de inicio del proceso

# COMMAND ----------

from datetime import datetime

hora_ini = datetime.now()
dia_ejecucion_x = hora_ini.strftime('%Y%m%d')
hora_ini_x = hora_ini.strftime('%H%M%S')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setea Parámetros

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("fecha_w","20251030","01-Fecha:")
dbutils.widgets.text("tipo_proceso_w","C","02-Tipo de Proceso (C o PC):")

dbutils.widgets.text("bd_silver_w","dsr_gld_bciwork_db","03-Nombre BD bciwork:")
dbutils.widgets.text("bd_golden_w","dsr_gld_prodservicios_db","04-Nombre BD prodservicios:")
dbutils.widgets.text("bd_golden_seg_w","dsr_gld_clientes_db","05-Nombre BD golden clientes:")

dbutils.widgets.text("ruta_silver_w","abfss://gold@bcirg2dlssbx.dfs.core.windows.net/tecyseginfo/gld_bciwork_db","06-Ruta BD bciwork:")
dbutils.widgets.text("ruta_gold_w","abfss://gold@bcirg2dlssbx.dfs.core.windows.net/gld_prodservicios_db","07-BD golden:")

dbutils.widgets.text("lir_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_Clientes_Personas_db/fn/","08-LIR:")
dbutils.widgets.text("ssff_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_FuncionesInt_FilialesNormativos_db/ssff_cardet","09-SSFF:")
dbutils.widgets.text("fact_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_ProdServicios_Factoring_db/CARDET_FAC","10-Fact:")
dbutils.widgets.text("curse_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_RiesgoCred_RiesgoCredPer_db/reestrucforzosa_cur/","11-CURSE:") 
dbutils.widgets.text("detcae_w","abfss://silver@bcirg2dlsprd.dfs.core.windows.net/slv_ProdServicios_ProdPersona_db/detalle_incumplimiento_cierre_cae","12-DETCAE:")




# COMMAND ----------


fecha_x = dbutils.widgets.get("fecha_w") 
base_silver_x = dbutils.widgets.get("bd_silver_w")
base_golden_x = dbutils.widgets.get("bd_golden_w")
tipo_proceso_x = dbutils.widgets.get("tipo_proceso_w")
lir_x = dbutils.widgets.get("lir_w")
ssff_x = dbutils.widgets.get("ssff_w")
fact_x = dbutils.widgets.get("fact_w")
curse_x = dbutils.widgets.get("curse_w")
detcae_x = dbutils.widgets.get("detcae_w")
bd_golden_seg_x = dbutils.widgets.get("bd_golden_seg_w")
ruta_silver_x = dbutils.widgets.get("ruta_silver_w")
ruta_gold_x = dbutils.widgets.get("ruta_gold_w")

spark.conf.set("bci.fecha", fecha_x)
spark.conf.set("bci.dbnamesilver", base_silver_x)
spark.conf.set("bci.dbnamegolden", base_golden_x)
spark.conf.set("bci.tipo_proceso", tipo_proceso_x)
spark.conf.set("bci.lir", lir_x)
spark.conf.set("bci.ssff", ssff_x)
spark.conf.set("bci.fact", fact_x)
spark.conf.set("bci.curse", curse_x)
spark.conf.set("bci.detcae", detcae_x)
spark.conf.set("bci.bd_golden_seg", bd_golden_seg_x)
spark.conf.set("bci.ruta_destino", ruta_silver_x)
spark.conf.set("bci.rutagold", ruta_gold_x)

print(f"fecha_x : {fecha_x}")
print(f"base_silver_x : {base_silver_x}")
print(f"base_golden_x : {base_golden_x}")
print(f"tipo_proceso_x : {tipo_proceso_x}")
print(f"lir_x : {lir_x}")
print(f"ssff_x : {ssff_x}")
print(f"fact_x : {fact_x}")
print(f"curse_x : {curse_x}")
print(f"detcae_x : {detcae_x}")
print(f"bd_golden_seg_x : {bd_golden_seg_x}")
print(f"ruta_silver_x: {ruta_silver_x}")
print(f"ruta_gold_x: {ruta_gold_x}")



# COMMAND ----------

# Calcula periodo en base a la fecha
periodo_x=fecha_x[:6]
guardar_log_x="S"

spark.conf.set("bci.periodo", periodo_x)
spark.conf.set("bci.guarda_log", guardar_log_x)

print(f"periodo_x : {periodo_x}")      
print(f"guardar_log_x : {guardar_log_x}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Notebook con funciones genéricas

# COMMAND ----------

# MAGIC %run "./Funciones_Comunes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valida parámetros

# COMMAND ----------

valida_parametro(fecha_x)

# COMMAND ----------

valida_parametro(base_silver_x)

# COMMAND ----------

valida_parametro(base_golden_x)

# COMMAND ----------

valida_parametro(tipo_proceso_x)

# COMMAND ----------

valida_tipo_proceso(tipo_proceso_x)

# COMMAND ----------

valida_fecha_entrada(fecha_x)

# COMMAND ----------

valida_ruta_destino(lir_x)

# COMMAND ----------

valida_ruta_destino(ssff_x)

# COMMAND ----------

valida_ruta_destino(fact_x)

# COMMAND ----------

valida_ruta_destino(curse_x)

# COMMAND ----------

valida_ruta_destino(detcae_x)

# COMMAND ----------

valida_ruta_destino(ruta_silver_x)

# COMMAND ----------

valida_ruta_destino(ruta_gold_x)

# COMMAND ----------

valida_parametro(bd_golden_seg_x)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creacion modelo datos 
# MAGIC - crea tablas y bd si no existen

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_00_Crea_Modelo_Datos
# MAGIC - crea tablas work y Gold sino existen

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_00_Crea_Modelo_Datos", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x,"bd_golden_w":base_golden_x,"ruta_silver_w":ruta_silver_x,"ruta_gold_w":ruta_gold_x },
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ## INICIO Flujo ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_01_Valida_Parametros

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_01_Valida_Parametros", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x, "bd_golden_w": base_golden_x, "tipo_proceso_w": tipo_proceso_x, "guardar_log_w": guardar_log_x}, 
                fecha_x, 
                periodo_x,
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_05_CarDet_Datos_Entrada

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_05_CarDet_Datos_Entrada", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x, "bd_golden_w": base_golden_x, "lir_w": lir_x, "ssff_w": ssff_x, "fact_w": fact_x, "curse_w": curse_x, "detcae_w": detcae_x, "bd_golden_seg_w": bd_golden_seg_x, "tipo_proceso_w": tipo_proceso_x },  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_19_CarDet_Prepara_Entrada

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_19_CarDet_Prepara_Entrada", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ejecucion BCI_55_CarDet_Validaciones 
# MAGIC * Valida Tablas de Entrada

# COMMAND ----------

resultado = 0
tipo_validacion_x = 'E'  
resultado = ejecuta_notebook("BCI_55_CarDet_Validaciones", 0 , 
                {"fecha_w": fecha_x, "bd_silver_w": base_silver_x, "bd_golden_w": base_golden_x, "tipo_validacion_w": tipo_validacion_x, "periodo_w": periodo_x },  
                periodo_x, 
                fecha_x, 
                tipo_validacion_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_20_CarDet_Evalua_Datos_Ent

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_20_CarDet_Evalua_Datos_Ent", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x },  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_21_CarDet_Evalua_Datos_Exc

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_21_CarDet_Evalua_Datos_Exc", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_22_CarDet_Crit_Entrada

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_22_CarDet_Crit_Entrada", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_23_CarDet_Crit_Entrada_Ant

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_23_CarDet_Crit_Entrada_Ant", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_24_CarDet_Crit_Entrada_Pri

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_24_CarDet_Crit_Entrada_Pri", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_39_CarDet_Prepara_Salida

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_39_CarDet_Prepara_Salida", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_40_CarDet_Evalua_Salida

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_40_CarDet_Evalua_Salida", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_41_CarDet_Evalua_Sal_Exc

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_41_CarDet_Evalua_Sal_Exc", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_42_CarDet_Criterios_Salida

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_42_CarDet_Criterios_Salida", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_43_CarDet_Crit_Salida_Fam

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_43_CarDet_Crit_Salida_Fam", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_44_CarDet_Crit_Salida_Pri

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_44_CarDet_Crit_Salida_Pri", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_50_CarDet_Calcula_Stock

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_50_CarDet_Calcula_Stock", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución BCI_60_CarDet_Calc_Fec_Ini_Cd

# COMMAND ----------

resultado = 0
resultado = ejecuta_notebook("BCI_60_CarDet_Calc_Fec_Ini_Cd", 0 , 
                {"fecha_w":fecha_x,"bd_silver_w": base_silver_x},  
                periodo_x, 
                fecha_x, 
                tipo_proceso_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ejecucion BCI_55_CarDet_Validaciones 
# MAGIC * Valida Tablas de Salidas

# COMMAND ----------

resultado = 0
tipo_validacion_x='S'
resultado = ejecuta_notebook("BCI_55_CarDet_Validaciones", 0 , 
                {"fecha_w": fecha_x, "bd_silver_w": base_silver_x, "bd_golden_w": base_golden_x, "tipo_validacion_w": tipo_validacion_x, "periodo_w": periodo_x },  
                periodo_x, 
                fecha_x, 
                tipo_validacion_x,
                guardar_log_x)
if valida_ejecucion_notebook(resultado) != 0:
    resp = json.loads(str(resultado), strict=False)
    msgerrorx =resp['msgerror']
    coderror = resp['coderror']
    raise ValueError("{\"coderror\":\""+str(coderror)+"\", \"msgerror\":\""+msgerrorx+"\"}")  

# COMMAND ----------

# MAGIC %md
# MAGIC ## FIN Flujo ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen Duración Proceso

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tiempo de fin del proceso

# COMMAND ----------


from datetime import datetime

hora_fin = datetime.now()

delta = hora_fin - hora_ini
delta = str(delta).split(".")[0]

horafinx = hora_fin.strftime('%H%M%S')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estadísticas de ejecución

# COMMAND ----------

print("Inicio Proceso:", hora_ini.strftime('%d/%m/%Y %H:%M:%S'))
print("Fin Proceso   :", hora_fin.strftime('%d/%m/%Y %H:%M:%S'))
print("Duración      :", delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")