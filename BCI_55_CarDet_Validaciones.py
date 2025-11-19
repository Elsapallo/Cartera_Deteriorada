# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: BCI_55_CarDet_Validaciones
# MAGIC *********************************************************************************
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: BCI_55_CarDet_Validaciones.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/2997520011899327
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 31/05/2023
# MAGIC * Descripcion: Validaciones de tablas y de negocio
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gagriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 10/02/2025 
# MAGIC * Descripción: Se eliminaron comentarios de tabla que ya no van.      
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 20/04/2025 
# MAGIC * Descripción: Se agregó un indicador que permite generar gráficos de los estados de entrada para el proceso de Cierre.
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Entrada y Salida
# MAGIC **************************************************************************
# MAGIC #### Tablas Entrada: 
# MAGIC - gld_bciwork_db.tbl_cd_cliente_consolidado
# MAGIC - gld_bciwork_db.tbl_cd_segmentacion_cliente
# MAGIC - gld_bciwork_db.tbl_cd_d00_segmentado_pant
# MAGIC - gld_bciwork_db.tbl_cd_ope_condicion_mora
# MAGIC - gld_bciwork_db.tbl_cd_ope_condicion_mora_pant
# MAGIC - gld_bciwork_db.tbl_cd_ope_curse_bajo_mora
# MAGIC - gld_bciwork_db.tbl_cd_cliente_lir
# MAGIC - gld_bciwork_db.tbl_cd_cliente_det_ssff
# MAGIC - gld_bciwork_db.tbl_cd_cliente_det_fact
# MAGIC - gld_bciwork_db.tbl_cd_ope_condicion_ren
# MAGIC - gld_bciwork_db.tbl_cd_ope_condicion_mora_pant
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_ope_ini_cd
# MAGIC ***************************************************************************
# MAGIC #### Tablas Salida: 
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_crit_sal_ope_eval
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_crit_sal_crit
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_crit_sal_crit_matriz
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_crit_sal_crit_fam
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_crit_sal_prin
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_stock 
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_ope_ini_cd
# MAGIC - gld_bciwork_db.tbl_cd_cartdet_cli_ini_cd
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
dbutils.widgets.text("bd_golden_w","","03-Nombre BD Golden:")
dbutils.widgets.text("tipo_validacion_w","E","04-Tipo de Validacion (E o S):")

fecha_x = dbutils.widgets.get("fecha_w") 
base_silver_x = dbutils.widgets.get("bd_silver_w")
base_golden_x = dbutils.widgets.get("bd_golden_w")
tipo_validacion_x = dbutils.widgets.get("tipo_validacion_w")

spark.conf.set("bci.fecha", fecha_x)
spark.conf.set("bci.dbnamesilver", base_silver_x)
spark.conf.set("bci.dbname_golden", base_golden_x)
spark.conf.set("bci.tipo_validacion", tipo_validacion_x)

print(f"Fecha de Proceso actual: [fecha_x] {fecha_x}")
print(f"Nombre BD Silver: [base_silver_x] {base_silver_x}")
print(f"Nombre BD Golden: [base_golden_x] {base_golden_x}")
print(f"Tipo de Validacion: [tipo_validacion_x] {tipo_validacion_x}")


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

# DBTITLE 1,Valida parámetro "base_goldX"
valida_parametro(base_golden_x)

# COMMAND ----------

# DBTITLE 1,Valida tipo "tipo_validacionX"
valida_parametro(tipo_validacion_x)

# COMMAND ----------

# Calcula periodo en base a la fecha
periodo_x=fecha_x[:6]
spark.conf.set("bci.periodo", periodo_x)
print(f"periodo_x: {periodo_x}")      

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

data_default = {
  "bd": f"{base_silver_x}"
}

tablas_E = {
 "tbl_cd_d00_segmentado": data_default,  
"tbl_cd_cliente_consolidado": data_default,  
"tbl_cd_cliente_consolidado_pant": data_default,  
"tbl_cd_segmentacion_cliente": data_default,  
"tbl_cd_d00_segmentado_pant": data_default,  
"tbl_cd_cliente_lir": data_default,  
"tbl_cd_cliente_det_ssff": data_default,  
"tbl_cd_cliente_det_fact": data_default,  
"tbl_cd_cae_ope_det_incumplimiento": data_default,  
"tbl_cd_curses": data_default,  
"tbl_cd_cartdet_ope_ini_cd_pant": data_default,  
"tbl_cd_cartdet_cli_ini_cd_pant": data_default,  
"tbl_cd_ope_condicion_deterioro": data_default,      
}

tablas_E

# COMMAND ----------

data_default = {
  "bd": f"{base_silver_x}"
}

tablas_S = {
  "tbl_cd_cartdet_crit_sal_ope_eval": data_default,
  "tbl_cd_cartdet_crit_sal_crit": data_default,
  "tbl_cd_cartdet_crit_sal_crit_matriz": data_default,
  "tbl_cd_cartdet_crit_sal_crit_fam": data_default,
  "tbl_cd_cartdet_crit_sal_prin": data_default,
  "tbl_cd_cartdet_stock": data_default,
  "tbl_cd_cartdet_ope_ini_cd": data_default,
  "tbl_cd_cartdet_cli_ini_cd": data_default,
}

tablas_S

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validacion Tablas de Entradas y Salidas

# COMMAND ----------

import sys  # Importar sys si aún no está importado

if tipo_validacion_x == 'E':
    print(f"Se valida tablas de Entrada")
    print(f"fecha_x: {fecha_x}")

    array_no_datos = []
    array_datos = []

    for tabla in tablas_E:
        try:
            query = f"SELECT COUNT(1) FROM {tablas_E[tabla]['bd']}.{tabla} "
            print(f"Consulta SQL generada: {query}")
            df = spark.sql(query)
            total = df.first()[0]
            print(f"Total de registros en la tabla {tabla}: {total}")
            if total > 0:
                array_datos.append(f"La tabla {tabla} tiene {total} registros ")
            else:
                array_no_datos.append(f"La tabla {tabla} NO tiene registros ")
        except Exception as e:
            array_no_datos.append(f"Error al consultar la tabla {tablas_E[tabla]['bd']}.{tabla}: {e}")

    if array_no_datos:
        print("Se encontraron problemas con algunas tablas. La ejecución se cancelará.")
        for error in array_no_datos:
            print(error)
        sys.exit("La ejecución del notebook ha sido cancelada.")

elif tipo_validacion_x == 'S':
    print(f"Se valida tablas de Salidas")
    print(f"fecha_x: {fecha_x}")

    array_no_datos = []
    array_datos = []

    for tabla in tablas_S:
        try:
            query = f"SELECT COUNT(1) FROM {tablas_S[tabla]['bd']}.{tabla} "
            print(f"Consulta SQL generada: {query}")
            df = spark.sql(query)
            total = df.first()[0]
            print(f"Total de registros en la tabla {tabla}: {total}")
            if total > 0:
                array_datos.append(f"La tabla {tabla} tiene {total} registros ")
            else:
                array_no_datos.append(f"La tabla {tabla} NO tiene registros ")
        except Exception as e:
            array_no_datos.append(f"Error al consultar la tabla {tablas_S[tabla]['bd']}.{tabla}: {e}")

    if array_no_datos:
        print("Se encontraron problemas con algunas tablas. La ejecución se cancelará.")
        for error in array_no_datos:
            print(error)
        sys.exit("La ejecución del notebook ha sido cancelada.")

else:
    print(f"Tipo de validación no existe, debe ser E o S y se ingreso valor: {tipo_validacion_x}")
    sys.exit("La ejecución del notebook ha sido cancelada.")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Revision Comportamiento de Datas

# COMMAND ----------

print (periodo_x)

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x)

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'1')

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'7')

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'8')

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'9')

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'10')

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'11')

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'12')

# COMMAND ----------

if tipo_validacion_x == 'S' or tipo_validacion_x == 'F':
  mostrar_variacion_criterio(int(periodo_x),base_silver_x,base_golden_x,'13')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mensaje termino OK

# COMMAND ----------

msgerrorx="OK"
dbutils.notebook.exit("{\"coderror\":0, \"msgerror\":\""+msgerrorx+"\"}")