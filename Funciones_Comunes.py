# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: Funciones_Comunes
# MAGIC **************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informacion del Notebook 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encabezado
# MAGIC **************************************************************************
# MAGIC * Nombre: Funciones_Comunes.ipynb
# MAGIC * Ruta: https://adb-5512273708018582.2.azuredatabricks.net/?o=5512273708018582#notebook/3570959530595695
# MAGIC * Autor: Gabriel Martínez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 23/09/2023
# MAGIC * Descripcion: Notebook con funciones genéricas que pueden ser usadas por otros notebooks.
# MAGIC * Documentacion:
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mantenciones
# MAGIC **************************************************************************
# MAGIC #### Mantención Nro: 1
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 15/02/2025 
# MAGIC * Descripción: Se cambio el metodo de cancelacion utilizando el comando (raise) y se incorporada la funcion de ir a buscar el ultimo dia calendario. Tambien se agrego una nueva funcion (obtener_estados_tablas).  
# MAGIC ***************************************************************************
# MAGIC #### Mantención Nro: 2
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Jonatan Cancino
# MAGIC * Fecha: 25/04/2025 
# MAGIC * Descripción: Se modifico la funcion extension_archivos para que cuando la vigencia sea previa, asigne extencion .PRV.  
# MAGIC ***************************************************************************
# MAGIC #### Mantención Nro: 3
# MAGIC * Autor: Gabriel Martinez (SimpleData) - Ing. SW BCI: Claudia Yañez
# MAGIC * Fecha: 08/07/2025 
# MAGIC * Descripción: Se realiza una mejora en la funcion mostrar_variacion_criterio.  
# MAGIC ***************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga librerias

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

from pyspark.sql.functions import date_format, expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## INICIO definición de funciones

# COMMAND ----------

# DBTITLE 1,existe_ruta
def existe_ruta(ruta):  
    try:                  
        dbutils.fs.ls(ruta) 
        return True         
    except Exception as e:  
        return False

# COMMAND ----------

# DBTITLE 1,es_vacio
def es_vacio(var_x):
  if (var_x is None):
    return True
  if not var_x.strip():
    return True
  
  return False

# COMMAND ----------

# DBTITLE 1,valida_parametro
def valida_parametro(param_x, mensaje=""):
    try:
        param_x
    except NameError:
        param_x = None
        raise ValueError("{\"coderror\":28001, \"msgerror\":\"Parámetro no existe: " + mensaje + "\"}")
    else:
        if isinstance(param_x, str) and es_vacio(param_x.strip()):
            raise ValueError("{\"coderror\":28002, \"msgerror\":\"Falta ingresar parámetro: " + mensaje + "\"}")


# COMMAND ----------

# DBTITLE 1,valida_ruta
def valida_ruta(ruta_x,ruta_bdx):
  try:
    valor = dbutils.fs.ls(f"{ruta_x}/{ruta_bdx}")
  except ValueError:
      raise ValueError("{\"coderror\":28010, \"msgerror\":\"Ruta no encontrada: "+str(ruta_x)+"\"}")

# COMMAND ----------

# DBTITLE 1,valida_bd
def valida_bd(nombre_bdx):
  database = spark.sql(f"SHOW DATABASES LIKE '{nombre_bdx}'").collect()

  if len(database) == 0:
    raise ValueError("{\"coderror\":28009, \"msgerror\":\"Base de Datos no encontrada: "+str(nombre_bdx)+"\"}")

# COMMAND ----------

# DBTITLE 1,sql_safe
def sql_safe(query_x):
  try:
    print ("sql_safe: query -> " + query_x)
    return spark.sql(query_x)
  except Exception as e:
    raise ValueError("{\"coderror\":28015, \"msgerror\":\"Error al ejecutar query: "+str(query_x)+" , error: "+str(e)+"\"}")

# COMMAND ----------

# DBTITLE 1,obtiene_parametro
def obtiene_parametro(proceso_x, tipo_x):
  try:
    paso_query = f"""select parametro1 from {base_silver_x}.tbl_cierreriesgo_parametro WHERE proceso = '{proceso_x}' AND tipo_parametro = '{tipo_x}' and vigencia = 'V'""" 

    df_param=sql_safe(paso_query)
    data_collect = df_param.collect()

    parametros_salida = ""

    for row in data_collect:
      parametros_salida = parametros_salida + (row["parametro1"] + ",")

    parametros_salida = parametros_salida[:-1]
    
    if es_vacio(parametros_salida):
      raise ValueError("{\"coderror\":28011, \"msgerror\":\"Parámetro proceso = "+str(proceso_x)+" ,tipo_parametro "+str(tipo_x)+" no encontrado en BD\"}")
    
    return parametros_salida
  except Exception as e:
    raise ValueError("{\"coderror\":28012, \"msgerror\":\"Error al ejecutar query "+str(query_x)+" , error: "+str(e)+"\"}")

# COMMAND ----------

# DBTITLE 1,valida_periodo_entrada
def valida_periodo_entrada(periodo_x):
  try:
    if len(periodo_x) != 6:
      raise ValueError("{\"coderror\":28005, \"msgerror\":\"Largo de parámetro de entrada período inválido: "+periodo_x+"\"}")
    periodo = datetime.strptime(periodo_x, '%Y%m')
  except ValueError:
    raise ValueError("{\"coderror\":28006, \"msgerror\":\"Parámetro de entrada período inválido: "+periodo_x+"\"}")

# COMMAND ----------

# DBTITLE 1,valida_tipo_proceso
def valida_tipo_proceso(tipo_proceso_x):
  try:
    if tipo_proceso_x not in ["C","PC"]:
      raise ValueError("{\"coderror\":28007, \"msgerror\":\"Tipo de proceso inválido: "+str(tipo_proceso_x)+" .Valores permitidos: C o PC\"}")    
  except ValueError:
    raise ValueError("{\"coderror\":28008, \"msgerror\":\"Parámetro de entrada Tipo de proceso inválido: "+str(tipo_proceso_x)+"\"}")

# COMMAND ----------

# DBTITLE 1,valida_fecha_entrada
def valida_fecha_entrada(fecha_x):
    from datetime import datetime
    try:
        if len(fecha_x) != 8:
            raise ValueError("{\"coderror\":28003, \"msgerror\":\"Largo de parámetro de entrada fecha inválido: "+fecha_x+"\"}")
        fecha = datetime.strptime(fecha_x, '%Y%m%d')
    except ValueError:
        raise ValueError("{\"coderror\":28004, \"msgerror\":\"Parámetro de entrada fecha inválido: "+fecha_x+"\"}")


# COMMAND ----------

# DBTITLE 1,ejecuta_notebook
def ejecuta_notebook(nombre_notebook, timeout, parametros_notebook, periodo_cierre, fecha_cierre, tipo_proceso, guardar_log) :
  """
  Esta función ejecuta un notebook.
  Para esto: recupera el estado en que terminó el notebook y en caso de error, recupera el código de error del notebook ejecutado y cancela la ejecución del notebook.
  Almacena estado de ejecución de notebook en base de datos, especificamente: dsr_gld_bciwork_db.tbl_cd_cierreriesgo_log
  
  Parámetros: 
  1: nombre_notebook --> Nombre y path del notebook a ejecutar
  2: timeout --> timeout seconds
  3: parametros_notebook --> arguments, parametros de ejecución del notebook
  4: periodo_cierre --> corresponde a parametros de entrada de notebook orquestador
  5: fecha_cierre --> corresponde a parametros de entrada de notebook orquestador
  6: tipo_proceso --> corresponde a parametros de entrada de notebook orquestador
  
  Salida:
  1: JSON con coderror y msgerror
  """
  from datetime import datetime
  dia_ini_x = datetime.now()
  
  print("inicio")
  print(dia_ini_x)
  
  dia_ejecucion = dia_ini_x.strftime('%d-%m-%Y')
  hora_inicio =  dia_ini_x.strftime('%H:%M:%S')
  
  if es_vacio(periodo_cierre):
    periodo_cierre = 0

  if es_vacio(fecha_cierre):
    fecha_cierre = 0
    
  return_value = dbutils.notebook.run(nombre_notebook, timeout , parametros_notebook)
  resp = json.loads(return_value, strict=False)
  
  user_x = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson() 
  user_resp = json.loads(user_x)
  usuario = user_resp['tags']['user']

  dia_fin_x = datetime.now()
  hora_fin =  dia_fin_x.strftime('%H:%M:%S')
  
  if guardar_log == "S" : 
    registra_log(periodo_cierre, fecha_cierre, tipo_proceso, nombre_notebook, usuario, dia_ejecucion, hora_inicio, hora_fin, resp['coderror'], resp['msgerror'])
    
  if resp['coderror'] != 0 :
    raise ValueError("{\"coderror\":"+str(resp['coderror'])+", \"msgerror\":\""+resp['msgerror']+"\"}") 
  
  print("fin")
  print(datetime.now())
  
  return(return_value)

# COMMAND ----------

# DBTITLE 1,registra_log
def registra_log(periodo_cierre, 
                fecha_cierre, 
                tipo_proceso, 
                nombre_notebook, 
                id_ejecutor, 
                fecha_ejecucion, 
                hora_inicio, 
                hora_termino, 
                codigo_status, 
                mensaje_status, 
                tabla=None):
  
  if tabla is None:
      tabla = f"{base_silver_x}.tbl_cd_cierreriesgo_log"

  try: 
    query_insert = f"""
    INSERT INTO {tabla}
    VALUES ({periodo_cierre},
            {fecha_cierre}, 
            '{tipo_proceso}', 
            '{nombre_notebook}',
            '{id_ejecutor}',
            '{fecha_ejecucion}', 
            '{hora_inicio}', 
            '{hora_termino}', 
            {codigo_status},  
            "{mensaje_status}") 
    """

    return spark.sql(query_insert)
            
  except Exception as e:
    raise ValueError("{\"coderror\":28014, \"msgerror\":\"Error al ejecutar query que graba log de notebook, query: "+str(query_insert)+" , error: "+str(e)+"\"}")



# COMMAND ----------

# DBTITLE 1,periodo_int_to_string
def periodo_int_to_string(periodo_x):
  try:
    periodo = datetime.strptime(str(periodo_x), "%Y%m").strftime('%Y/%m/%d')
    return periodo
  
  except Exception as e:
    raise ValueError("{\"coderror\":28013, \"msgerror\":\"Error al ejecutar formatear período: "+str(periodo_x)+" , error: "+str(e)+"\"}")

# COMMAND ----------

# DBTITLE 1,valida_archivo_creado
def valida_archivo_creado(pathfinal):
  try:
    valor = dbutils.fs.ls(f"{pathfinal}")
    print("Archivo "+str(pathfinal)+" Fué Generado Correctamente")
  except ValueError:
    raise ValueError("{\"coderror\":28015, \"msgerror\":\"Archivo no encontrado: "+str(pathfinal)+"\"}")

# COMMAND ----------

# DBTITLE 1,validacion_estado
import datetime
fecha_hora_actual = datetime.datetime.now()
fecha_actual = fecha_hora_actual.date().strftime("%Y%m%d")

def validacion_estado(estado_x, fec_proc_x, base_silver_x):
   
    paso_query = f"SELECT ind_vig FROM {base_silver_x}.tbl_cd_fec_proc WHERE nro_corr = (SELECT MAX(nro_corr) FROM {base_silver_x}.tbl_cd_fec_proc)"
    df_param = sql_safe(paso_query)
    data_vig = df_param.collect()
   
    if estado_x == 'PC' and data_vig[0][0] != 'F' and data_vig[0][0] != 'N' :

        print('Para comenzar el Pre-Cierre debe estar confirmado el Cierre, estado actual:',data_vig[0][0])
        raise ValueError("{\"coderror\":28016, \"msgerror\":\"Para comenzar el Pre-Cierre debe estar confirmado el Cierre, estado actual: "+str(data_vig[0][0])+"\"}")
    
    elif estado_x == 'PC' and data_vig[0][0] == 'F' :

        paso_query = f"INSERT INTO {base_silver_x}.tbl_cd_fec_proc select nro_corr +1,{fec_proc_x},'{fecha_actual}','N',{fec_proc_x}	from {base_silver_x}.tbl_cd_fec_proc a where nro_corr = (select max(nro_corr) from  {base_silver_x}.tbl_cd_fec_proc where ind_vig = 'F')"
        df_param = sql_safe(paso_query)
        data_collect = df_param.collect()

    elif estado_x == 'PC' and data_vig[0][0] == 'N' :

        print ('Se ejecuta REPROCESO del Pre-Cierre')
 
        paso_query = f"DELETE FROM {base_silver_x}.tbl_cd_fec_proc WHERE ind_vig = 'N'"
        df_param = sql_safe(paso_query)
        data_collect = df_param.collect()
        
        paso_query2 = f"INSERT INTO {base_silver_x}.tbl_cd_fec_proc select nro_corr +1,{fec_proc_x},'{fecha_actual}','N',{fec_proc_x}	from {base_silver_x}.tbl_cd_fec_proc a where nro_corr = (select max(nro_corr) from  {base_silver_x}.tbl_cd_fec_proc where ind_vig = 'F')"
        df_param2 = sql_safe(paso_query2)
        data_collect2 = df_param2.collect()

    elif estado_x == 'C' and data_vig[0][0] != 'S' and data_vig[0][0] != 'E' :

        print('Para comenzar el Cierre debe estar confirmado el Pre-Cierre, estado actual:',data_vig[0][0])
        raise ValueError("{\"coderror\":28016, \"msgerror\":\"Para comenzar el Cierre debe estar confirmado el Pre-Cierre, estado actual: "+str(data_vig[0][0])+"\"}")
    
    elif estado_x == 'C' and data_vig[0][0] == 'S' :

        paso_query = f"DELETE FROM {base_silver_x}.tbl_cd_fec_proc WHERE ind_vig = 'S'"
        df_param = sql_safe(paso_query)
        data_collect = df_param.collect()

        paso_query2 = f"INSERT INTO {base_silver_x}.tbl_cd_fec_proc select nro_corr +1,{fec_proc_x},'{fecha_actual}','E',{fec_proc_x}	from {base_silver_x}.tbl_cd_fec_proc a where nro_corr = (select max(nro_corr) from  {base_silver_x}.tbl_cd_fec_proc where ind_vig = 'F')"
        df_param2 = sql_safe(paso_query2)
        data_collect2 = df_param2.collect()

    elif estado_x == 'C' and data_vig[0][0] == 'E' :

        paso_query = f"DELETE FROM {base_silver_x}.tbl_cd_fec_proc WHERE ind_vig = 'E'"
        df_param = sql_safe(paso_query)
        data_collect = df_param.collect()
        
        paso_query2 = f"INSERT INTO {base_silver_x}.tbl_cd_fec_proc select nro_corr +1,{fec_proc_x},'{fecha_actual}','E',{fec_proc_x}	from {base_silver_x}.tbl_cd_fec_proc a where nro_corr = (select max(nro_corr) from  {base_silver_x}.tbl_cd_fec_proc where ind_vig = 'F')"
        df_param2 = sql_safe(paso_query2)
        data_collect2 = df_param2.collect()


# COMMAND ----------

# DBTITLE 1,def confirmacion_estado
import datetime
fecha_hora_actual = datetime.datetime.now()
fecha_actual = fecha_hora_actual.date().strftime("%Y%m%d")

def confirmacion_estado(estado_x, base_silver_x):

    paso_query = f"SELECT ind_vig FROM {base_silver_x}.tbl_cd_fec_proc WHERE nro_corr = (SELECT MAX(nro_corr) FROM {base_silver_x}.tbl_cd_fec_proc)"
    df_param = sql_safe(paso_query)
    data_vig = df_param.collect()

    if estado_x == 'PC' and data_vig[0][0] != 'N' :

        print('Para confirmar el Pre-Cierre debe estar con Estado '"N"', estado actual:',data_vig[0][0])
        raise ValueError("{\"coderror\":28016, \"msgerror\":\"Para confirmar el Pre-Cierre debe estar con Estado 'N', estado actual: "+str(data_vig[0][0])+"\"}")

    elif estado_x == 'PC' and data_vig[0][0] == 'N' :
        paso_query = f"update {base_silver_x}.tbl_cd_fec_proc set fec_ejec = '{fecha_actual}', ind_vig = 'S' where ind_vig = 'N'"
        df_param = sql_safe(paso_query)
        data_collect = df_param.collect()

    elif estado_x == 'C' and data_vig[0][0] != 'E' :

        print('Para confirmar el Cierre debe estar con Estado '"E"', estado actual:',data_vig[0][0])
        raise ValueError("{\"coderror\":28016, \"msgerror\":\"Para confirmar el Cierre debe estar con Estado 'E', estado actual: "+str(data_vig[0][0])+"\"}")

    elif estado_x == 'C' and data_vig[0][0] == 'E' :
        paso_query = f"update {base_silver_x}.tbl_cd_fec_proc set fec_ejec = '{fecha_actual}', ind_vig = 'F' where ind_vig = 'E'"
        df_param = sql_safe(paso_query)
        data_collect = df_param.collect()



# COMMAND ----------

# DBTITLE 1,reproceso_forzoso
import datetime
fecha_actual = datetime.datetime.now().date().strftime("%Y%m%d")

def reproceso_forzoso(estado_rep_x,fec_proc_x, base_silver_x):

    paso_query = f"SELECT nro_corr FROM {base_silver_x}.tbl_cd_fec_proc ORDER BY nro_corr DESC LIMIT 1"
    df_param = sql_safe(paso_query)
    data_vig = df_param.collect()

    if estado_rep_x == 'RPC' :

        paso_query8 = f"DELETE FROM {base_silver_x}.tbl_cd_fec_proc WHERE nro_corr = {data_vig[0][0]}"
        df_param8 = sql_safe(paso_query8)
        data_collect = df_param8.collect()

        paso_query9 = f"INSERT INTO {base_silver_x}.tbl_cd_fec_proc select {data_vig[0][0]},{fec_proc_x},{fecha_actual},'N',{fec_proc_x}"
        df_param9 = sql_safe(paso_query9)
        data_pact = df_param9.collect()

    elif estado_rep_x == 'RC' :

        paso_query8 = f"DELETE FROM {base_silver_x}.tbl_cd_fec_proc WHERE nro_corr = {data_vig[0][0]}"
        df_param8 = sql_safe(paso_query8)
        data_collect = df_param8.collect()

        paso_query9 = f"INSERT INTO {base_silver_x}.tbl_cd_fec_proc select {data_vig[0][0]},{fec_proc_x},{fecha_actual},'E',{fec_proc_x}"
        df_param9 = sql_safe(paso_query9)
        data_pact = df_param9.collect()


# COMMAND ----------

# DBTITLE 1,confirmacion_o_reproceso
def confirmacion_o_reproceso(estado_rep_x,tipo_proceso_x,fec_proc_x,base_silver_x):

   if estado_rep_x == 'N':
      confirmacion_estado(tipo_proceso_x,base_silver_x)
   else:
      reproceso_forzoso(estado_rep_x,fec_proc_x,base_silver_x)


# COMMAND ----------

# DBTITLE 1,vigencia_estado
def vigencia_estado(base_silver_x):

    paso_query = f"SELECT ind_vig FROM {base_silver_x}.tbl_cd_fec_proc WHERE nro_corr = (SELECT MAX(nro_corr) FROM {base_silver_x}.tbl_cd_fec_proc)"
    df_param = sql_safe(paso_query)
    ind_estado = df_param.collect()

    return ind_estado[0][0]


# COMMAND ----------

# DBTITLE 1,valida_crea_directorio
def valida_crea_directorio(ruta_adls_tbx,fecha_x,est_carp):
    ruta = ruta_adls_tbx+"/"+fecha_x+"/"+est_carp
    exist = existe_ruta(ruta)
    if exist is True:
        ruta_adls_tbxpath = ruta
        print("Existe: " + ruta_adls_tbxpath)
        return ruta_adls_tbxpath
    else:
        dbutils.fs.mkdirs(ruta)
        ruta_adls_tbxpath = ruta
        print("Ahora existe la ruta: " + ruta_adls_tbxpath)
        return ruta_adls_tbxpath


# COMMAND ----------

# DBTITLE 1,directorio_archivo_salida
def directorio_archivo_salida(ind_estado,ruta_adls_tbx,fecha_x):

  if ind_estado == 'N' or ind_estado == 'S':
    valida_crea_directorio(ruta_adls_tbx,fecha_x,'PreCierre')
    est_carp = 'PreCierre'
    print("estado carpeta:",est_carp)
    return est_carp
    return ruta_adls_tbxpath
  
  else:
    valida_crea_directorio(ruta_adls_tbx,fecha_x,'Cierre')
    est_carp = 'Cierre'
    print("estado carpeta:",est_carp)
    return est_carp
    return ruta_adls_tbxpath
  


# COMMAND ----------

def extension_archivos(ind_estado):
    ind_estado = vigencia_estado(base_silver_x)

    if ind_estado == 'N' or ind_estado == 'E':
        ext_arch = 'PRV'
        return ext_arch
    elif ind_estado == 'S' or ind_estado == 'F':
        ext_arch = 'TXT'
        return ext_arch
    else:
        return None  


# COMMAND ----------

# DBTITLE 1,elimina_archivos_prv
def elimina_archivos_prv(ind_estado, ruta_adls):
    ind_estado=vigencia_estado(base_silver_x)
    print(ruta_adls + "/CARDET_*.PRV")
    if ind_estado == 'N' or ind_estado == 'S':
        archivos = glob.glob(ruta_adls + "/CARDET_*.PRV")
        for archivo in archivos:
            dbutils.fs.rm(archivo, True)
    elif ind_estado == 'E' or ind_estado == 'F':
        archivos = glob.glob(ruta_adls + "/CARDET_*.PRV")
        for archivo in archivos:
            dbutils.fs.rm(archivo, True)

# COMMAND ----------

# DBTITLE 1,validacion_de_tablas

def validacion_de_tablas(tablas):
    for tabla in tablas:
        query = f"SELECT COUNT(*) FROM {tabla}"
        resultado = sql_safe(query)
        cantidad_filas = resultado.collect()[0][0]
        
        if cantidad_filas == 0:
            print(f"La tabla {tabla} está vacía.")
            sys.exit("La ejecución del notebook ha sido cancelada.")
        else:
            print(f"La tabla {tabla} contiene {cantidad_filas} registros.")


# COMMAND ----------

# DBTITLE 1,obtener_parametros_tb_fecha
from pyspark.sql import SparkSession

def obtener_parametros_tb_fecha(base_silver_x):
    # Crear la sesión de Spark
    spark = SparkSession.builder.getOrCreate()

    # Consulta 1
    consulta1 = f"""
    SELECT substring(fec_proc, 1, 6) AS periodo
    FROM {base_silver_x}.tbl_cd_fec_proc
    WHERE nro_corr = (SELECT MAX(nro_corr) FROM {base_silver_x}.tbl_cd_fec_proc)
    """

    # Ejecutar consulta 1
    resultado1 = spark.sql(consulta1)
    periodo_act = resultado1.collect()[0][0]

    # Consulta 2
    consulta2 = f"""
    SELECT fec_proc
    FROM {base_silver_x}.tbl_cd_fec_proc
    WHERE nro_corr = (SELECT MAX(nro_corr) FROM {base_silver_x}.tbl_cd_fec_proc WHERE ind_vig = 'F')
    """

    # Ejecutar consulta 2
    resultado2 = spark.sql(consulta2)
    fecha_ant = resultado2.collect()[0][0]
    
    # Consulta 3
    consulta3 = f"""
    SELECT substring(fec_proc, 1, 6) AS periodo
    FROM {base_silver_x}.tbl_cd_fec_proc
    WHERE nro_corr = (SELECT MAX(nro_corr) FROM {base_silver_x}.tbl_cd_fec_proc WHERE ind_vig = 'F')
    """

    # Ejecutar consulta 3
    resultado3 = spark.sql(consulta3)
    periodo_ant = resultado3.collect()[0][0]

    # Consulta 4
    consulta4 = f"""
    SELECT 
      CASE WHEN ind_vig = 'N' OR ind_vig = 'S' THEN 'PC' 
           WHEN ind_vig = 'E' OR ind_vig = 'F' THEN 'C'
      END AS tipo_proceso
    FROM 
      {base_silver_x}.tbl_cd_fec_proc 
    WHERE 
      nro_corr = (SELECT MAX(nro_corr) FROM {base_silver_x}.tbl_cd_fec_proc)    
    """

    # Ejecutar consulta 4
    resultado4 = spark.sql(consulta4)
    tipo_proceso = resultado4.collect()[0][0]

    return periodo_act, fecha_ant, periodo_ant, tipo_proceso


# COMMAND ----------

# DBTITLE 1,obtener_parametros_de_tbl

from pyspark.sql import SparkSession

def obtener_parametros_de_tbl(base_silver_x):
    # Crear la sesión de Spark
    spark = SparkSession.builder.getOrCreate()

    # Definir las consultas
    consultas = [
        ("CONDEVAL", "param_cal"),
        ("CONDEVAL", "fecCv"),
        ("CONDEVAL", "CritDetSalTotIfrs"),
        ("CONDEVAL", "VALORDVCN"),
        ("CONDEVAL", "VALORDVCX"),
        ("CONDEVAL", "VALORCDIR"),
        ("CONDEVAL", "VALORDMOR"),
        ("CONDEVAL", "CANTMINMES"),
        ("CONDEVAL", "DIASMORA"),
        ("CONDEVAL", "pagCons"),
        ("CONDEVAL", "cant_min_pag"),
        ("CONDEVAL", "MONTOSBIF"),
        ("CONDEVAL", "ccontable"),
        ("CONDEVAL", "sal_cart_venc"),
        ("CONDEVAL", "dias_mora_ent"),
        ("CONDEVAL", "cod_ren"),
        ("CONDEVAL", "tio_esp"),
        ("CONDEVAL", "VALOREST90180"),
        ("CONDEVAL", "VALOREST180"),
        ("CONDEVAL", "VALOREST3090")        
    ]

    resultados = []
    for tipo_parametro, codigo in consultas:
        consulta = f"""
        SELECT parametro1
        FROM {base_silver_x}.tbl_cierreriesgo_parametro
        WHERE tipo_parametro = '{tipo_parametro}' AND codigo = '{codigo}' AND vigencia = 'V'
        """
        resultado = spark.sql(consulta).collect()
        if resultado:
            valores = [row['parametro1'] for row in resultado]
            resultados.append(tuple(valores) if len(valores) > 1 else valores[0])
        else:
            resultados.append(None)
    
    return resultados


# COMMAND ----------

# DBTITLE 1,def valida_ejecucion_notebook
"""
Retorna el codigo de error de una ejecucion de un notebook
espera un objeto con el formato {"coderror": "0", "msgerror": "Mensaje..."}
"""
def valida_ejecucion_notebook(resultado):
  resp = json.loads(resultado, strict=False)
  return resp['coderror']

# COMMAND ----------

# DBTITLE 1,valida_ruta_destino
def valida_ruta_destino(ruta_x):
  try:
    valor = dbutils.fs.ls(f"{ruta_x}")
  except Exception as e:
      raise ValueError("{\"coderror\":28010, \"msgerror\":\"Ruta no encontrada: "+str(ruta_x)+"\"}")

# COMMAND ----------

# DBTITLE 1,valida_tipo_reproceso
def valida_tipo_reproceso(estado_rep_x):
  try:
    if estado_rep_x not in ["N","RC","RPC"]:
      raise ValueError("{\"coderror\":28007, \"msgerror\":\"Tipo de reproceso inválido: "+str(estado_rep_x)+" .Valores permitidos: N ; RPC ; RPC\"}")    
  except ValueError:
    raise ValueError("{\"coderror\":28008, \"msgerror\":\"Parámetro de entrada Tipo de reproceso inválido: "+str(estado_rep_x)+"\"}")

# COMMAND ----------

# DBTITLE 1,def ultimo_dia_habil_mes_anterior(fecha):
import datetime
import calendar

def ultimo_dia_habil_mes_anterior(fecha):

    fecha_dt = datetime.datetime.strptime(fecha, '%Y%m%d')
    primer_dia_mes_anterior = fecha_dt.replace(day=1) - datetime.timedelta(days=1)
    ultimo_dia_mes_anterior = primer_dia_mes_anterior.replace(day=calendar.monthrange(primer_dia_mes_anterior.year, primer_dia_mes_anterior.month)[1])

    while ultimo_dia_mes_anterior.weekday() >= 5:   
        ultimo_dia_mes_anterior -= datetime.timedelta(days=1)
    
    ult_dia_mant = ultimo_dia_mes_anterior.strftime('%Y%m%d')
    
    return ult_dia_mant


# COMMAND ----------

# DBTITLE 1,obtener_parametros_tb_fecha_ant
from pyspark.sql import SparkSession

def obtener_parametros_tb_fecha_ant(base_silver_x):
    # Crear la sesión de Spark
    spark = SparkSession.builder.getOrCreate()

    # Consulta 1
    consulta1 = f"""
    SELECT fec_proc
    FROM {base_silver_x}.tbl_cd_fec_proc
    WHERE nro_corr = (SELECT MAX(nro_corr) - 1 FROM {base_silver_x}.tbl_cd_fec_proc WHERE ind_vig = 'F')
    """

    # Ejecutar consulta 1
    resultado1 = spark.sql(consulta1)
    fecha_ant = resultado1.collect()[0][0]
    
    # Consulta 2
    consulta2 = f"""
    SELECT substring(fec_proc, 1, 6) AS periodo
    FROM {base_silver_x}.tbl_cd_fec_proc
    WHERE nro_corr = (SELECT MAX(nro_corr) - 1 FROM {base_silver_x}.tbl_cd_fec_proc WHERE ind_vig = 'F')
    """

    # Ejecutar consulta 2
    resultado2 = spark.sql(consulta2)
    periodo_ant = resultado2.collect()[0][0]

    return fecha_ant, periodo_ant


# COMMAND ----------

# DBTITLE 1,calcula_fecha_otorgamiento
 def calcula_fecha_otorgamiento(fecha_x) :

# Esta funcion calcula la fecha minima para considerar los creditos renegociados. 
# A la fecha de proceso (fecha_x) le resta 6 meses

    df = spark.createDataFrame([(fecha_x,)], ['fecha_x'])
    df = df.select(date_format(expr(f"to_date(fecha_x, 'yyyyMMdd') - interval 6 month + interval 1 day - interval 1 day"), 'yyyyMMdd').alias('calcula_fecha_otorgamiento'))
    resultado = df.collect()[0]['calcula_fecha_otorgamiento']
    return resultado


# COMMAND ----------

# DBTITLE 1,mostrar_variacion_criterio
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Inicializa una sesión de Spark (asumiendo que aún no está iniciada)
spark = SparkSession.builder.appName("ConsultaVariacionCriterio").getOrCreate()

def mostrar_variacion_criterio(periodo_consulta, base_silver_x, bd_golden_x, criterio=None):
    # Calcular el periodo anterior
    ano_actual = periodo_consulta // 100
    mes_actual = periodo_consulta % 100
    
    ano_anterior = ano_actual
    mes_anterior = mes_actual - 1 if mes_actual > 1 else 12
    
    if mes_actual == 1:  # Si el mes actual es enero, ajustar el ano anterior
        ano_anterior -= 1

    periodo_anterior = ano_anterior * 100 + mes_anterior
    
    # Consulta para el periodo actual
    consulta_actual = f"""
    SELECT
        Periodo_cierre AS Periodo_cierre_actual,
        COUNT(1) AS Cant_criterios_actual,
        criterio_entrada AS criterio_entrada_actual
    FROM
        {base_silver_x}.tbl_cd_cartdet_stock
    WHERE
        Periodo_cierre = {periodo_consulta}
        {'AND criterio_entrada = "' + criterio + '"' if criterio else ''}
    GROUP BY
        Periodo_cierre_actual, criterio_entrada_actual
    """

    # Consulta para el periodo anterior
    consulta_anterior = f"""
    SELECT
        Periodo_cierre AS Periodo_cierre_anterior,
        COUNT(1) AS Cant_criterios_anterior,
        criterio_entrada AS criterio_entrada_anterior
    FROM
        {bd_golden_x}.tbl_hcd_cartdet_stock
    WHERE
        Periodo_cierre = {periodo_anterior}
        {'AND criterio_entrada = "' + criterio + '"' if criterio else ''}
    GROUP BY
        Periodo_cierre_anterior, criterio_entrada_anterior
    """

    # Ejecutar las consultas y obtener los resultados
    df_actual = spark.sql(consulta_actual)
    df_anterior = spark.sql(consulta_anterior)

    # Verificar si la consulta anterior no retorna datos
    if df_anterior.rdd.isEmpty():
        print(f"No hay datos para el periodo anterior ({periodo_anterior}).")
        return

    # Convertir los resultados a listas
    periodo_actual = df_actual.select('Periodo_cierre_actual').rdd.flatMap(lambda x: x).collect()
    cant_criterios_actual = df_actual.select('Cant_criterios_actual').rdd.flatMap(lambda x: x).collect()
    criterio_entrada_actual = df_actual.select('criterio_entrada_actual').rdd.flatMap(lambda x: x).collect()

    periodo_anterior = df_anterior.select('Periodo_cierre_anterior').rdd.flatMap(lambda x: x).collect()
    cant_criterios_anterior = df_anterior.select('Cant_criterios_anterior').rdd.flatMap(lambda x: x).collect()
    criterio_entrada_anterior = df_anterior.select('criterio_entrada_anterior').rdd.flatMap(lambda x: x).collect()

    if not criterio_entrada_actual or not cant_criterios_actual:
        print(f"No hay datos para el periodo actual ({periodo_consulta}) con criterio = {criterio}.")
        return

    if not criterio_entrada_anterior or not cant_criterios_anterior:
        print(f"No hay datos para el periodo anterior ({periodo_anterior}) con criterio = {criterio}.")
        return

    # Ordenar los criterios de entrada de forma ascendente
    criterio_entrada_anterior, cant_criterios_anterior = zip(*sorted(zip(criterio_entrada_anterior, cant_criterios_anterior)))
    criterio_entrada_actual, cant_criterios_actual = zip(*sorted(zip(criterio_entrada_actual, cant_criterios_actual)))

    # Crear el gráfico de barras
    plt.figure(figsize=(10, 6))
    plt.bar(range(len(criterio_entrada_anterior)), cant_criterios_anterior, width=0.4, align='center', label='Periodo Anterior')
    plt.bar(range(len(criterio_entrada_actual)), cant_criterios_actual, width=0.4, align='edge', label='Periodo Actual')

    plt.xlabel('Criterio de Entrada')
    plt.ylabel('Cantidad de Registros')
    plt.title(f'Variación del Criterio de Entrada entre Periodos ({periodo_anterior} - {periodo_consulta})')
    plt.legend()

    # Calcular la variación porcentual
    variacion_porcentual = [
        (actual - anterior) / anterior * 100
        for anterior, actual in zip(cant_criterios_anterior, cant_criterios_actual)
    ]

    # Verificar si hay alguna variación mayor al 10%
    hay_alerta = any(variacion > 10 for variacion in variacion_porcentual)

    if hay_alerta:
        print("¡Alerta! Hay una variación mayor al 10% en el criterio de entrada entre periodos.\n")

    for i, criterio in enumerate(criterio_entrada_actual):
        print(f"Criterio: {criterio}")
        print(f"Periodo Anterior: {cant_criterios_anterior[i]} registros")
        print(f"Periodo Actual: {cant_criterios_actual[i]} registros")
        print(f"Variación Porcentual: {variacion_porcentual[i]:.2f}%\n")

    plt.xticks(range(len(criterio_entrada_actual)), criterio_entrada_actual)  # Establecer valores y etiquetas en el eje x
    plt.show()


# COMMAND ----------

# DBTITLE 1,ejecuta_notebooks
def ejecuta_notebooks(nombre_notebook, timeout, parametros_notebook) :
  """
  Esta función ejecuta un notebook.
  Para esto: recupera el estado en que terminó el notebook y en caso de error, recupera el código de error del notebook ejecutado y retorna la ejecución.
  
  Parámetros: 
  1: nombre_notebook --> Nombre y path del notebook a ejecutar
  2: timeout --> timeout seconds
  3: parametros_notebook --> arguments, parametros de ejecución del notebook
  
  Salida:
  1: JSON con coderror y msgerror
  """
  
  diainix = datetime.now()
  
  print("inicio")
  print(diainix)
  
  dia_ejecucion = diainix.strftime('%d-%m-%Y')
  hora_inicio =  diainix.strftime('%H:%M:%S')
  
  return_value = dbutils.notebook.run(nombre_notebook, timeout , parametros_notebook)

  diafinx = datetime.now()
  hora_fin =  diafinx.strftime('%H:%M:%S')
  
  print("fin")
  print(datetime.now())
  
  return(return_value)

# COMMAND ----------

# MAGIC %md
# MAGIC ### obtiene_parametro_seg
# MAGIC

# COMMAND ----------

def obtiene_parametro_seg(bdwork_x, proceso_x, tipo_x):
  try:
    paso_query = f"""
		select parametro1
		from 
		{bdwork_x}.tbl_cierreriesgo_parametro a
		,(select max(fecha_informada) as max_fec from {bdwork_x}.tbl_cierreriesgo_parametro WHERE proceso = '{proceso_x}' AND tipo_parametro = '{tipo_x}' and vigencia = 'V') b 
	where
	a.fecha_informada = b.max_fec
	and proceso = '{proceso_x}' AND tipo_parametro = '{tipo_x}' and vigencia = 'V'
""" 

    df_param=sql_safe(paso_query)
    data_collect = df_param.collect()

    parametros_salida = ""

    for row in data_collect:
      parametros_salida = parametros_salida + (row["parametro1"] + ",")

    parametros_salida = parametros_salida[:-1]
    
    if es_vacio(parametros_salida):
      raise ValueError("{\"coderror\":28011, \"msgerror\":\"Parámetro proceso = "+str(proceso_x)+" ,tipo_parametro "+str(tipo_x)+" no encontrado en BD\"}")
    
    return parametros_salida
  except Exception as e:
    raise ValueError("{\"coderror\":28012, \"msgerror\":\"Error al ejecutar query "+str(query_x)+" , error: "+str(e)+"\"}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dia_pre_prox_mes

# COMMAND ----------

def dia_pre_prox_mes(fecha):
    import datetime
    fecha_dt = datetime.datetime.strptime(fecha, '%Y%m%d')
    
    if fecha_dt.month == 12:
        primer_dia_proximo_mes = fecha_dt.replace(year=fecha_dt.year + 1, month=1, day=1)
    else:
        primer_dia_proximo_mes = fecha_dt.replace(month=fecha_dt.month + 1, day=1)
    
    dia_20 = primer_dia_proximo_mes.replace(day=20)
    
    dia_20_str = dia_20.strftime('%Y%m%d')
    
    return dia_20_str

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extra ultimo mes cargado en location

# COMMAND ----------

from pyspark.sql import SparkSession

def obtiene_ult_fec(location_x, fec_x):
   # Crear la sesión de Spark
    spark = SparkSession.builder.getOrCreate()

    # Consulta 1
    consulta1 = f"""
		select max(Fecha_informada) as fec_inf
		from delta.`{location_x}`  a
	  WHERE Fecha_informada <= '{fec_x}' 
    """ 

    # Ejecutar consulta 1
    resultado1 = spark.sql(consulta1)
    fec_ant = resultado1.collect()[0][0]

    return fec_ant

# COMMAND ----------

# MAGIC %md
# MAGIC ### ultimo_dia_mes

# COMMAND ----------

from datetime import datetime, timedelta

def ultimo_dia_mes(fecha):
  
    fecha_objeto = datetime.strptime(fecha, '%Y%m%d')
    fecha_objeto = fecha_objeto.replace(day=1) + timedelta(days=32)
    fecha_objeto = fecha_objeto.replace(day=1)
    ultimo_dia_mes = fecha_objeto - timedelta(days=1)
    resultado = ultimo_dia_mes.strftime('%Y%m%d')
    
    return resultado


# COMMAND ----------

# MAGIC %md
# MAGIC ### obtener_estados_tablas

# COMMAND ----------

def obtener_estados_tablas(tabla_x, tabla_x2, tabla_x3):
    # Consultas para obtener los conteos de cada tabla
    df1 = spark.sql(f"SELECT COUNT(*) AS total FROM {tabla_x}")
    conteo1 = df1.collect()[0]["total"]

    df2 = spark.sql(f"SELECT COUNT(*) AS total FROM {tabla_x2}")
    conteo2 = df2.collect()[0]["total"]

    df3 = spark.sql(f"SELECT COUNT(*) AS total FROM {tabla_x3}")
    conteo3 = df3.collect()[0]["total"]

    # Definir variables de control basadas en el conteo real de cada tabla
    tbl_cal_cam = "Sin_datos" if conteo1 == 0 else "Con_datos"
    tbl_cal_sin_cam = "Sin_datos" if conteo2 == 0 else "Con_datos"
    tbl_con_cam = "Sin_datos" if conteo3 == 0 else "Con_datos"

    # Crear widgets para pasar los estados
    dbutils.widgets.text("tbl_cal_cam", str(tbl_cal_cam))
    dbutils.widgets.text("tbl_cal_sin_cam", str(tbl_cal_sin_cam))
    dbutils.widgets.text("tbl_con_cam", str(tbl_con_cam))

    # Imprimir los resultados para que el orquestador los capture
    print(f"tbl_cal_cam={tbl_cal_cam}")
    print(f"tbl_cal_sin_cam={tbl_cal_sin_cam}")
    print(f"tbl_con_cam={tbl_con_cam}")

    # Retornar los estados para usar en otro lugar si es necesario
    return tbl_cal_cam, tbl_cal_sin_cam, tbl_con_cam


# COMMAND ----------

# MAGIC %md
# MAGIC ### obtener archivo periodo anterior

# COMMAND ----------

from datetime import datetime

def validar_concatenacion(ruta_adls_tbxpath, fecha_x, periodo_ant_y, fecha_ant_y,archpart3):
    ruta_larga = ruta_adls_tbxpath
    ruta_adls_out = "/".join(ruta_larga.split("/")[:7]) + "/"
    
    rutas = dbutils.fs.ls(ruta_adls_out)
    fechas = sorted([f.name.strip('/') for f in rutas if f.name.strip('/').isdigit()])
    
    periodo_actual = fecha_x  
    periodo_ant = periodo_ant_y  
    fecha_ant = fecha_ant_y  
    
    print(f"periodo_actual: {periodo_actual}")
    print(f"periodo_ant: {periodo_ant}")
    print(f"fecha_ant: {fecha_ant}")
    
    fecha_ant_dt = datetime.strptime(str(fecha_ant), "%Y%m%d") 
    periodo_actual_dt = datetime.strptime(str(periodo_actual), "%Y%m%d")  
    
    periodos_validos = []
    for f in fechas:
        if f.startswith(periodo_ant):
            f_str = str(f)  
            f_dt = datetime.strptime(f_str, "%Y%m%d")  
            if f_str != str(fecha_ant) and f_dt < periodo_actual_dt:
                periodos_validos.append(f_str)
    
    print(f"periodos_validos: {periodos_validos}")
    
    validacion_concatenacion = {
        "periodo_valido": False,
        "archivo_valido": False
    }
    
    if periodos_validos:
        periodo_anterior = periodos_validos[-1]
        print(f"Período anterior seleccionado: {periodo_anterior}")
        validacion_concatenacion["periodo_valido"] = True 
        
        try:
            archivos_pre_cierre = dbutils.fs.ls(f"{ruta_adls_out}{periodo_anterior}/PreCierre/")
        except Exception as e:
            print(f"Error al buscar archivos: {e}")
            validacion_concatenacion["mensaje"] = f"Error al buscar archivos: {e}"
            return validacion_concatenacion

        archivos_filtrados = [f for f in archivos_pre_cierre if f.name.startswith(archpart3)]
        archivos_validos = [f.path for f in archivos_filtrados if f.size > 0]
        
        if archivos_validos:
            print("Archivos encontrados y no vacíos:", archivos_validos)
            validacion_concatenacion["archivo_valido"] = True  
            validacion_concatenacion["archivos_validos"] = archivos_validos
        else:
            print("No se encontro archivo periodo anterior o están vacíos. Se genera solo con el periodo actual")
            validacion_concatenacion["mensaje"] = "No se encontro archivo periodo anterior o están vacíos. Se genera solo con el periodo actual."
    else:
        print("No se encontró un período anterior válido para concatenar.")
        validacion_concatenacion["mensaje"] = "No se encontró un período anterior válido para concatenar."
    
    print(f"Validación de concatenación: {validacion_concatenacion}")
    return validacion_concatenacion


# COMMAND ----------

# MAGIC %md
# MAGIC ### concatena archivos

# COMMAND ----------

from pyspark.sql import SparkSession

def concatena_archivos(validar_concatenacion, pathfinal2, pathfinal3):
    if validar_concatenacion["periodo_valido"] and validar_concatenacion["archivo_valido"]:
        print("Se cumplen las condiciones. Ejecutando lógica de concatenación...")

        archivo_ant = validar_concatenacion.get("archivos_validos")
        archivo_act = pathfinal2

        print(f"Archivo anterior: {archivo_ant}")
        print(f"Archivo actual: {archivo_act}")

        # Leer archivos con Spark
        df_ant = spark.read.option("header", "false").text(archivo_ant)
        df_act = spark.read.option("header", "false").text(archivo_act)

        # Concatenar, eliminar duplicados y renombrar columna
        df_concat = df_ant.unionByName(df_act).distinct()
        df_concat = df_concat.withColumnRenamed("value", "codigo")

        df_concat.show(truncate=False)

        # Guardar temporalmente
        ruta_temporal = "/tmp/concat_arch"
        df_concat.coalesce(1).write.mode("overwrite").text(ruta_temporal)

        # Obtener nombre del archivo generado
        archivo_generado = [f.path for f in dbutils.fs.ls(ruta_temporal) if f.name.startswith("part-")][0]

        # Mover archivo final
        dbutils.fs.mv(archivo_generado, pathfinal3)

        print(f"Archivo guardado como: {pathfinal3}")
    else:
        print(f"Atención: {validar_concatenacion.get('mensaje', 'Sin mensaje')}")
        # Copia el archivo actual como respaldo si no hay período anterior válido
        dbutils.fs.cp(pathfinal2, pathfinal3)


# COMMAND ----------

# MAGIC %md
# MAGIC ###primer_dia_mes_sig

# COMMAND ----------

def primer_dia_mes_sig(fecha):
    from datetime import datetime

    fecha_dt = datetime.strptime(fecha, '%Y%m%d')
    
    if fecha_dt.month == 12:
        primer_dia_mes_sig = datetime(fecha_dt.year + 1, 1, 1)
    else:
        primer_dia_mes_sig = datetime(fecha_dt.year, fecha_dt.month + 1, 1)
    
    pri_dia_mes = primer_dia_mes_sig.strftime('%Y%m%d')
    
    return pri_dia_mes

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calcula fecha X meses atras

# COMMAND ----------

def fecha_x_meses_atras(fecha,xmeses):
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    fecha_dt = datetime.strptime(fecha, '%Y%m%d')
    fecha_x_meses_atras = fecha_dt - relativedelta(months=xmeses)
      
    return fecha_x_meses_atras.strftime('%Y%m%d')

# COMMAND ----------

# MAGIC %md
# MAGIC ## FIN definición de funciones