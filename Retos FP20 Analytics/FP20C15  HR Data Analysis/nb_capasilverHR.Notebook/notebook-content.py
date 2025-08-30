# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5465ee51-bcd0-4333-afa0-1bfd25e4e264",
# META       "default_lakehouse_name": "lh_crudo_HR",
# META       "default_lakehouse_workspace_id": "0cd16d15-c661-4d9d-899a-376896ab25f0",
# META       "known_lakehouses": [
# META         {
# META           "id": "5465ee51-bcd0-4333-afa0-1bfd25e4e264"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Inicio leyendo la tabla desde mi lakehouse **

# CELL ********************

import pandas as pd

excel_path = "abfss://nmd_diosvely_perez@onelake.dfs.fabric.microsoft.com/lh_crudo_HR.Lakehouse/Files/HR Data Analysis_Spanish.xlsx"


df_pandas = pd.read_excel(excel_path, sheet_name="Datos")

df_spark = spark.createDataFrame(df_pandas)
df_spark.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **CAPA SILVER**

# MARKDOWN ********************

# **Buscar Duplicados**
# 
# Aqui se puede ver que el campo ID Empleado no es unico 

# CELL ********************

from pyspark.sql.functions import count
df_duplicates = df_spark.groupBy("ID Empleado").agg(count("*").alias("conteo"))

df_duplicates = df_duplicates.filter("conteo > 1")
 

df_duplicates.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fechas: asegurarnos que estén en formato date.**

# CELL ********************

from pyspark.sql.functions import to_date

df_spark = df_spark.withColumn("Fecha Contratación", to_date("Fecha Contratación")) \
                   .withColumn("Fecha Salida", to_date("Fecha Salida"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Nulos: revisar cuántos campos críticos están vacíos.**

# CELL ********************

from pyspark.sql.functions import col, sum

nulos = df_spark.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_spark.columns])
nulos.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# **IDENTIFICAR FECHAS VACIAS**

# CELL ********************

df_spark.select("Fecha Contratación", "Fecha Salida").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum

df_spark.select([
    sum(col(c).isNull().cast("int")).alias(c + "_nulas")
    for c in ["Fecha Contratación", "Fecha Salida"]
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **VALIDAR NOMBRES DE LAS COLUMNAS**

# CELL ********************

df_spark.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **CREANDO TABLA EN EL DWH SILVER**

# MARKDOWN ********************

# Antes de pasar este codigo Crear un datawarehaouse y crear una Schema con
# este codigo   
# 
# **CREATE  SCHEMA silver**

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  

df_spark.write \
    .mode("overwrite") \
    .synapsesql("DWH_FP20C15.Silver.tabla_completa_HR")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

df_dwh = spark.read.synapsesql("DWH_FP20C15.Silver.tabla_completa_HR")

df_dwh.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

