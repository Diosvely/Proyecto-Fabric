# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2dcda3d3-fd08-41e2-8585-69d737eb3fc4",
# META       "default_lakehouse_name": "lh_crudo_tenerife",
# META       "default_lakehouse_workspace_id": "0cd16d15-c661-4d9d-899a-376896ab25f0",
# META       "known_lakehouses": [
# META         {
# META           "id": "2dcda3d3-fd08-41e2-8585-69d737eb3fc4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# **Cargar la libreria pandas y crear el DataFrame**

# CELL ********************

import pandas as pd

# Ruta de tu archivo Excel en el Lakehouse
ruta_excel ="/lakehouse/default/Files/establecimientos-de-hosteleria-y-restauracion-de-tenerife.csv"  

# Leer el archivo Excel con pandas
df_pandas = pd.read_csv(ruta_excel)

# Opcional: convertir el DataFrame de pandas a un DataFrame de Spark
df_spark = spark.createDataFrame(df_pandas)

# Mostrar las primeras filas
df_spark.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **CAPA SILVER **

# CELL ********************

# Leo las columnas del dataframe 
df_spark.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# creando tabla en DWH en silver 

# CELL ********************

#Aqui importo los datos del dataframe en un dataWarehouse

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  

df_spark.write \
    .mode("overwrite") \
    .synapsesql("DWH_Silver.Silver.tablaHoteles")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **para saber si se inserto o no la informacion es como hacer un select * from **

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

df_dwh = spark.read.synapsesql("DWH_Silver.Silver.tablaHoteles")

df_dwh.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

