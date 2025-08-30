# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4bde9b09-848b-4e90-abc3-6bfd35f2a1d9",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "0cd16d15-c661-4d9d-899a-376896ab25f0",
# META       "known_lakehouses": [
# META         {
# META           "id": "4bde9b09-848b-4e90-abc3-6bfd35f2a1d9"
# META         },
# META         {
# META           "id": "de76f0fe-6205-45e5-aa76-7b2c8e557725"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Construccion de una dimension de tiempo a partir de los datos de temperatura
from pyspark.sql.functions import col, to_date, date_format

# cargar datos 
df= spark.table("lh_bronze.temp_min")
# me quedo solo con la columna de time
df_time = df.select("time").distinct()  # quita duplicados 

# agragar fecha y hora respectivamente 
df_th = (
    df_time.withColumn("fecha",to_date(col("time")))   # para sacar la fecha 
            .withColumn("hora",date_format(col("time"),"HH:mm:ss")) # para sacar la hora 
)
df_th.show()  #mostrar el resultado 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#agragar mas campos a partir de df_th
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, dayofweek, weekofyear, quarter, date_format

df_time = df_th.withColumn("fecha", to_date(col("fecha"))) \
    .withColumn("anio", year(col("fecha"))) \
    .withColumn("mes", month(col("fecha"))) \
    .withColumn("dia", dayofmonth(col("fecha"))) \
    .withColumn("nombre_mes", date_format(col("fecha"), "MMMM")) \
    .withColumn("trimestre", quarter(col("fecha"))) \
    .withColumn("semana_anio", weekofyear(col("fecha"))) \
    .withColumn("dia_semana", dayofweek(col("fecha"))) \
    .withColumn("nombre_dia", date_format(col("fecha"), "EEEE"))\
    .withColumn("dia_corr_anual", dayofyear(col("fecha")))
df_time.show()
               
                


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Materializar en un tabla en el lh_silver 
df_time.write.mode("overwrite").saveAsTable("lh_silver.dim_tiempo_fecha_hora")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
