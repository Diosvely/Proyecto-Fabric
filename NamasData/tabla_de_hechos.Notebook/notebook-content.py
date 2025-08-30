# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "de76f0fe-6205-45e5-aa76-7b2c8e557725",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "0cd16d15-c661-4d9d-899a-376896ab25f0",
# META       "known_lakehouses": [
# META         {
# META           "id": "de76f0fe-6205-45e5-aa76-7b2c8e557725"
# META         },
# META         {
# META           "id": "4bde9b09-848b-4e90-abc3-6bfd35f2a1d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# EXPLORAMOS LOS DATOS PARA CREAR LA TABLA DE HECHOS

# temperaturas maximas
tmax = spark.sql(
    """ -- estas comillas es para indicar que es otro codigo 
    SELECT date(time) as fecha , txPM, CodigoNacional from lh_bronze.temp_max 
    -- si el FROM no me da error con temp_max tengo que poner complero lh_bronze.temp_max
    """
    
)
tmax.show(5)

# temperaturas minimas

tmin = spark.sql(
    """
    SELECT date(time) as fecha, tnAM, CodigoNacional FROM temp_min
    """
)
tmin.show(5)

# tabla de hechos inferida por union de consultas
temperaturas = spark.sql(
    """
SELECT 
    DATE(A.time) AS fecha,
    A.txPM as Tmaxima,
    B.tnAM as Tminima,
    A.CodigoNacional
FROM temp_max A
JOIN temp_min B
    ON A.CodigoNacional = B.CodigoNacional
    AND DATE(A.time) = DATE(B.time);

    """
    )

temperaturas.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#guardo la tabla de hechos en el lh_Silver

temperaturas.write.mode("overwrite").saveAsTable("lh_silver.ft_temperaturas_estacion_dia")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# NOTAS PERSONALES DE ESTO 
# Puedo hacerlo en codigo SQL puro de esta manera pero solo seria para explotar pues no puedo llevarlo a un dataframe que despues lo pudiera ingresar en el lakehouse 

# CELL ********************

# MAGIC %%sql    -- esto fue posible porqye cambie el lenguaje a spark SQL 
# MAGIC SELECT 
# MAGIC     DATE(A.time) AS fecha,
# MAGIC     A.txPM as Tmaxima,
# MAGIC     B.tnAM as Tminima,
# MAGIC     A.CodigoNacional
# MAGIC FROM temp_max A
# MAGIC JOIN temp_min B
# MAGIC     ON A.CodigoNacional = B.CodigoNacional
# MAGIC     AND DATE(A.time) = DATE(B.time);
# MAGIC 
# MAGIC     --esto se elejuta bien pero no lo puedo guardar en un dataframe,
# MAGIC     -- puediera utilizarlo para explorar 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# esto es para ver como se commitea en GIT 

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
