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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#hacemos agregaciones desde la tabla de hechos y guardamos

mensual = spark.sql(
"""
SELECT 
year(fecha) as anno,
month(fecha) as mes,
CodigoNAcional,
ROUND(AVG(Tmaxima),6) as p_tmax,
ROUND(AVG(Tminima),6) as p_tminima
from
ft_temperaturas_estacion_dia
group by 
CodigoNacional, year(fecha) , month(fecha)

"""
)
mensual.show(5)

#guardamos la tabla de hechos agregada

mensual.write.mode("overwrite").saveAsTable("lh_silver.ft_temperaturas_estacion_mes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select approx_count_distinct (anno), anno from ft_temperaturas_estacion_mes
# MAGIC GROUP by anno
# MAGIC order by anno

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
