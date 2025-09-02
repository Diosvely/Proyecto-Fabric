# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#carga los datos desde el DWH
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

# **Creacion de la dim_municipio**

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df_dim_municipio = df_dwh.select("municipio").distinct()

window_spec = Window.orderBy("municipio")

df_dim_municipio = df_dim_municipio.withColumn("Id_municipio", row_number().over(window_spec))

df_dim_municipio = df_dim_municipio.select("Id_municipio", "municipio")

df_dim_municipio.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **carga de dim_municipio  a la capa Gold **

# CELL ********************

df_dim_municipio.write \
    .mode("overwrite") \
    .synapsesql("DWH_Silver.Gold.dim_municipio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **creacion dim_modalidad**

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df_dim_modalidad = df_dwh.select("modalidad").distinct()

window_spec = Window.orderBy("modalidad")

df_dim_modalidad = df_dim_modalidad.withColumn("Id_modalidad", row_number().over(window_spec))

df_dim_modalidad = df_dim_modalidad.select("Id_modalidad", "modalidad")

df_dim_modalidad.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_modalidad.write \
    .mode("overwrite") \
    .synapsesql("DWH_Silver.Gold.dim_modalidad")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Creacion dim_tipo**

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df_dim_tipo = df_dwh.select("tipo").distinct()

window_spec = Window.orderBy("tipo")

df_dim_tipo = df_dim_tipo.withColumn("Id_tipo", row_number().over(window_spec))

df_dim_tipo = df_dim_tipo.select("Id_tipo", "tipo")

df_dim_tipo.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **insertar datos de la dim_tipo en el DHW capa Gold**

# CELL ********************

df_dim_tipo.write \
    .mode("overwrite") \
    .synapsesql("DWH_Silver.Gold.dim_tipo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Creacion dim_codigo_postal**

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df_dim_codigopostal = df_dwh.select("codigo_postal").distinct()

window_spec = Window.orderBy("codigo_postal")

df_dim_codigopostal = df_dim_codigopostal.withColumn("Id_codigopostal", row_number().over(window_spec))

df_dim_codigopostal = df_dim_codigopostal.select("Id_codigopostal", "codigo_postal")

df_dim_codigopostal.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_codigopostal.write \
    .mode("overwrite") \
    .synapsesql("DWH_Silver.Gold.dim_codigopostal")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Creacion de la Tabla de Hechos Final**

# CELL ********************

from pyspark.sql.functions import trim, upper, col, monotonically_increasing_id

# Normalizar columnas base
df_dwh = spark.read.synapsesql("DWH_Silver.Silver.tablaHoteles") \
    .withColumn("municipio", trim(upper(col("municipio")))) \
    .withColumn("codigo_postal", col("codigo_postal").cast("string"))

# Leer dimensiones normalizadas
dim_municipio = spark.read.synapsesql("DWH_Silver.Gold.dim_municipio") \
    .withColumnRenamed("Id_municipio", "FK_Id_municipio")

dim_tipo = spark.read.synapsesql("DWH_Silver.Gold.dim_tipo") \
    .withColumnRenamed("Id_tipo", "FK_Id_tipo")

dim_modalidad = spark.read.synapsesql("DWH_Silver.Gold.dim_modalidad") \
    .withColumnRenamed("Id_modalidad", "FK_Id_modalidad")

dim_codigopostal = spark.read.synapsesql("DWH_Silver.Gold.dim_codigopostal") \
    .withColumnRenamed("Id_codigopostal", "FK_Id_codigopostal")

# Joins
df_fact = df_dwh \
    .join(dim_municipio, on="municipio", how="left") \
    .join(dim_tipo, on="tipo", how="left") \
    .join(dim_modalidad, on="modalidad", how="left") \
    .join(dim_codigopostal, on="codigo_postal", how="left")

# Selección de claves + métricas
df_fact_final = df_fact.select(
    monotonically_increasing_id().alias("Id_fact_sitios"),
    "FK_Id_municipio",
    "FK_Id_tipo",
    "FK_Id_modalidad",
    "FK_Id_codigopostal",
    "nombre",
    "direccion",
    "aforo_interior",
    "aforo_terraza"
)

# Guardar en Gold
df_fact_final.write.mode("overwrite").synapsesql("DWH_Silver.Gold.fact_sitios")

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
