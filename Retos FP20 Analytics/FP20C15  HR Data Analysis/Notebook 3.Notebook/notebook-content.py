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

# MARKDOWN ********************

# Me conecto con el DHW silver 

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

# **Crear y cargar la dim_genero**

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df_dim_genero = df_dwh.select("Género").distinct()

window_spec = Window.orderBy("Género")

df_dim_genero = df_dim_genero.withColumn("Id_genero", row_number().over(window_spec))

df_dim_genero = df_dim_genero.select("Id_genero", "Género")

df_dim_genero.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_genero.write \
    .mode("overwrite") \
    .synapsesql("DWH_FP20C15.Gold.dim_genero")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Crear y cargar la dim_departamento**

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df_dim_departamento = df_dwh.select("Departmento").distinct()

window_spec = Window.orderBy("Departmento")

df_dim_departamento = df_dim_departamento.withColumn("Id_dpto", row_number().over(window_spec))

df_dim_departamento = df_dim_departamento.select("Id_dpto", "Departmento")

df_dim_departamento.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_departamento.write \
    .mode("overwrite") \
    .synapsesql("DWH_FP20C15.Gold.dim_departamento")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
