# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f6c2679f-5d8f-4dad-9d97-ecc345fca96b",
# META       "default_lakehouse_name": "lh_crudo_FP20C14",
# META       "default_lakehouse_workspace_id": "0cd16d15-c661-4d9d-899a-376896ab25f0",
# META       "known_lakehouses": [
# META         {
# META           "id": "f6c2679f-5d8f-4dad-9d97-ecc345fca96b"
# META         }
# META       ]
# META     }
# META   }
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

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/Combustibles_Precios_Argentina_2016-2023.csv"
df = pd.read_csv("/lakehouse/default/Files/Combustibles_Precios_Argentina_2016-2023.csv")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
