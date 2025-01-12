# ESCUDERO FINAL ejercio 2 transformación rev 12-01-2025
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import lit
from pyspark.sql.functions import round, col
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import lower, col


# 0. Configuración de SparkSession con soporte para Hive
spark = SparkSession.builder \
    .appName("Airport Trips Processing") \
    .enableHiveSupport() \
    .getOrCreate()
    
    
# 1.- leemos los datos y creamos los DataFrames
df_rentacar = spark.read.option("header", "true").option("sep", ",").csv("hdfs://172.17.0.2:9000/ingest4/CarRentalData.csv")

# header=True: Indica que la primera fila del archivo es el encabezado con los nombres de las columnas.
#inferSchema=True: Permite que PySpark detecte automáticamente los tipos de datos para cada columna.
#ep=";": Especifica que el delimitador de columnas es un punto y coma (;) en lugar de la coma (,) predeterminada.

df_georef2 = spark.read.csv("hdfs://172.17.0.2:9000/ingest4/georef_usa.csv", header=True, inferSchema=True, sep=";")



# 1.1 Modificamos las columnas

# df_rentacar

current_columns = df_rentacar.columns

new_columns = [ col.replace('.', '_').replace(' ', '_').lower() if col != 'fuelType' else 'fueltype' for col in current_columns ]

df_rentacar = df_rentacar.toDF(*new_columns)

# 1.2 Redondear la columna 'rating' y castear a int
df_rentacar = df_rentacar.withColumn("rating", round(col("rating")).cast("int"))

# df_georef2

#  Reemplazar espacios " " por guiones bajos "_" en todos los nombres de columnas
df_georef2 = df_georef2.toDF(*[col.replace(" ", "_") for col in df_georef2.columns])

# Paso 2: Renombrar las columnas específicas según lo solicitado
df_georef2 = df_georef2.withColumnRenamed("Official_Code_State", "code_state") \
                       .withColumnRenamed("Official_Name_State", "name_state") \
                       .withColumnRenamed("United_States_Postal_Service_state_abbreviation", "location_state") \
                       .withColumnRenamed("Iso_3166-3_Area_Code", "iso_area_code")

# Paso 3: Borrar la columna "Geo_Shape"
df_georef2 = df_georef2.drop("Geo_Shape")



# Mostrar el resultado
#df_rentacar.show(5, truncate=False)

# 3 vamos a hacer la union de los dos DF rev 12-01-25

# Realizar un inner join por la columna 'location_state'

renta_join = df_rentacar.join(df_georef2, on="location_state", how="inner")


#se genero el nuevo DF unido

# Eliminar los registros con rating nulo
renta_join_cleaned = renta_join.filter(col("rating").isNotNull())

# Cambiar mayúsculas por minúsculas en la columna 'fuelType'
renta_join_cleaned = renta_join_cleaned.withColumn('fuelType', col('fuelType').lower())

# Excluir el estado Texas (TX)
renta_join_cleaned = renta_join_cleaned.filter(col('location_state') != 'TX')

# Mostrar el resultado después de las operaciones
#renta_join_cleaned.show(100, truncate=False)

#vamos a prepara el DF para la insercion en HIVE

# primero sacamos las columnas que no vamos a utilizar en HIVE

df_rental_hive = renta_join_cleaned.drop('Geo_Point', 'Code_State', 'Name_State', 'iso_area_code', 'Type','US_Postal_state', 'State_FIPS_Code', 'State_GNIS_Code', 'state_code')

#renombramos y casteamos las columnas y datos

df_rental_hive = df_rental_hive.selectExpr(
    "fuelType as fueltype",
    "CAST(rating AS INT) as rating",
    "CAST(rentertripstaken AS INT) as rentertripstaken",
    "CAST(reviewcount AS INT) as reviewcount",
    "location_city as city",
    "location_state as state_name",
    "CAST(rate_daily AS INT) as rate_daily",  
    "vehicle_make as make",
    "vehicle_model as model",
    "CAST(Year AS INT) as year",
    "CAST(vehicle_year AS INT) as vehicle_year"
)
# insertamos en hive

df_rental_hive.write.saveAsTable('car_rental_db.car_rental_analytics_dag', mode='overwrite')

#df_rental_hive.write.saveAsTable('car_rental_db.car_rental_analyticss', mode='overwrite')
