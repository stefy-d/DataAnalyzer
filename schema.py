from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType

bestknown_foreign_language = StructType([
    StructField("freq", StringType(), True),
    StructField("age", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("lev_know", StringType(), True),
    StructField("deg_urb", StringType(), True),
    StructField("geo", StringType(), True),
    StructField("year_2022", DoubleType(), True)
])

early_leavers_from_education = StructType([
    StructField("freq", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("wstatus", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("age", StringType(), True),
    StructField("deg_urb", StringType(), True),
    StructField("geo", StringType(), True),
    *[StructField(f"year_{year}", DoubleType(), True) for year in range(2005, 2024)]
])    

educational_attainment_level = StructType([
    StructField("freq", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("isced11", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("deg_urb", StringType(), True),
    StructField("age", StringType(), True),
    StructField("geo", StringType(), True),
    *[StructField(f"year_{year}", DoubleType(), True) for year in range(2004, 2024)]
]) 

employment_rates = StructType([
    StructField("freq", StringType(), True),
    StructField("deg_urb", StringType(), True),
    StructField("citizen", StringType(), True),
    StructField("isced11", StringType(), True),
    StructField("age", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("geo", StringType(), True),
    *[StructField(f"year_{year}", DoubleType(), True) for year in range(2002, 2024)]
]) 

income_group_dwelling_type = StructType([
    StructField("freq", StringType(), True),
    StructField("incgrp", StringType(), True),
    StructField("building", StringType(), True),
    StructField("deg_urb", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("geo", StringType(), True),
    *[StructField(f"year_{year}", DoubleType(), True) for year in range(2004, 2024)]
]) 

self_employment = StructType([
    StructField("freq", StringType(), True),
    StructField("deg_urb", StringType(), True),
    StructField("wstatus", StringType(), True),
    StructField("c_birth", StringType(), True),
    StructField("age", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("geo", StringType(), True),
    *[StructField(f"year_{year}", DoubleType(), True) for year in range(2005, 2024)]
]) 

self_perceived_health = StructType([
    StructField("freq", StringType(), True),
    StructField("deg_urb", StringType(), True),
    StructField("levels", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("age", StringType(), True),   
    StructField("unit", StringType(), True),
    StructField("geo", StringType(), True),
    *[StructField(f"year_{year}", DoubleType(), True) for year in range(2005, 2024)]
]) 

schema_dispatcher = {
    "processed_bestknown_foreign_language.csv": bestknown_foreign_language,
    "processed_early_leavers_from_education.csv": early_leavers_from_education,
    "processed_educational_attainment_level.csv": educational_attainment_level,
    "processed_employment_rates.csv": employment_rates, 
    "processed_income_group_dwelling_type.csv": income_group_dwelling_type, 
    "processed_self_employment.csv": self_employment, 
    "processed_self_perceived_health.csv": self_perceived_health
}
