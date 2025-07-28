from pyspark.sql import SparkSession
import re
import sys
import os
from pyspark.sql.functions import trim, col, regexp_extract, count, when, split, round
from pyspark.sql.types import FloatType, IntegerType, DoubleType
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler
from pyspark.ml.functions import vector_to_array


spark = SparkSession.builder \
    .appName("DataAnalyzer") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

input_path = sys.argv[1]
input_name = os.path.splitext(os.path.basename(input_path))[0]
output_path = f"output_{input_name}"

df = spark.read.option('header', 'true').csv(input_path, inferSchema=True)

# curat numele coloanelor si le redenumesc 
for column in df.columns:
    new_col = column.strip()
    if re.match(r'^\d', new_col):
        new_col = f"year_{new_col}"
        df = df.withColumn(column, regexp_extract(col(column), r'(^\d+(\.\d+))', 1))
    df = df.withColumnRenamed(column, new_col).withColumn(new_col, trim(col(new_col)))

df = df.na.replace('', None)

# calculez nr de valori nule pe fiecare coloana 
col_null_cnt_df =  df.select([count(when(col(c).isNull(),c)).alias(c) for c in df.columns])
df_count = df.count()
# elimin coloanele cu mai mult de 65% valori nule
df = df.select([col for col in df.columns if col_null_cnt_df.first()[col] < df_count * 0.65])

# castez la float valorile de pe coloanele cu ani 
for column in df.columns:
    if column.startswith("year_"):
        df = df.withColumn(column, col(column).cast(FloatType()))

# elimin randurile care au mai putin de jumatate din valori nenule
year_cols = [col for col in df.columns if col.startswith("year_")]
df = df.dropna(thresh=int(len(year_cols) * 0.5) + 1, subset=year_cols)

# selectez coloanele numerice
numeric_column_names = [column.name for column in df.schema.fields
                        if isinstance(column.dataType, (IntegerType, FloatType, DoubleType))]

# completez valorile lipsa din coloanele numerice cu media
imputer = Imputer(
    inputCols= numeric_column_names,
    outputCols=numeric_column_names,
    strategy="mean"
)

model = imputer.fit(df)
df = model.transform(df)

# normalizare date cu StandardScaler
assembler = VectorAssembler(
    inputCols=numeric_column_names, 
    outputCol="features"
)
df = assembler.transform(df)

scaler = StandardScaler(
    inputCol="features", 
    outputCol="scaled_features", 
    withStd=True, 
    withMean=False
) 
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)

df = df.withColumn("scaled_array", vector_to_array("scaled_features"))
for i, col_name in enumerate(numeric_column_names):
    df = df.withColumn(col_name, col("scaled_array")[i])

df = df.drop("features", "scaled_features", "scaled_array")

# rotunjesc la 3 zecimale
for c in numeric_column_names:
    df = df.withColumn(c, round(col(c), 3))

# salvez intr un sg fisier
df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

