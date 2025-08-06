from pyspark.sql import functions as F

def bestknown_foreign_language(df):
    year_cols = [c for c in df.columns if c.startswith("year_")]
    aggregations = [F.avg(col).alias(f"avg_{col}") for col in year_cols]

    # Grupează și calculează media
    result = df.groupBy("lev_know", "deg_urb").agg(*aggregations)

    # Afișează rezultatul
    result.show(truncate=False)
    

def early_leavers_from_education(df):
    year_columns = [c for c in df.columns if c.startswith("year_")]

    # Extragem anii din denumirea coloanelor
    batches = {
        "year2005_2009": [f"year_{y}" for y in range(2005, 2010)],
        "year2010_2014": [f"year_{y}" for y in range(2010, 2015)],
        "year2015_2019": [f"year_{y}" for y in range(2015, 2020)],
        "year2020_2024": [f"year_{y}" for y in range(2020, 2024)],
    }

    # Construim lista de medii pentru fiecare batch
    aggregations = []
    for batch_name, cols in batches.items():
        valid_cols = [c for c in cols if c in df.columns]  # poate lipsesc unele coloane
        if valid_cols:
            avg_expr = sum([F.col(c) for c in valid_cols]) / len(valid_cols)
            aggregations.append(avg_expr.alias(f"avg_{batch_name}"))

    # Grupăm după geo și calculăm mediile pe batch-uri
    result = df.groupBy("geo").agg(*aggregations)

    # Afișăm rezultatul
    result.show(truncate=False)
    

def educational_attainment_level(df):
    year_columns = [c for c in df.columns if c.startswith("year_")]

    # Extragem anii din denumirea coloanelor
    batches = {
        "year2005_2009": [f"year_{y}" for y in range(2004, 2010)],
        "year2010_2014": [f"year_{y}" for y in range(2010, 2015)],
        "year2015_2019": [f"year_{y}" for y in range(2015, 2020)],
        "year2020_2024": [f"year_{y}" for y in range(2020, 2024)],
    }

    # Construim lista de medii pentru fiecare batch
    aggregations = []
    for batch_name, cols in batches.items():
        valid_cols = [c for c in cols if c in df.columns]  # poate lipsesc unele coloane
        if valid_cols:
            avg_expr = sum([F.col(c) for c in valid_cols]) / len(valid_cols)
            aggregations.append(avg_expr.alias(f"avg_{batch_name}"))

    # Grupăm după geo și calculăm mediile pe batch-uri
    result = df.groupBy("age", "deg_urb").agg(*aggregations)

    # Afișăm rezultatul
    result.show(truncate=False)
    

def employment_rates(df):
    year_columns = [c for c in df.columns if c.startswith("year_")]

    # Extragem anii din denumirea coloanelor
    batches = {
        "year2005_2009": [f"year_{y}" for y in range(2004, 2010)],
        "year2010_2014": [f"year_{y}" for y in range(2010, 2015)],
        "year2015_2019": [f"year_{y}" for y in range(2015, 2020)],
        "year2020_2024": [f"year_{y}" for y in range(2020, 2024)],
    }

    # Construim lista de medii pentru fiecare batch
    aggregations = []
    for batch_name, cols in batches.items():
        valid_cols = [c for c in cols if c in df.columns]  # poate lipsesc unele coloane
        if valid_cols:
            avg_expr = sum([F.col(c) for c in valid_cols]) / len(valid_cols)
            aggregations.append(avg_expr.alias(f"avg_{batch_name}"))

    # Grupăm după geo și calculăm mediile pe batch-uri
    result = df.groupBy("geo", "deg_urb").agg(*aggregations)

    # Afișăm rezultatul
    result.show(truncate=False)
    

def income_group_dwelling_type(df):
    year_columns = [c for c in df.columns if c.startswith("year_")]

    # Extragem anii din denumirea coloanelor
    batches = {
        "year2005_2009": [f"year_{y}" for y in range(2004, 2010)],
        "year2010_2014": [f"year_{y}" for y in range(2010, 2015)],
        "year2015_2019": [f"year_{y}" for y in range(2015, 2020)],
        "year2020_2024": [f"year_{y}" for y in range(2020, 2024)],
    }

    # Construim lista de medii pentru fiecare batch
    aggregations = []
    for batch_name, cols in batches.items():
        valid_cols = [c for c in cols if c in df.columns]  # poate lipsesc unele coloane
        if valid_cols:
            avg_expr = sum([F.col(c) for c in valid_cols]) / len(valid_cols)
            aggregations.append(avg_expr.alias(f"avg_{batch_name}"))

    # Grupăm după geo și calculăm mediile pe batch-uri
    result = df.groupBy("building", "deg_urb").agg(*aggregations)

    # Afișăm rezultatul
    result.show(truncate=False)
    

def self_employment(df):
    year_columns = [c for c in df.columns if c.startswith("year_")]

    # Extragem anii din denumirea coloanelor
    batches = {
        "year2005_2009": [f"year_{y}" for y in range(2005, 2010)],
        "year2010_2014": [f"year_{y}" for y in range(2010, 2015)],
        "year2015_2019": [f"year_{y}" for y in range(2015, 2020)],
        "year2020_2024": [f"year_{y}" for y in range(2020, 2024)],
    }

    # Construim lista de medii pentru fiecare batch
    aggregations = []
    for batch_name, cols in batches.items():
        valid_cols = [c for c in cols if c in df.columns]  # poate lipsesc unele coloane
        if valid_cols:
            avg_expr = sum([F.col(c) for c in valid_cols]) / len(valid_cols)
            aggregations.append(avg_expr.alias(f"avg_{batch_name}"))

    # Grupăm după geo și calculăm mediile pe batch-uri
    result = df.groupBy("wstatus", "deg_urb").agg(*aggregations)

    # Afișăm rezultatul
    result.show(truncate=False)
    

def self_perceived_health(df):
    year_columns = [c for c in df.columns if c.startswith("year_")]

    # Extragem anii din denumirea coloanelor
    batches = {
        "year2005_2009": [f"year_{y}" for y in range(2005, 2010)],
        "year2010_2014": [f"year_{y}" for y in range(2010, 2015)],
        "year2015_2019": [f"year_{y}" for y in range(2015, 2020)],
        "year2020_2024": [f"year_{y}" for y in range(2020, 2024)],
    }

    # Construim lista de medii pentru fiecare batch
    aggregations = []
    for batch_name, cols in batches.items():
        valid_cols = [c for c in cols if c in df.columns]  # poate lipsesc unele coloane
        if valid_cols:
            avg_expr = sum([F.col(c) for c in valid_cols]) / len(valid_cols)
            aggregations.append(avg_expr.alias(f"avg_{batch_name}"))

    # Grupăm după geo și calculăm mediile pe batch-uri
    result = df.groupBy("levels", "deg_urb", "sex").agg(*aggregations)

    # Afișăm rezultatul
    result.show(truncate=False)
    



function_dispatcher = {
    "processed_bestknown_foreign_language.csv": bestknown_foreign_language,
    "processed_early_leavers_from_education.csv": early_leavers_from_education,
    "processed_educational_attainment_level.csv": educational_attainment_level,
    "processed_employment_rates.csv": employment_rates, 
    "processed_income_group_dwelling_type.csv": income_group_dwelling_type, 
    "processed_self_employment.csv": self_employment, 
    "processed_self_perceived_health.csv": self_perceived_health
}
