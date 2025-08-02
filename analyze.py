


def bestknown_foreign_language(df):

    return None

def early_leavers_from_education(df):

    return None

def educational_attainment_level(df):

    return None

def employment_rates(df):

    return None

def income_group_dwelling_type(df):

    return None

def self_employment(df):

    return None

def self_perceived_health(df):

    return None



function_dispatcher = {
    "processed_bestknown_foreign_language.csv": bestknown_foreign_language,
    "processed_early_leavers_from_education.csv": early_leavers_from_education,
    "processed_educational_attainment_level.csv": educational_attainment_level,
    "processed_employment_rates.csv": employment_rates, 
    "processed_income_group_dwelling_type.csv": income_group_dwelling_type, 
    "processed_self_employment.csv": self_employment, 
    "processed_self_perceived_health.csv": self_perceived_health
}