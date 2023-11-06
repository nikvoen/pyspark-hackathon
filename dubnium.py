from pyspark.sql.functions import col, mean, max, min, count
from pyspark.sql import functions

def df_sort(df, column, ascending=True):
    if ascending:
        sorted_df = df.sort(column)
    else:
        sorted_df = df.sort(column, ascending=False)
    
    return sorted_df

def df_filter(df, column, condition, value):
    if condition == '>':
        filtered_df = df.filter(col(column) > value)
    elif condition == '<':
        filtered_df = df.filter(col(column) < value)
    elif condition == '==':
        filtered_df = df.filter(col(column) == value)
    elif condition == '!=':
        filtered_df = df.filter(col(column) != value)
    else:
        raise ValueError("Invalid condition. Supported conditions are >, <, ==, !=")
    
    return filtered_df

def df_mean(df, column):
    mean_value = df.select(mean(column)).first()[0]
    return mean_value

def df_max(df, column):
    max_value = df.select(max(col(column))).first()[0]
    return max_value

def df_min(df, column):
    min_value = df.select(min(col(column))).first()[0]
    return min_value

def df_count(df, column):
    count = df.select(count(col(column))).first()[0]
    return count

def df_quantile(df, column, quantile):
    if quantile not in [0.25, 0.5, 0.75]:
        raise ValueError("Invalid quantile value. Supported values are 0.25, 0.5, 0.75")
    
    expr = functions.expr(f'percentile_approx({column}, {quantile})')
    quantile_value = df.agg(expr.alias(column)).first()[0]
    return quantile_value
