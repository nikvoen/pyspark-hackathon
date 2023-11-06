# DataFrame Utilities with Pyspark

This simple set of DataFrame utilities was originally developed for a hackathon and is designed to simplify common data manipulation tasks in pyspark.

## Functions

### `df_sort(df, column, ascending=True)`

Sorts the DataFrame based on a specified column in ascending or descending order.

### `df_filter(df, column, condition, value)`

Filters the DataFrame based on a given condition and value. Supported conditions are '>', '<', '==', and '!='.

### `df_mean(df, column)`

Calculates the mean of the specified column in the DataFrame.

### `df_max(df, column)`

Finds the maximum value in the specified column of the DataFrame.

### `df_min(df, column)`

Finds the minimum value in the specified column of the DataFrame.

### `df_count(df, column)`

Counts the number of rows in the DataFrame that meet a specific condition based on the given column.

### `df_quantile(df, column, quantile)`

Calculates the specified quantile (0.25, 0.5, or 0.75) for the specified column in the DataFrame.

## Usage

Simply import these functions and apply them to your Spark DataFrame for quick and easy data manipulation.

```python
from dubnium import df_sort, df_filter, df_mean, df_max, df_min, df_count, df_quantile

# Example usage:
sorted_df = df_sort(df, "column_name")
filtered_df = df_filter(df, "column_name", ">", 5)
mean = df_mean(df, "column_name")
max_value = df_max(df, "column_name")
min_value = df_min(df, "column_name")
count = df_count(df, "column_name")
quantile_value = df_quantile(df, "column_name", 0.5)
