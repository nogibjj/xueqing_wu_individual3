"""
query and viz file
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = ("""
    SELECT year, SUM(births) AS total_birth
    FROM (
        SELECT year, births
        FROM birth2000_delta
        UNION ALL
        SELECT year, births
        FROM birth1994_delta
    ) AS merged_tables 
    GROUP BY year
    ORDER BY year
""")
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def viz():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    pandas_df = query.select("total_birth", "year").toPandas()

    # Plot a bar plot
    plt.figure(figsize=(15, 8))
    plt.bar(pandas_df["year"], pandas_df["total_birth"], color='skyblue')
    plt.title("Total Births for Each Year")
    plt.xlabel("Year")
    plt.ylabel("Total Births")
    plt.show()



    # Plot a single histogram for all years
    plt.figure(figsize=(15, 8))
    plt.hist(pandas_df["total_birth"], bins=20, edgecolor='black')  
    # Adjust the number of bins as needed
    plt.title("Total Births for All Years")
    plt.xlabel("Total Births")
    plt.ylabel("Frequency")
    plt.show()
    

if __name__ == "__main__":
    query_transform()
    viz()