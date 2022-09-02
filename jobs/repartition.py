""" This module is used to repartition the data into n_partitions """

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types as T

# from pyspark.functions.udf import udf as UDF

# Repartition function
def dataset_repartition(
    spark_session: SparkSession,
    # schema,
    n_partitions: int = 4,
    input_path: str = None,
    output_path: str = None,
) -> None:
    """
    Repartition a specific pyspark DataFrame into n_partitions,
    taking a file path as input and a directory as output

    Args:
        spark_session (pyspark.sql.session.SparkSession): spark session used for the repartition
        n_partition (int): number of partitions for the resulting spark DataFrame
        input_path (str): dir [or file] path for the desired object to be repartitioned
        output_path (str): dir path for the desired localtion where the reapartition will be stored

    Returns:
        None
    """
    if n_partitions < 1:
        raise ValueError("n_partitions must be greater than 0")

    df = spark_session.read.option("header", "true").parquet(input_path)

    df.repartition(n_partitions).write.parquet(output_path, mode="overwrite")
    return df


# Main function
def main():
    n_partitions = 4

    with (
        SparkSession.builder.master("local[4]").appName("repartition").getOrCreate()
    ) as spark:

        for year in range(2021, 2022):
            for month in range(10, 13):
                if month < 10:
                    month = f"0{month}"
                else:
                    month = str(month)

                for taxi_type in ["yellow", "green", "fhv"]:
                    input_path = f"../data/raw/{taxi_type}/{year}/{month}/"
                    output_path = f"../data/part/{taxi_type}/{year}/{month}/"

                    print(
                        f"Repartitioning {input_path} into {n_partitions} partitions at {output_path}..."
                    )
                    dataset_repartition(spark, 4, input_path, output_path)


if __name__ == "__main__":
    main()
