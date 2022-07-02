from typing import List
from datetime import date

import pyspark.sql.functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

try:
    from delta.tables import DeltaTable  # pylint: disable=import-error
except ModuleNotFoundError:
    DeltaTable = None  # testing

class MergeToDelta:
    """Class to merge spark df into a delta table"""

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        delta_path: str,
        updated_partitions_path: str,
        write_schema: StructType,
        key_columns: list,
        partition_columns: list,
    ):

        self.spark = spark
        self.df = df
        self.key_columns = key_columns
        self.write_schema = write_schema
        self.partition_columns = partition_columns
        self.delta_path = delta_path
        self.updated_partitions_path = updated_partitions_path

    def _get_delta_table_if_exists(self) -> "DeltaTable":
        """Returns delta table if exists, otherwise returns None"""
        try:
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
        except AnalysisException:  # delta table doesn't exist yet
            delta_table = None
        return delta_table

    def _make_key_columns_criteria(self) -> str:
        """Makes merge criteria expression based on key columns of dataset
        Example output:
            'delta_table.Timestamp = df.Timestamp AND
            delta_table.some_col = df.some_col
        """
        key_columns_criteria = [
            "delta_table." + column + " = " + "df." + column
            for column in self.key_columns
        ]
        key_columns_criteria = " AND ".join(key_columns_criteria)
        return key_columns_criteria

    def _get_dates_from_df(self) -> List[date]:
        """Gets all unique date or year-month combinations
        present in df and returns it as list
        """
        if "date" in self.partition_columns:
            df_dates = self.df.select("date").distinct()
            dates = [row.date for row in df_dates.collect()]
        else:
            df_year_month = self.df.select("year", "month").distinct()
            dates = [date(row.year, row.month, 1) for row in df_year_month.collect()]
        return dates

    def _make_search_space_reduction_criteria_by_date(self, dates: List[date]) -> str:
        """makes merge criteria expression for reduction
        of search space for datasets partitioned on date"""
        if len(dates) == 1:
            reduction_criteria = f'delta_table.date = "{next(iter(dates))}"'
        else:
            dates_string = '("' + '","'.join(str(d) for d in dates) + '")'
            reduction_criteria = f"delta_table.date IN {dates_string}"
        return reduction_criteria

    def _make_search_space_reduction_criteria_by_month(self, dates: List[date]) -> str:
        """Makes merge criteria expression for reduction of
        search space for datasets partitioned on year and month
        """
        months = []
        for date_ in dates:
            month = date(date_.year, date_.month, 1)
            if month not in months:
                months.append(month)
        reduction_criteria = (
            "("
            + " OR ".join(
                f"(delta_table.year = {m.year} AND delta_table.month = {m.month})"
                for m in months
            )
            + ")"
        )
        return reduction_criteria

    def _make_search_space_reduction_criteria(self) -> str:
        """makes merge criteria expression for reduction of search space"""
        dates = self._get_dates_from_df()
        if "date" in self.partition_columns:
            search_space_reduction_criteria = (
                self._make_search_space_reduction_criteria_by_date(dates)
            )
        else:
            search_space_reduction_criteria = (
                self._make_search_space_reduction_criteria_by_month(dates)
            )
        return search_space_reduction_criteria

    def _make_merge_criteria(self) -> str:
        """Makes merge criteria expression to be used in Delta merge operation"""
        key_columns_criteria = self._make_key_columns_criteria()
        search_space_reduction_criteria = self._make_search_space_reduction_criteria()
        merge_criteria = (
            f"({key_columns_criteria}) AND ({search_space_reduction_criteria})"
        )
        return merge_criteria

    def _make_merge_of_df_with_delta_table(
        self, delta_table: "DeltaTable"
    ) -> "DeltaTable":
        """Makes Delta merge operation that inserts all rows from
        df that do not have a matching row in the delta table
        """
        criteria = self._make_merge_criteria()
        merge = (
            delta_table.alias("delta_table")
            .merge(self.df.alias("df"), criteria)
            .whenNotMatchedInsertAll()
        )
        return merge

    def _merge_df_with_delta_table(self, delta_table: "DeltaTable") -> None:
        """Merges dataframe with existing delta table"""
        merge = self._make_merge_of_df_with_delta_table(delta_table)
        merge.execute()

    def _create_partitions_df(self) -> DataFrame:
        """Creates df with partition values present in input df
        Example output:
        +----+-----+---+-------+--------------+
        |year|month|day|  delta|partition_cols|
        +----+-----+---+-------+-------+------+
        |1999|    1|  1|myDelta|year,month,day|
        +----+-----+---+-------+-------+------+
        """
        partition_cols = self.partition_columns
        partitions_schema = StructType(
            [
                StructField("delta", StringType(), True),
                StructField("day", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("partition_cols", StringType(), True),
            ]
        )

        partitions_df = self.df.select(*partition_cols).distinct()
        for column in partitions_schema.jsonValue()["fields"]:

            partitions_df = partitions_df.withColumn(
                column["name"], f.lit(None).cast(column["type"])
            )

        return partitions_df

    def _write_partitions(self) -> None:
        """Writes partition values present in df to partitions delta table"""
        partitions_df = self._create_partitions_df()

        (
            partitions_df.coalesce(1)
            .write.format("delta")
            .mode("append")
            .partitionBy("productline", "dataset")
            .save(self.updated_partitions_path)
        )

    def _create_delta_table_from_df(self) -> None:
        """Creates delta table from dataframe."""
        (
            self.df.repartition(*self.partition_columns)
            .write.format("delta")
            .partitionBy(*self.partition_columns)
            .save(self.delta_path)
        )

    def merge_to_delta(self) -> str:
        """Merges to or creates delta table"""
        delta_table = self._get_delta_table_if_exists()
        if delta_table is None:
            self._create_delta_table_from_df()
        else:
            self._merge_df_with_delta_table(delta_table)
        self._write_partitions()
        return "success"
