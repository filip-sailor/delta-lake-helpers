import pyspark.sql.functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType


class DataReader:
    """Class to load parquet data"""

    def __init__(
        self,
        spark: SparkSession,
        source_files: list,
        options: dict = {},
    ):
        self.spark = spark
        self.source_files = source_files
        self.options = options

    def read_parquet(self) -> DataFrame:
        """Reads parquet spark df"""
        # Parquet preserves schema
        if self.options:
            df = self.spark.read.options(**self.options).parquet(*self.source_files)

        else:
            df = self.spark.read.parquet(*self.source_files)

        return df


class DataProcessor:
    """Contains steps and logic that transform parquet files"""

    def __init__(self, df: DataFrame, write_schema: StructType, key_columns: list):

        self.df = df
        self.write_schema = write_schema
        self.key_columns = key_columns

    def _drop_invalid_cols(self, cols: list) -> None:
        """Drops cols and columns with invalida characters.
        Is permissive: if column does not exist,
        function continues without raising exception"""

        invalid_chars = [" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]

        cols_invalid_chars = [
            col
            for col in self.df.columns
            if any(inv_char in col for inv_char in invalid_chars)
        ]
        cols_to_drop = cols_invalid_chars + cols
        self.df = self.df.drop(*cols_to_drop)

    def _rename_cols(self, old_new_map: dict) -> None:
        """Renames columns in a dict: {old_name: new_name}.
        Is permissive, if col is not in df.columns, ignores it"""

        for old_nm, new_nm in old_new_map.items():
            self.df = self.df.withColumnRenamed(old_nm, new_nm)

    def _add_date_cols(self, tstmp_col: str = "Timestamp") -> None:
        """Adds TDS date columns into a df"""
        temp_tstmp_col = "temp_tstmp"

        self.df = self.df.withColumn(temp_tstmp_col, f.to_timestamp(f.col(tstmp_col)))
        self.df = self.df.withColumn("year", f.year(f.col(temp_tstmp_col)))
        self.df = self.df.withColumn("month", f.month(f.col(temp_tstmp_col)))
        self.df = self.df.withColumn("day", f.dayofmonth(f.col(temp_tstmp_col)))

        self.df = self.df.withColumn(
            "date",
            f.concat(
                f.col("year"), f.lit("-"), f.col("month"), f.lit("-"), f.col("day")
            ).cast("date"),
        )

        self.df = self.df.drop(temp_tstmp_col)

    def _cast_dataframe(self):
        """Cast dataframe columns to write_schema types"""
        expr_list = []

        for field in self.write_schema.fields:

            if field.name in self.df.columns:
                expr_list.append(
                    f"cast(`{field.name}` as"
                    f" {field.dataType.simpleString()})"
                    f" `{field.name}`"
                )

        self.df = self.df.selectExpr(*expr_list)

    def _add_empty_cols(self):
        """Adds empty columns from write_schema that are not in df.
        Used union instead of f.lit() due to a performnce reasons.
        """

        df_empty = self.spark.createDataFrame([], self.write_schema)
        self.df = df_empty.unionByName(self.df, allowMissingColumns=True)

    def _drop_key_duplicates(self):
        """Drops duplicates based on a subset of key columns"""

        self.df = self.df.dropDuplicates(subset=self.key_columns)

    def process_df(self):
        """Executes all steps of a pipeline"""
        self._drop_invalid_cols()
        self._rename_cols()
        self._add_date_cols()
        self._cast_dataframe()
        self._add_empty_cols()
        self._drop_key_duplicates()

        return self.df


