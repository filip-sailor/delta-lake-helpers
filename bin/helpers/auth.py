from pyspark.sql.session import SparkSession

try:
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
except ImportError:
    pass


def authenticate_spark(
    spark,
) -> None:
    """
    Sets config of a Spark Session to enable OAuth access.
    SparkSession object is returned inplace (function returns None).

    Args:
        spark: SparkSession object
        tenant_id: Azure Tenant id
        client_id: Azure Client id
        client_secret: Azure Secret val
    """

    configs = {"Add Your Configs Here": ""}

    for config_key, config_val in configs.items():
        spark.conf.set(config_key, config_val)


def mount_storage(
    mount_source: str, mount_point: str, unmount_first: bool = True
) -> None:

    if unmount_first:
        try:
            dbutils.fs.unmount(mount_point)
        except:
            pass

    configs = {"Add Your Configs Here": ""}

    dbutils.fs.mount(
        source=mount_source, mount_point=mount_point, extra_configs=configs
    )

    print(f"Successfully mounted {mount_point}.")
