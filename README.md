# Delta Lake Helpers
Repo contains a skeleton of the app for processing large volumes of data using [Apache (Py)Spark](https://spark.apache.org/docs/latest/api/python/). The helpers contained in the app are platform agnostic, although some basic operations while running the code in [Databricks](https://databricks.com/) are included.

## Structure

### Dataset Config
Configuration files for datasets are located in `bin/dataset_config`:
* `dataset.ini` - powerful and simple way to store your config info for each dataset, like schema location, key columns or duplicates policy. It enables setting default values under `[DEFAULT]` section. More details on using `.ini` files in Python can be found [here](https://docs.python.org/3/library/configparser.html).
* `schemas/mySchema.json` - example schema file.


### Helpers
Main processing logic is located in `bin/helpers` folder. It contains Python files used to read, process and write data.
* `auth.py` - helpers for spark authentication<sup>*</sup>. See how to configure authentication, for Azure Data Lake Storage Gen2: [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access), for AWS S3: [here](https://docs.databricks.com/data/data-sources/aws/amazon-s3.html).
* `delta.py` - helpers for merging into [Delta tables](https://docs.delta.io/latest/quick-start.html).
* `logger.py` - helpers for logging into simple `.txt` files.
* `processing.py` - helpers for data reading and manipulation.
* `other.py` - other helpers.

<sup>*</sup> Bear in mind that [mounting](https://docs.databricks.com/data/databricks-file-system.html) storage enables all users of the workspace to access the mounted volume. If that's a security concern, consider adding auth credentials to `SparkSession`, through `spark.conf.set()`.

## Running in Databricks
File `bin/main.py` can be considered an entry point for running the code in Databricks.
### How to use helpers from this repo in Databricks
* Option 1: code can be copied into the notebooks and executed by importing specific functions into main.py.
* Option 2: one can use CI/CD pipeline that would create a library in artifactory on specific actions. This library can next be [installed on our cluster](https://docs.databricks.com/libraries/index.html) (mind that re-boot of a cluster is needed to include new version of a library) and imported into main.py.

### Using env variables in Databricks
* Option 1: one can store them in files (i.e., `config/processing.py`), then import them into our notebook using `MAGIC %run ../config/processing` command - the code will be executed and all associated variables would be now accessible. 
* Option 2: use [Job Parameters](https://docs.databricks.com/data-engineering/jobs/jobs.html#create-a-job) - then, if we add `"my_key": "my_val"` pair as a job parameter, it would be accessible in the notebook through `dbutils.widgets.get("my_key")` command.

Happy coding!