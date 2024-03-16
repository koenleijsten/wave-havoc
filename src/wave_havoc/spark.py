from typing import Optional, Self
from pyspark.sql import SparkSession


class Spark:
    """
    A Singleton class to get a valid Spark session
    """

    _spark: Optional[SparkSession] = None
    _configurations = {
        "spark.sql.session.timeZone": "Etc/UTC",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
    }
    _master = None

    @classmethod
    def set_master(cls, master: str) -> Self:
        """Set the master location for the Spark session.

        Args:
            master (str): The location of the Spark master.

        Raises:
            RuntimeError: If attempting to set master after Spark session creation.

        Returns:
            Spark: The Spark class for method chaining.
        """
        if cls._master is not None:
            raise RuntimeError(
                "Cannot set master after Spark session has been created."
            )
        cls._master = master
        return cls

    @classmethod
    def get_session(cls) -> SparkSession:
        """Get or create the Spark session with configured settings.

        Returns:
            SparkSession: The Spark session with configured settings.
        """
        if cls._spark is not None:
            return cls._spark

        builder = SparkSession.builder
        if cls._master is not None:
            builder = builder.master(cls._master)
        for key, value in cls._configurations.items():
            builder = builder.config(key, value)
        cls._spark = builder.getOrCreate()
        return cls._spark
