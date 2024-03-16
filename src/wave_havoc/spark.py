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
    def add_config(cls, key: str, value: str) -> Self:
        """
        Add configuration to Spark session settings.

        Args:
            key (str):  The configuration key.
            value (str): The configuration value.

        Raises:
            Exception: If attempting to modify configuration after Spark session creation.
            ValueError: If either key or value is None.

        Returns:
            Spark: The Spark class for method chaining.
        """
        if cls._spark is not None:
            raise Exception("Configuration method called after spark session build.")
        if key is None or value is None:
            raise ValueError("Both key and value must be provided.")

        cls._configurations[key] = value
        return cls

    @classmethod
    def delete_config(cls, key: str) -> Self:
        """
        Delete configuration from Spark session settings based on the key.

        Args:
            key (str):  The configuration key.

        Raises:
            Exception: If attempting to modify configuration after Spark session creation.
            ValueError: If either key is None.
            KeyError: If the key is not found in configurations.

        Returns:
            Spark: The Spark class for method chaining.
        """
        if cls._spark is not None:
            raise Exception("Configuration method called after spark session build.")
        if key is None:
            raise ValueError("Key must be provided.")
        try:
            del cls._configurations[key]
        except KeyError:
            raise KeyError(f"Key '{key}' not found in configurations.")
        return cls

    @classmethod
    def get_configurations(cls) -> dict:
        """
        Get the current configurations from the Spark session settings.

        Returns:
            dict: A dictionary containing the current configurations.
        """
        return cls._configurations.copy()

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
