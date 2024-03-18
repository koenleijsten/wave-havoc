import unittest
from pyspark.sql import SparkSession
from src.wave_havoc.spark import Spark


class TestSpark(unittest.TestCase):
    def tearDown(self):
        if Spark._spark is not None:
            Spark._spark.stop()
        Spark._master = None
        Spark._spark = None

    def test_get_session(self):
        session = Spark.set_master("local[*]").get_session()
        self.assertIsInstance(session, SparkSession)

    def test_get_existing_session(self):
        session1 = Spark.set_master("local[*]").get_session()
        session2 = Spark.get_session()
        self.assertIsInstance(session2, SparkSession)
        self.assertEqual(session2.sparkContext.master, "local[*]")

    def test_set_master_success(self):
        Spark.set_master("local[*]").get_session()
        self.assertEqual(Spark._master, "local[*]")

    def test_set_master_after_creation(self):
        Spark.set_master("local[*]").get_session()

        with self.assertRaises(RuntimeError):
            Spark.set_master("local[*]")


if __name__ == "__main__":
    unittest.main()
