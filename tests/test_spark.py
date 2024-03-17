import unittest
from pyspark.sql import SparkSession
from src.wave_havoc.spark import Spark


class TestSpark(unittest.TestCase):
    def test_get_session(self):
        session = Spark.set_master("local[*]").get_session()
        self.assertIsInstance(session, SparkSession)

    def test_get_existing_session(self):
        session = Spark.get_session()
        self.assertIsInstance(session, SparkSession)
        self.assertEqual(session.sparkContext.master, "local[*]")
        session.stop()

    def test_set_master_success(self):
        Spark._master = None
        spark = Spark.set_master("local[*]").get_session()
        self.assertEqual(Spark._master, "local[*]")
        spark.stop()

    def test_set_master_after_creation(self):
        spark = Spark.get_session()

        with self.assertRaises(RuntimeError):
            Spark.set_master("local[*]")
        spark.stop()


if __name__ == "__main__":
    unittest.main()
