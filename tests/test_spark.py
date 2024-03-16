import unittest
from src.wave_havoc.spark import Spark

class TestSparkImport(unittest.TestCase):
    def test_spark(self):
        spark = Spark.master("local[*]").get()
        df = spark.sql("select 42")
        self.assertEqual(42, df.collect()[0][0])


if __name__ == "__main__":
    unittest.main()