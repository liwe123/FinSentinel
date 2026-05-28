import os
import sys

# Ensure PySpark uses the correct Python interpreter running pytest
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
