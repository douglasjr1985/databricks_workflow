import json
import time
import subprocess

import json
import time
import subprocess
import datetime


from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from concurrent.futures import ProcessPoolExecutor
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


import os;
# Imprime o diretório de trabalho atual
print("Diretório de trabalho atual:", os.getcwd())

arquivo_json = 'config/param.json'

with open(arquivo_json, 'r') as file:
    data = json.load(file)

print(data)
