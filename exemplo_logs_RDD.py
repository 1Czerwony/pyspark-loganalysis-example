from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Inicialize uma sessão Spark
conf = SparkConf().setAppName("AnaliseDeLogsEmLotes")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Configure o caminho para a pasta que contém os arquivos de log
pasta_logs = "logs/"

# Leia os arquivos de log de texto como RDD
dados_logs = sc.textFile(pasta_logs, 4)

# Separe os registros de log com base em um delimitador (espaço em branco)
registros = dados_logs.map(lambda line: line.split(' '), 4)

# Extraia o tipo de evento (erro, aviso, informação) do registro de log
tipo_evento = registros.map(lambda entry: (entry[0], 1), 4)

# Realize a contagem de eventos por tipo
qtd_eventos = tipo_evento.reduceByKey(lambda a, b: a + b, 4)

# Junte os resultados em uma única partição e salve-os em um arquivo de texto na pasta "saida"
qtd_eventos.coalesce(1).saveAsTextFile("saida")

# Encerre a sessão Spark
spark.stop()
