from pyspark.sql import SparkSession
from pyspark.sql.functions import split, count

# Inicialize uma sessão Spark
spark = SparkSession.builder.appName("AnaliseDeLogsEmLotes").getOrCreate()

# Configure o caminho para a pasta que contém os arquivos de log
pasta_logs = "logs/"

# Leia os arquivos de log de texto
dados_log = spark.read.text(pasta_logs)

# Separe os registros de log com base em um delimitador (por exemplo, espaço em branco)
registros = dados_log.selectExpr("split(value, ' ') as dados_log")

# Extraia o tipo de evento (por exemplo, erro, aviso, informação) do registro de log
tipo_evento = registros.select(registros.dados_log[0].alias("tipo_evento"))

# Realize a contagem de eventos por tipo
qtd_evento = tipo_evento.groupBy("tipo_evento").agg(count("tipo_evento").alias("qtd_evento"))

# Exiba os resultados
qtd_evento.show()
