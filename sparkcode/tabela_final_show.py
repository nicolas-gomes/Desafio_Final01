from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("nico_ibge_pucminas_parquet").getOrCreate()

tabela_final = (
    spark
    .read
    .parquet("s3://armazenamento-arquivos-20221511/resultado/")
)
tabela_final.show()





