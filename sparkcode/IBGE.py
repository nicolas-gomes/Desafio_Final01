from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.getOrCreate()

ibge = (
    spark
    .read
    .csv("s3://armazenamento-arquivos-20221511/",
         header=True, sep=";", inferSchema = True, encoding="utf-8"
        )
)

ibge.printSchema()

ibge = (
    ibge
    .withColumnRenamed(ibge.columns[0], 'codigo_ibge_municipio')
    .withColumnRenamed(ibge.columns[1], 'nome_grandes_regioes')
    .withColumnRenamed(ibge.columns[2], 'nome_microregiao')
    .withColumnRenamed(ibge.columns[3], 'incompleto_amarela')
    .withColumnRenamed(ibge.columns[4], 'incompleto_branca')
    .withColumnRenamed(ibge.columns[5], 'incompleto_indigena')
    .withColumnRenamed(ibge.columns[6], 'incompleto_parda')
    .withColumnRenamed(ibge.columns[7], 'incompleto_preta')
    .withColumnRenamed(ibge.columns[8], 'superior_completo_amarela')
    .withColumnRenamed(ibge.columns[9], 'superior_completo_branca')
    .withColumnRenamed(ibge.columns[10], 'superior_completo_indigena')
    .withColumnRenamed(ibge.columns[11], 'superior_completo_parda')
    .withColumnRenamed(ibge.columns[12], 'superior_completo_preta')
)

ibge.printSchema()

(
    ibge
    .groupBy("nome_microregiao")
    .agg(
        f.round( f.mean("incompleto_amarela") , 2).alias("media")
    )
    .show()
)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("nico_ibge_pucminas").getOrCreate()

ibge.createOrReplaceTempView("ibge")

microregiao=spark.sql("""

    SELECT 
        nome_microregiao
        ,ROUND(AVG(incompleto_amarela),2) media_incompleto_amarela
        ,ROUND(AVG(incompleto_branca),2) media_incompleto_branca
        ,ROUND(AVG(incompleto_indigena),2) media_incompleto_indigena
        ,ROUND(AVG(incompleto_parda),2) media_incompleto_parda
        ,ROUND(AVG(incompleto_preta),2) media_incompleto_preta
    FROM ibge
    GROUP BY nome_microregiao
    
""")

(
    microregiao
    .write
    .format('parquet')
    .save("s3://nicolas-pucminas-parquet-20221511/microregiao/")
)

grandes_regioes=spark.sql("""

    SELECT 
        nome_grandes_regioes
        ,ROUND(AVG(incompleto_amarela),2) media_incompleto_amarela
        ,ROUND(AVG(incompleto_branca),2) media_incompleto_branca
        ,ROUND(AVG(incompleto_indigena),2) media_incompleto_indigena
        ,ROUND(AVG(incompleto_parda),2) media_incompleto_parda
        ,ROUND(AVG(incompleto_preta),2) media_incompleto_preta
    FROM ibge
    GROUP BY nome_grandes_regioes
    
""")

(
    grandes_regioes
    .write
    .format('parquet')
    .save("s3://nicolas-pucminas-parquet-20221511/grandesregioes/")
)




