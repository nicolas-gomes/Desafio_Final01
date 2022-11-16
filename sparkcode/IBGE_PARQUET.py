from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("nico_ibge_pucminas_parquet").getOrCreate()


microregioes_parquet = (
    spark
    .read
    .parquet("s3://nicolas-pucminas-parquet-20221511/microregiao/")
)
microregioes_parquet.printSchema()


grandes_regioes_parquet = (
    spark
    .read
    .parquet("s3://nicolas-pucminas-parquet-20221511/grandesregioes/")
)
grandes_regioes_parquet.printSchema()


microregioes_parquet.createOrReplaceTempView("microregioes_parquet")
grandes_regioes_parquet.createOrReplaceTempView("grandes_regioes_parquet")


tabela_final=spark.sql("""

    SELECT 
        'microregiao' tipo_regiao
        ,nome_microregiao 
        ,media_incompleto_amarela
        ,media_incompleto_branca
        ,media_incompleto_indigena
        ,media_incompleto_parda
        ,media_incompleto_preta
    FROM microregioes_parquet
    
    UNION ALL
    
    SELECT 
        'grande_regiao' tipo_regiao
        ,nome_grandes_regioes
        ,media_incompleto_amarela
        ,media_incompleto_branca
        ,media_incompleto_indigena
        ,media_incompleto_parda
        ,media_incompleto_preta
    FROM grandes_regioes_parquet

    
""")


(
    tabela_final
    .write
    .format('parquet')
    .save("s3://armazenamento-arquivos-20221511/resultado/")
)





