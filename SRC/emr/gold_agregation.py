import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum as fsum, count as fcount, avg as favg

def main():
    if len(sys.argv) < 2:
        raise ValueError("Uso: spark-submit gold_aggregation.py <S3_BUCKET>")

    bucket = sys.argv[1]

    spark = SparkSession.builder.appName("gold-aggregation").getOrCreate()

    vendas_path = f"s3://{bucket}/silver/vendas/"
    clientes_path = f"s3://{bucket}/silver/clientes/"
    gold_path = f"s3://{bucket}/gold/mart_vendas/"

    vendas = spark.read.parquet(vendas_path)
    clientes = spark.read.parquet(clientes_path)

    # Join
    df = vendas.join(clientes, on="cliente_id", how="inner")

    # Features/Agregações
    df_gold = (
        df.withColumn("ano", year(col("data_venda")))
          .withColumn("mes", month(col("data_venda")))
          .groupBy("ano", "mes", "cliente_id", "cidade", "estado")
          .agg(
              fsum("valor_total").alias("faturamento_total"),
              fcount("*").alias("total_vendas"),
              favg("valor_total").alias("ticket_medio")
          )
    )

    # Escrita particionada
    (
        df_gold.write
        .mode("overwrite")
        .partitionBy("ano", "mes")
        .parquet(gold_path)
    )

    print("Gold mart gerado com sucesso:", gold_path)
    spark.stop()

if __name__ == "__main__":
    main()