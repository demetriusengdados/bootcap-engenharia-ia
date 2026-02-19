import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from pyspark.sql.functions import col, to_date


# parâmetros do Glue
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET"
    ]
)

bucket = args["S3_BUCKET"]


sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session


# ========================
# CLIENTES
# ========================

clientes_input = f"s3://{bucket}/bronze/clientes/"

clientes_df = spark.read.option(
    "header", True
).csv(clientes_input)


clientes_df = clientes_df.select(

    col("cliente_id").cast("int"),

    col("nome"),

    col("email"),

    col("cidade"),

    col("estado"),

    to_date(col("data_nascimento"), "yyyy-MM-dd")
    .alias("data_nascimento")

)


clientes_output = f"s3://{bucket}/silver/clientes/"


clientes_df.write.mode(
    "overwrite"
).parquet(clientes_output)



# ========================
# VENDAS
# ========================

vendas_input = f"s3://{bucket}/bronze/vendas/"

vendas_df = spark.read.option(
    "header", True
).csv(vendas_input)


vendas_df = vendas_df.select(

    col("venda_id").cast("int"),

    col("cliente_id").cast("int"),

    to_date(col("data_venda"), "yyyy-MM-dd")
    .alias("data_venda"),

    col("produto"),

    col("quantidade").cast("int"),

    col("valor_unitario").cast("double"),

    col("valor_total").cast("double")

)


vendas_output = f"s3://{bucket}/silver/vendas/"


vendas_df.write.mode(
    "overwrite"
).partitionBy(
    "data_venda"
).parquet(vendas_output)


print("ETL Bronze → Silver finalizado")