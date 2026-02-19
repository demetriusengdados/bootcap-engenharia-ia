CREATE TABLE gold.vendas_gold

WITH (

format='PARQUET',

external_location='s3://bootcamp-data-lake-dev/gold/vendas/',

partitioned_by = ARRAY['ano','mes']

)

AS

SELECT

year(v.data_venda) AS ano,

month(v.data_venda) AS mes,

v.cliente_id,

c.cidade,

c.estado,

SUM(v.valor_total) AS faturamento_total,

COUNT(*) AS total_vendas,

AVG(v.valor_total) AS ticket_medio

FROM silver.vendas v

JOIN silver.clientes c

ON v.cliente_id = c.cliente_id

GROUP BY

1,2,3,4,5;