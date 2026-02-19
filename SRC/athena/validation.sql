-- quantidade total

SELECT COUNT(*) FROM silver.vendas;



-- nulos cliente

SELECT

COUNT(*) AS total,

SUM(
CASE
WHEN cliente_id IS NULL
THEN 1
ELSE 0
END
) AS nulos

FROM silver.vendas;



-- integridade referencial

SELECT COUNT(*)

FROM silver.vendas v

LEFT JOIN silver.clientes c

ON v.cliente_id = c.cliente_id

WHERE c.cliente_id IS NULL;



-- distribuição de valores

SELECT

MIN(valor_total),

MAX(valor_total),

AVG(valor_total)

FROM silver.vendas;