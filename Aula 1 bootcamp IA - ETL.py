import pandas as pd

INPUT_PATH = "data/raw/vendas_1M.csv"
OUTPUT_PATH = "data/processed/vendas_tratadas.csv"

CHUNK_SIZE = 100_000  # processar em partes (boa prática)

processed_chunks = []

print("Iniciando ETL...")

for chunk in pd.read_csv(INPUT_PATH, chunksize=CHUNK_SIZE):
    # --- TRANSFORM ---
    
    # Remover dados inválidos
    chunk = chunk[(chunk["price"] > 0) & (chunk["quantity"] > 0)]

    # Converter data
    chunk["order_date"] = pd.to_datetime(chunk["order_date"])

    # Criar imposto e valor final
    chunk["tax"] = chunk["total_value"] * 0.10
    chunk["final_value"] = chunk["total_value"] + chunk["tax"]

    # Padronizar categoria
    chunk["category"] = chunk["category"].str.upper()

    # Selecionar colunas finais
    chunk = chunk[
        [
            "order_id",
            "customer_id",
            "product",
            "category",
            "quantity",
            "price",
            "total_value",
            "tax",
            "final_value",
            "order_date",
            "country",
        ]
    ]

    processed_chunks.append(chunk)

# --- LOAD ---
df_final = pd.concat(processed_chunks)

df_final.to_csv(OUTPUT_PATH, index=False)

print(f"ETL finalizado com sucesso! Arquivo salvo em {OUTPUT_PATH}")
