import random
from faker import Faker
from sqlalchemy import Table, Column, Integer, String, Date, Float, MetaData
from tqdm import tqdm
from config import RECORDS_PER_TABLE, BATCH_SIZE

fake = Faker("pt_BR")

def create_fact_table(engine):
    metadata = MetaData()

    fact = Table(
        "fato_vendas",
        metadata,
        Column("venda_id", Integer, primary_key=True),
        Column("cliente_id", Integer),
        Column("data_venda", Date),
        Column("produto", String(120)),
        Column("quantidade", Integer),
        Column("valor_unitario", Float),
        Column("valor_total", Float),
    )

    metadata.drop_all(engine, tables=[fact])
    metadata.create_all(engine)
    return fact

def generate_fact(engine):
    fact = create_fact_table(engine)
    rows = []

    with engine.begin() as conn:
        for i in tqdm(range(1, RECORDS_PER_TABLE + 1)):
            qtd = random.randint(1, 5)
            valor = round(random.uniform(10, 500), 2)

            rows.append({
                "venda_id": i,
                "cliente_id": random.randint(1, RECORDS_PER_TABLE),
                "data_venda": fake.date_between(start_date="-2y", end_date="today"),
                "produto": fake.word(),
                "quantidade": qtd,
                "valor_unitario": valor,
                "valor_total": qtd * valor,
            })

            if len(rows) >= BATCH_SIZE:
                conn.execute(fact.insert(), rows)
                rows.clear()

        if rows:
            conn.execute(fact.insert(), rows)
    print(f"Fato 'fato_vendas' criado com {RECORDS_PER_TABLE} registros.")