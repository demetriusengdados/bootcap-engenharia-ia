from faker import Faker
from sqlalchemy import Table, Column, Integer, String, Date, MetaData
from tqdm import tqdm
from config import RECORDS_PER_TABLE, BATCH_SIZE

fake = Faker("pt_BR")

def create_dim_table(engine):
    metadata = MetaData()

    dim = Table(
        "dim_cliente",
        metadata,
        Column("cliente_id", Integer, primary_key=True),
        Column("nome", String(120)),
        Column("email", String(120)),
        Column("cidade", String(80)),
        Column("estado", String(2)),
        Column("data_nascimento", Date),
    )

    metadata.drop_all(engine, tables=[dim])
    metadata.create_all(engine)
    return dim

def generate_dim(engine):
    dim = create_dim_table(engine)
    rows = []

    with engine.begin() as conn:
        for i in tqdm(range(1, RECORDS_PER_TABLE + 1)):
            rows.append({
                "cliente_id": i,
                "nome": fake.name(),
                "email": fake.email(),
                "cidade": fake.city(),
                "estado": fake.state_abbr(),
                "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=80),
            })

            if len(rows) >= BATCH_SIZE:
                conn.execute(dim.insert(), rows)
                rows.clear()

        if rows:
            conn.execute(dim.insert(), rows)
    print(f"Dimensão 'dim_cliente' criada com {RECORDS_PER_TABLE} registros.")
    