from db import get_engine
from config import DATABASES
from generate_dim import generate_dim
from generate_fact import generate_fact

for db_name, conn_string in DATABASES.items():
    print(f"\nGerando dados para {db_name}")
    engine = get_engine(conn_string)

    generate_dim(engine)
    generate_fact(engine)

    print(f"Base {db_name} finalizada")
