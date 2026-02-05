from sqlalchemy import create_engine

def get_engine(conn_string):
    return create_engine(conn_string, pool_pre_ping=True)
