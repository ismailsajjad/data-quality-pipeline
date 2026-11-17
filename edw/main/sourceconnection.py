import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
load_dotenv()

def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER", "postgres"),
        password=os.environ["DB_PASSWORD"],   # code mein nahi, bahar se aayega
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "dq_db"),
    )
    return create_engine(url)

# print(get_engine())