from config import POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT
import uuid
import psycopg2
from kafka import KafkaProducer


conn = psycopg2.connect(
    host = POSTGRES_HOST,
    dbname = POSTGRES_DB,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD,
    port = POSTGRES_PORT
)
cur = conn.cursor()

uid = str(uuid.uuid4())

cur.execute(
    "INSERT INTO notification (notification_id, status, recipient, message) VALUES (%s, %s, %s, %s)",
    (uid, 'PENDING', 'KEN', 'HELLO WORLD')
)
conn.commit()

s_byte = uid.encode()
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('notifications', s_byte)
producer.flush()