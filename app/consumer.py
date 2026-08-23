from config import POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT
import psycopg2
from kafka import KafkaConsumer





conn = psycopg2.connect(
    host = POSTGRES_HOST,
    dbname = POSTGRES_DB,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD,
    port = POSTGRES_PORT
)

cur = conn.cursor()

consumer = KafkaConsumer(
    'notifications',
    bootstrap_servers='localhost:9092')


for message in consumer:
    text = message.value.decode()
    print(text)
    cur.execute(
    "SELECT status FROM notification WHERE notification_id = %s",
    (text,)
    )
    row = cur.fetchone()
    if row is None:
        print(f"Notification ID not found {text}")
    else:
        status = row[0]
        if status == 'PENDING':
            print("delivering")
            cur.execute(
                "UPDATE notification SET status = %s WHERE notification_id = %s", ('SENT', text)
            )
            conn.commit()
        elif status == 'SENT':
            print("already sent, skipping")