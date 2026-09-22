import redis
from config import POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from config import REDIS_HOST, REDIS_PORT





conn = psycopg2.connect(
    host = POSTGRES_HOST,
    dbname = POSTGRES_DB,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD,
    port = POSTGRES_PORT
)
redis_conn = redis.Redis(
    host = REDIS_HOST,
    port = REDIS_PORT,
    decode_responses=True
)
cur = conn.cursor()

consumer = KafkaConsumer(
    'notifications',
    bootstrap_servers='localhost:9092')
print("consumer ready, waiting for messages")

dlq_producer = KafkaProducer(bootstrap_servers='localhost:9092')
# temporary test hook
def send_notification(notification_id):
    return True

for message in consumer:
    notification_id = message.value.decode()
    print(notification_id)
    cur.execute(
    "SELECT status FROM notification WHERE notification_id = %s",
    (notification_id,)
    )
    row = cur.fetchone()
    if row is None:
        print(f"Notification ID not found {notification_id}")
    else:
        status = row[0]
        if status == 'PENDING':
            print("delivering")
            if send_notification(notification_id):
                cur.execute(
                "UPDATE notification SET status = %s WHERE notification_id = %s", ('SENT', notification_id)
                )
                conn.commit()
                print("delivering success")
            else:
                key = f"retry:{notification_id}"
                retry_count = redis_conn.incr(key)  
                if retry_count >= 3:
                    print("moving to DLQ")
                    dlq_producer.send('notifications-dlq', notification_id.encode())
                    dlq_producer.flush()
                    cur.execute(
                    "UPDATE notification SET status = %s WHERE notification_id = %s", ('FAILED', notification_id)
                    )
                    conn.commit()

                else:
                    print("failed again")
                    dlq_producer.send('notifications', notification_id.encode())
                    dlq_producer.flush()
        elif status == 'SENT':
            print("already sent, skipping")
        elif status == "FAILED":
            print("already FAILED, in DLQ, skipping")


   
