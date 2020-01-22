from kafka import KafkaProducer
from datetime import datetime

import json
import uuid
import time
import random


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


while True:
    new_id = str(uuid.uuid4())
    price = random.randint(50, 10000)
    user_id = random.randint(1, 6)
    payload = {
        "order_id": new_id,
        "user_id": user_id,
        "price": price,
        "collect_dt_tm": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    str_payload = json.dumps(payload)
    producer.send(topic='orders.v4', key=bytes(new_id, 'utf-8'),
                  value=bytes(str_payload, 'utf-8'))
    time.sleep(1)
