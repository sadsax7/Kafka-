import json, time, random, uuid, socket
from datetime import datetime
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:29092',
                'client.id': socket.gethostname()})

products = [
    {"id":"p01","name":"Mouse","price":15.9,"stock":120},
    {"id":"p02","name":"Keyboard","price":39.0,"stock":80},
    {"id":"p03","name":"Headset","price":59.0,"stock":60},
    {"id":"p04","name":"USB-C Cable","price":8.5,"stock":300}
]

def send(topic, value):
    p.produce(topic, json.dumps(value).encode("utf-8"))
    p.poll(0)

if __name__ == "__main__":
    while True:
        prod = random.choice(products)
        qty = random.randint(1, 5)
        prod["stock"] = max(0, prod["stock"] - qty)
        evt = {
            "order_id": str(uuid.uuid4()),
            "ts": int(time.time()*1000),
            "customer": f"c{random.randint(1,50):03d}",
            "product_id": prod["id"],
            "product_name": prod["name"],
            "quantity": qty,
            "price": prod["price"],
            "inventory_left": prod["stock"]
        }
        send("orders", evt)
        time.sleep(random.uniform(0.3, 1.0))
