from confluent_kafka import Producer
import socket
import requests
import time

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':50030'
BASE_URL = 'https://icanhazdadjoke.com/'


def delivery_callback(err, msg):
    if err:
        print(f'Message failed delivery: {err}')
    else:
        print(f'Message was delivered to {msg.topic()} topic')


def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'client.id': socket.gethostname()
    }
    producer = Producer(conf)
    try:
        while True:
            joke_json = requests.get(BASE_URL, headers={'Accept': 'application/json'}).json()
            joke = joke_json['joke']
            producer.produce(KAFKA_TOPIC, value=joke, callback=delivery_callback)
            producer.flush()
            time.sleep(5)
    except Exception as e:
        print(f'Producer experience uncaught error: {e}')
    finally:
        producer.flush()


if __name__ == '__main__':
    main()
