from confluent_kafka import Consumer

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':50030'
num_jokes = 0
total_words = 0


def main():
    global num_jokes
    global total_words
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': 'joke_count'}
    consumer = Consumer(conf)
    try:
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error():
                    print(f'Error receiving message: {msg.topic()}')
            else:
                msg = msg.value().decode()
                word_count = len((msg.split()))
                total_words += word_count
                num_jokes += 1
                if num_jokes % 5 == 0:
                    avg_joke = total_words / num_jokes
                    print(f'Average words per joke: {avg_joke}')

    except Exception as e:
        print(f'Producer experience uncaught error: {e}')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
