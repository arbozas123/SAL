import pika
import json
import requests


def test_write():
    credentials = pika.PlainCredentials('Certh', 'Certh2024')
    parameters = pika.ConnectionParameters('10.20.20.3', 30672, 'vCerth', credentials)
    print("here")
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue = channel.queue_declare(queue='SMA_Fire_Events', durable=True)

    channel.basic_publish(exchange='certh.ex', routing_key='certh.SMA_Fire_Events', body='test message')

    print(" [x] Message sent to the queue")
    connection.close()


def callback(ch, method, properties, body):
    print("Received message:", body)


def test():
    # Set up connection parameters
    credentials = pika.PlainCredentials('DemoUser', 'DemoUsrPaswrd1984')
    parameters = pika.ConnectionParameters('10.20.20.3', 30672, 'DemovHost', credentials)
    # Create a connection
    connection = pika.BlockingConnection(parameters)
    # Create a channel
    channel = connection.channel()
    # declare queue you want
    q = channel.queue_declare(queue='SMA_Fire_Events', durable=True)
    # Get number of messages inside queue
    q_len = q.method.message_count
    # print number of messages inside queue
    print(q_len)


def test_read():
    credentials = pika.PlainCredentials('Certh', 'Certh2024')
    parameters = pika.ConnectionParameters('10.20.20.3', 30672, 'vCerth', credentials)
    print("here")
    connection = pika.BlockingConnection(parameters)
    print("connected")
    channel = connection.channel()
    queue = channel.queue_declare(queue='SMA_Fire_Events', durable=True)

    channel.basic_consume(queue='SMA_Fire_Events',
                          on_message_callback=callback,
                          auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')

    # Start consuming messages
    channel.start_consuming()


def ingestion_test():

    # Demo script that shows how data can be ingested through rabbitmq.
    headers = {}

    # Read metadata.json and use its contents as the header
    # IMPORTANT uuid needs to be changed everytime data is ingested as system does not allow for duplicate data
    with open('metadata.json', 'r') as f:
        headers["metadata"] = json.load(f)

    headers['metadata'] = json.dumps(headers['metadata'])

    # Add the additional header
    headers['amqp$username'] = 'Certh'
    headers['amqp$password'] = 'Certh2024'
    headers['amqp$vhost'] = 'vCerth'
    headers['amqp$exchange'] = 'certh.ex'
    headers['amqp$routingKey'] = 'certh.SMA_Fire_Events'
    headers['amqp$deliveryMode']='1'

    # Read data.geojson and use its contents as the data to be posted
    with open('test.json', 'r') as f:
        data = f.read()

    # URL to post data
    url = 'http://10.20.20.3:30516/metadata/ingest'

    # Post the data with the metadata as the header
    response = requests.post(url, headers=headers, data=data)

    # Print the response
    print(response.text)

    # Optionally, handle the response. For example, check if the request was successful:
    if response.status_code == 200:
        print("Data successfully posted!")
    else:
        print(f"Failed to post data. Status code: {response.status_code}. Response text: {response.text}")


def main():
    #test_read()
    #test_write()
    #test()

    ingestion_test()



main()
