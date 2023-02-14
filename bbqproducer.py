"""
Brady Monks
2/3/23

    This program sends the temperature of each object (smoker, food A, food B) 
    to separate queues on the RabbitMQ server.


    

"""

import pika
import sys
import webbrowser
import csv
import time
from collections import deque



def open_rabbitmq_admin_site():
    """Define variable to determine whether to open RabbitMQ site"""
    show_offer = True
    """Offer to open the RabbitMQ Admin website if show_offer = True"""
    if show_offer == True:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message} to {queue_name}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

if __name__ == "__main__":
    # open the RabbitMQ Admin site
    open_rabbitmq_admin_site()
    # read from a file to get some temperature data
    with open("smoker-temps.csv", "r") as input_file:
        tasks = (input_file)

        # create a csv reader for our comma delimited data
        reader = csv.reader(tasks, delimiter=",")

        # Create the deques for each queue

        for row in reader:
            # read the second, third, and fourth columns of data
            column2 = row[1]
            column3 = row[2]
            column4 = row[3]

            # send the data from the second column to 01-smoker
            send_message("localhost", "01-smoker", column2)

            # send the data from the third column to 02-food-A
            send_message("localhost", "02-food-A", column3)

            # send the data from the fourth column to 02-food-B
            send_message("localhost", "02-food-B", column4)

            # sleep for a few seconds
            time.sleep(1)
