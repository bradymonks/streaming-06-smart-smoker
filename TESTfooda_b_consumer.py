"""
Brady Monks
2/3/23

    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  


"""

import pika
import sys
import time
import csv

# define a callback function to be called when a message is received
def foodA_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Received {body.decode()}")
    
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    print(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a callback function to be called when a message is received
def foodB_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Received {body.decode()}")
    
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    print(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(hn: str = "localhost", qn_a: str = "02-food-A", qn_b: str = "02-food-B"):
    """ Continuously listen for task messages on named queues."""

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        channel_a = connection.channel()
        channel_a.queue_declare(queue=qn_a, durable=True)
        channel_a.basic_qos(prefetch_count=1)
        channel_a.basic_consume(queue=qn_a, on_message_callback=foodA_callback)

        channel_b = connection.channel()
        channel_b.queue_declare(queue=qn_b, durable=True)
        channel_b.basic_qos(prefetch_count=1)
        channel_b.basic_consume(queue=qn_b, on_message_callback=foodB_callback)

        print(" [*] Ready for work. To exit press CTRL+C")

        channel_a.start_consuming()
        channel_b.start_consuming()

    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "02-food-A")

    # call the main function for queue "food-B"
    main("localhost", "food-B")





