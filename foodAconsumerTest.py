import pika
import sys
import time
import csv
from collections import deque

###

import smtplib
from email.message import EmailMessage
import tomli  # requires Python 3.11
import pprint

## Create Email Function

def createAndSendEmailAlert(email_subject: str, email_body: str):
    print(email_body)
    print(email_subject)
    """Read outgoing email info from a TOML config file"""

    with open(".env.toml", "rb") as file_object:
        secret_dict = tomli.load(file_object)
    pprint.pprint(secret_dict)

    # basic information

    host = secret_dict["outgoing_email_host"]
    port = secret_dict["outgoing_email_port"]
    outemail = secret_dict["outgoing_email_address"]
    outpwd = secret_dict["outgoing_email_password"]

    # Create an instance of an EmailMessage

    msg = EmailMessage()
    msg["From"] = secret_dict["outgoing_email_address"]
    msg["To"] = secret_dict["outgoing_email_address"]
    msg["Reply-to"] = secret_dict["outgoing_email_address"]
    email_subject1 = email_subject
    email_body1 = email_body

    msg["Subject"] = email_subject1
    msg.set_content(email_body1)

    print("========================================")
    print(f"Prepared Email Message: ")
    print("========================================")
    print()
    print(f"{str(msg)}")
    print("========================================")
    print()

    # Communications can fail, so use:

    # try -   to execute the code
    # except - when you get an Exception, do something else
    # finally - clean up regardless

    # Create an instance of an email server, enable debug messages

    server = smtplib.SMTP(host)
    server.set_debuglevel(2)

    print("========================================")
    print(f"SMTP server created: {str(server)}")
    print("========================================")
    print()

    try:
        print()
        server.connect(host, port)  # 465
        print("========================================")
        print(f"Connected: {host, port}")
        print("So far so good - will attempt to start TLS")
        print("========================================")
        print()

        server.starttls()
        print("========================================")
        print(f"TLS started. Will attempt to login.")
        print("========================================")
        print()

        try:
            server.login(outemail, outpwd)
            print("========================================")
            print(f"Successfully logged in as {outemail}.")
            print("========================================")
            print()

        except smtplib.SMTPHeloError:
            print("The server did not reply properly to the HELO greeting.")
            exit()
        except smtplib.SMTPAuthenticationError:
            print("The server did not accept the username/password combination.")
            exit()
        except smtplib.SMTPNotSupportedError:
            print("The AUTH command is not supported by the server.")
            exit()
        except smtplib.SMTPException:
            print("No suitable authentication method was found.")
            exit()
        except Exception as e:
            print(f"Login error. {str(e)}")
            exit()

        try:
            server.send_message(msg)
            print("========================================")
            print(f"Message sent.")
            print("========================================")
            print()
        except Exception as e:
            print()
            print(f"ERROR: {str(e)}")
        finally:
            server.quit()
            print("========================================")
            print(f"Session terminated.")
            print("========================================")
            print()

    # Except if we get an Exception (we call e)

    except ConnectionRefusedError as e:
        print(f"Error connecting. {str(e)}")
        print()

    except smtplib.SMTPConnectError as e:
        print(f"SMTP connect error. {str(e)}")
        print()

# Define the deque with a max length of 20
q = deque(maxlen=20)

def foodA_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # Decode the binary message body to a string
    message = body.decode().strip()
    try:
        # Split the message at the comma
        timestamp, temp = message.split(',')
        # Remove any leading or trailing white space
        timestamp = timestamp.strip()
        temp = temp.strip()
        # convert the temperature to type float
        temp = float(temp)
    except ValueError:
        # ignore the error and continue the process
        pass
    print(f" [x] Received {body.decode()}")
    
     # Check if the message is type float
    if isinstance(temp, float):
        q.append(temp)
        
    # Check the deque
    if len(q) == 20:
        new = q.pop()
        old = q.popleft()
        if abs(new - old) < 1:
            alert = "ALERT *** FOOD A HAS STALLED OUT AT"+str(timestamp)+ "*** ALERT"
            print(new, old)
            subject_str = "FOOD A ALERT"
            content_str = alert
            createAndSendEmailAlert(email_subject=subject_str, email_body=content_str)

    # Simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # When done with task, tell the user
    print(" [x] Done.")
    # Acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, on_message_callback=foodA_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
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