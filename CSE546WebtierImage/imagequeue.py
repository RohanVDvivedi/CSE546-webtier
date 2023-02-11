import boto3
import base64
import threading

sqs = None
RequestQueue = None
ResponseQueue = None
ReceiverThread = None
ReceivedMessages = None
ReceivedMessagesLock = None
ReceivedMessagesConditionVariable = None

def init() :
    global sqs
    global RequestQueue
    global ResponseQueue
    global ReceiverThread
    global ReceivedMessages
    global ReceivedMessagesLock
    global ReceivedMessagesConditionVariable
    sqs = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id="", aws_secret_access_key= "")
    RequestQueue = sqs.get_queue_by_name(QueueName='RequestQueue')
    ResponseQueue = sqs.get_queue_by_name(QueueName='ResponseQueue')
    print("RequestQueue and ResponseQueue initialized")
    ReceivedMessages = {}
    ReceivedMessagesLock = threading.Lock()
    ReceivedMessagesConditionVariable = threading.Condition(ReceivedMessagesLock)
    ReceiverThread = threading.Thread(target = receiverThreadFunction)
    ReceiverThread.start()
    print("Received Thread started")

def sendImage(name, image) :
    global RequestQueue
    return RequestQueue.send_message(MessageBody = name + ':' + base64.b64encode(image).decode("ascii"))

def parse_message(data) :
    splits = data.split(':', 1)
    return splits[0], splits[1]

def receiverThreadFunction() :
    global ResponseQueue
    global ReceivedMessages
    global ReceivedMessagesLock
    global ReceivedMessagesConditionVariable
    while(True) :
        for message in ResponseQueue.receive_messages():
            message_content = message.body
            image_filename, image_result = parse_message(message_content)

            ReceivedMessagesLock.acquire()
            print(image_filename + " -> " + image_result)
            if (image_filename in ReceivedMessages) and (ReceivedMessages[image_filename] == "discard on arrival") :
                del ReceivedMessages[image_filename]
            else :
                ReceivedMessages[image_filename] = image_result
                ReceivedMessagesConditionVariable.notifyAll()
            ReceivedMessagesLock.release()

            message.delete()


def waitAndGetResult(name) :
    global ReceivedMessages
    global ReceivedMessagesLock
    global ReceivedMessagesConditionVariable
    ReceivedMessagesLock.acquire()

    while name not in ReceivedMessages :
        ReceivedMessagesConditionVariable.wait()
    
    result = ReceivedMessages[name]

    ReceivedMessagesLock.release()

    return result