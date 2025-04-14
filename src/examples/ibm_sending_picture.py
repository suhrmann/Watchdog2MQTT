# Source: https://developer.ibm.com/recipes/tutorials/sending-and-receiving-pictures-from-a-raspberry-pi-via-mqtt/
# Archived: https://web.archive.org/web/20201202115516/https://developer.ibm.com/recipes/tutorials/sending-and-receiving-pictures-from-a-raspberry-pi-via-mqtt/


# Once the image is taken, encode the image as Base64, then split it into segments to publish.
import base64

def convertImageToBase64():
    with open("image_test.jpg", "rb") as image_file:
        encoded = base64.b64encode(image_file.read())
    return encoded

# In this example we will be publishing to the IBM Watson IoT Platform, using our specific device ID and credentials.
import ibmiotf.device

options = ibmiotf.device.ParseConfigFile("/home/pi/device2.cfg")
client = ibmiotf.device.Client(options)
client.connect()

# When sending the image, we add some additional fields in order to identify it and make the reconstruction process easier.
# We will make use of this function that generates a random string to be used as the picture ID.
import random, string

def randomword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

# We then split the data into chunks of size 3000, append some identifying information, then publish.
import math

packet_size=3000

def publishEncodedImage(encoded):

    end = packet_size
    start = 0
    length = len(encoded)
    picId = randomword(8)
    pos = 0
    no_of_packets = math.ceil(length/packet_size)


    while start <= len(encoded):
        data = {"data": encoded[start:end], "pic_id":picId, "pos": pos, "size": no_of_packets}
    client.publishEvent("Image-Data",json.JSONEncoder().encode(data))
    end += packet_size
    start += packet_size
    pos = pos +1
