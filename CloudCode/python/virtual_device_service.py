from kafka import KafkaConsumer, KafkaProducer
from const import *
import threading
import bcrypt

from concurrent import futures
import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

bancoDeDados = {"davi":{"codigoHash":"21393191", "password": "fmc"} }

# Twin state
current_temperature = 'void'
current_light_level = 'void'
led_state = {'red':0, 'green':0}

# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print ('Received Temperature: ', msg.value.decode())
        current_temperature = msg.value.decode()

# Kafka consumer to run on a separate thread
def consume_light_level():
    global current_light_level
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        print ('Received Light Level: ', msg.value.decode())
        current_light_level = msg.value.decode()

def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state
        
class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        if bancoDeDados[request.userTry]["codigoHash"] != request.chaveHash:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Token hash invalido! Tente novamente.')

        return iotservice_pb2.TemperatureReply(temperature=current_temperature)
    
    def BlinkLed(self, request, context):
        if bancoDeDados[request.userTry]["codigoHash"] != request.chaveHash:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Token hash invalido! Tente novamente.')

        print ("Blink led ", request.ledname)
        print ("...with state ", request.state)
        produce_led_command(request.state, request.ledname)
        # Update led state of twin
        led_state[request.ledname] = request.state
        return iot_service_pb2.LedReply(ledstate=led_state)

    def SayLightLevel(self, request, context):
        if bancoDeDados[request.userTry]["codigoHash"] != request.chaveHash:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Conta invalida! Tente novamente.')
            
        return iot_service_pb2.LightLevelReply(lightLevel=current_light_level)
        
    def Authenticate(self, request, context):
        userTry = request.userTry
        passTry = request.passTry
        if userTry in bancoDeDados:
            if bancoDeDados[userTry]["password"] == passTry:
                hashGerado = "F4b10FmC1995"   #trocar por uma funcao que realmente gera hash
                bancoDeDados[userTry]["codigoHash"] = hashGerado
                return iot_service_pb2.codigoHash(chaveHash = hashGerado)
            else:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Conta invalida! Tente novamente.')
        else:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Conta invalida! Tente novamente.')       
              
        
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()

    trd1 = threading.Thread(target=consume_temperature)
    trd1.start()

    trd2 = threading.Thread(target=consume_light_level)
    trd2.start()

    # Initialize the state of the leds on the actual device
    for color in led_state.keys():
        produce_led_command (led_state[color], color)
    serve()
