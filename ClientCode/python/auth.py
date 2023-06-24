#from __future__ import print_function

import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

from const import *


def run():
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub(channel)
        response = stub.Authenticate(iot_service_pb2.TryLogin(userTry= sys.argv[1], passTry= sys.argv[2]))
        
if __name__ == '__main__':
    logging.basicConfig()
    run()
