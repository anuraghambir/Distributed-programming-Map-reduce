from socket import *
import json
import os
import _thread as t
import sys
from struct import pack,unpack
from configparser import ConfigParser
import logging


parser = ConfigParser()
parser.read("./properties.config")
IP = parser.get('server','server_ip').split(':')
serverLog = parser.get('server','logfilePath')

logging.basicConfig(filename = serverLog, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)
logging.info('Server starting....')
host = IP[0]
serverPort = int(IP[1])
threads = 0

serverSocket = socket(AF_INET, SOCK_STREAM)
serverSocket.bind((host, serverPort))

logging.info('Server ready to receive...')
print(('Server ready to receive...'))

def multithreading(connSocket,client_addr):
    
    while True: 
        try:   
            output = b''
            bufSize = 2048
            bf = connSocket.recv(8)
            (len_msg,) = unpack('>Q',bf)

            while len(output) < len_msg:
                    data_left = len_msg - len(output)
                    output += connSocket.recv(bufSize if data_left > bufSize else data_left)
            #connSocket.send(b'OK')
        
        
            cli_request = json.loads(output.decode())
            key = list(cli_request.keys())[0]
            if key == 'set':
                logging.info('Received request for storing data from client {}:{}'.format(str(client_addr[0]),str(client_addr[1])))
                path = cli_request[key]
                data = cli_request['data']
                output_file = open(path, 'w')
                if cli_request['source'] in ['mapper','reducer']:
                    output_file.write(json.dumps(data))
                else:
                    output_file.write(data)
                output_file.close()
                logging.info('Data stored successfully!')

            if key == 'get':
                logging.info('Received request to retrieve from client {}:{}'.format(str(client_addr[0]),str(client_addr[1])))
                source = cli_request[key]
                if source == 'mapper':
                    print('Pulling data for mapper...')
                    # print(cli_request['path'])
                    fp = open(cli_request['path'], errors='ignore')
                    read_data = fp.read()
                    # print('data len: ',len(read_data))
                    len_msg_get = pack('>Q', len(read_data))
                    connSocket.send(len_msg_get)
                    connSocket.send(read_data.encode())
                    fp.close()
                    logging.info('Data retrieved and sent!')

                # Pull data for reducer    
                if source == 'reducer':
                    fp = open(cli_request['path'], errors='ignore')
                    read_data = fp.read()
                    len_msg_get = pack('>Q', len(read_data))
                    connSocket.send(len_msg_get)
                    connSocket.send(read_data.encode())
                    # print(connSocket.recv(4096))
                    fp.close() 
                    logging.info('Data retrieved and sent!')

            if cli_request == " ":
                break
            
        except:
            pass
                
                
    logging.info('Disconnecting from client....')
    connSocket.close()
    logging.info('Disconnected.')


serverSocket.listen()
while True:
    try:
        connSocket, addr = serverSocket.accept()
    except:
        logging.info('Could not connect to client..')
        print('Could not connect to client..')
    print('Connected with client- {}:{}'.format(str(addr[0]),str(addr[1])))
    logging.info('Connected with client- {}:{}'.format(str(addr[0]),str(addr[1])))
    t.start_new_thread(multithreading,(connSocket,addr,))
    threads += 1
    print("Client number: {}".format(str(threads)))
    logging.info("Client number: {}".format(str(threads)))

    #connSocket.close()
serverSocket.close()

