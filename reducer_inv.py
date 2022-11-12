from socket import *
import json
from struct import pack, unpack
from collections import defaultdict
import logging
# import time

# def reducer_wc(p_id, n_reducers, partitions_loc, IP, outputPath):
def reducer_inv(p_id, n_reducers, IP, outputPath, n_mappers, partitionPath):
    IP = IP.split(':')
    host = IP[0].strip()
    serverPort = int(IP[1].strip())
    clientSocket = socket(AF_INET, SOCK_STREAM)
    clientSocket.connect((host, serverPort))
    
    reducer_dict = defaultdict(list)

    print('Started reducer {}'.format(str(p_id)))
    
    # for i,path in enumerate(partitions_loc):
    for i in range(n_mappers):
        partition_kv = {}
        path = partitionPath + '/partition_' + str(i)
        request_json = {
            'get':'reducer',
            'index':i,
            'path':path
        }
        logging.info('Reducer {} requesting data from partition {}'.format(str(p_id), str(i)))
        # Request data from KV store
        request_json_str = json.dumps(request_json)
        len_msg = pack('>Q', len(request_json_str))

        clientSocket.send(len_msg)
        clientSocket.send(request_json_str.encode())

        # Receive data from KV store
        output = b''
        data_left = 0
        while True:
            try:       
                bufSize = 2048
                bf = clientSocket.recv(8)
                (len_msg_get,) = unpack('>Q',bf)
                
                while len(output) < len_msg_get:
                    data_left = len_msg_get - len(output)
                    output += clientSocket.recv(bufSize if data_left > bufSize else data_left)
                # clientSocket.send(b'OK')  
                break
            except:
                pass
        logging.info('Reducer {} received data from partition {}'.format(str(p_id), str(i)))
        logging.info('Reducer {} starting the hashing and reduce operation on partition {}'.format(str(p_id), str(i)))
        partition_kv = json.loads(output)
        
        for key, value in partition_kv.items():
            if (ord(key[0]) - 97) % n_reducers == p_id:
                if key not in reducer_dict.keys():
                    reducer_dict[key] = list(set(value))
                else:
                    reducer_dict[key] += value
                    reducer_dict[key] = list(set(value))
        logging.info('Reducer {} finished the hashing and reduce operation on partition {}'.format(str(p_id), str(i)))

        del partition_kv
        
    
    # Send reduced data back to KV Store for final storage
    set_json = {
        'set' : outputPath+'/reducer_'+str(p_id),
        'data' : reducer_dict,
        'source' : 'reducer'
    }
    logging.info('Reducer {} sending final reducer output to KV Store'.format(str(p_id)))
    set_json_str = json.dumps(set_json)
    len_msg_set = pack('>Q', len(set_json_str))
    clientSocket.send(len_msg_set)
    clientSocket.send(set_json_str.encode())
    # ack = clientSocket.recv(1024).decode()
    logging.info('Reducer {} output sent successfully!'.format(str(p_id)))
    logging.info('Exiting Reducer {}....'.format(str(p_id)))

    # print(len(reducer_dict))
    
    print('Finished reducer {}'.format(str(p_id)))
    clientSocket.close()

    
    
