import multiprocessing
from socket import *
from collections import defaultdict
import sys
from mapper_wc import mapper_wc
from mapper_inv import mapper_inv
from reducer_wc import reducer_wc
from reducer_inv import reducer_inv
from multiprocessing import Process
import os
from configparser import ConfigParser
import math
import json
from struct import pack
import logging
import shutil


class Master(object):
    def __init__(self):
        # parser = ConfigParser()
        # parser.read("../properties.config")
        # self.master_ip = parser.get('master','master_IP_address')
        # self.master_port = int(parser.get('master','master_port'))
        # self.kvStorePath = parser.get('master','kvStorePath')
        # self.outputPath = parser.get('master', 'outputPath')
        # self.logfilePath = parser.get('master','logfilePath')
        # self.n_mappers = int(parser.get('mapper','n_mapper'))
        # self.mapper_ips = parser.get('mapper','mapper_ips')
        # self.n_reducers = int(parser.get('reducer','n_reducer'))
        # self.reducer_ips = parser.get('reducer','reducer_ips')
        # self.partitionPath = parser.get('master','partitionPath')
        # self.mapPath = parser.get('master','mapPath')
        # self.app = parser.get('app','operation')
        # self.wcInputPath = parser.get('app','wc_input_path')
        # self.invInputPath = parser.get('app','inv_input_path')

        parser = ConfigParser()
        parser.read("./properties.config")
        self.master_ip = parser.get('master','server_IP_address')
        self.master_port = int(parser.get('master','server_port'))
        self.kvStorePath = parser.get('master','kvStorePath')
        self.outputPath = parser.get('master', 'outputPath')
        self.logfilePath = parser.get('master','logfilePath')
        self.n_mappers = int(parser.get('mapper','n_mapper'))
        self.mapper_server_ip = parser.get('mapper','server_ip')
        self.n_reducers = int(parser.get('reducer','n_reducer'))
        self.reducer_server_ip = parser.get('reducer','server_ip')
        self.partitionPath = parser.get('master','partitionPath')
        self.mapPath = parser.get('master','mapPath')
        self.app = parser.get('app','operation')
        self.wcInputPath = parser.get('app','wc_input_path')
        self.invInputPath = parser.get('app','inv_input_path')
        



    def init_cluster(self, ip_port):
        
        if len(ip_port) > 1 or len(ip_port) == 0:
            print('System takes a single IP address and port currently, please retry')
            sys.exit()
        else:
            try:
                self.host = ip_port[0][0]
                self.serverPort = int(ip_port[0][1])
            except:
                print('Invalid arguments!')
                sys.exit()
            self.clientSocket = socket(AF_INET, SOCK_STREAM)
            self.clientSocket.connect((self.host, self.serverPort))
            self.mapper_server_ip = ip_port[0][0] + ':' + str(ip_port[0][1])
            self.reducer_server_ip = ip_port[0][0] + ':' + str(ip_port[0][1])
            logging.basicConfig(filename = self.logfilePath, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)
        return self.clientSocket
        # parser = ConfigParser()
        # parser.read("../properties.config")
        # self.master_ip = parser.get('master','master_IP_address')
        # self.master_port = int(parser.get('master','master_port'))
        # self.kvStorePath = parser.get('master','kvStorePath')
        # self.outputPath = parser.get('master', 'outputPath')
        # self.logfilePath = parser.get('master','logfilePath')
        # self.n_mappers = int(parser.get('mapper','n_mapper'))
        # self.mapper_server_ip = parser.get('mapper','server_ip')
        # self.n_reducers = int(parser.get('reducer','n_reducer'))
        # self.reducer_server_ip = parser.get('reducer','server_ip')
        # self.partitionPath = parser.get('master','partitionPath')
        # self.mapPath = parser.get('master','mapPath')
        # self.app = parser.get('app','operation')
        # self.wcInputPath = parser.get('app','wc_input_path')
        # self.invInputPath = parser.get('app','inv_input_path')
        
        # if not os.path.exists(self.mapPath):
        #     os.mkdir(self.mapPath)
        # if not os.path.exists(self.outputPath):
        #     os.mkdir(self.outputPath)
        # if not os.path.exists(self.partitionPath):
        #     os.mkdir(self.partitionPath)
        # self.wcMapPath = self.mapPath+'/wc/'
        # if not os.path.exists(self.wcMapPath):
        #     os.mkdir(self.wcMapPath)
        # self.invMapPath = self.mapPath+'/inv/'
        # if not os.path.exists(self.invMapPath):
        #     os.mkdir(self.invMapPath)
        # self.wcPartitionPath = self.partitionPath + '/wc/'
        # if not os.path.exists(self.wcPartitionPath):
        #     os.mkdir(self.wcPartitionPath)
        # self.invPartitionPath = self.partitionPath + '/inv/'
        # if not os.path.exists(self.invPartitionPath):
        #     os.mkdir(self.invPartitionPath)
        # self.wcOutputPath = self.outputPath + '/wc/'
        # if not os.path.exists(self.wcOutputPath):
        #     os.mkdir(self.wcOutputPath)
        # self.invOutputPath = self.outputPath + '/inv/'
        # if not os.path.exists(self.invOutputPath):
        #     os.mkdir(self.invOutputPath)

        
        
        

    def run_mapred(self, input_data, map_fn, reduce_fn, output_location):
        list_invMap = []
        try:
            multiprocessing.set_start_method('fork', force = True)
        except: 
            pass

        self.mapPath = output_location+'/map/'
        if not os.path.exists(self.mapPath):
            os.mkdir(output_location+'/map/')

        # if not os.path.exists(self.outputPath):
            # os.mkdir(self.outputPath)

        self.partitionPath = output_location+'/partition'
        if not os.path.exists(self.partitionPath):
            os.mkdir(output_location+'/partition')

        # MapReduce for Word count
        if map_fn == 'mapper_wc' and reduce_fn == 'reducer_wc':
            print('Getting word count for input file....')
            
            self.wcMapPath = self.mapPath+'/wc/'
            if not os.path.exists(self.wcMapPath):
                os.mkdir(self.wcMapPath)
            
            self.wcPartitionPath = self.partitionPath + '/wc/'
            if not os.path.exists(self.wcPartitionPath):
                os.mkdir(self.wcPartitionPath)
            
            self.wcOutputPath = output_location+ '/output_wc/'
            if not os.path.exists(self.wcOutputPath):
                os.mkdir(self.wcOutputPath)
            
            # Split the file into multiple files as per the number of mappers
            logging.info('Splitting files...')
            map_paths = self.split_files_v2(input_data, self.n_mappers, self.clientSocket, self.wcMapPath, flag='wc')
            
            logging.info('Starting mapper processes...')
            # Run mapper function
            self.run_mapper(self.n_mappers, self.mapper_server_ip, self.wcPartitionPath, map_paths, op_flag='wc')
            logging.info('Finished mapper processes!')

            logging.info('Starting reducer processes...')
            # Run reducer function
            self.run_reducer(self.n_reducers, self.reducer_server_ip, self.wcOutputPath, self.n_mappers, self.wcPartitionPath, op_flag = 'wc')
            logging.info('Finished reducer processes!')
            logging.info('Finished Map Reduce operations, Goodbye!')

        # MapReduce for Inverted Index
        elif map_fn == 'mapper_inv' and reduce_fn == 'reducer_inv':
            print('Getting inverted indexes for input files...')
            
            self.invMapPath = self.mapPath+'/inv/'
            if not os.path.exists(self.invMapPath):
                os.mkdir(self.invMapPath)

            self.invPartitionPath = self.partitionPath + '/inv/'
            if not os.path.exists(self.invPartitionPath):
                os.mkdir(self.invPartitionPath)

            self.invOutputPath = output_location + '/output_inv/'
            if not os.path.exists(self.invOutputPath):
                os.mkdir(self.invOutputPath)

            # Split the file into multiple files as per the number of mappers
            logging.info('Splitting files...')
            for i, filename in enumerate(os.listdir(self.invInputPath)):
                inputPath = os.path.join(self.invInputPath, filename)
                map_paths = self.split_files_v2(inputPath, self.n_mappers, self.clientSocket, self.invMapPath, flag='inv')
                list_invMap.extend(map_paths)
                map_paths = list_invMap
            
            logging.info('Starting mapper processes...')
            # Run mapper function
            self.run_mapper(self.n_mappers, self.mapper_server_ip, self.invPartitionPath, map_paths, op_flag='inv')
            logging.info('Finished mapper processes!')

            logging.info('Starting reducer processes...')
            # Run reducer function
            self.run_reducer(self.n_reducers, self.reducer_server_ip, self.invOutputPath, self.n_mappers, self.invPartitionPath, op_flag = 'inv')
            logging.info('Finished reducer processes!')
            logging.info('Finished Map Reduce operations, Goodbye!')

        else:
            print('Please input valid function (mapper_wc, reducer_wc/mapper_inv, reducer_inv)')
            logging.info('Please input valid function (mapper_wc, reducer_wc/mapper_inv, reducer_inv') 
            sys.exit()
        


        
    
    def destroy_cluster(self, clientSocket, flag = 0):
        if flag == 1:
            try:
                shutil.rmtree(self.mapPath)
            except:
                pass
            try:
                shutil.rmtree(self.partitionPath)
            except:
                pass
        clientSocket.close()
        logging.info('Cleaned up .')
        

    def split_files_v2(self, input_file_path, n_mappers, socket, mapPath, flag = None):
        map_paths = []
        file_size = os.path.getsize(input_file_path)
        chunkSize = math.ceil(file_size/n_mappers)
        chunk_no = 0
        print('Splitting files')

        fileName = input_file_path.split('/')[-1]
        with open(input_file_path,'r') as f:
            while True:
                data = f.read(chunkSize)
                if not data:
                    break 
                if flag == 'wc':
                    path = mapPath+'mapper_'+str(chunk_no)+'.txt'
                elif flag == 'inv':
                    path = mapPath+'mapper_'+fileName+'_'+str(chunk_no)+'.txt'
                set_json = {
                'set':path,
                'data': data,
                'source' : 'master'
                }

                # Reference for transfering data: https://stackoverflow.com/questions/42459499/what-is-the-proper-way-of-sending-a-large-amount-of-data-over-sockets-in-python

                map_paths.append(path)
                set_json_str = json.dumps(set_json)
                len_msg = pack('>Q', len(set_json_str))
                
                socket.send(len_msg)
                socket.send(set_json_str.encode())
                chunk_no += 1
                del data
        
        return map_paths

    # def split_files(self, input_file_path, n_mappers, socket, mapPath):
        
    #     n_chunks = n_mappers
    #     chunk_no = 0
    #     data = [] 
    #     print('Splitting files')
    #     map_paths = []
    #     with open(input_file_path,'r') as f:
    #         data = f.read().split()
    #     chunkSize = math.ceil(len(data)/n_chunks)

    #     for i in range(0, len(data), chunkSize):
            
    #         # Chunked files sent to KV Store 
    #         path = mapPath+'mapper_'+str(chunk_no)+'.txt'
    #         set_json = {
    #             'set':path,
    #             'data': " ".join(data[i:i+chunkSize]),
    #             'source' : 'master'
    #         }
    #         map_paths.append(path)
    #         set_json_str = json.dumps(set_json)
    #         len_msg = pack('>Q', len(set_json_str))
            
    #         socket.send(len_msg)
    #         socket.send(set_json_str.encode())
            
    #         chunk_no += 1
    #     return map_paths

    

    def run_mapper(self, n_mappers, IP, partitionPath, map_paths, op_flag = None):

        mappers = []
        partitions_loc = []
        
        if op_flag == 'inv':
            map_dict = defaultdict(list)
            for path in map_paths:
                path_spl = path.split('/')[-1].split('_')
                map_file = path_spl[1]
                map_ind = int(path_spl[-1].split('.')[0])
                map_dict[map_ind].append((map_file, path))
            map_paths = map_dict
            mapper_fn = mapper_inv

        elif op_flag == 'wc':
            mapper_fn = mapper_wc
            

        for p_id in range(n_mappers):
            # q = multiprocessing.Queue()
            
            proc = Process(target=mapper_fn, args=(p_id,IP,partitionPath,map_paths[p_id],))#,q,IP_list[p_id]))
            proc.start()
            logging.info('Started mapper process: {}'.format(str(p_id)))
            mappers.append(proc)
            #partitions_loc.append(q.get())
        
        # Barrier handling
        [p.join() for p in mappers]
        return partitions_loc

    # def run_reducer(self, n_reducers, partitions_loc, IP_list, outputPath):
    def run_reducer(self, n_reducers, IP, outputPath, n_mappers, partitionPath, op_flag = None):
        reducers = []

        if op_flag == 'inv':
            reducer_fn = reducer_inv
        else:
            reducer_fn = reducer_wc

        for p_id in range(n_reducers):
            #proc = Process(target=reducer_wc, args=(p_id, n_reducers, partitions_loc, IP_list[p_id], outputPath))
            proc = Process(target=reducer_fn, args=(p_id, n_reducers, IP, outputPath, n_mappers, partitionPath))
            proc.start()
            logging.info('Started reducer process: {}'.format(str(p_id)))
            
            reducers.append(proc)
        
        # Barrier handling
        [p.join() for p in reducers]
        
 

if __name__ == "__main__":
    

    master = Master()
    
    clientSocket = master.init_cluster([(master.master_ip, master.master_port)])
    # print(master.mapper_server_ip,master.reducer_server_ip)
    # master.destroy_cluster(clientSocket)
    # sys.exit()
   
    # if not os.path.exists(master.mapPath):
    #     os.mkdir(master.mapPath)
   
    # if not os.path.exists(master.outputPath):
    #     os.mkdir(master.outputPath)
   
    # if not os.path.exists(master.partitionPath):
    #     os.mkdir(master.partitionPath)
   
    # master.wcMapPath = master.mapPath+'/wc/'
    # if not os.path.exists(master.wcMapPath):
    #     os.mkdir(master.wcMapPath)
   
    # master.invMapPath = master.mapPath+'/inv/'
    # if not os.path.exists(master.invMapPath):
    #     os.mkdir(master.invMapPath)
   
    # master.wcPartitionPath = master.partitionPath + '/wc/'
    # if not os.path.exists(master.wcPartitionPath):
    #     os.mkdir(master.wcPartitionPath)
   
    # master.invPartitionPath = master.partitionPath + '/inv/'
    # if not os.path.exists(master.invPartitionPath):
    #     os.mkdir(master.invPartitionPath)
   
    master.wcOutputPath = master.outputPath + '/wc/'
    if not os.path.exists(master.wcOutputPath):
        os.mkdir(master.wcOutputPath)
   
    # master.invOutputPath = master.outputPath + '/inv/'
    # if not os.path.exists(master.invOutputPath):
    #     os.mkdir(master.invOutputPath)

    # logging.basicConfig(filename = master.logfilePath, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)
    # logging.info('Starting map reduce operations....')

    master.run_mapred(master.wcInputPath, 'mapper_wc', 'reducer_wc', master.wcOutputPath)

    # Socket connection
    # host = master.master_ip
    # serverPort = master.master_port
    # clientSocket = socket(AF_INET, SOCK_STREAM)
    # clientSocket.connect((host, serverPort))

    # Split the file into multiple files as per the number of mappers
    # logging.info('Starting the process to split the input into chunks..')
    # list_invMap = []
    # if master.app == 'wc':
    #     map_paths = master.split_files_v2(master.wcInputPath, master.n_mappers, clientSocket, master.wcMapPath, flag='wc')
    
    # elif master.app == 'inv':
    #     for i, filename in enumerate(os.listdir(master.invInputPath)):
    #         inputPath = os.path.join(master.invInputPath, filename)
    #         map_paths = master.split_files_v2(inputPath, master.n_mappers, clientSocket, master.invMapPath, flag='inv')
    #         list_invMap.extend(map_paths)
    #         map_paths = list_invMap

    # else:
    #     print('Please input valid application in Config file (wc/inv)')
    #     logging.info('Please input valid application in Config file (wc/inv)') 
    #     sys.exit()

    # logging.info('Finished splitting the input into chunks!')
    
    # # Run the mapper function
    # logging.info('Starting mapper processes...')

    # if master.app == 'wc':
    #     master.run_mapper(master.n_mappers, master.mapper_server_ip, master.wcPartitionPath, map_paths, op_flag='wc')
    
    # elif master.app == 'inv':    
    #     master.run_mapper(master.n_mappers, master.mapper_server_ip, master.invPartitionPath, map_paths, op_flag='inv')
    
    # logging.info('Finished mapper processes!')

    # # Run the reducer function
    # logging.info('Starting reducer processes...')
    # if master.app == 'wc':
    #     master.run_reducer(master.n_reducers, master.reducer_server_ip, master.wcOutputPath, master.n_mappers, master.wcPartitionPath, op_flag = 'wc')
    
    # elif master.app == 'inv':
    #     master.run_reducer(master.n_reducers, master.reducer_server_ip, master.invOutputPath, master.n_mappers, master.invPartitionPath, op_flag = 'inv')

    
    # logging.info('Finished reducer processes!')



    master.destroy_cluster(clientSocket, flag = 1)
    logging.info('Finished Map Reduce operations, Goodbye!')
    
    # clientSocket.close()    
