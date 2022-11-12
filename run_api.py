import master

m = master.Master()

# This contains only the ip, port number of the server. 
# If you wish to change the port, please update the config file named 'properties.config'
# Currently the program takes a single ip, port combination in a tuple, in a list.
ip_port_list = [('127.0.0.1',8500)] 

#Initialize cluster
conn = m.init_cluster(ip_port_list)

# Specify input and output location, relative or absolute path

# For word count application, the input is currently only a single file.
input_path_wc = './kvStore/input/doc1.txt'

# For inverted index, you can pass the path of a directory containing multiple files as given below
input_path_inv = './kvStore/input/'

# Output path can be same for both applications
output_path = './kvStore/output'

# Run map reduce operation on given input
# map_fn can be maper_wc/mapper_inv
# reduce_fn can be reducer_wc/reducer_inv

# Run MapReduce for word count
m.run_mapred(input_data= input_path_wc,
             map_fn='mapper_wc', 
             reduce_fn='reducer_wc',
             output_location=output_path)

# Run MapReduce for inverted index
# m.run_mapred(input_data= input_path_inv,
#              map_fn='mapper_inv', 
#              reduce_fn='reducer_inv',
#              output_location=output_path)

# Destroys cluster by removing intermediate state such as map and partition files. This is however optional.
# You can set an optional flag = 1 to destroy these files, by default it is set to 0.
# This function also closes the connection to the server.
m.destroy_cluster(conn)
#m.destroy_cluster(conn, flag = 1) # flag = 1 destroys the intermediate map and partition files.
