"""
CPSC 5520, Seattle University
Author: Leila Mirzaei
Created Date: Nov 18th 2022

This module look up data in a Chord network.
It takes a port number of an existing node in the Chord, played id and year.
This module sends player id and year to the Chord node whose port number is given as input.
It prints the data row or an appropriate message after querying the Chord network.
"""
import pickle
import socket
import sys
import chord_node

BUF_SZ = chord_node.BUF_SZ

if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) != 3:
        print(
            "Querier requires three values as input: the port number of an existing node, the player id, and the year.")
        exit()
    node_port = int(args[0])
    player_id = args[1]
    year = args[2]
    try:
        server_address = ('localhost', node_port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as query_socket:
            query_socket.settimeout(1500)
            query_socket.connect(server_address)
            request = ('query', player_id, year)  # RPC handler of the nodes receives a tuple with three items
            query_socket.sendto(pickle.dumps(request), server_address)
            query_result = pickle.loads(query_socket.recv(BUF_SZ))
            print(query_result)
    except Exception as e:
        print("Error occurred in populating data: {}".format(e))
