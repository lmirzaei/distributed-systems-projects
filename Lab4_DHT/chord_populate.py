"""
CPSC 5520, Seattle University
Author: Leila Mirzaei
Created Date: Nov 18th 2022

This module populates data in a Chord network.
It takes a port number of an existing node in the Chord and the file name of the data (CSV format).
This module sends data, row by row to an arbitrary Chord node whose port number is given as input.
"""
import csv
import hashlib
import pickle
import socket
import sys

import chord_node

BUF_SZ = chord_node.BUF_SZ


def sha1_hash(id_string):
    result = hashlib.sha1(id_string.encode())
    return int(result.hexdigest(), 16)


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) != 2:
        print("As input, Populate requires the port number of an existing node and the filename of the data file.")
        exit()
    node_port = int(args[0])
    data_file_name = args[1]
    first_row = True
    counter = 1
    try:
        with open(data_file_name, newline='') as csv_file:
            spam_reader = csv.reader(csv_file, delimiter=',')
            for row in spam_reader:
                # if counter == 31:
                #     break
                if first_row:
                    first_row = False
                    continue

                server_address = ('localhost', node_port)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as writer:
                    writer.settimeout(1500)
                    writer.connect(server_address)
                    request = ('populate', row, None)  # RPC handler of the nodes receives a tuple with three items
                    writer.sendto(pickle.dumps(request), server_address)
                    populate_result = writer.recv(BUF_SZ)

                    player_id = row[0]
                    year = row[3]
                    data_row_id = sha1_hash(str(player_id) + str(year))
                    bucket_id = data_row_id % 2 ** chord_node.M
                    print('Row {}, identifier string = {} \t This will be stored in id {}'.format(counter,
                                                                                                  player_id + year,
                                                                                                  bucket_id))
                    print(pickle.loads(populate_result))
                print('------------------------------')
                counter += 1
    except Exception as e:
        print("Error occurred in populating data: {}".format(e))
