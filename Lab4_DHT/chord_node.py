"""
CPSC 5520, Seattle University
Author: Leila Mirzaei
Created Date: Nov 18th 2022

This program implements a Chord (described here: https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)
The Chord software takes the form of a library to be linked with the client and server applications
that use it. The application interacts with Chord in two main ways:
    First, Chord provides a lookup(key) algorithm that yields the IP address of the node
    responsible for the key.

    Second, the Chord software on each node notifies the application of changes in the set of
    keys that the node is responsible for when a new node joins.

This program also allows a querier to talk to any arbitrary node in the network to query a
value for a given key or add a key/value pair (with replacement).


The `chord_node.py` module takes a port number of an existing node (or 0 to indicate it should start a new network).
Then, it joins a new node into the network using a system-assigned port number for itself.
The node joins and then listens for incoming connections (from other nodes or queriers).
For listening it uses a blocking TCP socket and pickle for the marshaling the messages.
"""
from datetime import datetime

import hashlib
import pickle
import socket
import sys
import threading

M = 7  # Network have at most 128 possible nodes (M=7, nodes count = 2^^7 = 128).
NODES = 2 ** M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43500


class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr
    <mrange [1,4)%100>
    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    >>> [i for i in ModRange(0, 0, 5)]
    [0, 1, 2, 3, 4]
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """

    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe

    >>> fe.successor = 1
    >>> fe

    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    """

    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2 ** (k - 1)) % NODES
        self.next_start = (n + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.successor = node  # This is the next active node. That is, what would
        # the node be if I wanted to store data in this interval?

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.next_start, self.successor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval


def sha1_hash(id_string):
    result = hashlib.sha1(id_string.encode())
    return int(result.hexdigest(), 16)


class ChordNode(object):
    def __init__(self, port_number):
        self.port_number = 0 if port_number > 0 else TEST_BASE
        self.if_first = True if port_number == 0 else False
        self.predecessor = None
        self.keys = {}  # dictionary <data_id, data_value> to store data
        self.identifier = None
        self.node = None
        self.node_socket = None
        threading.Thread(target=self.start_listening).start()
        self.finger_table = self.initialize_empty_finger_table()
        if self.if_first:
            self.join()
        else:
            endpoint_string = '127.0.0.0 ' + str(port_number)
            node_p = {'number': sha1_hash(endpoint_string) % 2 ** M, 'port': port_number}
            self.join(node_p)

    def start_listening(self):
        """
        This function starts a listener socket for handling incoming RPC requests.
        It wait for accepting requests.
        When it receives a request, it starts a thread, runs an RPC handler function on the
        new thread, and then returns to listening mode.
        :return:
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                self.node_socket = listener
                self.node_socket.bind(('localhost', self.port_number))
                self.node_socket.setblocking(True)
                self.node_socket.listen()
                self.port_number = self.node_socket.getsockname()[1]
                print('Node just started listening on port {}.'.format(self.port_number))
                while True:
                    client, client_addr = self.node_socket.accept()
                    threading.Thread(target=self.handle_rpc, args=(client,)).start()
        except Exception as e:
            print("Error occurred in starting a listener server for the node: {}. Error = {}".format(self.node, e))

    def initialize_empty_finger_table(self):
        """
        Each node maintains a finger table with (at most) M entries.
        The finger table is used to speed up the routing of query requests.
        For example if node identifier is 1 and M = 4 then node 1 puts 2,3,5,9 in its finger table:
        Finger Table for node 1:
                entry.start = 2 	 entry.stop = 3 	 entry.next_start = 3
                entry.start = 3 	 entry.stop = 5 	 entry.next_start = 5
                entry.start = 5 	 entry.stop = 9 	 entry.next_start = 9
                entry.start = 9 	 entry.stop = 1 	 entry.next_start = 1

        Finger Table for node 5:
                entry.start = 6 	 entry.stop = 7 	 entry.next_start = 7
                entry.start = 7 	 entry.stop = 9 	 entry.next_start = 9
                entry.start = 9 	 entry.stop = 13 	 entry.next_start = 13
                entry.start = 13 	 entry.stop = 5 	 entry.next_start = 5

        Finger Table for node 13:
                entry.start = 14 	 entry.stop = 15 	 entry.next_start = 15
                entry.start = 15 	 entry.stop = 1 	 entry.next_start = 1
                entry.start = 1 	 entry.stop = 5 	 entry.next_start = 5
                entry.start = 5 	 entry.stop = 13 	 entry.next_start = 13


        A finger table entry includes both the Chord identifier and the
        address (port number) of the relevant node.
        For each node, this program uses the string of node endpoint
        name (including local host IP and port number) then use SHA-1 to
        hash it (similar to what is suggested in the Stoica, et al. paper).
        :return: an empty finger table
        """
        while self.node_socket is None or self.port_number == 0:
            continue
        endpoint_string = '127.0.0.0' + str(self.port_number)
        self.identifier = sha1_hash(endpoint_string)
        self.node = self.identifier % (2 ** M)
        finger_table = [None] + [FingerEntry(self.node, k) for k in range(1, M + 1)]  # indexing starts at 1
        return finger_table

    def join(self, node_p=None):
        """
        This function joins this node to the Chord network.
        It takes an existing node as input, but if the input is None, it indicates that this
        is the first node in the Chord network and does not need to communicate with other
        nodes to notify and update them about its joining.
        When a node n joins the network, certain keys previously assigned to n’s successor
        now become assigned to n. To perform this, Chord node must perform three tasks when
        a node n joins the network:
            1. Initialize the predecessor and fingers of node n
            2. Update the fingers and predecessors of existing nodes to reflect
               the addition of n
            3. Notify the higher layer software so that it can transfer state
               (e.g. values) associated with keys that node is now responsible for.

        :param node_p: this is an arbitrary node in the network that this current node
        learns its predecessor and fingers by asking it to look them up.
        In the Stoica, et al. paper it is said that "We assume that the new node learns
        the identity of an existing Chord node by some external mechanism."
        In this program the port of other node in the Chord network is given as input.
        """
        if node_p is not None:
            self.init_finger_table(node_p)
            self.update_others()  # Move keys in (predecessor, node] from successor

        else:  # this is the only (first) node joining in the network
            for i in range(1, M + 1):
                self.finger_table[i].successor = {'number': self.node, 'port': self.port_number}
            self.predecessor = {'number': self.node, 'port': self.port_number}
        print('Node {} just joined the Chord network and listening on port {}'.format(self.node, self.port_number))
        print("--------------Finger table node {} after join:--------------".format(self.node))
        self.print_finger_table()
        self.print_node_info()

    def call_rpc(self, other_node, method, arg1=None, arg2=None):
        """
        This function calls other nodes in the network. It performs the rpc and receives the response.

        :param other_node: the address of the other nodes
        :param method: the remote function that should be called via rpc
        :param arg1: the first argument for the remote function
        :param arg2: the second argument for the remote function
        :return: the remote function's return value(s) or output
        """
        if other_node == self.port_number:
            result = self.dispatch_rpc(method, arg1, arg2)
            return result
        client = ('localhost', other_node)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as requester:
                requester.settimeout(1500)
                requester.connect(client)
                requester.sendto(pickle.dumps((method, arg1, arg2)), client)
                # print('calling receive & waiting to get result from {} for method {}'.format(other_node, method))
                response = requester.recv(BUF_SZ)
                return pickle.loads(response)
        except Exception as e:
            print("I'm node {}, Exception occurred in call function via rpc: {}".format(self.node, e))
            exit()

    def handle_rpc(self, client):
        """
        This function handles rpc requests from other nodes in the network and also clients and responds to them.

        :param client: is the network node or client that sent the rpc request.
        """
        try:
            rpc = client.recv(BUF_SZ)
            method, arg1, arg2 = pickle.loads(rpc)
            result = self.dispatch_rpc(method, arg1, arg2)
            client.sendall(pickle.dumps(result))
        except Exception as e:
            print("I'm node {}, Exception occurred in call function via rpc: {}".format(self.node, e))
            exit()

    def dispatch_rpc(self, method, arg1, arg2):
        if method == 'successor':
            return self.successor
        if method == 'update_finger_table':
            # arg1 = node details(identifier) and arg2 = index
            if arg1 is None or arg2 is None:
                print("Argument for calling `update_finger_table` method is not provided.")
                exit()
            else:
                self.update_finger_table(arg1, arg2)
        elif method == 'find_predecessor':
            if arg1['number'] == self.node:
                return self.predecessor
            else:
                return self.find_predecessor(arg1)
        elif method == 'update_your_predecessor':
            if arg1 is None:
                print("Argument for calling `update_your_predecessor` method is not provided.")
                exit()
            self.predecessor = arg1
        elif method == 'find_successor':
            if arg1 is None:
                print("Argument for calling `find_successor` method is not provided.")
                exit()
            return self.find_successor(arg1)
        elif method == 'closest_preceding_finger':
            if arg1 is None:
                print("Argument for calling `closest_preceding_finger` method is not provided.")
                exit()
            return self.closest_preceding_finger(arg1)
        elif method == 'populate':
            if arg1 is None:
                print("Argument for calling `save_data` method is not provided.")
                return "The row to populate is either missing or incorrect."
            return self.save_data(arg1)
        elif method == 'put_key':
            return self.put_key(arg1, arg2)
        elif method == 'query':
            if arg1 is None or arg2 is None:
                print("Arguments for calling `get_data` method is not provided.")
                return "Pass the player id and year to find the row."
            return self.query_data(arg1, arg2)
        elif method == 'get_key':
            return self.get_key(arg1)

    @property
    def successor(self):
        return self.finger_table[1].successor

    @successor.setter
    def successor(self, id_dic):
        self.finger_table[1].successor = id_dic

    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        node_p = self.find_predecessor(id)
        return self.call_rpc(node_p['port'], 'successor')

    def find_predecessor(self, id):
        node_p_number = self.node
        node_p_successor = self.successor
        node_p_port = self.port_number
        while id not in ModRange(node_p_number + 1, node_p_successor['number'] + 1, NODES):
            node_p = self.call_rpc(node_p_port, 'closest_preceding_finger', id)  # np = np.closest_preceding_finger(id)

            node_p_number = node_p['number']
            node_p_successor = self.call_rpc(node_p_port, 'successor')
            node_p_port = node_p['port']

        return {'number': node_p_number, 'port': node_p_port}

    def closest_preceding_finger(self, id):
        for i in range(M, 0, -1):
            if self.finger_table[i].successor['number'] in ModRange(self.node + 1, id, NODES):
                return self.finger_table[i].successor
        return {'number': self.node, 'port': self.port_number}

    def init_finger_table(self, node_p):
        """
        This function updates the values of entries in the finger table of local node.
        It also updates the predecessor.

        :param node_p: is an arbitrary node already in the network
        """
        self.finger_table[1].successor = self.call_rpc(node_p['port'], 'find_successor', self.finger_table[
            1].start)  # node_p.find_successor(self.finger_table[1].start)
        self.predecessor = self.call_rpc(self.successor['port'], 'find_predecessor',
                                         self.successor)  # self.predecessor = self.successor.predecessor
        self.call_rpc(self.successor['port'], 'update_your_predecessor',
                      {'number': self.node, 'port': self.port_number})  # self.successor.predecessor = self.node
        for i in range(1, M):
            if self.finger_table[i + 1].start in ModRange(self.node, self.finger_table[i].successor['number'], NODES):
                # self.node <= self.finger_table[i + 1].start < self.finger_table[i].successor['number']:
                self.finger_table[i + 1].successor = self.finger_table[i].successor
            else:
                self.finger_table[i + 1].successor = self.call_rpc(node_p['port'], 'find_successor', self.finger_table[
                    i + 1].start)  # node_p.find_successor(self.finger_table[i + 1].start)

    def update_finger_table(self, s, i):
        """
         This function updates this node's finger table with s, if s is the i-th finger of it
        :param s: new node for entry i
        :param i: the index of finger table
        """
        if self.finger_table[i].start != self.finger_table[i].successor['number'] \
                and s['number'] in \
                ModRange(self.finger_table[i].start, self.finger_table[i].successor['number'], NODES):
            self.finger_table[i].successor = s
            p = self.predecessor  # get first node preceding this local node
            self.call_rpc(p['port'], 'update_finger_table', s, i)
        print("--------------Finger table node {} after update:--------------".format(self.node))
        self.print_finger_table()
        self.print_node_info()

    def update_others(self):
        """
        This function updates all nodes whose finger tables should refer to this local node
        """
        for i in range(1, M + 1):
            # find the last node p whose i-th finger might be this local node
            node_p = self.find_predecessor((1 + self.node - 2 ** (i - 1) + NODES) % NODES)
            # node_p.update_finger_table(self.node, i)
            self.call_rpc(node_p['port'], 'update_finger_table', {'number': self.node, 'port': self.port_number}, i)

    def put_key(self, key_id, key_value):
        """
        This function updates the keys dictionary and adds or stores the new item given by the populate request.

        :return: True, indicates the <key_id, key_value> pair successfully added
        """
        self.keys[key_id] = key_value
        self.print_keys_dictionary()
        self.print_node_info()
        return True

    def get_key(self, key_id):
        """
        This function replies to the query question by returning the data row in the
        keys dictionary for 'key_id,' or None if the keys dictionary does not contain
        a pair with key equal to the input id.
        """
        if key_id not in self.keys.keys():
            print("This id is not available in node {} keys dictionary".format(self.node))
            return None
        return self.keys[key_id]

    def save_data(self, input):
        """
        This function save the input row in the appropriate node of the Chord network.
        As identifier or key of this row, the node uses the value in the first column (player id) concatenated
        with the value in the fourth column (year).
        :param input: data row in list format
        :return: a string describing the result of populating the input data row
        """
        row = input
        player_id = row[0]
        year = row[3]
        data_id = sha1_hash(str(player_id) + str(year))
        bucket_id = data_id % 2 ** M
        responsible = self.call_rpc(self.port_number, 'find_successor', bucket_id)
        done = self.call_rpc(responsible['port'], 'put_key', data_id, row)
        if done:
            return "Node {} saved the row".format(responsible['number'])
        else:
            return "Populate failed"

    def query_data(self, player_id, year):
        """
        This function finds the node in charge of keeping a row with the given
        player id and year and retrieves the row from that node.

        :param player_id: first part of the identifier for a row
        :param year: second part of the identifier for a row
        :return: a row or a string describing the result of query result
        """
        data_id = sha1_hash(str(player_id) + str(year))
        bucket_id = data_id % 2 ** M
        responsible = self.call_rpc(self.port_number, 'find_successor', bucket_id)
        try:
            row = self.call_rpc(responsible['port'], 'get_key', data_id)
            if row is None:
                return "Data is not available."
            if isinstance(row, list):
                return row
            else:
                return "Query failed!"
        except Exception as e:
            print("Query failed! Error = {}".format(e))
            return "Query failed!"

    def print_finger_table(self):
        for entry in self.finger_table:
            if entry is None:
                continue
            print("entry.start = {} \t entry.stop = {} \t entry.successor = {} \t "
                  .format(entry.start, entry.interval.stop,
                          entry.successor))

    def print_node_info(self):
        print('[{}] Node {} is listening on port {}'.format(datetime.now().strftime("%I:%M:%S.%f"), self.node,
                                                            self.port_number))
        print('\tsuccessor = {}\t\tpredecessor = {}'.format(self.successor['number'], self.predecessor['number']))

    def print_keys_dictionary(self):
        print("-------------- Keys in node {} --------------".format(self.node))
        for key, value in self.keys.items():
            player_id = value[0]
            year = value[3]
            data_id = sha1_hash(str(player_id) + str(year))
            bucket_id = data_id % 2 ** M
            print('\t\t', bucket_id, ' : ', value[0])


if __name__ == '__main__':
    args = sys.argv[1:]
    node_port = int(args[0])
    ChordNode(node_port)
