"""
CPSC 5520, Seattle University
Author: Leila Mirzaei
Created Date: Oct 9th 2022
This program simulates Bully algorithm to find and set a leader for a group of nodes
in a distributed system.

In this solution for implementing the algorithm the states dictionary is used to monitor
the I/O status, so the state for a given socket is what action is expected next and then
that action is triggered by an event coming back from the selector.

I grab the listener socket, which is unique in that it doesn't relate to any particular
peer, to store the states of my own self. I use the WAITING_FOR_OK state when I’m haven’t
received any OKs back yet. Once I receive one, I change my state to WAITING_FOR_VICTOR.
If I never receive one, then after the timeout I declare myself as leader and broadcast
the victory message. When I do determine I'm the leader, I note it and then change my own
state to QUIESCENT. I also create connections to all of my peers and set the states of
those sockets to SEND_VICTORY, since I need to wait for the selector to say those sockets
are ready to write a message to. The SEND_OK state says I’ve gotten an ELECTION message
from that peer and so I need to send along an OK response. When the selector returns this
socket saying it is ready to write, I’ll send it.
"""
import pickle
import selectors
import socket
import sys
from datetime import datetime
from enum import Enum
from typing import Dict

ASSUME_FAILURE_TIMEOUT = 10  # 10 seconds
CHECK_INTERVAL = 5  # 5 seconds
BUF_SZ = 1024  # tcp receive buffer size
PEER_DIGITS = 4
GCD_HOST = 'localhost'
GCD_PORT = 23203


class Reason(Enum):
    """
    Enumeration of reasons for which a socket begins sending or waiting for a message
    """
    # For the following reasons, the process intends to begin an election:
    JUST_JOINED = 'JUST JOIN GROUP'  # When I want to start an election on startup.
    TIMEOUT_NO_COORDINATOR_RECEIVED = 'COORDINATOR TIMEOUT OCCURRED'  # When I'm waiting to see who is the winner,
    # but I don't get before the timeout.
    GET_ELECTION_MESSAGE = 'A PEER SENT ELECTION TO ME'  # When a peer/a member sent me an ELECTION message

    # For the following reasons, the process intends to declare victory:
    TIMEOUT_NO_OK_RECEIVED = 'OK TIMEOUT OCCURRED'  # When the I reached timeout while waiting for peers to
    # send OK
    COORDINATOR_MYSELF = 'I HAVE HIGHEST PID'  # When ELECTION is finished and I'm the member with highest pid


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'
    SEND_PROBE = 'SEND PROBE TO LEADER'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server
    WAITING_FOR_PROBE = "WAITING FOR PROBE"

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)


class Lab2(object):
    """
    This is a peer or process who join a group of peers, and uses bully algorithm
    to find the leader of the group.

    To make it compatible with python3.6 I commented out types
    # Pid = tuple[int, int]
    # Address = tuple[str, int]
    # members: Dict[Pid, Address]
    # Peer = socket.socket
    # Timestamp = datetime
    # states: Dict[Peer, tuple[State, Timestamp]]
    """

    def __init__(self, gcd_address, next_birthday, su_id):
        """
        This init function ....
        :param gcd_address: the address of GCD server, includes the host and port
        :param next_birthday: the date of next birthday
        :param su_id: Seattle University id
        """
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))  # node identity is a pair of (days until the next birthday, SU ID)
        self.bully = None  # None means election is pending, otherwise this will be pid of the leader
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()
        self.selector.register(self.listener, selectors.EVENT_READ, None)
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))  # is a pair of IP and port of the GCD server
        self.states = {}  # dictionary with the socket peer as the key and the value is a tuple (Status, timestamp).
        self.members = {}  # dictionary with the pid of all peers as the key and addresses as the values (same as
        # returned format of JOIN message in gcd)
        self.join_group()
        self.run()

    def run(self):
        while True:
            events = self.selector.select(
                CHECK_INTERVAL)  # select function returns a list of tuples, one for each of the sockets
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()

    def accept_peer(self):
        """
        Create a peer socket for reading incoming messages
        """
        conn, addr = self.listener.accept()  # Should be ready to read
        print(f"Accepted connection from {addr}")
        conn.setblocking(False)
        events = selectors.EVENT_READ
        self.set_state(State.WAITING_FOR_ANY_MESSAGE, conn)
        self.selector.register(conn, events, None)

    def send_message(self, peer):  # send_message(self, peer: Peer):
        """
        Send the queued message to the given peer (based on its current state)
        :param peer: the socket object that should be used to send the message
        """
        state = self.get_state(peer)
        print('{}: sending {} [{}]'.format(self.pr_sock(peer), state.value, self.pr_now()))
        try:
            self.send(peer, state.value, self.members)  # should be ready, but may be a failed connect instead
        except ConnectionError as error:
            print('Connection error occurred in sending the message: ', error)
        except Exception as error:
            print('Error occurred in sending the message: ', error)

        # check to see if we want to wait for response immediately
        if state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer)
            self.set_state(State.WAITING_FOR_OK)
            # Switch to read and don't close the connection to receive OK response
            self.selector.modify(peer, selectors.EVENT_READ)
        else:
            # Nothing more to send or receive for now
            self.set_quiescent(peer)

    def receive_message(self, peer):  # receive_message(self, peer: Peer):
        """
        Receive the queued message from the given peer (based on its current state)
        :param peer: the socket object of the peer in the group that should be used for receiving the message
        """
        state = self.get_state(peer)
        message = self.receive(peer)
        if not message:
            # peer closed their connection
            print('Unregister {}. No messages was received. [{}]'.format(self.pr_sock(peer), self.pr_now()))
            self.selector.unregister(peer)
            peer.close()
            return
        message_name, their_members = message
        print('{}: received {} [{}]'.format(self.pr_sock(peer), message_name, self.pr_now()))
        if (state == State.WAITING_FOR_OK) and (message_name == State.SEND_OK.value):
            # wait to see who is the winner:
            self.set_state(state=State.WAITING_FOR_VICTOR)
            print('Waiting to see who is the winner [{}]'.format(self.pr_now()))
            # Nothing more to read or write from this peer
            self.set_quiescent(peer)
        elif self.get_state() == State.WAITING_FOR_VICTOR and message_name == State.SEND_VICTORY.value:
            self.update_members(their_members)
            self.set_leader(peer)
            # Nothing more to do for now with any peer
            for peer in self.states.keys():
                self.set_quiescent(peer)
        elif message_name == State.SEND_ELECTION.value:
            # When I receive an ELECTION message, I update the membership list with
            # any members I didn't already know about, then I respond with the text OK.
            # If I am currently in an election, that's all I do.
            # If I am not in an election, then proceed as though I am initiating a new election.
            self.update_members(their_members)
            # Switch to write and don't close the connection to immediately reply with OK
            self.selector.modify(peer, selectors.EVENT_WRITE)
            self.set_state(State.SEND_OK, peer)
            if not self.is_election_in_progress():
                self.start_election(Reason.GET_ELECTION_MESSAGE)
        return

    @classmethod
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, buffer_size=BUF_SZ):
        message = (message_name, message_data)
        peer.send(pickle.dumps(message))

    @staticmethod
    def receive(peer, buffer_size=BUF_SZ):
        raw = peer.recv(buffer_size)
        if not raw:
            return None
        return pickle.loads(raw)

    def check_timeouts(self):
        """
        This function checks if the last message that was sent has not received a response before the timeout period
        """
        if not self.is_election_in_progress():
            return

        self_state, self_timestamp = self.get_state(detail=True)
        if self_state == State.WAITING_FOR_VICTOR and self.is_expired(self_timestamp):
            print('Expired wait for victor, restarting election [{}]'.format(self.pr_now()))
            self.start_election(Reason.TIMEOUT_NO_COORDINATOR_RECEIVED)
            return
        elif self_state == State.WAITING_FOR_VICTOR:
            print('Unexpired wait for victor, not declaring victory yet [{}]'.format(self.pr_now()))
            return

        should_declare_victory = True
        for peer, (state, timestamp) in self.states.items():
            if peer == self:
                continue
            if state == State.WAITING_FOR_OK and not self.is_expired(timestamp):
                print('Unexpired wait for ok, not declaring victory yet [{}]'.format(self.pr_now()))
                should_declare_victory = False

        if should_declare_victory:
            self.declare_victory(Reason.TIMEOUT_NO_OK_RECEIVED)

    def get_connection(self, member):  # get_connection(self, member: tuple[Pid, Address]):
        """
        Creates a new connection to the specified member using the address (host and port)
        """
        pid, addr = member
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.setblocking(False)
        peer.connect_ex(addr)
        events = selectors.EVENT_WRITE
        self.selector.register(peer, events)
        return peer

    def is_election_in_progress(self):
        if self.get_state() == State.WAITING_FOR_OK or self.get_state() == State.WAITING_FOR_VICTOR:
            return True
        else:
            return False

    @staticmethod
    def is_expired(timestamp: datetime, threshold=ASSUME_FAILURE_TIMEOUT):
        return (datetime.now() - timestamp).total_seconds() > threshold

    def set_leader(self, new_leader):
        self.bully = new_leader
        print('Set the leader: leader = {} [{}]'.format(self.pr_leader(), self.pr_now()))

    def get_state(self, peer=None, detail=False):
        """
        Look up current state in the state dictionary for a given peer.

        :param peer: socket connected to peer process (None means self)
        :param detail: if True, then the state and timestamp are both returned
        :return: either the state or (state, timestamp) depending on detail (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_state(self, state, peer=None):
        """
        Set the state for a given peer in the states dictionary.
        states dictionary: <key:peer socket object, value: (Status, timestamp)>

        :param state: state of this peer
        :param peer: socket object connected to this given peer process (None means self)
        """
        if peer is None:
            peer = self
        self.states[peer] = (state, datetime.now())

    def set_quiescent(self, peer=None):  # set_quiescent(self, peer: Peer = None):
        if peer is None:
            peer = self
        print('Set {} to QUIESCENT [{}]'.format(self.pr_sock(peer), self.pr_now()))
        if peer != self:
            # Close any remaining open connection
            if self.get_state(peer) != State.QUIESCENT and peer.fileno() != -1:
                self.selector.unregister(peer)
                peer.close()
        self.states[peer] = (State.QUIESCENT, datetime.now())

    def start_election(self, reason):
        """
        When I first join the group or whenever I notice the leader has failed, I start
        an election. The ELECTION message is sent to each member with a higher process
        id than my own pid.  While I am awaiting responses from higher processes, I put
        myself in the election-in-progress (waiting for OK) state.
        The ELECTION message is a list of all the current (alive or failed) group members,
        including myself.
        """

        print('Starting election, reason = {} [{}]'.format(reason.value, self.pr_now()))
        i_am_highest = True
        for pid, address in self.members.items():
            if not self.is_self(pid) and not self.is_higher(pid):
                peer = self.get_connection((pid, address))
                self.set_state(State.SEND_ELECTION, peer)
                i_am_highest = False

        if i_am_highest:
            print('Was highest, declaring victory [{}]'.format(self.pr_now()))
            self.declare_victory(reason.COORDINATOR_MYSELF)
        else:
            print('Waiting for OK from higher processes [{}]'.format(self.pr_now()))
            self.set_state(State.WAITING_FOR_OK)

    def declare_victory(self, reason):
        # When declaring victory sends a COORDINATOR to everyone.
        print('Declaring victory, reason = {} [{}]'.format(reason.value, self.pr_now()))
        self.set_state(State.QUIESCENT)
        self.set_leader(self.listener.getsockname())
        any_declared_victory_sent = True
        for member in self.members.items():
            if self.is_self(member[0]):
                continue
            peer = self.get_connection(member)
            self.set_state(State.SEND_VICTORY, peer)
            any_declared_victory_sent = False
            print('Declared victory to {} [{}]'.format(self.pr_sock(peer), self.pr_now()))
        if any_declared_victory_sent:
            print('No members active, no COORDINATION message sent.')

    def update_members(self,
                       their_idea_of_membership):  # update_members(self, their_idea_of_membership: Dict[Pid, Address]):
        # If I receive a COORDINATOR message, then change my state to not be election-in-progress and update
        # my group membership list as necessary. Note the (possibly) new leader.
        self.members.update(their_idea_of_membership)
        print('The membership dictionary is updated [{}]'.format(self.pr_now()))

    @staticmethod
    def start_a_server():
        """
        This function sets up the listening socket
        :return: the listener socket, and address of the socket

        The function creates the listening socket with address ('localhost', 0) and then use
        listener.getsockname() to report this listener to the GCD when joining the group.
        The port number of zero asks the socket library to allocate any free port for you.
        """
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener_socket.bind(('localhost', 0))
        listener_socket.listen()  # Calling listen() makes a socket ready for accepting connections
        listener_socket.setblocking(False)
        return listener_socket, listener_socket.getsockname()

    def join_group(self):
        """
        JOIN the group by talking to the GCD.
        This function uses a blocking socket to communicate with the GCD server, in contrast to
        other sockets in the program (sockets in the send and receive functions). To continue
        running the program
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(15)  # 15 seconds
            server.connect(self.gcd_address)
            try:
                # GCD accepts this join format: ('JOIN', ((days_to_bd, su_id), (host, port)))
                message_ask_gcd = ('JOIN', (self.pid, self.listener_address))
                server.sendall(pickle.dumps(message_ask_gcd))
                raw = server.recv(BUF_SZ)
                all_peers_listeners = pickle.loads(raw)
                print('Received from GCD2: ', repr(all_peers_listeners))
                self.members = all_peers_listeners
            except (pickle.PickleError, KeyError):
                response = bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8')
                print(response)
            except Exception as e:
                print('An error occurred: ', e)

        self.start_election(Reason.JUST_JOINED)

    def is_self(self, pid):  # : Pid):
        return pid == self.pid

    def is_higher(self, pid):  # : Pid):
        if (self.pid[0] > pid[0]) or (self.pid[0] == pid[0] and self.pid[1] > pid[1]):
            return True
        return False

    @staticmethod
    def pr_now():
        """Printing helper for current timestamp."""
        return datetime.now().strftime('%H:%M:%S.%f')

    def pr_sock(self, sock):
        """Printing helper for given socket."""
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        """Static version of helper for printing given socket."""
        try:
            l_port = sock.getsockname()[1] % PEER_DIGITS
        except OSError:
            l_port = '???'
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{}({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        """Printing helper for current leader's name."""
        return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) == 2:
        birthday = datetime.strptime(args[0], "%m/%d/%Y")
        su_id = args[1]
    else:
        birthday = datetime(2023, 5, 19)
        su_id = 9120032
    print(f'{birthday}, {su_id}')
    node_one = Lab2(gcd_address=[GCD_HOST, GCD_PORT], next_birthday=birthday, su_id=su_id)
