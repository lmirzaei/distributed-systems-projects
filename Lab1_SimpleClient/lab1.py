"""
CPSC 5520, Seattle University
Author: Leila Mirzaei
Created Date: Sep 28th 2022

This program is a client that retrieves a list of other peers in the network
from the GDC web service on the 'cs2.seattleu.edu' host. It then sends out
greetings individual messages to other members in the network, receives
responses from them, and prints appropriate message to display each peer's response.
"""
import pickle
import socket
import sys

GCD_HOST = 'cs2.seattleu.edu'
GCD_PORT = 23600
BUF_SZ = 1024  # tcp receive buffer size


class Lab1Server:
    @staticmethod
    def requestPeers():
        """
        This method will be called at the beginning and handles the incoming
        message for the GDC server. It gets the response, creates a list of
        peers in the network called `nodesData` from it and returns it to the caller.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(1500)
            server.connect((GCD_HOST, GCD_PORT))
            try:
                server.sendall(pickle.dumps('JOIN'))
                raw = server.recv(BUF_SZ)
                nodesData = pickle.loads(raw)
            except (pickle.PickleError, KeyError):
                response = bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8')
            except Exception as e:
                print(e)
            print('Received', repr(nodesData))
            return nodesData

    @staticmethod
    def greetWithPeers(nodesData):
        """
        This method will be called after asking other peers addresses from the GDC
        server. It gets the `nodesData` as an input and sends a "Hello" message for
        each host in the list. It then prints the result based on the responses of the peers.
        """
        for peer in nodesData:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                server.settimeout(1500)
                try:
                    server.connect((peer['host'], peer['port']))
                    request = pickle.dumps('HELLO')
                    server.sendall(request)
                    raw = server.recv(BUF_SZ)
                except socket.timeout:
                    print('Connection to {} timed out.'.format(peer['host']))
                    continue
                except Exception as e:
                    print(str(e) + '! Host: {}, Port: {}'.format(peer['host'], peer['port']))
                    continue
                try:
                    response = pickle.loads(raw)
                    print('Received', repr(response))
                except (pickle.PickleError, KeyError):
                    response = bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8')
                    print('Error Happened!', repr(response))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 lab1.py cs2.seattleu.edu 23600")
        exit(1)
    GCD_HOST = sys.argv[1]
    GCD_PORT = int(sys.argv[2])
    nodesData = Lab1Server.requestPeers()
    Lab1Server.greetWithPeers(nodesData)
