"""
The format of the published message is a series of 1 to 50 of the following 32-byte records:

32-byte:
    [0, 8) ---> The timestamp is a 64-bit integer number of microseconds
    [8, 14) ---> The currency names, for example, 'USD', 'GBP'
    [14, 22) ---> The exchange rate is 64-bit floating point number
    [22, 32) ---> Reserved! These are not currently used and set to 0 for now.

"""
import math
import socket

from bellman_ford import bellman_ford, find_negative_cycle
from fxp_bytes_subscriber import *

STALENESS_TIMEOUT = 1.5  # A published price is assumed to remain in force for 1.5 seconds or until a
# superseding rate for the same market is observed.
SUBSCRIPTION_DURATION = 600  # Subscriptions last for 10 minutes

TOLERANCE = 0


class Subscriber(object):
    def __init__(self):
        """
        1- subscribe to the forex publishing service,
        2- for each message published, update a graph based on the published prices,
        3- run Bellman-Ford, and
        4- report any arbitrage opportunities.
        """
        self.start_time = datetime.now()
        self.latest_received_price = None
        self.graph = {}
        self.subscribe()

    def subscribe(self):
        listener_address = ('localhost', 0)
        forex_publisher_address = ('localhost', 43122)
        # You subscribe or renew your subscriptions to the price feed by sending your listening
        # IP address and port number to the forex provider process
        print('starting up on {} port {}'.format(*listener_address))

        is_subscribed = False

        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            sock.bind(listener_address)  # subscriber binds the socket to the publishers address
            while True:
                if not is_subscribed:
                    print("Subscribing")
                    self.send_subscription_request(forex_publisher_address, sock.getsockname())
                    is_subscribed = True
                print('\nblocking, waiting to receive message form forex publisher')
                data = sock.recv(4096)
                print('received {} bytes form forex publisher'.format(len(data)))
                quotes_list = unmarshal_message(
                    data)  # extract quotes from byte stream that was received from publisher
                self.update_graph(quotes_list)  # update the graph from based on the publisher response
                distance, previous, neg_edge = bellman_ford(self.graph, 'USD', TOLERANCE)  # call bellman ford starting
                # from USD to find a negative edge
                if neg_edge is None:
                    continue
                arbitrage_opportunity = find_negative_cycle(self.graph, neg_edge[1], previous)
                self.report_arbitrage(
                    arbitrage_opportunity)
                self.check_duration()

    def check_duration(self):
        if (datetime.now() - self.start_time).total_seconds() > SUBSCRIPTION_DURATION:
            exit()

    def update_graph(self, quotes):
        for item in quotes:
            timestamp = item['timestamp']
            source_currency = item['cross'].split('/')[0]
            destination_currency = item['cross'].split('/')[1]
            log_rate = -1.0 * math.log10(item['price'])
            # update this in the graph
            if self.graph.keys().__contains__(source_currency) and \
                    self.graph[source_currency].keys().__contains__(destination_currency):
                latest_timestamp = self.graph[source_currency][destination_currency][1]
                if timestamp < latest_timestamp:
                    # Quotes may come out of order since this is UDP/IP, so the process should ignore any
                    # quotes with timestamps before the latest one seen for that market.
                    print('ignoring out-of-sequence message \n \t {} {} {}'.format(timestamp, source_currency,
                                                                                   destination_currency, item['price']))
                    continue
            if not self.graph.keys().__contains__(source_currency):
                self.graph[source_currency] = {}
            self.graph[source_currency].update({destination_currency: (log_rate, timestamp)})
            if not self.graph.keys().__contains__(destination_currency):
                self.graph[destination_currency] = {}
            self.graph[destination_currency].update({source_currency: (-log_rate, timestamp)})
        self.remove_stale_quotes()

    def remove_stale_quotes(self):
        now = datetime.now(timezone.utc)
        source_to_remove = set()
        for source_currency, entry in self.graph.items():
            dest_to_remove = set()
            for destination_currency, (value, timestamp) in entry.items():
                if (now - timestamp).total_seconds() > STALENESS_TIMEOUT:
                    dest_to_remove.add(destination_currency)
                    print("removing stale quote for ('{}', '{}')".format(source_currency, destination_currency))
            for currency in dest_to_remove:
                entry.pop(currency)
            if len(entry) == 0:
                source_to_remove.add(source_currency)
        for currency in source_to_remove:
            self.graph.pop(currency)

    def report_arbitrage(self, cycle):
        print('ARBITRAGE: \n')
        for i in range(len(cycle) - 1):
            source_currency = cycle[i]
            destination_currency = cycle[i + 1]
            exchange_rate = self.graph[source_currency][destination_currency][0]
            print("\t \t Exchange {} for {} at {} --> {} {}".format(source_currency, destination_currency,
                                                                    math.fabs(exchange_rate), destination_currency,
                                                                    math.fabs(exchange_rate) * 100))

    @staticmethod
    def send_subscription_request(forex_publisher_address, listener_address):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as send_sub_socket:
            send_sub_socket.sendto(serialize_address(listener_address), forex_publisher_address)


if __name__ == '__main__':
    print('Leila Lab3 Assignment')
    Subscriber()
