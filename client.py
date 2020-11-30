import socket
import threading
import logging
import pickle
import sys
import time
import datetime

# macros
CONFIG_FILE = 'state.cfg'
SERVER = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECTED"


class Client:
    def __init__(self, port):
        ADDRESS = (SERVER, port)
        self.clientID = int(port) % 6000
        self.leader = 5051  # need to retrieve from state.cfg
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.connect((SERVER, self.leader))
        input_transactions = threading.Thread(target=self.inputTransactions)
        input_transactions.start()
        listen_transactions = threading.Thread(target=self.listenTransactions)
        listen_transactions.start()

    def inputTransactions(self):
        while True:
            raw_type = input("Please enter your transaction:")
            s = raw_type.split(' ')

            if s[0] == 'T' or s[0] == 't':
                logging.debug("[TRANSFER TRANSACTION] {}".format(s))
                transaction = {'Type': 'CLIENT_MESSAGE', 'Transaction': 'T', 'Sender': s[1], 'Receiver': s[2],
                               'Amount': s[3]}

            elif s[0] == 'B' or s[0] == 'b':
                transaction = {'Type': 'CLIENT_MESSAGE', 'Transaction': 'B', 'Client': self.clientID}

            message = pickle.dumps(transaction)
            self.client_socket.sendall(bytes(message))

    def listenTransactions(self):
        while True:
            msg = self.client_socket.recv(1024)
            x = pickle.loads(msg)
            print(x)


if __name__ == '__main__':
    PORT = sys.argv[1]
    logging.basicConfig(filename='Client' + str(PORT) + '.log', level=logging.DEBUG, filemode='w')
    client = Client(PORT)
