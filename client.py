import socket
import threading
import logging
import pickle
import sys
import json
import time
import datetime

# macros
CONFIG_FILE = 'config.json'
SERVER = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'


class Client:
    def __init__(self, port):
        ADDRESS = (SERVER, port)
        self.clientID = chr(ord('A') + (port % 6000))
        self.leader = 5051  # need to retrieve from state.cfg
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(ADDRESS)
        self.server_socket.listen()
        self.status = 1

        input_transactions = threading.Thread(target=self.inputTransactions)
        input_transactions.start()
        while True:
            connection, address = self.server_socket.accept()
            logging.debug("[CLIENT CONNECTED] {}".format(str(connection)))

            listen_transactions = threading.Thread(target=self.listenTransactions, args=(connection, address))
            listen_transactions.start()

    def checkLeader(self):
        while True:
            with open(CONFIG_FILE, 'r') as file:
                config = json.load(file)
                if config['Leader'] != '':
                    self.leader = int(config['Leader'])
                    self.client_socket.connect((SERVER, self.leader))
                    return 1
                file.close()
            time.sleep(3)

    def inputTransactions(self):
        if self.checkLeader():
            while True:
                raw_type = input("Please enter your transaction:")
                s = raw_type.split(' ')

                if s[0] == 'T' or s[0] == 't':
                    logging.debug("[TRANSFER TRANSACTION] {}".format(s))
                    transaction = {'Type': 'CLIENT_MESSAGE', 'Transaction': 'T', 'S': s[1], 'R': s[2],
                                   'A': int(s[3])}

                elif s[0] == 'B' or s[0] == 'b':
                    transaction = {'Type': 'CLIENT_MESSAGE', 'Transaction': 'B', 'S': self.clientID}

                else:
                    print("Incorrect Transaction")

                message = pickle.dumps(transaction)
                # TODO check who is the leader
                self.client_socket.sendall(bytes(message))
                self.status = 0

    def listenTransactions(self, connection, address):
        while True:
            msg = connection.recv(1024).decode(FORMAT)
            if msg:
                print(msg)


if __name__ == '__main__':
    PORT = sys.argv[1]
    logging.basicConfig(filename='Client' + str(PORT) + '.log', level=logging.DEBUG, filemode='w')
    client = Client(int(PORT))
