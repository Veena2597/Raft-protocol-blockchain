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
LEADER_CHANGE = 'LEADER_CHANGE'


class Client:
    def __init__(self, port):
        ADDRESS = (SERVER, port)
        self.clientID = chr(ord('A') + (port % 6000))
        self.leader = 5051  # need to retrieve from state.cfg

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(ADDRESS)
        self.server_socket.listen()

        self.status = 1
        self.server_start = 0
        input_transactions = threading.Thread(target=self.inputTransactions)
        input_transactions.start()
        while True:
            connection, address = self.server_socket.accept()
            logging.debug("[CLIENT CONNECTED] {}".format(str(connection)))

            listen_transactions = threading.Thread(target=self.listenTransactions, args=(connection, address))
            listen_transactions.start()

    def sendServer(self, message):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            client.connect((SERVER, int(self.leader)))
            client.sendall(bytes(message))
        except socket.error as exc:
            logging.debug("[EXCEPTION] {}".format(exc))
        client.close()

    def inputTransactions(self):
        print("Establishing connection with Servers")
        while True:
            if self.status == 1:
                self.status = 0
                raw_type = input("Please enter your transaction:")
                s = raw_type.split(' ')

                if s[1] == self.clientID:
                    if s[0] == 'T' or s[0] == 't':
                        logging.debug("[TRANSFER TRANSACTION] {}".format(s))
                        transaction = {'Type': 'CLIENT_MESSAGE', 'Transaction': 'T', 'S': s[1], 'R': s[2],
                                       'A': int(s[3])}
                        message = pickle.dumps(transaction)

                        try:
                            self.sendServer(message)
                        except socket.error as exc:
                            logging.debug("[EXCEPTION] {}".format(exc))
                        i = 0
                        while self.status == 0:
                            time.sleep(1)
                            i = i + 1
                            if i == 6:
                                i = 0
                                print("TIMEOUT! Trying again")
                                self.sendServer(message)

                    elif s[0] == 'B' or s[0] == 'b':
                        transaction = {'Type': 'CLIENT_MESSAGE', 'Transaction': 'B', 'S': self.clientID}
                        logging.debug("[TRANSFER TRANSACTION] {}".format(s))
                        message = pickle.dumps(transaction)

                        try:
                            self.sendServer(message)
                        except socket.error as exc:
                            logging.debug("[EXCEPTION] {}".format(exc))
                        i = 0
                        while self.status == 0:
                            time.sleep(1)
                            i = i + 1
                            if i == 8:
                                self.sendServer(message)

                    else:
                        print("Incorrect Transaction")
                        self.status = 1
                else:
                    print("Incorrect Transaction")
                    self.status = 1

    def listenTransactions(self, connection, address):
        while True:
            msg = connection.recv(1024).decode(FORMAT)
            if 'NEW_LEADER' in msg:
                self.leader = msg.split(' ')[1]
            elif msg:
                print(msg)
                self.status = 1


if __name__ == '__main__':
    PORT = sys.argv[1]
    logging.basicConfig(filename='Client' + str(PORT) + '.log', level=logging.DEBUG, filemode='w')
    client = Client(int(PORT))
