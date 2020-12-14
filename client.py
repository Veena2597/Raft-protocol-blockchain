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
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.connect_ex((SERVER, self.leader))

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(ADDRESS)
        self.server_socket.listen()

        self.status = 1
        self.response = None
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
                    print(self.leader)
                    try:
                        self.client_socket.connect((SERVER, self.leader))
                    except Exception as exc:
                        self.client_socket.close()
                file.close()

    def checkTimeout(self, timeout):
        while self.status != 1:
            if datetime.datetime.now() > (timeout + datetime.timedelta(seconds=(10))):
                print("TIMEOUT! PLEASE TRY AGAIN")
                #self.checkLeader()
                self.status = 1

    def inputTransactions(self):
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
                            self.client_socket.sendall(bytes(message))
                        except socket.error as exc:
                            logging.debug("[EXCEPTION] {}".format(exc))
                            self.client_socket.close()
                            self.checkLeader()
                        self.checkTimeout(datetime.datetime.now())

                    elif s[0] == 'B' or s[0] == 'b':
                        transaction = {'Type': 'CLIENT_MESSAGE', 'Transaction': 'B', 'S': self.clientID}
                        message = pickle.dumps(transaction)
                        try:
                            self.client_socket.sendall(bytes(message))
                        except socket.error as exc:
                            logging.debug("[EXCEPTION] {}".format(exc))
                            self.client_socket.close()
                            self.checkLeader()
                        self.checkTimeout(datetime.datetime.now())
                    else:
                        print("Incorrect Transaction")
                else:
                    print("Incorrect Transaction")

    def listenTransactions(self, connection, address):
        while True:
            msg = connection.recv(1024).decode(FORMAT)
            if msg == LEADER_CHANGE:
                temp = self.checkLeader()
            elif 'NEW_LEADER' in msg:
                print(msg.split(' ')[1])
            elif msg:
                print(msg)
                self.status = 1
                self.response = datetime.datetime.now()


if __name__ == '__main__':
    PORT = sys.argv[1]
    logging.basicConfig(filename='Client' + str(PORT) + '.log', level=logging.DEBUG, filemode='w')
    client = Client(int(PORT))
