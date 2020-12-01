import socket
import threading
import logging
import pickle
import sys
import time
import datetime
import hashlib
import json

# macros
CONFIG_FILE = 'config.json'
SERVER = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
INSUFFICIENT_FUNDS = 'INSUFFICIENT FUNDS'
SUCCESS = 'TRANSFER SUCCESSFUL'


class Node:
    def __init__(self, term):
        self.term = term
        self.prevhash = 'NULL'
        self.nonce = 0
        self.tx = ['NULL', 'NULL', 'NULL']
        self.numTx = 0

    def addTx(self, txdata):
        self.tx[self.numTx] = txdata
        self.numTx += 1
        if self.numTx < 3:
            if self.validatePhash(self.tx, self.nonce):
                return 1
            else:
                self.nonce += 1
                return 0
        else:
            while self.nonce < 50:
                if self.validatePhash(self.tx, self.nonce):
                    return 1
                else:
                    self.nonce += 1

    def validatePhash(self, tx, nonce):
        tx_string = ''
        for i in tx:
            tx_string += i
        hash_string = tx_string + str(nonce)
        phash = hashlib.sha256(hash_string.encode()).hexdigest()
        if (phash[-1] == '0') or (phash[-1] == '1') or (phash[-1] == '2'):
            self.prevhash = phash
            return 1
        else:
            return 0


class Blockchain:
    def __init__(self):
        self.index = 0
        self.chain = []
        self.prevPhash = 'NULL'

    def addBlock(self, block):
        temp = block.prevhash
        block.prevhash = self.prevPhash
        self.prevPhash = temp
        self.chain.append(block)
        self.index += 1


class Server:
    def __init__(self, port):
        ADDRESS = (SERVER, port)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(ADDRESS)
        self.server_socket.listen()
        self.servers = []
        self.config = {}
        self.message_sockets = {}

        self.leader = None
        self.current_node = None
        self.candidateID = str(port)
        self.timeout = 10
        self.election_timeout = 8 + 5 * (port % 5050)
        self.current_role = 'FOLLOWER'
        self.heartbeat_received = 0
        self.transactions_log = []
        self.blockchain = Blockchain()
        self.balance_table = {}
        self.client_connections = {}

        with open(CONFIG_FILE, 'r') as file:
            self.config = json.load(file)
            for i in self.config['Servers']:
                if i != port:
                    self.servers.append(i)

            if self.config['Leader'] != '':
                self.leader = self.config['Leader']

            self.current_node = Node(self.config['Current_node']['Term'])
            self.current_node.nonce = self.config['Current_node']['Nonce']
            self.current_node.tx = self.config['Current_node']['Tx']
            self.current_node.prevhash = self.config['Current_node']['Hash']
            self.current_node.numTx = self.config['Current_node']['NumTx']

            self.balance_table = self.config['Balance']

            self.blockchain.index = self.config['Blockchain']['Index']
            # self.blockchain.chain = self.config['Blockchain']['Chain']
            self.blockchain.prevPhash = self.config['Blockchain']['Prevhash']

            self.transactions_log = self.config['Transaction_log']
            file.close()

        for i in self.servers:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.connect_ex((SERVER, i))
                self.message_sockets[str(i)] = sock
            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))

        # if self.leader is None:
        self.current_phase = 0
        self.current_term = 0
        self.vote_casted = 0
        self.phase1_votes = 0
        self.phase2_votes = 0

    def startNetwork(self):
        if self.leader is None:
            raft_thread = threading.Thread(target=self.beginRAFT)
            raft_thread.start()
        while True:
            connection, address = self.server_socket.accept()
            logging.debug("[CLIENT CONNECTED] {}".format(str(connection)))

            listen_transactions = threading.Thread(target=self.listenTransaction, args=(connection, address))
            listen_transactions.start()

    def beginRAFT(self):
        time.sleep(self.election_timeout)
        if (self.leader is None) and (self.current_role == 'FOLLOWER') and (self.current_phase == 0):
            logging.debug("[PHASE 1] Candidate {} contesting for election".format(self.candidateID))
            self.current_role = 'CANDIDATE'
            self.requestVotes()

    def requestVotes(self):
        log_term = 1
        self.current_term += 1
        self.phase1_votes = 1
        vote_request = {'Type': 'LEADER_ELECTION', 'ID': self.candidateID, 'Term': self.current_term,
                        'LogIndex': self.blockchain.index, 'LogTerm': log_term}
        message = pickle.dumps(vote_request)
        for sock in self.message_sockets.values():
            sock.sendall(bytes(message))

    def updateBlockchain(self, transaction):
        new_node = self.current_node.addTx(transaction['S'] + ' ' + transaction['R'] + ' ' + str(transaction['A']))
        self.updateBalanceTable(transaction['S'], transaction['R'], transaction['A'])
        if new_node:
            self.blockchain.addBlock(self.current_node)
            self.current_node = Node(self.current_term)

        if self.leader == self.candidateID:
            with open(CONFIG_FILE, 'w') as file:
                self.config['Balance'] = self.balance_table
                self.config['Transaction_log'] = self.transactions_log
                self.config['Current_node']['Term'] = self.current_node.term
                self.config['Current_node']['Nonce'] = self.current_node.nonce
                self.config['Current_node']['Tx'] = self.current_node.tx
                self.config['Current_node']['Hash'] = self.current_node.prevhash
                self.config['Current_node']['NumTx'] = self.current_node.numTx
                if new_node:
                    self.config['Blockchain']['Index'] = self.blockchain.index
                    # self.config['Blockchain']['Chain'] = self.blockchain.chain
                    self.config['Blockchain']['Prevhash'] = self.blockchain.prevPhash
                json.dump(self.config, file)
                file.close()

    def updateBalanceTable(self, sender, receiver, amount):
        self.balance_table[sender] -= amount
        self.balance_table[receiver] += amount

    def sendHeartbeat(self):
        while self.leader == self.candidateID:
            time.sleep(self.timeout)
            send_heartbeat = {'Type': 'HEARTBEAT', 'LeaderID': self.candidateID, 'Term': self.current_term}
            message = pickle.dumps(send_heartbeat)
            for sock in self.message_sockets.values():
                sock.sendall(bytes(message))

    def listenTransaction(self, connection, address):
        heartbeat_time = datetime.datetime.now()
        while True:
            msg = connection.recv(1024)
            x = pickle.loads(msg)
            logging.debug("[MESSAGE] {}".format(x))

            if datetime.datetime.now() > (heartbeat_time + datetime.timedelta(seconds=(1.5 * self.timeout))):
                self.heartbeat_received = 0
                self.beginRAFT()

            if x['Type'] == 'LEADER_ELECTION':
                if (self.current_term <= x['Term']) and (self.vote_casted == 0):
                    self.current_phase = 1
                    self.current_role = 'FOLLOWER'
                    self.current_term = x['Term']
                    self.vote_casted = 1
                    send_vote = {'Type': 'VOTE_RECEIVED', 'ID': self.candidateID, 'Term': x['Term']}
                    message = pickle.dumps(send_vote)
                    self.message_sockets[x['ID']].sendall(bytes(message))

            elif x['Type'] == 'VOTE_RECEIVED':
                if (x['Term'] == self.current_term) and (self.leader is None):
                    self.phase1_votes += 1
                    if self.phase1_votes == 2:
                        self.phase1_votes = 0
                        self.current_role = 'LEADER'
                        self.leader = self.candidateID
                        with open(CONFIG_FILE, 'w') as file:
                            self.config['Leader'] = self.candidateID
                            json.dump(self.config, file)
                            file.close()
                        send_heartbeat = threading.Thread(target=self.sendHeartbeat)
                        send_heartbeat.start()
                        new_leader = {'Type': 'NEW_LEADER', 'LeaderID': self.candidateID, 'Term': self.current_term}
                        message = pickle.dumps(new_leader)
                        for sock in self.message_sockets.values():
                            sock.sendall(bytes(message))

            elif x['Type'] == 'NEW_LEADER':
                self.current_term = x['Term']
                self.leader = x['LeaderID']
                self.current_role = 'FOLLOWER'

            elif x['Type'] == 'CLIENT_MESSAGE':
                if self.leader == self.candidateID:
                    if x['Transaction'] == 'T':
                        if self.balance_table[x['S']] >= x['A']:
                            new_transaction = {'S': x['S'], 'R': x['R'], 'A': x['A']}
                            self.transactions_log.append(new_transaction)
                            add_transaction = {'Type': 'ADD_TRANSACTION', 'LeaderID': self.candidateID,
                                               'Term': self.current_term, 'Transaction': new_transaction}
                            message = pickle.dumps(add_transaction)
                            for sock in self.message_sockets.values():
                                sock.sendall(bytes(message))

                            connection.send(SUCCESS.encode(FORMAT))
                            with open(CONFIG_FILE, 'w') as file:
                                self.config['Transaction_log'] = self.transactions_log
                                json.dump(self.config, file)
                                file.close()
                        else:
                            connection.send(INSUFFICIENT_FUNDS.encode(FORMAT))
                    elif x['Transaction'] == 'B':
                        message = self.balance_table[x['Client']]
                        connection.sendall(bytes(message))

            elif x['Type'] == 'ADD_TRANSACTION':
                if x['Term'] == self.current_term:
                    self.transactions_log.append(x['Transaction'])
                    self.phase2_votes = 1
                    added_transaction = {'Type': 'ADDED_TRANSACTION', 'ID': self.candidateID,
                                         'Term': self.current_term, 'Transaction': x['Transaction']}
                    message = pickle.dumps(added_transaction)
                    self.message_sockets[self.leader].sendall(bytes(message))

            elif x['Type'] == 'ADDED_TRANSACTION':
                if x['Term'] == self.current_term:
                    self.phase2_votes += 1
                    if self.phase2_votes == 2:
                        self.phase2_votes = 0
                        self.updateBlockchain(x['Transaction'])
                        commit_transaction = {'Type': 'COMMIT_TRANSACTION', 'ID': self.candidateID,
                                              'Term': self.current_term, 'Transaction': x['Transaction']}
                        message = pickle.dumps(commit_transaction)
                        for sock in self.message_sockets.values():
                            sock.sendall(bytes(message))

            elif x['Type'] == 'COMMIT_TRANSACTION':
                if x['Term'] == self.current_term:
                    self.transactions_log.remove(x['Transaction'])
                    self.updateBlockchain(x['Transaction'])

            elif x['Type'] == 'HEARTBEAT':
                heartbeat_time = datetime.datetime.now()
                self.heartbeat_received = 1


if __name__ == '__main__':
    PORT = sys.argv[1]
    logging.basicConfig(filename='Server' + str(PORT) + '.log', level=logging.DEBUG, filemode='w')
    serv = Server(int(PORT))
    serv.startNetwork()
