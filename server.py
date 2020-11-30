import socket
import threading
import logging
import pickle
import sys
import time
import datetime
import hashlib

# macros
CONFIG_FILE = 'config.cfg'
SERVER = socket.gethostbyname(socket.gethostname())


class Node:
    def __init__(self, term):
        self.term = term
        self.Phash = None
        self.nonce = None
        self.tx = []
        self.numTx = 0

    def addTx(self, tx):
        self.tx.append(tx)
        self.numTx += 1
        self.updateNonce()

    def updatePhash(self, tx, nonce):
        tx_string = ''
        for i in tx:
            if i is not None:
                tx_string += i['Sender'] + ' ' + i['Receiver'] + ' ' + str(i['Amount']) + ' '
            else:
                tx_string += 'NULL '
        hash_string = tx_string + str(nonce)
        self.Phash = hashlib.sha256(hash_string.encode()).hexdigest()

    def updateNonce(self):
        self.nonce = len(self.tx)


class Blockchain:
    def __init__(self):
        self.index = 0
        self.chain = []
        self.prevTx = []
        self.prevNonce = None

    def addBlock(self, block):
        block.updateHash(self.prevTx, self.prevNonce)
        self.chain.append(block)
        self.index += 1
        self.prevTx = block.tx
        self.prevNonce = block.nonce


class Log:
    def __init__(self, index, term, transaction):
        self.index = index
        self.term = term
        self.transaction = transaction

    def getTerm(self):
        return self.term


class Server:
    def __init__(self, port):
        ADDRESS = (SERVER, port)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(ADDRESS)
        self.server_socket.listen()
        self.servers = []
        self.message_sockets = []

        with open(CONFIG_FILE, 'r') as file:
            for line in file:
                line = line.strip()
                if line != PORT:
                    self.servers.append(line)

        for i in self.servers:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.connect_ex((SERVER, i))
                self.message_sockets.append(sock)
            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))

        self.candidateID = int(port) % 5050
        self.leader = None
        self.timeout = 8 + 2 * self.candidateID
        self.current_role = 'FOLLOWER'
        self.current_phase = 'Leader_election'
        self.current_term = 0
        self.heartbeat_received = 0
        self.majority_votes = 1

        self.current_node = None  # retreive from saved configuration
        self.transactions_log = []
        self.local_log = []
        self.blockchain = Blockchain()
        self.blockchain.addBlock(self.current_node)
        self.balance_table = []

    def startNetwork(self):
        while True:
            connection, address = self.server_socket.accept()
            logging.debug("[CLIENT CONNECTED] {}".format(str(connection)))

            listen_transactions = threading.Thread(target=self.listenTransaction, args=(connection, address))
            listen_transactions.start()

    def beginRAFT(self):
        time.sleep(self.timeout)
        if (self.leader is None) and (self.current_role == 'FOLLOWER'):
            self.current_role = 'CANDIDATE'
            self.requestVotes()

    def requestVotes(self):
        log_term = 1
        if len(self.local_log) > 0:
            log_term = self.local_log[len(self.local_log) - 1].getTerm()
        self.current_term += 1
        vote_request = {'Type': 'LEADER_ELECTION', 'ID': self.candidateID, 'Term': self.current_term,
                        'LogIndex': len(self.local_log), 'LogTerm': log_term}
        message = pickle.dumps(vote_request)
        for sock in self.message_sockets:
            sock.sendall(bytes(message))

    def appendEntries(self):
        if self.current_node is None:
            self.current_node = Node(self.current_term)
        for i in self.transactions_log:
            new_entry = Log(len(self.local_log) + 1, self.current_term, i)
            self.local_log.append(new_entry)
            self.transactions_log.remove(i)

    def addBlockchain(self, transaction):
        if self.current_node.numTx < 3:
            self.current_node.addTx(transaction)
        else:
            self.blockchain.addBlock(self.current_node)
            self.current_node = Node(self.current_term)

    def sendHeartbeat(self):
        while self.leader == self.candidateID:
            time.sleep(self.timeout - 1)
            send_heartbeat = {'Type': 'HEARTBEAT', 'ID': self.candidateID, 'Term': self.current_term}
            message = pickle.dumps(send_heartbeat)
            for sock in self.message_sockets:
                sock.sendall(bytes(message))

    def listenTransaction(self, connection, address):
        heartbeat_time = 0
        while True:
            msg = connection.recv(1024)
            x = pickle.loads(msg)

            if datetime.datetime.now() > (heartbeat_time + datetime.timedelta(seconds=(self.timeout))):
                self.heartbeat_received = 0

            if x['Type'] == 'LEADER_ELECTION':
                if self.current_term < x['Term']:
                    self.current_role = 'FOLLOWER'
                    send_vote = {'Type': 'VOTE_LEADER', 'Term': x['Term']}
                    message = pickle.dumps(send_vote)
                    connection.sendall(bytes(message))

            elif x['Type'] == 'VOTE_LEADER':
                if x['Term'] == self.current_term:
                    self.majority_votes += 1
                    if self.majority_votes == 2:
                        self.majority_votes = 1
                        self.current_role = 'LEADER'
                        send_heartbeat = threading.Thread(target=self.sendHeartbeat)
                        send_heartbeat.start()
                        append_entries = threading.Thread(target=self.appendEntries)
                        append_entries.start()
                        new_leader = {'Type': 'NEW_LEADER', 'ID': self.candidateID, 'Term': self.current_term}
                        message = pickle.dumps(new_leader)
                        for sock in self.message_sockets:
                            sock.sendall(bytes(message))

            elif x['Type'] == 'NEW_LEADER':
                self.current_term = x['Term']
                self.leader = x['ID']
                self.current_role = 'FOLLOWER'

            elif x['Type'] == 'ACCEPT_MESSAGE':
                pass

            elif x['Type'] == 'COMMIT_MESSAGE':
                trans = {'Sender': x['Sender'], 'Receiver': x['Receiver'], 'Amount': x['Amount']}
                self.addBlockchain(trans)

            elif x['Type'] == 'HEARTBEAT':
                heartbeat_time = datetime.datetime.now()
                self.heartbeat_received = 1

            elif x['Type'] == 'CLIENT_MESSAGE':
                if self.leader == self.candidateID:
                    if x['Transaction'] == 'T':
                        new_transaction = {'Sender': x['Sender'], 'Receiver': x['Receiver'], 'Amount': x['Amount']}
                        self.transactions_log.append(new_transaction)
                    elif x['Transaction'] == 'B':
                        message = self.balance_table[x['Client']]
                        connection.sendall(bytes(message))


if __name__ == '__main__':
    PORT = sys.argv[1]
    logging.basicConfig(filename='Server' + str(PORT) + '.log', level=logging.DEBUG, filemode='w')
    serv = Server(PORT)
    serv.startNetwork()
    serv.beginRAFT()
