import socket
import threading
import logging
import pickle
import time
import datetime
import hashlib
import json
import sys

# macros
CONFIG_FILE = 'config.json'
SERVER = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
INSUFFICIENT_FUNDS = 'INSUFFICIENT FUNDS'
SUCCESS = 'TRANSFER SUCCESSFUL'
CLIENTS = {'A': '6000', 'B': '6001', 'C': '6002'}
LEADER_CHANGE = 'LEADER_CHANGE'


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
        if self.numTx > 2:
            while self.nonce < 50:
                value = self.validatePhash(self.tx, self.nonce)

    def checkNonce(self):
        if self.validatePhash(self.tx, self.nonce):
            return 1
        else:
            return 0

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
            self.nonce = nonce + 1
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

    def writeChain(self):
        write_chain = []
        for i in range(len(self.chain)):
            write_chain.append({'Term': self.chain[i].term, 'Phash': self.chain[i].prevhash,
                                'Nonce': self.chain[i].nonce, 'Tx': self.chain[i].tx, 'numTx': self.chain[i].numTx})
        return write_chain

    def readChain(self, write_chain):
        for i in range(len(write_chain)):
            node = Node(write_chain[i]['Term'])
            node.prevhash = write_chain[i]['Phash']
            node.nonce = write_chain[i]['Nonce']
            node.tx = write_chain[i]['Tx']
            node.numTx = write_chain[i]['numTx']
            self.chain.append(node)


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
        self.hb_sockets = {}
        self.hbcheck = 0
        self.heartbeat_time = datetime.datetime.now()

        self.leader = None
        self.current_node = None
        self.candidateID = str(port)
        self.timeout = 10
        self.election_timeout = 5 + 3 * (port % 5050)
        self.current_role = 'FOLLOWER'
        self.transactions_log = []
        self.blockchain = Blockchain()
        self.balance_table = {}
        self.raddr = {}
        self.leader_raddr = None

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

            self.blockchain.index = self.config['Blockchain' + self.candidateID]['Index']
            # TODO Read block chain data from config file and create array of Nodes
            self.blockchain.readChain(self.config['Blockchain' + str(port)]['Chain'])
            self.blockchain.prevPhash = self.config['Blockchain' + self.candidateID]['Prevhash']

            self.transactions_log = self.config['Transaction_log']
            file.close()

        for i in self.servers:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.connect_ex((SERVER, i))
                self.message_sockets[str(i)] = sock
                host, add = sock.getsockname()
                self.raddr[str(i)] = add
            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))
        print(self.raddr)
        with open(CONFIG_FILE, 'w') as file:
            self.config['raddr'+self.candidateID] = self.raddr
            json.dump(self.config, file)
            file.close()

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
            logging.debug("[CLIENT CONNECTED] {} {}".format(str(connection), str(address)))

            listen_transactions = threading.Thread(target=self.listenTransaction, args=(connection, address))
            listen_transactions.start()

    def beginRAFT(self):
        time.sleep(self.election_timeout)
        if (self.leader is None) and (self.current_role == 'FOLLOWER') and (self.current_phase == 0):
            logging.debug("[PHASE 1] Candidate {} contesting for election".format(self.candidateID))
            self.current_role = 'CANDIDATE'
            self.requestVotes()

    def requestVotes(self):
        log_term = self.current_term
        self.current_term += 1
        self.phase1_votes = 1
        vote_request = {'Type': 'LEADER_ELECTION', 'ID': self.candidateID, 'Term': self.current_term,
                        'LogIndex': self.blockchain.index, 'LogTerm': log_term}
        message = pickle.dumps(vote_request)
        for sock in self.message_sockets.values():
            sock.sendall(bytes(message))

    def updateBlockchain(self):
        while True:
            if len(self.transactions_log) > 0:
                print(self.transactions_log)
                if self.current_node.numTx < 2:
                    if self.transactions_log[0]['Type'] == 'T':
                        self.current_node.addTx(self.transactions_log[0]['S'] + ' ' +
                                                self.transactions_log[0]['R'] + ' ' +
                                                str(self.transactions_log[0]['A']))
                    elif self.transactions_log[0]['Type'] == 'B':
                        self.current_node.addTx(self.transactions_log[0]['S'])
                    self.transactions_log.remove(self.transactions_log[0])
                else:
                    self.current_node = Node(self.current_term)

            if (len(self.transactions_log) == 0) and (self.current_node.numTx > 0):
                if self.current_node.checkNonce():
                    add_transaction = {'Type': 'ADD_TRANSACTION', 'LeaderID': self.candidateID,
                                       'Term': self.current_term, 'Node': self.current_node}
                    message = pickle.dumps(add_transaction)
                    self.phase2_votes = 1

                    self.sendServers(message)

                    if self.leader == self.candidateID:
                        with open(CONFIG_FILE, 'r') as file:
                            self.config = json.load(file)
                            file.close()
                        with open(CONFIG_FILE, 'w') as file:
                            self.config['Balance'] = self.balance_table
                            self.config['Transaction_log'] = self.transactions_log
                            self.config['Current_node']['Term'] = self.current_node.term
                            self.config['Current_node']['Nonce'] = self.current_node.nonce
                            self.config['Current_node']['Tx'] = self.current_node.tx
                            self.config['Current_node']['Hash'] = self.current_node.prevhash
                            self.config['Current_node']['NumTx'] = self.current_node.numTx
                            json.dump(self.config, file)
                            file.close()

                    self.current_node = Node(self.current_term)

    def updateBalanceTable(self, block):
        for i in range(block.numTx):
            x = block.tx[i].split(' ')
            if len(x) > 1:
                self.balance_table[x[0]] -= int(x[2])
                self.balance_table[x[1]] += int(x[2])

                if self.leader == self.candidateID:
                    self.sendClient(int(CLIENTS[x[0]]), SUCCESS)

    def sendClient(self, id, message):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            client.connect((SERVER, id))
            client.sendall(message.encode(FORMAT))
        except socket.error as exc:
            logging.debug("[EXCEPTION] {}".format(exc))
        client.close()

    def sendServers(self, message):
        for port in list(self.message_sockets):
            try:
                self.message_sockets[port].sendall(bytes(message))
            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))
                self.message_sockets[port].close()
                try:
                    del self.message_sockets[port]
                except KeyError as exc:
                    pass
            time.sleep(3)

    def sendHeartbeat(self):
        for i in self.servers:
            hbsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            hbsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                hbsock.connect_ex((SERVER, i))
                self.hb_sockets[str(i)] = hbsock
            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))

        while self.leader == self.candidateID:
            time.sleep(self.timeout)
            send_heartbeat = {'Type': 'HEARTBEAT', 'LeaderID': self.candidateID, 'Term': self.current_term}
            message = pickle.dumps(send_heartbeat)
            for port in list(self.hb_sockets):
                try:
                    self.hb_sockets[port].sendall(bytes(message))
                except socket.error as exc:
                    logging.debug("[EXCEPTION] {}".format(exc))
                    self.hb_sockets[port].close()
                    try:
                        del self.hb_sockets[port]
                    except KeyError as exc:
                        pass

    def listenTransaction(self, connection, address):
        while True:
            try:
                if (datetime.datetime.now() > (
                        self.heartbeat_time + datetime.timedelta(seconds=(1.5 * self.timeout)))) and (
                        self.hbcheck == 1):
                    # check if connection had leader_raddr
                    try:
                        message = {'AVAILABLE': 1}
                        y = pickle.dumps(message)
                        connection.sendall(bytes(message))
                    except socket.error as exc:
                        print(connection)
                        print("Beginning raft")
                        connection.close()
                        self.leader = None
                        self.current_phase = 0
                        self.hbcheck = 0
                        self.beginRAFT()

                msg = connection.recv(1024)
                if msg:
                    x = pickle.loads(msg)
                    logging.debug("[MESSAGE] {}".format(x))

                    if x['Type'] == 'LEADER_ELECTION':
                        if (self.current_term <= x['Term']) and (self.vote_casted == 0):
                            self.current_phase = 1
                            self.current_role = 'FOLLOWER'
                            self.current_term = x['Term']
                            self.vote_casted = 1
                            send_vote = {'Type': 'VOTE_RECEIVED', 'ID': self.candidateID, 'Term': x['Term']}
                            message = pickle.dumps(send_vote)
                            try:
                                self.message_sockets[x['ID']].sendall(bytes(message))
                            except socket.error as exc:
                                logging.debug("[EXCEPTION] {}".format(exc))
                                self.message_sockets[x['ID']].close()
                                del self.message_sockets[x['ID']]
                            time.sleep(2)

                    elif x['Type'] == 'VOTE_RECEIVED':
                        print(x['Term'], self.current_term, self.leader)
                        if (x['Term'] == self.current_term) and (self.leader is None):
                            self.phase1_votes += 1
                            if self.phase1_votes == 2:
                                self.phase1_votes = 0
                                self.current_role = 'LEADER'
                                self.leader = self.candidateID
                                self.current_phase = 2
                                with open(CONFIG_FILE, 'r') as file:
                                    self.config = json.load(file)
                                    file.close()
                                with open(CONFIG_FILE, 'w') as file:
                                    self.config['Leader'] = self.candidateID
                                    self.config['State_variables']['Current_phase'] = self.current_phase
                                    self.config['State_variables']['Current_term'] = self.current_term
                                    json.dump(self.config, file)
                                    file.close()

                                send_heartbeat = threading.Thread(target=self.sendHeartbeat)
                                send_heartbeat.start()
                                append_entries = threading.Thread(target=self.updateBlockchain)
                                append_entries.start()
                                new_leader = {'Type': 'NEW_LEADER', 'LeaderID': self.candidateID,
                                              'Term': self.current_term}
                                message = pickle.dumps(new_leader)
                                self.sendServers(message)
                                self.sendClient(6000, 'NEW_LEADER ' + self.leader)

                    elif x['Type'] == 'NEW_LEADER':
                        self.current_term = x['Term']
                        self.leader = x['LeaderID']
                        self.current_role = 'FOLLOWER'
                        self.vote_casted = 0
                        self.leader_raddr = self.config['raddr' + self.leader][self.candidateID]

                    elif x['Type'] == 'CLIENT_MESSAGE':
                        if self.leader == self.candidateID:
                            if x['Transaction'] == 'T':
                                if self.balance_table[x['S']] >= x['A']:
                                    new_transaction = {'Type': 'T', 'S': x['S'], 'R': x['R'], 'A': x['A']}
                                    self.transactions_log.append(new_transaction)
                                    with open(CONFIG_FILE, 'r') as file:
                                        self.config = json.load(file)
                                        file.close()
                                    with open(CONFIG_FILE, 'w') as file:
                                        self.config['Transaction_log'] = self.transactions_log
                                        json.dump(self.config, file)
                                        file.close()
                                else:
                                    self.sendClient(int(CLIENTS[x['S']]), INSUFFICIENT_FUNDS)
                            elif x['Transaction'] == 'B':
                                new_transaction = {'Type': 'B', 'S': x['S']}
                                self.transactions_log.append(new_transaction)
                                message = self.balance_table[x['S']]
                                print(message)
                                self.sendClient(int(CLIENTS[x['S']]), str(message))
                        elif (self.leader != self.candidateID) and (self.leader is not None):
                            message = pickle.dumps(x)
                            self.message_sockets[self.leader].sendall(bytes(message))
                            self.sendClient(int(CLIENTS[x['S']]), LEADER_CHANGE)
                            time.sleep(2)

                    elif x['Type'] == 'ADD_TRANSACTION':
                        if x['Term'] == self.current_term:
                            self.current_node = x['Node']
                            added_transaction = {'Type': 'ADDED_TRANSACTION', 'ID': self.candidateID,
                                                 'Term': self.current_term, 'Node': x['Node']}
                            message = pickle.dumps(added_transaction)
                            try:
                                self.message_sockets[self.leader].sendall(bytes(message))
                            except socket.error as exc:
                                logging.debug("[EXCEPTION] {}".format(exc))
                                del self.message_sockets[self.leader]
                            time.sleep(2)

                    elif x['Type'] == 'ADDED_TRANSACTION':
                        if x['Term'] == self.current_term:
                            self.phase2_votes += 1
                            if self.phase2_votes == 2:
                                self.phase2_votes = 0
                                self.blockchain.addBlock(x['Node'])
                                self.updateBalanceTable(x['Node'])
                                print(self.balance_table)
                                with open(CONFIG_FILE, 'r') as file:
                                    self.config = json.load(file)
                                    file.close()
                                with open(CONFIG_FILE, 'w') as file:
                                    self.config['Blockchain' + self.candidateID]['Index'] = self.blockchain.index
                                    self.config['Blockchain' + self.candidateID]['Chain'] = self.blockchain.writeChain()
                                    self.config['Blockchain' + self.candidateID]['Prevhash'] = self.blockchain.prevPhash
                                    json.dump(self.config, file)
                                    file.close()

                                commit_transaction = {'Type': 'COMMIT_TRANSACTION', 'ID': self.candidateID,
                                                      'Term': self.current_term, 'Node': x['Node']}
                                message = pickle.dumps(commit_transaction)
                                self.sendServers(message)

                    elif x['Type'] == 'COMMIT_TRANSACTION':
                        if x['Term'] == self.current_term:
                            self.blockchain.addBlock(x['Node'])
                            self.updateBalanceTable(x['Node'])
                            with open(CONFIG_FILE, 'r') as file:
                                self.config = json.load(file)
                                file.close()
                            with open(CONFIG_FILE, 'w') as file:
                                # check
                                print('Blockchain' + self.candidateID)
                                self.config['Blockchain' + self.candidateID]['Index'] = self.blockchain.index
                                self.config['Blockchain' + self.candidateID]['Chain'] = self.blockchain.writeChain()
                                self.config['Blockchain' + self.candidateID]['Prevhash'] = self.blockchain.prevPhash
                                json.dump(self.config, file)
                                file.close()
                            print(self.balance_table)

                    elif x['Type'] == 'HEARTBEAT':
                        self.heartbeat_time = datetime.datetime.now()
                        self.hbcheck = 1

            except socket.error as exc:
                logging.debug("[EXCEPTION] {}".format(exc))


if __name__ == '__main__':
    PORT = sys.argv[1]
    logging.basicConfig(filename='Server' + str(PORT) + '.log', level=logging.DEBUG, filemode='w')
    serv = Server(int(PORT))
    serv.startNetwork()
