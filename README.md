The bank has three servers to keep track of all transactions made by clients. It uses Raft as its underlying consensus protocol to keep an updated transaction history on all of its servers to ensure proper blockchain replication, and at the same time, to ensure fault tolerance from server crash failures and the network partition failures

Since Raft is a leader-based approach to consensus, we decompose this project into three parts.
• Leader Election: Following the Raft leader election protocol, there will be exactly one leader, and two followers after the leader election stage.
• Normal Operations: Maintaining the blockchain by generating Hash with SHA256 encryption, leader handles messages to servers and commits the transcation when majority acknowledgements are received from followers.
• Fault Tolerance: Node Failure, network partition and node recovery


