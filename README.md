The bank has three servers to keep track of all transactions made by clients. It uses Raft as its underlying consensus protocol to keep an updated transaction history on all of its servers to ensure proper blockchain replication, and at the same time, to ensure fault tolerance from server crash failures and the network partition failures

Since Raft is a leader-based approach to consensus, we decompose this project into three parts.
• Leader Election
• Normal Operations
• Fault Tolerance
