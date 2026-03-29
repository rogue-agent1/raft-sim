#!/usr/bin/env python3
"""Raft consensus — leader election and log replication simulator."""
import random, sys, enum

class State(enum.Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class RaftNode:
    def __init__(self, nid, cluster_size):
        self.id = nid
        self.cluster_size = cluster_size
        self.state = State.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.timeout = random.randint(150, 300)
        self.votes = 0
    def start_election(self):
        self.state = State.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes = 1
    def receive_vote(self):
        self.votes += 1
        if self.votes > self.cluster_size // 2:
            self.state = State.LEADER
            return True
        return False
    def append_entry(self, entry):
        self.log.append((self.term, entry))
        return len(self.log) - 1
    def __repr__(self):
        return f"Node({self.id}, {self.state.value}, term={self.term}, log={len(self.log)})"

def simulate(n=5, rounds=20):
    nodes = [RaftNode(i, n) for i in range(n)]
    print(f"Raft cluster: {n} nodes, {rounds} rounds\n")
    for r in range(rounds):
        # Random timeout triggers election
        if all(n.state != State.LEADER for n in nodes):
            candidate = random.choice(nodes)
            candidate.start_election()
            print(f"Round {r}: Node {candidate.id} starts election (term {candidate.term})")
            for node in nodes:
                if node.id != candidate.id and node.term <= candidate.term:
                    if node.voted_for is None or node.voted_for == candidate.id:
                        node.voted_for = candidate.id
                        node.term = candidate.term
                        if candidate.receive_vote():
                            print(f"  Node {candidate.id} elected leader!")
                            break
        else:
            leader = next(n for n in nodes if n.state == State.LEADER)
            entry = f"cmd-{r}"
            idx = leader.append_entry(entry)
            acks = 1
            for node in nodes:
                if node.id != leader.id and random.random() > 0.1:
                    node.log.append((leader.term, entry))
                    node.term = leader.term
                    acks += 1
            if acks > n // 2:
                leader.commit_index = idx
                print(f"Round {r}: Leader {leader.id} committed '{entry}' (acks={acks})")
            if random.random() < 0.05:
                print(f"Round {r}: Leader {leader.id} crashed!")
                leader.state = State.FOLLOWER
                leader.voted_for = None
                for node in nodes: node.voted_for = None
    print(f"\nFinal state:")
    for node in nodes: print(f"  {node}")

if __name__ == "__main__":
    simulate(int(sys.argv[1]) if len(sys.argv) > 1 else 5)
