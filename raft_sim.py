#!/usr/bin/env python3
"""Simplified Raft consensus protocol simulation."""
import sys, random

class RaftNode:
    def __init__(self, nid, peers):
        self.nid, self.peers = nid, peers
        self.state = "follower"  # follower/candidate/leader
        self.term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
    def start_election(self):
        self.term += 1
        self.state = "candidate"
        self.voted_for = self.nid
        return self.term
    def vote(self, candidate_id, term):
        if term > self.term:
            self.term = term
            self.voted_for = candidate_id
            self.state = "follower"
            return True
        if term == self.term and self.voted_for in (None, candidate_id):
            self.voted_for = candidate_id
            return True
        return False
    def become_leader(self):
        self.state = "leader"
    def append_entry(self, entry, term):
        self.log.append({"term": term, "data": entry})
    def commit(self, index):
        self.commit_index = index

def simulate_election(nodes):
    candidate = random.choice(nodes)
    term = candidate.start_election()
    votes = 1
    for node in nodes:
        if node.nid != candidate.nid:
            if node.vote(candidate.nid, term):
                votes += 1
    if votes > len(nodes) // 2:
        candidate.become_leader()
        return candidate
    return None

def test():
    nodes = [RaftNode(i, list(range(5))) for i in range(5)]
    random.seed(42)
    leader = simulate_election(nodes)
    assert leader is not None
    assert leader.state == "leader"
    assert sum(1 for n in nodes if n.voted_for == leader.nid) > 2
    leader.append_entry("x=1", leader.term)
    assert len(leader.log) == 1
    leader.commit(0)
    assert leader.commit_index == 0
    print("  raft_sim: ALL TESTS PASSED")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test": test()
    else: print("Raft consensus simulation")
