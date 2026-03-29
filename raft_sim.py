#!/usr/bin/env python3
"""raft_sim - Simplified Raft consensus simulation."""
import sys, random, time

class LogEntry:
    def __init__(self, term, command):
        self.term = term
        self.command = command

class RaftNode:
    FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"
    
    def __init__(self, node_id, peers):
        self.id = node_id
        self.peers = peers
        self.state = self.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.leader_id = None
        self.votes_received = set()
        self.next_index = {}
        self.match_index = {}
    
    def start_election(self):
        self.state = self.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.votes_received = {self.id}
        return {"type": "request_vote", "term": self.current_term,
                "candidate_id": self.id, "last_log_index": len(self.log) - 1,
                "last_log_term": self.log[-1].term if self.log else 0}
    
    def handle_vote_request(self, msg):
        if msg["term"] > self.current_term:
            self.current_term = msg["term"]
            self.state = self.FOLLOWER
            self.voted_for = None
        
        grant = False
        if msg["term"] >= self.current_term and self.voted_for in (None, msg["candidate_id"]):
            my_last_term = self.log[-1].term if self.log else 0
            my_last_idx = len(self.log) - 1
            if msg["last_log_term"] > my_last_term or \
               (msg["last_log_term"] == my_last_term and msg["last_log_index"] >= my_last_idx):
                grant = True
                self.voted_for = msg["candidate_id"]
        
        return {"type": "vote_response", "term": self.current_term,
                "voter_id": self.id, "granted": grant}
    
    def handle_vote_response(self, msg):
        if msg["granted"]:
            self.votes_received.add(msg["voter_id"])
        if len(self.votes_received) > (len(self.peers) + 1) / 2:
            self.state = self.LEADER
            self.leader_id = self.id
            for p in self.peers:
                self.next_index[p] = len(self.log)
                self.match_index[p] = -1
            return True
        return False
    
    def append_entry(self, command):
        if self.state != self.LEADER:
            return False
        self.log.append(LogEntry(self.current_term, command))
        return True
    
    def create_append_entries(self, peer_id):
        prev_idx = self.next_index.get(peer_id, 0) - 1
        prev_term = self.log[prev_idx].term if prev_idx >= 0 and prev_idx < len(self.log) else 0
        entries = self.log[self.next_index.get(peer_id, 0):]
        return {"type": "append_entries", "term": self.current_term,
                "leader_id": self.id, "prev_log_index": prev_idx,
                "prev_log_term": prev_term,
                "entries": [(e.term, e.command) for e in entries],
                "leader_commit": self.commit_index}
    
    def handle_append_entries(self, msg):
        if msg["term"] >= self.current_term:
            self.current_term = msg["term"]
            self.state = self.FOLLOWER
            self.leader_id = msg["leader_id"]
        
        if msg["term"] < self.current_term:
            return {"type": "append_response", "term": self.current_term,
                    "success": False, "node_id": self.id}
        
        # Append entries
        for term, cmd in msg["entries"]:
            self.log.append(LogEntry(term, cmd))
        
        if msg["leader_commit"] > self.commit_index:
            self.commit_index = min(msg["leader_commit"], len(self.log) - 1)
        
        return {"type": "append_response", "term": self.current_term,
                "success": True, "node_id": self.id,
                "match_index": len(self.log) - 1}

def test():
    # Create 3-node cluster
    nodes = {i: RaftNode(i, [j for j in range(3) if j != i]) for i in range(3)}
    
    # Node 0 starts election
    vote_req = nodes[0].start_election()
    assert nodes[0].state == "candidate"
    assert nodes[0].current_term == 1
    
    # Collect votes
    for i in [1, 2]:
        resp = nodes[i].handle_vote_request(vote_req)
        assert resp["granted"]
        nodes[0].handle_vote_response(resp)
    
    assert nodes[0].state == "leader"
    
    # Leader appends entries
    assert nodes[0].append_entry("SET x=1")
    assert nodes[0].append_entry("SET y=2")
    assert len(nodes[0].log) == 2
    
    # Replicate to followers
    for peer_id in [1, 2]:
        ae = nodes[0].create_append_entries(peer_id)
        resp = nodes[peer_id].handle_append_entries(ae)
        assert resp["success"]
        assert len(nodes[peer_id].log) == 2
    
    # Follower rejects stale term
    nodes[1].current_term = 5
    ae = nodes[0].create_append_entries(1)
    resp = nodes[1].handle_append_entries(ae)
    # Term 1 < 5, rejected
    assert not resp["success"]
    
    print("All tests passed!")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: raft_sim.py test")
