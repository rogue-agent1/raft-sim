#!/usr/bin/env python3
"""raft_sim - Raft consensus simulation (educational)."""
import argparse, random, json

class RaftNode:
    def __init__(self, id, peers):
        self.id = id; self.peers = peers
        self.state = "follower"; self.term = 0; self.voted_for = None
        self.log = []; self.commit_index = -1
        self.timeout = random.randint(150, 300)
        self.votes_received = 0; self.leader = None

    def start_election(self):
        self.state = "candidate"; self.term += 1
        self.voted_for = self.id; self.votes_received = 1
        return {"type": "RequestVote", "term": self.term, "candidate": self.id, "last_log": len(self.log) - 1}

    def handle_vote_request(self, msg):
        if msg["term"] > self.term:
            self.term = msg["term"]; self.state = "follower"; self.voted_for = None
        if msg["term"] == self.term and self.voted_for in (None, msg["candidate"]):
            self.voted_for = msg["candidate"]
            return {"type": "VoteGranted", "term": self.term, "voter": self.id}
        return {"type": "VoteDenied", "term": self.term, "voter": self.id}

    def handle_vote_response(self, msg, total_nodes):
        if msg["type"] == "VoteGranted":
            self.votes_received += 1
            if self.votes_received > total_nodes // 2:
                self.state = "leader"; self.leader = self.id
                return True
        return False

    def append_entry(self, entry):
        if self.state != "leader": return False
        self.log.append({"term": self.term, "data": entry}); return True

def simulate(n_nodes=5, rounds=20):
    nodes = {i: RaftNode(i, list(range(n_nodes))) for i in range(n_nodes)}
    log = []
    for r in range(rounds):
        # Random timeout triggers election
        candidate = random.choice(list(nodes.values()))
        if candidate.state != "leader":
            msg = candidate.start_election()
            log.append(f"Round {r}: Node {candidate.id} starts election (term {candidate.term})")
            votes = 1
            for pid, peer in nodes.items():
                if pid == candidate.id: continue
                resp = peer.handle_vote_request(msg)
                if resp["type"] == "VoteGranted": votes += 1
            if votes > n_nodes // 2:
                candidate.state = "leader"
                for n in nodes.values(): n.leader = candidate.id
                log.append(f"  Node {candidate.id} elected leader with {votes}/{n_nodes} votes")
                candidate.append_entry(f"entry_{r}")
    return log, {i: {"state": n.state, "term": n.term, "log_size": len(n.log)} for i, n in nodes.items()}

def main():
    p = argparse.ArgumentParser(description="Raft consensus simulation")
    p.add_argument("-n", "--nodes", type=int, default=5)
    p.add_argument("-r", "--rounds", type=int, default=20)
    args = p.parse_args()
    log, states = simulate(args.nodes, args.rounds)
    for entry in log: print(entry)
    print("\nFinal states:")
    print(json.dumps(states, indent=2))

if __name__ == "__main__":
    main()
