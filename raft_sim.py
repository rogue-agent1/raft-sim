#!/usr/bin/env python3
"""raft_sim - Raft consensus protocol simulator."""
import random, sys, argparse, json, time
from enum import Enum

class State(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class LogEntry:
    def __init__(self, term, command):
        self.term = term; self.command = command
    def to_dict(self):
        return {"term": self.term, "command": self.command}

class RaftNode:
    def __init__(self, node_id, peers):
        self.id = node_id; self.peers = peers; self.state = State.FOLLOWER
        self.term = 0; self.voted_for = None; self.log = []
        self.commit_index = -1; self.last_applied = -1
        self.next_index = {}; self.match_index = {}
        self.votes_received = set(); self.leader_id = None
        self.election_timeout = random.randint(150, 300)
        self.elapsed = 0

    def start_election(self):
        self.state = State.CANDIDATE; self.term += 1
        self.voted_for = self.id; self.votes_received = {self.id}
        self.election_timeout = random.randint(150, 300); self.elapsed = 0
        return [{"type": "RequestVote", "from": self.id, "to": p,
                 "term": self.term, "last_log_index": len(self.log)-1,
                 "last_log_term": self.log[-1].term if self.log else 0} for p in self.peers]

    def handle_vote_request(self, msg):
        if msg["term"] > self.term:
            self.term = msg["term"]; self.state = State.FOLLOWER; self.voted_for = None
        grant = False
        if msg["term"] >= self.term and self.voted_for in (None, msg["from"]):
            my_last_term = self.log[-1].term if self.log else 0
            my_last_idx = len(self.log) - 1
            if msg["last_log_term"] > my_last_term or (msg["last_log_term"] == my_last_term and msg["last_log_index"] >= my_last_idx):
                grant = True; self.voted_for = msg["from"]; self.elapsed = 0
        return {"type": "VoteResponse", "from": self.id, "to": msg["from"], "term": self.term, "granted": grant}

    def handle_vote_response(self, msg):
        if msg["term"] > self.term:
            self.term = msg["term"]; self.state = State.FOLLOWER; return []
        if self.state != State.CANDIDATE: return []
        if msg["granted"]:
            self.votes_received.add(msg["from"])
            if len(self.votes_received) > (len(self.peers) + 1) // 2:
                self.state = State.LEADER; self.leader_id = self.id
                for p in self.peers:
                    self.next_index[p] = len(self.log)
                    self.match_index[p] = -1
                return self._send_heartbeats()
        return []

    def _send_heartbeats(self):
        msgs = []
        for p in self.peers:
            ni = self.next_index.get(p, len(self.log))
            prev_idx = ni - 1
            prev_term = self.log[prev_idx].term if prev_idx >= 0 and prev_idx < len(self.log) else 0
            entries = [e.to_dict() for e in self.log[ni:]]
            msgs.append({"type": "AppendEntries", "from": self.id, "to": p, "term": self.term,
                         "prev_log_index": prev_idx, "prev_log_term": prev_term,
                         "entries": entries, "leader_commit": self.commit_index})
        return msgs

    def handle_append_entries(self, msg):
        if msg["term"] >= self.term:
            self.term = msg["term"]; self.state = State.FOLLOWER
            self.leader_id = msg["from"]; self.elapsed = 0
        if msg["term"] < self.term:
            return {"type": "AppendResponse", "from": self.id, "to": msg["from"], "term": self.term, "success": False, "match_index": -1}
        if msg["prev_log_index"] >= 0:
            if msg["prev_log_index"] >= len(self.log) or self.log[msg["prev_log_index"]].term != msg["prev_log_term"]:
                return {"type": "AppendResponse", "from": self.id, "to": msg["from"], "term": self.term, "success": False, "match_index": -1}
        for i, e in enumerate(msg["entries"]):
            idx = msg["prev_log_index"] + 1 + i
            if idx < len(self.log): self.log[idx] = LogEntry(e["term"], e["command"])
            else: self.log.append(LogEntry(e["term"], e["command"]))
        if msg["leader_commit"] > self.commit_index:
            self.commit_index = min(msg["leader_commit"], len(self.log) - 1)
        return {"type": "AppendResponse", "from": self.id, "to": msg["from"], "term": self.term, "success": True, "match_index": msg["prev_log_index"] + len(msg["entries"])}

    def client_request(self, command):
        if self.state != State.LEADER: return False
        self.log.append(LogEntry(self.term, command))
        return True

    def status(self):
        return {"id": self.id, "state": self.state.value, "term": self.term, "log_len": len(self.log),
                "commit_index": self.commit_index, "leader": self.leader_id}

class RaftCluster:
    def __init__(self, n=5):
        ids = [f"node_{i}" for i in range(n)]
        self.nodes = {nid: RaftNode(nid, [p for p in ids if p != nid]) for nid in ids}
        self.messages = []

    def tick(self, ms=50):
        new_msgs = []
        for node in self.nodes.values():
            node.elapsed += ms
            if node.state != State.LEADER and node.elapsed >= node.election_timeout:
                new_msgs.extend(node.start_election())
            elif node.state == State.LEADER:
                new_msgs.extend(node._send_heartbeats())
        self.messages.extend(new_msgs)

    def deliver(self, drop_rate=0.0):
        delivered = 0
        pending = list(self.messages); self.messages = []
        for msg in pending:
            if random.random() < drop_rate: continue
            target = self.nodes.get(msg["to"])
            if not target: continue
            if msg["type"] == "RequestVote":
                resp = target.handle_vote_request(msg)
                self.messages.append(resp)
            elif msg["type"] == "VoteResponse":
                new = target.handle_vote_response(msg)
                self.messages.extend(new)
            elif msg["type"] == "AppendEntries":
                resp = target.handle_append_entries(msg)
                self.messages.append(resp)
            delivered += 1
        return delivered

    def run(self, ticks=20, drop_rate=0.0):
        for t in range(ticks):
            self.tick()
            self.deliver(drop_rate)
        return self.status()

    def status(self):
        return {nid: n.status() for nid, n in self.nodes.items()}

def main():
    p = argparse.ArgumentParser(description="Raft consensus simulator")
    p.add_argument("-n", "--nodes", type=int, default=5)
    p.add_argument("-t", "--ticks", type=int, default=20)
    p.add_argument("--drop-rate", type=float, default=0.0)
    p.add_argument("--requests", type=int, default=5)
    args = p.parse_args()
    cluster = RaftCluster(args.nodes)
    cluster.run(10)
    leader = None
    for nid, n in cluster.nodes.items():
        if n.state == State.LEADER: leader = n; break
    if leader:
        print(f"Leader elected: {leader.id} (term {leader.term})")
        for i in range(args.requests):
            leader.client_request(f"SET x={i}")
        cluster.run(args.ticks, args.drop_rate)
    print(json.dumps(cluster.status(), indent=2))

if __name__ == "__main__":
    main()
