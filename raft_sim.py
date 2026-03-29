#!/usr/bin/env python3
"""Raft Consensus - Simulate leader election and log replication."""
import sys, random

class Node:
    def __init__(self, id):
        self.id=id;self.term=0;self.state="follower";self.voted_for=None
        self.log=[];self.commit_idx=0;self.timeout=random.randint(150,300)
        self.votes=0;self.leader=None
    def __repr__(self): return f"Node({self.id}, {self.state}, term={self.term})"

class RaftSim:
    def __init__(self, n=5):
        self.nodes=[Node(i) for i in range(n)];self.tick=0;self.messages=[]
    def step(self):
        self.tick += 1
        for node in self.nodes:
            if node.state == "leader": self._heartbeat(node)
            else:
                node.timeout -= 1
                if node.timeout <= 0: self._start_election(node)
        self._process_messages()
    def _start_election(self, node):
        node.term += 1; node.state = "candidate"; node.voted_for = node.id; node.votes = 1
        node.timeout = random.randint(150, 300)
        for other in self.nodes:
            if other.id != node.id:
                self.messages.append(("vote_req", node.id, other.id, node.term))
    def _heartbeat(self, leader):
        for node in self.nodes:
            if node.id != leader.id:
                node.timeout = random.randint(150, 300)
                node.leader = leader.id
    def _process_messages(self):
        msgs = self.messages[:]; self.messages = []
        for msg_type, src, dst, term in msgs:
            dst_node = self.nodes[dst]
            if msg_type == "vote_req":
                if term > dst_node.term and dst_node.voted_for in (None, src):
                    dst_node.term = term; dst_node.voted_for = src; dst_node.state = "follower"
                    self.messages.append(("vote_resp", dst, src, term))
            elif msg_type == "vote_resp":
                src_node = self.nodes[dst]
                if src_node.state == "candidate" and term == src_node.term:
                    src_node.votes += 1
                    if src_node.votes > len(self.nodes) // 2:
                        src_node.state = "leader"; src_node.leader = src_node.id
                        self._heartbeat(src_node)

def main():
    random.seed(42)
    sim = RaftSim(5)
    print("=== Raft Consensus Simulation ===\n")
    for _ in range(500): sim.step()
    leaders = [n for n in sim.nodes if n.state == "leader"]
    print(f"After 500 ticks:")
    for n in sim.nodes:
        print(f"  {n}, leader={n.leader}")
    if leaders:
        print(f"\nLeader: Node {leaders[0].id} (term {leaders[0].term})")
    sim.nodes[leaders[0].id if leaders else 0].state = "down"
    print(f"\nKilling leader...")
    for _ in range(500): sim.step()
    leaders2 = [n for n in sim.nodes if n.state == "leader"]
    if leaders2:
        print(f"New leader: Node {leaders2[0].id} (term {leaders2[0].term})")
    for n in sim.nodes: print(f"  {n}")

if __name__ == "__main__":
    main()
