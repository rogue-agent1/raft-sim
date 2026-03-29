#!/usr/bin/env python3
"""raft_sim - Raft consensus protocol simulator."""
import random, argparse, enum

class Role(enum.Enum):
    FOLLOWER=0;CANDIDATE=1;LEADER=2

class LogEntry:
    def __init__(self, term, cmd): self.term,self.cmd=term,cmd

class RaftNode:
    def __init__(self, id, peers):
        self.id,self.peers=id,peers;self.role=Role.FOLLOWER;self.term=0
        self.voted_for=None;self.log=[];self.commit_idx=-1;self.votes=0
        self.leader=None;self.timeout=random.randint(5,15);self.timer=0
    def tick(self):
        self.timer+=1
        if self.role!=Role.LEADER and self.timer>=self.timeout: return self.start_election()
        if self.role==Role.LEADER: return self.send_heartbeat()
        return []
    def start_election(self):
        self.term+=1;self.role=Role.CANDIDATE;self.voted_for=self.id;self.votes=1
        self.timer=0;self.timeout=random.randint(5,15)
        return [("vote_req",self.id,p,self.term) for p in self.peers]
    def handle_vote_req(self, candidate, term):
        if term>self.term: self.term=term;self.role=Role.FOLLOWER;self.voted_for=None
        if term>=self.term and self.voted_for in (None,candidate):
            self.voted_for=candidate;self.timer=0
            return ("vote_resp",self.id,candidate,True)
        return ("vote_resp",self.id,candidate,False)
    def handle_vote_resp(self, granted, total_nodes):
        if granted: self.votes+=1
        if self.votes>total_nodes//2 and self.role==Role.CANDIDATE:
            self.role=Role.LEADER;self.leader=self.id;return True
        return False
    def send_heartbeat(self):
        self.timer=0
        return [("heartbeat",self.id,p,self.term) for p in self.peers]
    def handle_heartbeat(self, leader, term):
        if term>=self.term:
            self.term=term;self.role=Role.FOLLOWER;self.leader=leader;self.timer=0;self.voted_for=None

def simulate(n_nodes, ticks=50, seed=42):
    random.seed(seed)
    ids=list(range(n_nodes))
    nodes={i:RaftNode(i,[j for j in ids if j!=i]) for i in ids}
    log=[]
    for t in range(ticks):
        msgs=[]
        for nid,node in nodes.items(): msgs.extend(node.tick())
        for msg in msgs:
            if msg[0]=="vote_req":
                _,src,dst,term=msg
                resp=nodes[dst].handle_vote_req(src,term)
                if resp[3]: nodes[src].handle_vote_resp(True,n_nodes)
            elif msg[0]=="heartbeat":
                _,src,dst,term=msg
                nodes[dst].handle_heartbeat(src,term)
        leaders=[nid for nid,n in nodes.items() if n.role==Role.LEADER]
        if leaders and (not log or log[-1][1]!=leaders):
            log.append((t,leaders,nodes[leaders[0]].term))
    return nodes, log

def main():
    p=argparse.ArgumentParser(description="Raft consensus simulator")
    p.add_argument("-n",type=int,default=5);p.add_argument("-t","--ticks",type=int,default=50)
    p.add_argument("--seed",type=int,default=42)
    args=p.parse_args()
    nodes,log=simulate(args.n,args.ticks,args.seed)
    print(f"Raft simulation: {args.n} nodes, {args.ticks} ticks\n")
    for t,leaders,term in log: print(f"  Tick {t:3d}: Leader={leaders} Term={term}")
    print(f"\nFinal state:")
    for nid,n in sorted(nodes.items()):
        print(f"  Node {nid}: {n.role.name:10s} term={n.term} leader={n.leader}")

if __name__=="__main__":
    main()
