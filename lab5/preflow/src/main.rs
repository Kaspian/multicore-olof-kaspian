#[macro_use]
extern crate text_io;

use std::cmp;
use std::collections::{LinkedList, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

struct Node {
    i: usize, // index (for debugging)
    e: i32,   // excess preflow
    h: i32,   // height
}

struct Edge {
    u: usize,
    v: usize,
    f: i32,
    c: i32,
}

impl Node {
    fn new(ii: usize) -> Node {
        Node { i: ii, e: 0, h: 0 }
    }
}

impl Edge {
    fn new(uu: usize, vv: usize, cc: i32) -> Edge {
        Edge { u: uu, v: vv, f: 0, c: cc }
    }
}


struct WorkState {
    queue: VecDeque<usize>,
    active: usize,   // workers currently processing a node
    shutdown: bool,  // signal workers to exit
}

struct WorkQueue {
    state: Mutex<WorkState>,
    cv: Condvar,
}

impl WorkQueue {
    fn new() -> Self {
        Self {
            state: Mutex::new(WorkState {
                queue: VecDeque::new(),
                active: 0,
                shutdown: false,
            }),
            cv: Condvar::new(),
        }
    }

    fn enqueue(&self, u: usize) {
        let mut st = self.state.lock().unwrap();
        st.queue.push_back(u);
        self.cv.notify_one();
    }

    fn dequeue(&self) -> Option<usize> {
        let mut st = self.state.lock().unwrap();
        loop {
            if st.shutdown {
                return None;
            }
            if let Some(u) = st.queue.pop_front() {
                st.active += 1;
                return Some(u);
            }
            st = self.cv.wait(st).unwrap();
        }
    }

    fn done_one(&self) {
        let mut st = self.state.lock().unwrap();
        st.active -= 1;
        if st.active == 0 && st.queue.is_empty() {
            // wake the waiter in main when drained
            self.cv.notify_all();
        }
    }

    fn wait_for_drain(&self) {
        let mut st = self.state.lock().unwrap();
        while !(st.active == 0 && st.queue.is_empty()) {
            st = self.cv.wait(st).unwrap();
        }
    }

    fn shutdown(&self) {
        let mut st = self.state.lock().unwrap();
        st.shutdown = true;
        self.cv.notify_all();
    }
}

fn relabel(u: &mut Node, work: &Arc<WorkQueue>) {
    u.h += 1;
    work.enqueue(u.i);
}

fn push(u: &mut Node, v: &mut Node, e: &mut Edge, work: &Arc<WorkQueue>) {
    let d = if u.i == e.u {
        let d = cmp::min(u.e, e.c - e.f);
        e.f += d;
        d
    } else {
        let d = cmp::min(u.e, e.c + e.f);
        e.f -= d;
        d
    };

    println!("Push {} from {} to {}", d, u.i, v.i);

    u.e -= d;
    v.e += d;

    assert!(d >= 0);
    assert!(u.e >= 0);
    assert!(e.f.abs() <= e.c);

    if u.e > 0 {
        work.enqueue(u.i);
    }
    if v.e == d {
        work.enqueue(v.i);
    }
}

/// discharge one node `u`.
/// for simplicity & safety we take a global graph lock
/// so we never deadlock on (u, v, edge) mutexes.
fn discharge(
    u: usize,
    node: &Arc<Vec<Arc<Mutex<Node>>>>,
    edge: &Arc<Vec<Arc<Mutex<Edge>>>>,
    adj_edges: &LinkedList<usize>,
    work: &Arc<WorkQueue>,
    graph_lock: &Arc<Mutex<()>>,
) {
    let _g = graph_lock.lock().unwrap(); // global lock

    if u == 0 || u == node.len() - 1 {
        return;
    }

    let mut from = node[u].lock().unwrap();

    // try edge and push once
    for &ei in adj_edges.iter() {
        let mut e_guard = edge[ei].lock().unwrap();

        // determin neighbor and direction
        let (v_idx, b): (usize, i32) = if e_guard.u == u {
            (e_guard.v, 1)
        } else {
            (e_guard.u, -1)
        };

        let mut to = node[v_idx].lock().unwrap();

        if from.h > to.h && b * e_guard.f < e_guard.c {
            push(&mut from, &mut to, &mut e_guard, work);
            return;
        }
    }

    // no push possible -> relabel
    relabel(&mut from, work);
}


fn main() {
    let n: usize = read!();
    let m: usize = read!();
    let _c: usize = read!();
    let _p: usize = read!();

    println!("n = {}", n);
    println!("m = {}", m);

    let mut node_vec = Vec::with_capacity(n);
    let mut edge_vec = Vec::with_capacity(m);
    let mut adj: Vec<LinkedList<usize>> = Vec::with_capacity(n);

    for i in 0..n {
        node_vec.push(Arc::new(Mutex::new(Node::new(i))));
        adj.push(LinkedList::new());
    }

    for i in 0..m {
        let u: usize = read!();
        let v: usize = read!();
        let c: i32 = read!();
        edge_vec.push(Arc::new(Mutex::new(Edge::new(u, v, c))));
        adj[u].push_back(i);
        adj[v].push_back(i);
    }

    let node = Arc::new(node_vec);
    let edge = Arc::new(edge_vec);
    let adj = Arc::new(adj);

    let s = 0usize;
    let t = n - 1;

    // shared work queue and global graph lock
    let work = Arc::new(WorkQueue::new());
    let graph_lock = Arc::new(Mutex::new(()));

    println!("initial pushes");

    // init source height to n and do initial pushes
    {
        let _g = graph_lock.lock().unwrap();

        node[s].lock().unwrap().h = n as i32;

        // initial pushes along all edges from s
        for &ei in &adj[s] {
            let mut e_guard = edge[ei].lock().unwrap();

            let nbr = if e_guard.u == s {
                e_guard.v
            } else {
                e_guard.u
            };

            {
                let mut s_lock = node[s].lock().unwrap();
                s_lock.e += e_guard.c;
            }

            let mut s_lock = node[s].lock().unwrap();
            let mut u_lock = node[nbr].lock().unwrap();
            push(&mut s_lock, &mut u_lock, &mut e_guard, &work);
        }
    }

    // spawn workers
    let threads = 8;

    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let node_cl = Arc::clone(&node);
        let edge_cl = Arc::clone(&edge);
        let adj_cl = Arc::clone(&adj);
        let work_cl = Arc::clone(&work);
        let gl_cl = Arc::clone(&graph_lock);

        let h = thread::spawn(move || {
            while let Some(u) = work_cl.dequeue() {
                // process one node
                discharge(u, &node_cl, &edge_cl, &adj_cl[u], &work_cl, &gl_cl);
                work_cl.done_one();
            }
            // thread exits on shutdown
        });
        handles.push(h);
    }

    // wait until the work queue drains (no active workers queue empty)
    work.wait_for_drain();
    // tell workers to exit and join them
    work.shutdown();
    for h in handles {
        let _ = h.join();
    }

    let sink_excess = node[t].lock().unwrap().e;
    println!("f = {}", sink_excess);
}