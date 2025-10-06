#[macro_use]
extern crate text_io;

use std::cmp;
use std::collections::{LinkedList, VecDeque};
use std::sync::{Arc, Condvar, Mutex}; use std::thread;
use std::sync::atomic::{AtomicI32, Ordering};

struct Node {
    i: usize, // index (for debugging)
    e: AtomicI32,
    h: AtomicI32,
}

struct Edge {
    u: usize,
    v: usize,
    f: AtomicI32,
    c: i32,
}

impl Node {
    fn new(ii: usize) -> Node {
        Node { i: ii, e: AtomicI32::new(0), h: AtomicI32::new(0) }
    }
}

impl Edge {
    fn new(uu: usize, vv: usize, cc: i32) -> Edge {
        Edge { u: uu, v: vv, f: AtomicI32::new(0), c: cc }
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

    // fn dequeue(&self) -> Option<usize> {
    //     let mut st = self.state.lock().unwrap();
    //     loop {
    //         if st.shutdown {
    //             return None;
    //         }
    //         if let Some(u) = st.queue.pop_front() {
    //             st.active += 1;
    //             return Some(u);
    //         }
    //         st = self.cv.wait(st).unwrap();
    //     }
    // }

    fn dequeue_batch(&self, n: usize) -> Option<Vec<usize>> {
        let mut st = self.state.lock().unwrap();
        loop {
            if st.shutdown {
                return None;
            }
            if !st.queue.is_empty() {
                let mut batch = Vec::with_capacity(n);
                for _ in 0..n {
                    if let Some(u) = st.queue.pop_front() {
                        st.active += 1;
                        batch.push(u);
                    } else {
                        break;
                    }
                }
                return Some(batch);
            }
            st = self.cv.wait(st).unwrap();
        }
    }

    fn done_batch(&self, k: usize) {
        let mut st = self.state.lock().unwrap();
        st.active -= k;
        if st.active == 0 && st.queue.is_empty() {
            self.cv.notify_all();
        }
    }

    // fn done_one(&self) {
    //     let mut st = self.state.lock().unwrap();
    //     st.active -= 1;
    //     if st.active == 0 && st.queue.is_empty() {
    //         // wake the waiter in main when drained
    //         self.cv.notify_all();
    //     }
    // }

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
    let new_h = u.h.load(Ordering::SeqCst) + 1;
    u.h.store(new_h, Ordering::SeqCst);
    //u.h += 1;
    work.enqueue(u.i);
}

fn push(u: &mut Node, v: &mut Node, e: &mut Edge, work: &Arc<WorkQueue>) {
    let dir = if u.i == e.u { 1 } else { -1 };
    let u_excess = u.e.load(Ordering::SeqCst);
    if u_excess <= 0 { return; }

    let f_val = e.f.load(Ordering::SeqCst);
    
    let delta = if dir == 1 {
        e.c - f_val
    } else {
        e.c + f_val
    };

    let d = cmp::min(u_excess, delta);
    if d <= 0 {
        return;
    }

    // if dir == 1 {
    //     e.f.fetch_add(delta, Ordering::SeqCst);
    // } else {
    //     e.f.fetch_sub(delta, Ordering::SeqCst);
    // }

    println!("Push {} from {} to {}", d, u.i, v.i);

    let new_f = if dir == 1 { f_val + d } else { f_val - d };
    e.f.store(new_f, Ordering::SeqCst);

    let prev_u = u.e.fetch_sub(d, Ordering::SeqCst); // returns old value
    let prev_v = v.e.fetch_add(d, Ordering::SeqCst); // returns old value

    if prev_u > d {
        work.enqueue(u.i);
    }
    if prev_v == 0 {
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
    if u == 0 || u == node.len() - 1 {
        return;
    }

    let _g = graph_lock.lock().unwrap(); // global lock

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

        let fv = e_guard.f.load(Ordering::SeqCst);
        let residual = if b == 1 { e_guard.c - fv } else { e_guard.c + fv };
        let fh = from.h.load(Ordering::SeqCst);
        let th = node[v_idx].lock().unwrap().h.load(Ordering::SeqCst);

        if fh == th + 1 && residual > 0 {
            let mut to = node[v_idx].lock().unwrap();
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

    let s = 0;
    let t = n - 1;

    // shared work queue and global graph lock
    let work = Arc::new(WorkQueue::new());
    let graph_lock = Arc::new(Mutex::new(()));

    println!("initial pushes");

    // init source height to n and do initial pushes
    {
        let _g = graph_lock.lock().unwrap();

        node[s].lock().unwrap().h.store(n as i32, Ordering::SeqCst);

        // initial pushes along all edges from s
        for &ei in &adj[s] {
            let mut e_guard = edge[ei].lock().unwrap();

            let nbr = if e_guard.u == s {
                e_guard.v
            } else {
                e_guard.u
            };

            {
                let s_lock = node[s].lock().unwrap();
                s_lock.e.fetch_add(e_guard.c, Ordering::SeqCst);
                //s_lock.e += e_guard.c;
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
            let batch_size = 10;
            while let Some(batch) = work_cl.dequeue_batch(batch_size) {
                // handle batch of nodes
                for &u in &batch {
                    discharge(u, &node_cl, &edge_cl, &adj_cl[u], &work_cl, &gl_cl);
                }
                work_cl.done_batch(batch.len());
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

    let sink_excess = &node[t].lock().unwrap().e;
    println!("f = {}", sink_excess.load(Ordering::SeqCst));
}
