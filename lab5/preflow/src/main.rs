#[macro_use] extern crate text_io;

use std::sync::{Mutex,Arc};
use std::collections::LinkedList;
use std::cmp;
use std::thread;
use std::collections::VecDeque;

struct Node {
	i:	usize,			/* index of itself for debugging.	*/
	e:	i32,			/* excess preflow.			*/
	h:	i32,			/* height.				*/
}

struct Edge {
        u:      usize,  
        v:      usize,
        f:      i32,
        c:      i32,
}

impl Node {
	fn new(ii:usize) -> Node {
		Node { i: ii, e: 0, h: 0 }
	}

}

impl Edge {
        fn new(uu:usize, vv:usize,cc:i32) -> Edge {
                Edge { u: uu, v: vv, f: 0, c: cc }      
        }
}

fn relabel(u:&mut Node, excess_list:&Arc<Mutex<VecDeque<usize>>>) {
	u.h += 1;
	excess_list.lock().unwrap().push_back(u.i);
}

fn push(u:&mut Node, v:&mut Node, e:&mut Edge, excess_list:&Arc<Mutex<VecDeque<usize>>>) {

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
	
	// The following are always true.
    assert!(d >= 0);
    assert!(u.e >= 0);
    assert!(e.f.abs() <= e.c);

	if u.e > 0 {
		excess_list.lock().unwrap().push_back(u.i);
	}

	if v.e == d {
		excess_list.lock().unwrap().push_back(v.i);
	}
}

fn discharge(
	u: usize, 
	node: &Vec<Arc<Mutex<Node>>>, 
	edge: &Vec<Arc<Mutex<Edge>>>, 
	adj_edges: &LinkedList<usize>, 
	excess: &Arc<Mutex<VecDeque<usize>>>
) {
	let mut b: i32;

	if u == 0 || u == node.len() - 1 {
		return;
	}

	let mut from = node[u].lock().unwrap();

	for &e in adj_edges.iter() {
		let mut edge = edge[e].lock().unwrap();
		let mut to;

		if edge.u == u {
			to = node[edge.v].lock().unwrap();
			b = 1;
		} else {
			to = node[edge.u].lock().unwrap();
			b = -1;
		}

		if from.h > to.h && b * edge.f < edge.c {
			push(&mut from, &mut to, &mut edge, excess);
			return;
		}
	}

	relabel(&mut from, excess);
}


fn main() {

	let n: usize = read!();		/* n nodes.						*/
	let m: usize = read!();		/* m edges.						*/
	let _c: usize = read!();	/* underscore avoids warning about an unused variable.	*/
	let _p: usize = read!();	/* c and p are in the input from 6railwayplanning.	*/
	let mut node: Vec<Arc<Mutex<Node>>> = Vec::new();
	let mut edge: Vec<Arc<Mutex<Edge>>> = Vec::new();
	let mut adj: Vec<LinkedList<usize>> = Vec::with_capacity(n);
	let excess: Arc<Mutex<VecDeque<usize>>> = Arc::new(Mutex::new(VecDeque::new()));
	let debug = false;

	let s = 0;
	let _t = n-1;

	println!("n = {}", n);
	println!("m = {}", m);

	for i in 0..n {
		let u:Node = Node::new(i);
		node.push(Arc::new(Mutex::new(u))); 
		adj.push(LinkedList::new());
	}

	for i in 0..m {
		let u: usize = read!();
		let v: usize = read!();
		let c: i32 = read!();
		let e:Edge = Edge::new(u,v,c);
		adj[u].push_back(i);
		adj[v].push_back(i);
		edge.push(Arc::new(Mutex::new(e))); 
	}

	if debug {
		for i in 0..n {
			print!("adj[{}] = ", i);
			let iter = adj[i].iter();

			for e in iter {
				print!("e = {}, ", e);
			}
			println!("");
		}
	}

	println!("initial pushes");

	// Initialize source height to n
	node[s].lock().unwrap().h = n as i32;

	// do initial pushes
	for &e in adj[s].iter() {
		let mut e = edge[e].lock().unwrap();
		let mut u = if e.u == s {
			node[e.v].lock().unwrap()
		} else {
			node[e.u].lock().unwrap()
		};

		{
    		let mut s_lock = node[s].lock().unwrap();
    		s_lock.e += e.c;
		}

		push(&mut node[s].lock().unwrap(), &mut u, &mut e, &excess);
	}

	let node_cl = Arc::new(node);
	let edge_cl = Arc::new(edge);
	let adj_cl = Arc::new(adj);

	let num_workers = 8;
	let mut handles = Vec::new();

	for _ in 0..num_workers {
		let node_cl = Arc::clone(&node_cl);
		let edge_cl = Arc::clone(&edge_cl);
		let adj_cl = Arc::clone(&adj_cl);
		let excess_cl = Arc::clone(&excess);

		let handle = thread::spawn(move || {
			loop {
				let u_opt = {
					let mut q = excess_cl.lock().unwrap();
					q.pop_front()
					
				};


				if let Some(u) = u_opt {
					discharge(u, &node_cl, &edge_cl, &adj_cl[u], &excess_cl);
				} else {
					break;
				}
			}
		});
	handles.push(handle);
	}

	// join workers
	for h in handles {
		h.join().unwrap();
	}

	let sink_excess = node_cl[node_cl.len()-1].lock().unwrap().e;
	println!("f = {}", sink_excess);

}
