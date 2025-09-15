import java.util.Scanner;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import java.util.ListIterator;
import java.util.LinkedList;
import java.lang.Thread;

import java.io.*;

class PreflowThread extends Thread {
	private final Graph g;

	PreflowThread(Graph g) {
		this.g = g;
	}

	@Override
	public void run() {
		Node u;

		while (true) {
			u = g.getNextActiveNode();
			if (u == null) {
				break;
			}
			g.discharge(u);
		}
	}
}

class PushResult {
	public boolean uActive = false;
	public boolean vActive = false;
}

class Graph {

	int s;
	int t;
	int n;
	int m;
	int activeWorkers;
	boolean done;
	Thread threads[];
	Node excess; // list of nodes with excess preflow
	Node node[];
	Edge edge[];
	ReentrantLock gLock;
	Condition gCond;

	Graph(Node node[], Edge edge[]) {
		this.node = node;
		this.n = node.length;
		this.edge = edge;
		this.m = edge.length;
	}

	public void enterExcess(Node u) {
		gLock.lock();
		try {
			enterExcessLocked(u);
		} finally {
			gLock.unlock();
		}
	}

	/* REQUIRE: Must hold global lock before calling this. */
	public void enterExcessLocked(Node u) {
		if (u == node[s] || u == node[t])
			return;
		if (!u.inQueue) {
			u.inQueue = true;
			u.next = excess;
			excess = u;
			gCond.signal();
		}
	}

	/* REQUIRE: Must hold global lock before calling this. */
	public Node leaveExcessLocked() {
		Node u = excess;
		if (u != null) {
			u.inQueue = false;
			excess = u.next;
			u.next = null;
		}
		return u;
	}

	public Node leaveExcess() {
		Node u;

		gLock.lock();
		try {
			u = leaveExcessLocked();
		} finally {
			gLock.unlock();
		}
		return u;
	}

	Node other(Edge a, Node u) {
		if (a.u == u)
			return a.v;
		else
			return a.u;
	}

	private int findMinResidualCap(Node u) {
		int minH = Integer.MAX_VALUE;
		ListIterator<Edge> iter = u.adj.listIterator();
		Edge a;
		Node v;

		while (iter.hasNext()) {
			a = iter.next();
			a.lockEdgeNodes();
			v = other(a, u);
			int rf;

			if (u == a.u) {
				rf = a.c - a.f;
			} else {
				rf = a.c + a.f;
			}
			if (rf > 0) {
				minH = Math.min(minH, v.h);
			}
			a.unlockEdgeNodes();
		}

		return minH;

	}

	void _relabel(int minH, Node u) {
		u.nLock.lock();
		if (minH < Integer.MAX_VALUE) {
			u.h = minH + 1;
		} else {
			u.h++;
		}
		u.nLock.unlock();

	}

	void relabel(Node u) {
		int minH = findMinResidualCap(u);
		_relabel(minH, u);

		enterExcess(u);
	}

	int newPreflow(int s, int t, Thread[] threads) {
		this.s = s;
		this.t = t;
		this.threads = threads;

		setup();

		waitForFinish();

		teardown();

		return node[t].e;

	}

	void setup() {
		initMutexes();
		initPreflow();
		initWorkers();
	}

	void initMutexes() {
		this.gLock = new ReentrantLock();
		this.gCond = gLock.newCondition();
		for (Node n : node) {
			n.nLock = new ReentrantLock();
		}
	}

	void initPreflow() {
		ListIterator<Edge> iter;
		Edge a;

		node[s].h = n;

		iter = node[s].adj.listIterator();
		while (iter.hasNext()) {
			a = iter.next();

			node[s].e += a.c;

			push(node[s], other(a, node[s]), a);
		}
	}

	void initWorkers() {
		activeWorkers = threads.length;
		this.done = false;
		Thread thread;
		for (int i = 0; i < threads.length; i++) {
			thread = new PreflowThread(this);
			threads[i] = thread;
			thread.start();
		}

	}

	void waitForFinish() {
		gLock.lock();
		try {
			while (!done) {
				gCond.await(); // handles spurious wakeups
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		} finally {
			gLock.unlock();
		}
	}

	void signalDone() {
		gLock.lock();
		try {
			done = true;
			gCond.signalAll();
		} finally {
			gLock.unlock();
		}
	}

	void teardown() {
		for (Thread th : threads) {
			try {
				th.join();
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt(); // If interrupted, stop trying to join the rest
				break;
			}
		}
	}

	PushResult pushCore(Node u, Node v, Edge a) {
		PushResult pr = new PushResult();
		int d;
		a.lockEdgeNodes();

		if (u == a.u) {
			d = Math.min(u.e, a.c - a.f); // forward residual
			a.f += d;
		} else {
			d = Math.min(u.e, a.c + a.f); // backward residual
			a.f -= d;
		}

		u.e -= d;
		v.e += d;

		int uAfter = u.e;
		int vAfter = v.e;
		a.unlockEdgeNodes();

		if (d < 0 || Math.abs(a.f) > a.c || u.e < 0) {
			throw new AssertionError("Something wrong");
		}

		pr.uActive = (uAfter > 0);
		pr.vActive = (vAfter == d);

		return pr;
	}

	void push(Node u, Node v, Edge a) {
		PushResult r = pushCore(u, v, a);
		if (r.uActive) {
			enterExcess(u);
		}
		if (r.vActive) {
			enterExcess(v);
		}
	}

	/* REQUIRE: Caller must not hold the global lock */
	Node getNextActiveNode() {
		Node u;

		gLock.lock();

		while (true) {
			u = leaveExcessLocked();
			if (u != null)
				break;

			activeWorkers--;

			if (activeWorkers == 0 && excess == null) {
				done = true;
				gCond.signalAll();
				gLock.unlock();
				return null;
			}

			while ((u = leaveExcessLocked()) == null && !done) {
				try {
					gCond.await();
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
			}

			activeWorkers++;

			if (done) {
				gLock.unlock();
				return null;
			}
			if (u != null) {
				break;
			}
		}
		gLock.unlock();
		return u;
	}

	void discharge(Node u) {
		boolean pushed = false;
		boolean canPush = false;
		ListIterator<Edge> iter;
		Edge e;
		Node v;
		int b;

		iter = u.adj.listIterator();
		while (iter.hasNext()) {
			e = iter.next();

			if (u == e.u) {
				v = e.v;
				b = 1;
			} else {
				v = e.u;
				b = -1;
			}

			e.lockEdgeNodes();
			canPush = (u.h > v.h) && (b * e.f < e.c);
			e.unlockEdgeNodes();

			if (canPush) {
				push(u, v, e);
				pushed = true;
				break;
			}
		}
		if (!pushed) {
			relabel(u);
		}
	}

	/*
	 * int preflow(int s, int t) {
	 * ListIterator<Edge> iter;
	 * int b;
	 * Edge a;
	 * Node u;
	 * Node v;
	 * 
	 * this.s = s;
	 * this.t = t;
	 * node[s].h = n;
	 * 
	 * iter = node[s].adj.listIterator();
	 * while (iter.hasNext()) {
	 * a = iter.next();
	 * 
	 * node[s].e += a.c;
	 * 
	 * push(node[s], other(a, node[s]), a);
	 * }
	 * 
	 * while (excess != null) {
	 * u = excess;
	 * v = null;
	 * a = null;
	 * excess = u.next;
	 * 
	 * iter = u.adj.listIterator();
	 * while (iter.hasNext()) {
	 * a = iter.next();
	 * }
	 * 
	 * if (v != null)
	 * push(u, v, a);
	 * else
	 * relabel(u);
	 * }
	 * 
	 * return node[t].e;
	 * }
	 */
}

class Node {
	int h;
	int e;
	int i;
	boolean inQueue = false;
	Node next;
	LinkedList<Edge> adj;
	ReentrantLock nLock;

	Node(int i) {
		this.i = i;
		adj = new LinkedList<Edge>();
	}
}

class Edge {
	Node u;
	Node v;
	int f;
	int c;

	Edge(Node u, Node v, int c) {
		this.u = u;
		this.v = v;
		this.c = c;

	}

	/* REQUIRE: Must not hold either lock when calling this function */
	public void lockEdgeNodes() {
		if (u.i < v.i) {
			u.nLock.lock();
			v.nLock.lock();
		} else {
			v.nLock.lock();
			u.nLock.lock();
		}
	}

	/* REQUIRE: Must hold both lock when calling this function */
	public void unlockEdgeNodes() {
		u.nLock.unlock();
		v.nLock.unlock();
	}
}

class Preflow {
	private static int threadCount = 8;

	private static Thread[] threads = new Thread[threadCount];

	public static void main(String args[]) {
		double begin = System.currentTimeMillis();
		Scanner s = new Scanner(System.in);
		int n;
		int m;
		int i;
		int u;
		int v;
		int c;
		int f;
		Graph g;

		n = s.nextInt();
		m = s.nextInt();
		s.nextInt();
		s.nextInt();
		Node[] node = new Node[n];
		Edge[] edge = new Edge[m];

		for (i = 0; i < n; i += 1)
			node[i] = new Node(i);

		for (i = 0; i < m; i += 1) {
			u = s.nextInt();
			v = s.nextInt();
			c = s.nextInt();
			edge[i] = new Edge(node[u], node[v], c);
			node[u].adj.addLast(edge[i]);
			node[v].adj.addLast(edge[i]);
		}

		g = new Graph(node, edge);

		f = g.newPreflow(0, n - 1, threads);
		double end = System.currentTimeMillis();
		System.out.println("t = " + (end - begin) / 1000.0 + " s");
		System.out.println("f = " + f);
	}
}
