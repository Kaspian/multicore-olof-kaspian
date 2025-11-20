#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <limits.h>

#define PRINT 0
#define TC 8
#define BATCH_SIZE 12
#define MIN(a, b)(((a) <= (b)) ? (a) : (b))
#define MAX(a, b)(((a) >= (b)) ? (a) : (b))

typedef int bool_t;

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct xedge_t xedge_t;
typedef struct thread_ctx_t thread_ctx_t;

static char * progname;

#if PRINT
#define pr(...) fprintf(stderr, __VA_ARGS__)
#else
#define pr(...)
#endif

struct xedge_t {
	int		u;	/* one of the two nodes.	*/
	int		v;	/* the other. 			*/
	int		c;	/* capacity.			*/
};

struct node_t {
  int h;
  int e;
  int cur;
  int start;
  int degree;
  int is_in_queue;
  char pad[64 - 6 * sizeof(int)]; /* pad to cache line size */
};

struct edge_t {
  int u; // source
  int v; // target
  int f; // flow
  int c; // capacity
  int rev;
};

struct graph_t {
  int n;
  int m;
  //edge_t *edges;
  int * edge_u; // [2*M]
  int * edge_v;
  int * edge_f;
  int * edge_c;
  int * edge_rev;

  int * adj;
  int length;

  node_t * nodes;

  int source, sink;
  int * active_queue;
  int queue_head, queue_tail, queue_size;
  pthread_mutex_t g_lock;
  pthread_cond_t cv;
  int done;
  int active_workers;
  int work_counter;
  int need_global_relabel;
  int global_relabel_freq;
  int work_done;
};

struct preflow_context_t {
  graph_t * g;
  pthread_t * threads;
  pthread_barrier_t * barrier;
  thread_ctx_t * thread_ctxs;
  int threadcount;
  int result;
};

typedef struct {
  int type; /* 0 push, 1 relabel */
  node_t * u;
  int delta;
  int new_height;
  graph_t * g;
  node_t * v; // target node
  //edge_t *edge; // edge used for push (?)
}
update_t;

struct thread_ctx_t {
  graph_t * g;
  update_t * plq;
  size_t count;
  size_t tid;
  thread_ctx_t * all_thread_ctx;
  size_t all_thread_ctx_size;
  node_t ** inqueue;
  int inqueue_size;
  int inqueue_count;
  int inqueue_index;
  bool_t did_work;
  pthread_barrier_t * barrier;
};

/* ------------------- Memory & Helpers ------------------- */
void error(const char * fmt, ...) {
  va_list ap;
  char buf[BUFSIZ];
  va_start(ap, fmt);
  vsprintf(buf, fmt, ap);
  if (progname) fprintf(stderr, "%s: ", progname);
  fprintf(stderr, "error: %s\n", buf);
  exit(1);
}

static void * xmalloc(size_t s) {
  void * p = malloc(s);
  if (!p)
    error("out of memory");
  return p;
}

static void * xcalloc(size_t n, size_t s) {
  void * p = xmalloc(n * s);
  memset(p, 0, n * s);
  return p;
}

static int next_int() {
  int x = 0, c;
  while (isspace(c = getchar())); // skip whitespace
  if (!isdigit(c)) return -1;
  do x = 10 * x + c - '0';
  while (isdigit(c = getchar()));
  return x;
}

/* ------------------- Queue Helpers ------------------- */
static void enter_excess_locked(graph_t * g, node_t * v) {
  int vid = (int)(v - g -> nodes);

  if (vid == g -> source || vid == g -> sink)
    return;

  if (!v -> is_in_queue) {
    v -> is_in_queue = 1;
    int next_tail = (g -> queue_tail + 1) % g -> queue_size;

    if (next_tail == g -> queue_head) // grow
    {
      int old_size = g -> queue_size;
      int new_size = old_size * 2;
      int * newq = xmalloc(new_size * sizeof(int));
      int i = 0;
      while (g -> queue_head != g -> queue_tail) {
        newq[i++] = g -> active_queue[g -> queue_head];
        g -> queue_head = (g -> queue_head + 1) % old_size;
      }
      free(g -> active_queue);
      g -> active_queue = newq;
      g -> queue_head = 0;
      g -> queue_tail = i;
      g -> queue_size = new_size;
      next_tail = (g->queue_tail + 1) & (g->queue_size - 1);      
      pr("Queue resized to %d\n", new_size);
    }
    g -> active_queue[g -> queue_tail] = vid;
    g -> queue_tail = next_tail;
    g -> work_counter++;
    pr("[ENTER_EXCESS] Node %d entering queue, queue_head=%d, queue_tail=%d\n", vid, g -> queue_head, g -> queue_tail);
  }
}

static void enter_excess(graph_t * g, node_t * v) {
  pthread_mutex_lock( & g -> g_lock);
  enter_excess_locked(g, v);
  pthread_mutex_unlock( & g -> g_lock);
}

static node_t * leave_excess_locked(graph_t * g) {
  if (g -> queue_head == g -> queue_tail) {
    pr("Queue empty\n");
    return NULL;
  }
  int vid = g -> active_queue[g -> queue_head];

  g -> queue_head = (g -> queue_head + 1) % g -> queue_size;
  node_t * v = & g -> nodes[vid];
  v -> is_in_queue = 0;
  g -> work_counter--;
  pr("Node %d leaving queue\n", vid);
  return v;
}

static node_t * leave_excess(graph_t * g) {
  pthread_mutex_lock( & g -> g_lock);
  node_t * v = leave_excess_locked(g);
  pthread_mutex_unlock( & g -> g_lock);
  return v;
}

/* ------------------- Push/Relabel ------------------- */
static void _push(thread_ctx_t * ctx, node_t * u, node_t * v, int eid) {
  graph_t * g = ctx -> g;
  int uid = (int)(u - g -> nodes);
  int d;

  update_t update;
  update.type = 0;
  //if(uid == g->source || uid == g->sink) {pr("THIS HAPPENS"); return;}

  /* compute forward edge id and current forward residual */
  if (uid == g -> edge_u[eid]) {
    d = MIN(u -> e, g -> edge_c[eid] - g -> edge_f[eid]);
    g -> edge_f[eid] += d;
    pr("d: %d ::", d);
    update.u = & g -> nodes[g -> edge_v[eid]];
  } else {
    d = MIN(u -> e, g -> edge_c[eid] + g -> edge_f[eid]);
    g -> edge_f[eid] -= d;
    pr("This allows negative deltas (?) - is this okay? Look later at implementation");
    pr("d: -%d\n", d);
    update.u = & g -> nodes[g -> edge_u[eid]];
  }
  pr("pushing %d\n", d);
  u -> e -= d;

  size_t index = ctx -> count;
  update.new_height = u -> h;
  update.delta = d;
  ctx -> plq[index] = update;
  ctx -> count++;

  pr("[SCHEDULE PUSH] Thread %zu: %ld -> %ld delta=%d\n", ctx -> tid, u - g -> nodes, v - g -> nodes, d);
  assert(d >= 0);
}

static void push(thread_ctx_t * ctx, node_t * u, node_t * v, int eid) {
  _push(ctx, u, v, eid);
}

static int _find_min_residual_cap(graph_t * g, node_t * u) {
  int uid = (int)(u - g -> nodes);
  int min_h = INT_MAX;

  int start = u -> start;
  int degree = u -> degree;
  int * edge_u = g -> edge_u;
  int * edge_v = g -> edge_v;
  int * edge_c = g -> edge_c;
  int * edge_f = g -> edge_f;
  for (int k = 0; k < degree; k++) {
    int eid = g -> adj[start + k];
    int eu = edge_u[eid];
    int ev = edge_v[eid];
    int ec = edge_c[eid];
    int ef = edge_f[eid];

    int is_fwd = (uid == eu);
    int vid = is_fwd ? ev : eu;

    int residual_fwd = ec - ef;
    int residual_back = ec + ef;
    int residual = is_fwd ? residual_fwd : residual_back;

    node_t * v = & g -> nodes[vid];

    if (residual > 0)
      min_h = MIN(min_h, v -> h);
  }
  return min_h;
}

static void relabel(thread_ctx_t * ctx, node_t * u) {
  graph_t * g = ctx -> g;
  int min_h = _find_min_residual_cap(g, u);
  int h = (min_h < INT_MAX) ? min_h + 1 : u -> h + 1;

  update_t update;
  update.type = 1;
  update.u = u;
  update.delta = 0;
  update.new_height = h;
  ctx -> plq[ctx -> count++] = update;

  pr("[SCHEDULE RELABEL] Thread %zu: Node %ld: h %d -> %d\n", ctx -> tid, u - g -> nodes, u -> h, h);
}

/* ------------------- Worker ------------------- */
static void _build_update_queue(thread_ctx_t * ctx, node_t * u) {
  bool_t pushed = 0;
  bool_t can_push = 0;
  edge_t * e;
  node_t * v;
  int b;
  int i;
  graph_t * g = ctx -> g;

  pr("[BUILD] Thread %zu: Node %ld start: e=%d, h=%d\n", ctx -> tid, u - g -> nodes, u -> e, u -> h);

  int uid = (int)(u - g -> nodes);
  int start = u -> start;
  int degree = u -> degree;
  int * edge_u = g -> edge_u;
  int * edge_v = g -> edge_v;
  int * edge_c = g -> edge_c;
  int * edge_f = g -> edge_f;

  for (i = u -> cur; i < u -> degree; i++) {
    int eid = g -> adj[start + i];

    int eu = edge_u[eid];
    int ev = edge_v[eid];
    int ef = edge_f[eid];
    int ec = edge_c[eid];

    int is_fwd = (uid == eu);
    int vid = is_fwd ? ev : eu;

    int residual_fwd = ec - ef;
    int residual_back = ec + ef;
    int residual = is_fwd ? residual_fwd : residual_back;

    node_t * v = & g -> nodes[vid];
    can_push = (u -> h > v -> h) && (residual > 0);

    pr("[MANUAL PUSH-CHECK] Sanity check: From %d -> %d, b = %d, and residual %d.\n height of u: %d, height of v: %d, and capacity e->c: %d\n",
      uid, (int)(v - g -> nodes), b, g -> edge_f[eid], u -> h, v -> h, g -> edge_c[eid]);

    if (can_push) {
      push(ctx, u, v, eid);
      pushed = 1;

      if (u -> e == 0) {
        u -> cur = i;
        break;
      }
    }
  }
  if (!pushed) {
    relabel(ctx, u);
    u -> cur = 0;
  } else if (u -> e > 0) {
    u -> cur = 0;
    size_t index = ctx -> count;

    update_t update;
    update.type = 0;
    update.u = u;
    update.delta = 0;
    update.new_height = u -> h;
    ctx -> plq[index] = update;
    ctx -> count++;
  }

  pr("[BUILD] Thread %zu: Node %ld end: e=%d, h=%d\n", ctx -> tid, u - g -> nodes, u -> e, u -> h);
}

/* ----------------- Batch grab and apply helpers (ported from OLD version) ----------------- */

static int grab_excess_batch(graph_t * g, node_t ** local_queue, int max_nodes) {
  int count = 0;
  pthread_mutex_lock(& g -> g_lock);
  while (count < max_nodes && (g -> queue_head != g -> queue_tail)) {
    int vid = g -> active_queue[g -> queue_head];
    g -> queue_head = (g -> queue_head + 1) % g -> queue_size;
    node_t * v = & g -> nodes[vid];
    v -> is_in_queue = 0;
    v -> start = v -> start; /* no-op to avoid unused warnings */
    local_queue[count++] = v;
    g -> work_counter--;
  }
  pthread_mutex_unlock(&g -> g_lock);
  return count;
}

static void global_relabel(graph_t *g)
{
    int n = g->n;
    int *queue = xmalloc(n * sizeof(int));
    int qhead = 0, qtail = 0;

    for (int i = 0; i < n; ++i) {
        g->nodes[i].h = INT_MAX;
    }

    int sink = g->sink;
    g->nodes[sink].h = 0;
    queue[qtail++] = sink;

    while (qhead < qtail) {
        int v = queue[qhead++];
        int hv = g->nodes[v].h;

        int start = g->nodes[v].start;
        int degree = g->nodes[v].degree;

        for (int k = 0; k < degree; ++k) {
            int eid = g->adj[start + k];

            int eu = g->edge_u[eid];
            int ev = g->edge_v[eid];
            int ec = g->edge_c[eid];
            int ef = g->edge_f[eid];

            int is_rev = (v == eu);           /* edge stored as v<-u? */
            int u = is_rev ? ev : eu;

            int residual = is_rev ? (ec - ef) : (ec + ef);

            if (residual > 0 && g->nodes[u].h == INT_MAX) {
                g->nodes[u].h = hv + 1;
                queue[qtail++] = u;
            }
        }
    }

    for (int i = 0; i < n; ++i) {
        if (g->nodes[i].h == INT_MAX)
            g->nodes[i].h = n;   /* standard large value */
        g->nodes[i].cur = 0;
    }

    free(queue);
}

static void rebuild_active_queue(graph_t *g)
{
    int N = g->n;

    g->queue_head = 0;
    g->queue_tail = 0;

    for (int i = 0; i < N; i++) {
        g->nodes[i].is_in_queue = 0;
    }

    for (int i = 0; i < N; i++) {
        if (i == g->source || i == g->sink)
            continue;

        node_t *u = &g->nodes[i];
        if (u->e > 0) {

            int next_tail = (g->queue_tail + 1) % g->queue_size;
            if (next_tail == g->queue_head) {
                int old_size = g->queue_size;
                int new_size = old_size * 2;
                int *newq = xmalloc(new_size * sizeof(int));
                int idx = 0;

                while (g->queue_head != g->queue_tail) {
                    newq[idx++] = g->active_queue[g->queue_head];
                    g->queue_head = (g->queue_head + 1) % old_size;
                }

                free(g->active_queue);
                g->active_queue = newq;
                g->queue_head = 0;
                g->queue_tail = idx;
                g->queue_size = new_size;

                next_tail = (g->queue_tail + 1) % g->queue_size;
            }

            g->active_queue[g->queue_tail] = i;
            g->queue_tail = next_tail;
            u->is_in_queue = 1;
        }
    }
}

static void _apply_updates(thread_ctx_t * tctx) {
    graph_t *g = tctx->g;

    if (g->need_global_relabel) {
        global_relabel(g);
        rebuild_active_queue(g);

        for (size_t i = 0; i < tctx->all_thread_ctx_size; i++) {
            tctx->all_thread_ctx[i].count = 0;
        }
        g->work_done = 0;
        g->need_global_relabel = 0;

        return; /* skip normal relabel updates */
    }

    for (size_t i = 0; i < tctx->all_thread_ctx_size; i++) {
        thread_ctx_t *thread_ctx = &tctx->all_thread_ctx[i];

        for (size_t k = 0; k < thread_ctx->count; k++) {
            update_t update = thread_ctx->plq[k];

            if (update.type == 0) { /* push */
                update.u->e += update.delta;
                enter_excess_locked(g, update.u);
            } else if (update.type == 1) { /* local relabel */
                update.u->h = update.new_height;
                update.u->cur = 0;
                enter_excess_locked(g, update.u);
            }
        }
        thread_ctx->count = 0;
    }
}

/*
static void _apply_updates(thread_ctx_t * tctx) {

  int i;
  int k;
  thread_ctx_t * all_thread_contexts = tctx -> all_thread_ctx;
  size_t all_thread_contexts_size = tctx -> all_thread_ctx_size;

  //#pragma omp parallel for schedule(static)
  for (i = 0; i < all_thread_contexts_size; i++) {
    thread_ctx_t * thread_ctx = & all_thread_contexts[i];
    update_t update;

    for (k = 0; k < thread_ctx -> count; k++) {
      update = thread_ctx -> plq[k];
      if (update.type == 0) {
        //pr("We are updating %d, with delta: %d", id(thread_ctx->g, update.u), update.delta);
        update.u -> e += update.delta;
      } else if (update.type == 1) {
        update.u -> h = update.new_height;
        update.u -> cur = 0; // reset current arc after relabel
      } else {
        error("ERROR");
      }

      enter_excess_locked(thread_ctx -> g, update.u);
    }

    thread_ctx -> count = 0;
  }
}
  */

static inline void sync_and_apply(thread_ctx_t * tctx) {
  int res = pthread_barrier_wait(tctx -> barrier);
  if (res == PTHREAD_BARRIER_SERIAL_THREAD) {
    _apply_updates(tctx);
    if (tctx->g->work_done >= tctx->g->global_relabel_freq) {
        tctx->g->need_global_relabel = 1;
    }
    if (tctx -> g -> work_counter == 0 && (tctx -> g -> queue_head == tctx -> g -> queue_tail))
      tctx -> g -> done = 1;
  }
  pthread_barrier_wait(tctx -> barrier);
}

void * worker(void * arg) {
  thread_ctx_t * tctx = (thread_ctx_t * ) arg;
  graph_t * g = tctx -> g;

  int i, n;
  //node_t *local_queue[tctx->inqueue_size];
  node_t * u;

  while (1) {
    /* grab a batch of excess nodes */
    int n = grab_excess_batch(g, tctx -> inqueue, tctx -> inqueue_size);
    if (n == 0) {
      sync_and_apply(tctx);
      if (g -> done)
        break;
      continue;
    }
    for (i = 0; i < n; i++) {
      u = tctx -> inqueue[i];
      int uid = (int)(u - g -> nodes);
      if (u && (uid != g -> source) && (uid != g -> sink)) {
        _build_update_queue(tctx, u);
      }
    }

    sync_and_apply(tctx);
    if (g -> done)
      break;
  }
  return NULL;
}


/* ------------------- Graph Init ------------------- */
/*
static graph_t * new_graph(FILE * in, int n, int m) {
  graph_t * g = xmalloc(sizeof(graph_t));
  g -> n = n;
  g -> m = m;
  g -> nodes = xcalloc(n, sizeof(node_t));
  //g->edges=xcalloc(2*m,sizeof(edge_t));
  int total_e = 2 * m;
  g -> edge_u = xmalloc(total_e * sizeof(int));
  g -> edge_v = xmalloc(total_e * sizeof(int));
  g -> edge_f = xcalloc(total_e, sizeof(int)); // f starts at 0
  g -> edge_c = xmalloc(total_e * sizeof(int));
  g -> edge_rev = xmalloc(total_e * sizeof(int));

  g -> source = 0;
  g -> sink = n - 1;
  g -> queue_head = 0;
  g -> queue_tail = 0;
  g -> queue_size = 0;
  g -> work_counter = 0;

  //int queue_head, queue_tail, queue_size;

  int * deg = xcalloc(n, sizeof(int));
  int * a = xmalloc(m * sizeof(int));
  int * b = xmalloc(m * sizeof(int));
  int * c = xmalloc(m * sizeof(int));

  pr("Creating graph with %d nodes, %d edges\n", n, m);

  for (int i = 0; i < m; i++) {
    a[i] = next_int();
    b[i] = next_int();
    c[i] = next_int();
    pr("Edge %d: %d -> %d, capacity=%d\n", i, a[i], b[i], c[i]);
    deg[a[i]]++;
    deg[b[i]]++;
  }

  int prefix = 0;
  for (int i = 0; i < n; i++) {
    g -> nodes[i].start = prefix;
    g -> nodes[i].degree = deg[i];
    prefix += deg[i];
    deg[i] = 0;
  }

  g -> adj = xmalloc(prefix * sizeof(int));
  for (int i = 0; i < m; i++) {
    int u = a[i], v = b[i];
    int ei = 2 * i, eri = 2 * i + 1;
    g -> edge_u[ei] = u;
    g -> edge_v[ei] = v;
    g -> edge_c[ei] = c[i];
    g -> edge_f[ei] = 0;
    g -> edge_rev[ei] = eri;
    g -> edge_u[eri] = v;
    g -> edge_v[eri] = u;
    g -> edge_c[eri] = c[i];
    g -> edge_f[eri] = 0;
    g -> edge_rev[eri] = ei;
    g -> adj[g -> nodes[u].start + deg[u]++] = ei;
    g -> adj[g -> nodes[v].start + deg[v]++] = eri;
  }

  for (int i = 0; i < n; i++)
    pr("Node %d: start=%d, degree=%d\n", i, g -> nodes[i].start, g -> nodes[i].degree);

  free(deg);
  free(a);
  free(b);
  free(c);
  return g;
} */

static graph_t* new_graph(int n, int m, int s, int t, xedge_t* xedges)
{
    graph_t* g = xmalloc(sizeof(graph_t));
    g->n = n;
    g->m = m;
    g->need_global_relabel = 0;
    g->global_relabel_freq = n;
    g->work_done = 0;

    g->nodes = xcalloc(n, sizeof(node_t));

    int total_e = 2 * m;
    g->edge_u  = xmalloc(total_e * sizeof(int));
    g->edge_v  = xmalloc(total_e * sizeof(int));
    g->edge_f  = xcalloc(total_e, sizeof(int));  /* all flow starts at 0 */
    g->edge_c  = xmalloc(total_e * sizeof(int));
    g->edge_rev = xmalloc(total_e * sizeof(int));

    g->source = s;
    g->sink   = t;

    g->queue_head = 0;
    g->queue_tail = 0;
    g->queue_size = 0;
    g->work_counter = 0;

    int* deg = xcalloc(n, sizeof(int));

    for (int i = 0; i < m; i++) {
        int u = xedges[i].u;
        int v = xedges[i].v;
        deg[u]++;
        deg[v]++;
    }

    int prefix = 0;
    for (int i = 0; i < n; i++) {
        g->nodes[i].start = prefix;
        g->nodes[i].degree = deg[i];
        prefix += deg[i];
        deg[i] = 0;
    }

    g->adj = xmalloc(prefix * sizeof(int));
    for (int i = 0; i < m; i++) {
        int u = xedges[i].u;
        int v = xedges[i].v;
        int c = xedges[i].c;

        int ei  = 2*i;
        int eri = 2*i + 1;

        g->edge_u[ei]  = u;
        g->edge_v[ei]  = v;
        g->edge_c[ei]  = c;
        g->edge_f[ei]  = 0;
        g->edge_rev[ei] = eri;

        g->edge_u[eri] = v;
        g->edge_v[eri] = u;
        g->edge_c[eri] = c;
        g->edge_f[eri] = 0;
        g->edge_rev[eri] = ei;

        g->adj[g->nodes[u].start + deg[u]++] = ei;
        g->adj[g->nodes[v].start + deg[v]++] = eri;
    }

    free(deg);
    return g;
}


static void init_mutexes(graph_t * g) {
  pthread_mutex_init(&g->g_lock, NULL);
  pthread_cond_init(&g->cv, NULL);
}

static void destroy_mutexes(graph_t * g) {
  pthread_mutex_destroy(&g -> g_lock);
  pthread_cond_destroy(&g -> cv);
}

static void init_preflow(graph_t * g) {
  node_t *s = &g->nodes[g->source];
  s->h = g->n;
  s->e = 0;

  for (int k = 0; k < s -> degree; k++) {
    int eid = g->adj[s->start + k];
    if (g->edge_u[eid] == g->source) s->e += g->edge_c[eid];
  }
  pr("Source node h=%d, e=%d\n", s->h, s->e);

  for (int k = 0; k < s->degree; k++) {
    int eid = g->adj[s->start + k];
    if (g -> edge_u[eid] == g -> source && g -> edge_c[eid] > 0) {
      node_t * v = & g -> nodes[g -> edge_v[eid]];
      int f = g -> edge_c[eid]; // push full capacity
      g -> edge_f[eid] = f; // record flow
      s -> e -= f;
      v -> e += f; // assign excess to neighbor
      pr("[INIT PUSH] %ld -> %ld | pushed %d, v.e=%d\n", s - g -> nodes, v - g -> nodes, f, v -> e);
      enter_excess(g, v);
    }
  }
}

/* ------------------- Worker Init ------------------- */
static void init_workers(preflow_context_t * algo) {
  graph_t * g = algo -> g;
  pthread_t * threads = algo -> threads;
  int threadcount = algo -> threadcount;
  int inqueue_batch_size = BATCH_SIZE;

  thread_ctx_t * ctxs = xcalloc(threadcount, sizeof(thread_ctx_t));
  algo -> thread_ctxs = ctxs;
  g -> active_workers = threadcount;
  g -> done = 0;

  for (int i = 0; i < threadcount; i++) {
    update_t *plq = xcalloc(g->m, sizeof(update_t));
    ctxs[i].g = g;
    ctxs[i].plq = plq;
    ctxs[i].barrier = algo->barrier;
    ctxs[i].tid = i;
    ctxs[i].count = 0;
    ctxs[i].all_thread_ctx = ctxs;
    ctxs[i].all_thread_ctx_size = threadcount;
    ctxs[i].inqueue_size = inqueue_batch_size;
    ctxs[i].inqueue = xcalloc(inqueue_batch_size, sizeof(node_t * ));
    pthread_create(&threads[i], NULL, worker, &ctxs[i]);
  }
}

static void join_workers(pthread_t *threads, int threadcount) {
  for (int i = 0; i < threadcount; i++) pthread_join(threads[i], NULL);
}
static void free_ctx(preflow_context_t *algo) {
  for (int i = 0; i < algo->threadcount; i++) {
    free(algo->thread_ctxs[i].plq);
    free(algo->thread_ctxs[i].inqueue);
  }
  free(algo->thread_ctxs);
}
static void free_graph(graph_t * g) {
  free(g->nodes);
  free(g->edge_u);
  free(g->edge_v);
  free(g->edge_f);
  free(g->edge_c);
  free(g->edge_rev);
  free(g->adj);
  free(g->active_queue);
  free(g);
}

int preflow(int n, int m, int s, int t, xedge_t* xedges) {
    graph_t* g = new_graph(n, m, s, t, xedges);

    preflow_context_t algo_ctx;
    //pthread_barrier_t *barrier = malloc(sizeof(pthread_barrier_t));

    pthread_t *threads = malloc(sizeof(pthread_t) * TC);

    init_mutexes(g);
    g -> queue_size = 2 * (g -> m);
    g -> active_queue = xmalloc(g -> queue_size * sizeof(int));

    algo_ctx.g = g;
    algo_ctx.threads = threads;
    pthread_barrier_t barrier;
    algo_ctx.barrier = &barrier;
    pthread_barrier_init(&barrier, NULL, TC);
    algo_ctx.threadcount = TC;

    init_preflow(g);

    init_workers(&algo_ctx);
    join_workers(threads, TC);

    algo_ctx.result = g->nodes[g -> sink].e;

    free_ctx(&algo_ctx);
    pthread_barrier_destroy(&barrier);
    free(threads);
    free_graph(g);

    return algo_ctx.result;
}

/* ------------------- Main ------------------- */
int main(int argc, char * argv[]) {
  int threadcount = TC;
  pthread_t threads[TC];
  pthread_barrier_t barrier;
  progname = argv[0];

  int n;
  int i;
	int m;

	n = next_int();
	m = next_int();
	i = next_int();
	i = next_int();

  xedge_t *edges = (xedge_t *)malloc(m * sizeof(xedge_t));
  if (!edges)
  {
    perror("Failed to allocate edges");
    exit(1);
  }

  for (i = 0; i < m; i += 1)
  {
    edges[i].u = next_int();
    edges[i].v = next_int();
    edges[i].c = next_int();   
	}


  int f = preflow(n, m, 0, n - 1, edges);
  printf("f = %d\n", f);
  free(edges);

  return 0;
}