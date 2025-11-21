#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// Compatibility additions to fit forsete environment -----
typedef struct xedge_t {
	int		u;	/* one of the two nodes.	*/
	int		v;	/* the other. 			*/
	int		c;	/* capacity.			*/
} xedge_t;
static char *progname;
#define INT_MAX 2147483647
#define MIN(a, b)(((a) <= (b)) ? (a) : (b))
#define MAX(a, b)(((a) >= (b)) ? (a) : (b))
// -----

#define PRINT 0
#define TC 8
#define BATCH_SIZE 12

typedef int bool_t;
typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct context_t context_t;
typedef struct thread_t thread_t;
typedef struct update_t update_t;
typedef enum { UPDATE_PUSH = 0, UPDATE_RELABEL = 1 } update_type_t;

struct node_t {
  int h;
  int e;
  int cur;
  int start;
  int degree;
  int is_in_queue;
};

struct edge_t {
  int u;
  int v;
  int f;
  int c;
};

struct graph_t {
  int n;
  int m;

  int *edge_u; // [2*M]
  int *edge_v;
  int *edge_f;
  int *edge_c;

  node_t *nodes;
  int source, sink;
  int *adj;
  int size_adj;

  // Work queue and lock associated with it
  int *active_queue;
  int queue_head, queue_tail, queue_size;
  pthread_mutex_t g_lock;

  // Global relabels to 'kill off' stale heights, speeds up for larger graphs
  int global_relabel_flag;
  int global_relabel_frequency;

  // Confusing but functional termination logic
  int done;
  int work_counter;
  int work_done;
};

struct context_t {
  graph_t *g;
  // List of threads, the barrier used for synchronization logic, and all the list of thread contexts (confusing naming : thread_t is a thread context)
  pthread_t *threads;
  pthread_barrier_t *barrier;
  thread_t *thread_ctxs;
  int tc;
  int result;
};

struct update_t {
  update_type_t type;
  int delta;
  int new_height;
  node_t *u;
};

// This one is too much of a mess not touching this right now
struct thread_t {
  graph_t  *g;
  update_t *plq;
  size_t count;
  size_t tid;

  thread_t *all_thread_ctx;
  size_t all_thread_ctx_size;

  node_t **inqueue;
  int inqueue_size;
  int inqueue_count;
  int inqueue_index;
  bool_t did_work;
  pthread_barrier_t *barrier;
};

/* ------------------- Memory & Helpers ------------------- */
static int next_int()
{
	int x, c;
	x = 0;
	while (isdigit(c = getchar()))
		x = 10 * x + c - '0';
	return x;
}

/* ------------------- Queue Helpers ------------------- */
static void resize_queue(graph_t *g)
{
  int size, size_new, head, tail, count;
  int *newq;
  
  size     = g->queue_size;
  size_new = size * 2;
  
  newq = malloc(size_new * sizeof(int));
  if(!newq)
  {
    perror("Failed to allocate memory for queue growth\n");
  }

  head = g->queue_head;
  tail = g->queue_tail;

  // Copy elements over to the new queue
  count = 0;
  while(head != tail)
  {
    newq[count++] = g->active_queue[head];
    head = (head + 1) % size;
  }

  free(g->active_queue);

  // Update graph to use new queue
  g->active_queue = newq;
  g->queue_head   = 0;
  g->queue_tail   = count;
  g->queue_size   = size_new;
}

static void enter_excess(graph_t *g, node_t *v) {
  int vid, source, sink, head, tail, size, tail_next;

  vid = (int)(v - g->nodes);
  source = g->source;
  sink   = g->sink;
  // Dont enque source, sink or already queued nodes.
  if (vid == source || vid == sink)
  {
    return;
  }

  if (v->is_in_queue)
  {
    return;
  }

  v->is_in_queue = 1;

  head = g->queue_head;
  tail = g->queue_tail;
  size = g->queue_size;
  tail_next = (tail + 1) % size;

  // When queue is full, grow it in size
  if (tail_next == head)
  {
    resize_queue(g);
    // Re-compute for the new queue
    tail = g->queue_tail;
    size = g->queue_size;
    tail_next = (tail + 1) % size;
  }

  // Inserting the new node
  g->active_queue[tail] = vid;
  g->queue_tail = tail_next;

  // Increment work counter as we now have one more node to process
  g->work_counter++;
}

/* ------------------- Push/Relabel ------------------- */
static void push(thread_t *ctx, node_t *u, node_t *v, int eid) {
  int uid, eu, ev, ec, ef, d;
  node_t *target;
  
  graph_t *g = ctx->g;
  uid = (int)(u - g->nodes);
  eu  = g->edge_u[eid];
  ev  = g->edge_v[eid];
  ec  = g->edge_c[eid];
  ef  = g->edge_f[eid];

  if (uid == eu)
  {
      d = MIN(u->e, ec - ef);
      g->edge_f[eid] = ef + d;
      target = &g->nodes[ev];
  }
  else
  {
      d = MIN(u->e, ec + ef);
      g->edge_f[eid] = ef - d;
      target = &g->nodes[eu];
  }

  u->e -= d;

  update_t upd = {
      .type  = 0,
      .u     = target,
      .delta = d
  };

  // Add to update list for later application in serialized step
  ctx->plq[ctx->count++] = upd;
}

static int _find_min_residual_cap(graph_t *g, node_t *u) {
  // -- Way too many variables but seems to improve a little bit...
  int uid, vid, eid, min_h, start, degree, eu, ev, ec, ef, residual_fwd, residual_back, residual, i;
  int *edge_u, *edge_v, *edge_c, *edge_f;
  bool_t is_fwd;
  node_t *v;

  min_h  = INT_MAX;
  uid    = (int)(u - g->nodes);
  start  = u->start;
  degree = u->degree;

  edge_u = g->edge_u;
  edge_v = g->edge_v;
  edge_c = g->edge_c;
  edge_f = g->edge_f;
  
  for (i = 0; i < degree; i++)
  {
    eid = g->adj[start + i];
    eu  = edge_u[eid];
    ev  = edge_v[eid];
    ec  = edge_c[eid];
    ef  = edge_f[eid];

    is_fwd = (uid == eu);
    vid    = is_fwd ? ev : eu;

    residual_fwd  = ec - ef;
    residual_back = ec + ef;
    residual      = is_fwd ? residual_fwd : residual_back;

    v = &g->nodes[vid];

    if (residual > 0)
    {
      min_h = MIN(min_h, v->h);
    }
  }

  return min_h;
}

static void relabel(thread_t *ctx, node_t *u) {
  int min_h, old_h, h;
  graph_t *g = ctx->g;

  old_h = u->h;
  min_h = _find_min_residual_cap(g, u);
  h     = (min_h < INT_MAX) ? (min_h + 1) : old_h + 1;

  update_t upd = {
    .type  = 1,
    .u     = u,
    .new_height = h
  };

  ctx->plq[ctx->count++] = upd;
}

/* ------------------- Worker ------------------- */
static void _build_update_queue(thread_t * ctx, node_t * u) {
  bool_t pushed, can_push;
  int uid, vid, start, degree, eid, eu, ev, ef, ec, is_fwd, residual_fwd, residual_back, residual, i;
  int *edge_u, *edge_v, *edge_c, *edge_f, *adj;
  node_t *v;

  graph_t *g = ctx->g;
  uid = (int)(u - g->nodes);
  pushed   = 0;
  can_push = 0;
  start    = u->start;
  degree   = u->degree;
  
  // ----
  edge_u = g->edge_u;
  edge_v = g->edge_v;
  edge_c = g->edge_c;
  edge_f = g->edge_f;
  adj    = g->adj;

  for (i = u->cur; i < degree; i++)
  {
    eid = adj[start + i];

    eu = edge_u[eid];
    ev = edge_v[eid];
    ef = edge_f[eid];
    ec = edge_c[eid];

    is_fwd = (uid == eu);
    vid    = is_fwd ? ev : eu;

    residual_fwd  = ec - ef;
    residual_back = ec + ef;
    residual      = is_fwd ? residual_fwd : residual_back;

    v = &g->nodes[vid];
    can_push = (u->h > v->h) && (residual > 0);

    if (can_push)
    {
      push(ctx, u, v, eid);
      pushed = 1;

      if (u->e == 0)
      {
        u->cur = i;
        break;
      }
    }
  }
  if (!pushed)
  {
    relabel(ctx, u);
    u->cur = 0;
  } 
  else if (u->e > 0)
  {
    u->cur = 0;

    update_t upd = {
      .type = 0,
      .u = u,
      .new_height = u->h
    };

    ctx->plq[ctx->count++] = upd;
  }
}

static int grab_excess_batch(graph_t *g, node_t **local_queue, int max_nodes) {
  int vid, count;
  node_t *v;

  count = 0;

  pthread_mutex_lock(&g->g_lock);
  while (count < max_nodes && (g->queue_head != g->queue_tail))
  {
    vid = g->active_queue[g->queue_head];

    g->queue_head = (g->queue_head + 1) % g->queue_size;

    v = &g->nodes[vid];
    v->is_in_queue = 0;

    local_queue[count++] = v;

    g->work_counter--;
  }
  pthread_mutex_unlock(&g->g_lock);
  return count;
}

static void global_relabel(graph_t *g)
{
  int n, sink, i, v, hv, start, degree, k, eid, eu, ev, ec, ef, u, residual, is_rev, qhead, qtail;
  int *edge_u, *edge_v, *edge_c, *edge_f, *queue, *adj;
  node_t *nodes;

  n      = g->n;
  nodes  = g->nodes;
  edge_u = g->edge_u;
  edge_v = g->edge_v;
  edge_c = g->edge_c;
  edge_f = g->edge_f;
  adj    = g->adj;
  sink   = g->sink;

  // The global relabel uses BFS so initialize queue
  queue = malloc(n * sizeof(int));
  qhead = 0;
  qtail = 0;

  for (i = 0; i < n; ++i)
  {
      nodes[i].h = INT_MAX;
  }

  // BFS from sink
  nodes[sink].h = 0;
  queue[qtail++] = sink;

  // Then the actual reverse BFS...
  while (qhead < qtail)
  {
    v  = queue[qhead++];
    hv = nodes[v].h;

    start  = nodes[v].start;
    degree = nodes[v].degree;

    for (k = 0; k < degree; ++k)
    {
      eid = adj[start + k];

      eu = edge_u[eid];
      ev = edge_v[eid];
      ec = edge_c[eid];
      ef = edge_f[eid];

      is_rev   = (v == eu);
      u        = is_rev ? ev : eu;
      residual = is_rev ? (ec - ef) : (ec + ef);

      if (residual > 0 && nodes[u].h == INT_MAX)
      {
          nodes[u].h = hv + 1;
          queue[qtail++] = u;
      }
    }
  }

  for (i = 0; i < n; ++i)
  {
    if (nodes[i].h == INT_MAX)
    {
      nodes[i].h = n;
    }
    nodes[i].cur = 0;
  }

  free(queue);
}

static void rebuild_active_queue(graph_t *g)
{
  int i, idx, next_tail, old_size, new_size, n;
  int *active_queue, *newq;
  node_t *nodes, *u;

  n            = g->n;
  nodes        = g->nodes;
  active_queue = g->active_queue;

  g->queue_head = 0;
  g->queue_tail = 0;

  for (i = 0; i < n; i++)
  {
    nodes[i].is_in_queue = 0;
  }

  for (i = 0; i < n; i++)
  {
    if (i == g->source || i == g->sink)
    {
      continue;
    }

    u = &nodes[i];

    if (u->e > 0)
    {
      next_tail = (g->queue_tail + 1) % g->queue_size;

      if (next_tail == g->queue_head)
      {
        old_size = g->queue_size;
        new_size = old_size * 2;
        newq     = malloc(new_size * sizeof(int));
        idx      = 0;

        while (g->queue_head != g->queue_tail)
        {
          newq[idx++]   = active_queue[g->queue_head];
          g->queue_head = (g->queue_head + 1) % old_size;
        }

        free(active_queue);

        g->active_queue = newq;
        active_queue    = newq;
        g->queue_head   = 0;
        g->queue_tail   = idx;
        g->queue_size   = new_size;

        next_tail = (g->queue_tail + 1) % g->queue_size;
      }

      active_queue[g->queue_tail] = i;
      g->queue_tail = next_tail;
      u->is_in_queue = 1;
    }
  }
}

static void _apply_updates(thread_t * tctx) {
    graph_t *g = tctx->g;

    if (g->global_relabel_flag) {
        global_relabel(g);
        rebuild_active_queue(g);

        for (size_t i = 0; i < tctx->all_thread_ctx_size; i++) {
            tctx->all_thread_ctx[i].count = 0;
        }
        g->work_done = 0;
        g->global_relabel_flag = 0;

        return; /* skip normal relabel updates */
    }

    for (size_t i = 0; i < tctx->all_thread_ctx_size; i++) {
        thread_t *thread_ctx = &tctx->all_thread_ctx[i];

        for (size_t k = 0; k < thread_ctx->count; k++) {
            update_t update = thread_ctx->plq[k];

            if (update.type == 0) { /* push */
                update.u->e += update.delta;
                enter_excess(g, update.u);
            } else if (update.type == 1) { /* local relabel */
                update.u->h = update.new_height;
                update.u->cur = 0;
                enter_excess(g, update.u);
            }
        }
        thread_ctx->count = 0;
    }
}

static inline void sync_and_apply(thread_t * tctx) {
  int res = pthread_barrier_wait(tctx->barrier);
  if (res == PTHREAD_BARRIER_SERIAL_THREAD) {
    _apply_updates(tctx);
    if (tctx->g->work_done >= tctx->g->global_relabel_frequency) {
        tctx->g->global_relabel_flag = 1;
    }
    if (tctx->g->work_counter == 0 && (tctx->g->queue_head == tctx->g->queue_tail))
      tctx->g->done = 1;
  }
  pthread_barrier_wait(tctx->barrier);
}

void * worker(void * arg) {
  thread_t * tctx = (thread_t *) arg;
  graph_t * g = tctx->g;

  int i;
  //node_t *local_queue[tctx->inqueue_size];
  node_t * u;

  while (1) {
    /* grab a batch of excess nodes */
    int n = grab_excess_batch(g, tctx->inqueue, tctx->inqueue_size);
    if (n == 0) {
      sync_and_apply(tctx);
      if (g->done)
        break;
      continue;
    }
    for (i = 0; i < n; i++) {
      u = tctx->inqueue[i];
      int uid = (int)(u - g->nodes);
      if (u && (uid != g->source) && (uid != g->sink)) {
        _build_update_queue(tctx, u);
      }
    }

    sync_and_apply(tctx);
    if (g->done)
      break;
  }
  return NULL;
}


/* ------------------- Graph Initialization ------------------- */
static graph_t* new_graph(int n, int m, int s, int t, xedge_t* xedges)
{
    graph_t* g = malloc(sizeof(graph_t));
    g->n = n;
    g->m = m;
    g->global_relabel_flag = 0;
    g->global_relabel_frequency = n;
    g->work_done = 0;

    g->nodes = calloc(n, sizeof(node_t));

    int total_e = 2 * m;
    g->edge_u  = malloc(total_e * sizeof(int));
    g->edge_v  = malloc(total_e * sizeof(int));
    g->edge_f  = calloc(total_e, sizeof(int));
    g->edge_c  = malloc(total_e * sizeof(int));

    g->source = s;
    g->sink   = t;

    g->queue_head = 0;
    g->queue_tail = 0;
    g->queue_size = 0;
    g->work_counter = 0;

    int* deg = calloc(n, sizeof(int));

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

    g->adj = malloc(prefix * sizeof(int));
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

        g->edge_u[eri] = v;
        g->edge_v[eri] = u;
        g->edge_c[eri] = c;
        g->edge_f[eri] = 0;

        g->adj[g->nodes[u].start + deg[u]++] = ei;
        g->adj[g->nodes[v].start + deg[v]++] = eri;
    }

    free(deg);
    return g;
}


static void init_mutexes(graph_t * g) {
  pthread_mutex_init(&g->g_lock, NULL);
}

static void destroy_mutexes(graph_t * g) {
  pthread_mutex_destroy(&g->g_lock);
}

static void init_preflow(graph_t * g) {
  node_t *s = &g->nodes[g->source];
  s->h = g->n;
  s->e = 0;

  for (int k = 0; k < s->degree; k++) {
    int eid = g->adj[s->start + k];
    if (g->edge_u[eid] == g->source) s->e += g->edge_c[eid];
  }

  for (int k = 0; k < s->degree; k++) {
    int eid = g->adj[s->start + k];
    if (g->edge_u[eid] == g->source && g->edge_c[eid] > 0) {
      node_t * v = & g->nodes[g->edge_v[eid]];
      int f = g->edge_c[eid]; // push full capacity
      g->edge_f[eid] = f; // record flow
      s->e -= f;
      v->e += f; // assign excess to neighbor
      enter_excess(g, v);
    }
  }
}

/* ------------------- Worker Init ------------------- */
static void init_workers(context_t * algo) {
  graph_t * g = algo->g;
  pthread_t * threads = algo->threads;
  int tc = algo->tc;
  int inqueue_batch_size = BATCH_SIZE;

  thread_t * ctxs = calloc(tc, sizeof(thread_t));
  algo->thread_ctxs = ctxs;
  g->done = 0;

  for (int i = 0; i < tc; i++) {
    update_t *plq = calloc(g->m, sizeof(update_t));
    ctxs[i].g = g;
    ctxs[i].plq = plq;
    ctxs[i].barrier = algo->barrier;
    ctxs[i].tid = i;
    ctxs[i].count = 0;
    ctxs[i].all_thread_ctx = ctxs;
    ctxs[i].all_thread_ctx_size = tc;
    ctxs[i].inqueue_size = inqueue_batch_size;
    ctxs[i].inqueue = calloc(inqueue_batch_size, sizeof(node_t * ));
    pthread_create(&threads[i], NULL, worker, &ctxs[i]);
  }
}

static void join_workers(pthread_t *threads, int tc) {
  for (int i = 0; i < tc; i++) pthread_join(threads[i], NULL);
}
static void free_ctx(context_t *algo) {
  for (int i = 0; i < algo->tc; i++) {
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
  free(g->adj);
  free(g->active_queue);
  free(g);
}

int preflow(int n, int m, int s, int t, xedge_t* xedges) {
    graph_t* g = new_graph(n, m, s, t, xedges);
    context_t algo_ctx;
    pthread_t *threads = malloc(sizeof(pthread_t) * TC);

    init_mutexes(g);
    g->queue_size   = 2 * (g->m);
    g->active_queue = malloc(g->queue_size * sizeof(int));

    algo_ctx.g = g;
    algo_ctx.threads = threads;
    pthread_barrier_t barrier;
    algo_ctx.barrier = &barrier;
    pthread_barrier_init(&barrier, NULL, TC);
    algo_ctx.tc = TC;

    init_preflow(g);
    init_workers(&algo_ctx);
    join_workers(threads, TC);

    algo_ctx.result = g->nodes[g->sink].e;

    free_ctx(&algo_ctx);
    destroy_mutexes(g);
    pthread_barrier_destroy(&barrier);
    free(threads);
    free_graph(g);

    return algo_ctx.result;
}

/* ------------------- Main ------------------- */
int main(int argc, char * argv[]) {
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