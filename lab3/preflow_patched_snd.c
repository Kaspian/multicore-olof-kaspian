/*
 * preflow_patched_soa.c
 *
 * Fully converted to Structure-of-Arrays (SoA) for nodes and edges.
 * Minimal functional changes otherwise; removed node_t pointers in favor of int node ids.
 *
 * Build: cc -O3 -pthread preflow_patched_soa.c -o preflow_patched_soa
 */

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
#define MIN(a, b) (((a) <= (b)) ? (a) : (b))
#define MAX(a, b) (((a) >= (b)) ? (a) : (b))

typedef int bool_t;

typedef struct graph_t graph_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct thread_ctx_t thread_ctx_t;

static char *progname;

#if PRINT
#define pr(...) fprintf(stderr, __VA_ARGS__)
#else
#define pr(...)
#endif

struct graph_t
{
    int n;
    int m;

    /* Edge arrays (SoA) for 2*m directed edges (forward + reverse) */
    int *edge_u; /* [2*m] */
    int *edge_v;
    int *edge_f;
    int *edge_c;
    int *edge_rev;

    /* adjacency list */
    int *adj; /* packed adjacency list */
    int length;

    /* Node arrays (SoA) */
    int *node_h;
    int *node_e;
    int *node_cur;
    int *node_start;
    int *node_degree;
    unsigned char *node_is_in_queue; /* use char to avoid padding */

    int source, sink;
    int *active_queue;
    int queue_head, queue_tail, queue_size;
    pthread_mutex_t g_lock;
    pthread_cond_t cv;
    int done;
    int active_workers;
    int work_counter;
};

struct preflow_context_t
{
    graph_t *g;
    pthread_t *threads;
    pthread_barrier_t *barrier;
    thread_ctx_t *thread_ctxs;
    int threadcount;
    int result;
};

typedef struct
{
    int type; /* 0 push, 1 relabel */
    int u;    /* node id */
    int delta;
    int new_height;
} update_t;

struct thread_ctx_t
{
    graph_t *g;
    update_t *plq;
    size_t count;
    size_t tid;
    thread_ctx_t *all_thread_ctx;
    size_t all_thread_ctx_size;
    int *inqueue;      /* array of node ids */
    int inqueue_size;
    int inqueue_count;
    int inqueue_index;
    bool_t did_work;
    pthread_barrier_t *barrier;
};

/* ------------------- Memory & Helpers ------------------- */
void error(const char *fmt, ...)
{
    va_list ap; char buf[BUFSIZ];
    va_start(ap, fmt);
    vsprintf(buf, fmt, ap);
    if (progname) fprintf(stderr, "%s: ", progname);
    fprintf(stderr, "error: %s\n", buf);
    exit(1);
}

static void *xmalloc(size_t s) { void *p = malloc(s); if(!p) error("out of memory"); return p; }
static void *xcalloc(size_t n, size_t s) { void *p = calloc(n,s); if(!p) error("out of memory"); return p; }

static void *xmalloc_aligned(size_t s, size_t align)
{
    void *p = NULL;
    int rc = posix_memalign(&p, align, s);
    if (rc != 0 || p == NULL) error("posix_memalign failed");
    return p;
}

static int next_int()
{
    int x = 0, c;
    while (isspace(c = getchar())); /* skip whitespace */
    if (!isdigit(c)) return -1;
    do x = 10*x + c - '0';
    while (isdigit(c = getchar()));
    return x;
}

/* ------------------- Queue Helpers (node ids) ------------------- */
static void enter_excess_locked(graph_t *g, int v)
{
    if (v == g->source || v == g->sink) return;
    if (!g->node_is_in_queue[v])
    {
        g->node_is_in_queue[v] = 1;
        int next_tail = (g->queue_tail + 1) % g->queue_size;
        if (next_tail == g->queue_head) /* grow */
        {
            int old_size = g->queue_size;
            int new_size = old_size * 2;
            int *newq = xmalloc(new_size * sizeof(int));
            int i = 0;
            while (g->queue_head != g->queue_tail)
            {
                newq[i++] = g->active_queue[g->queue_head];
                g->queue_head = (g->queue_head + 1) % old_size;
            }
            free(g->active_queue);
            g->active_queue = newq;
            g->queue_head = 0;
            g->queue_tail = i;
            g->queue_size = new_size;
            next_tail = (g->queue_tail + 1) % g->queue_size;
            pr("Queue resized to %d\n", new_size);
        }
        g->active_queue[g->queue_tail] = v;
        g->queue_tail = next_tail;
        g->work_counter++;
        pr("[ENTER_EXCESS] Node %d entering queue, queue_head=%d, queue_tail=%d\n", v, g->queue_head, g->queue_tail);
    }
}

static void enter_excess(graph_t *g, int v)
{
    pthread_mutex_lock(&g->g_lock);
    enter_excess_locked(g, v);
    pthread_mutex_unlock(&g->g_lock);
}

static int leave_excess_locked(graph_t *g)
{
    if (g->queue_head == g->queue_tail) { pr("Queue empty\n"); return -1; }
    int vid = g->active_queue[g->queue_head];
    g->queue_head = (g->queue_head + 1) % g->queue_size;
    g->node_is_in_queue[vid] = 0;
    g->work_counter--;
    pr("Node %d leaving queue\n", vid);
    return vid;
}

static int leave_excess(graph_t *g)
{
    pthread_mutex_lock(&g->g_lock);
    int v = leave_excess_locked(g);
    pthread_mutex_unlock(&g->g_lock);
    return v;
}

/* ------------------- Push/Relabel (node ids, SoA) ------------------- */
static void _push(thread_ctx_t *ctx, int u, int v, int eid)
{
    graph_t *g = ctx->g;
    int d;

    update_t update;
    update.type = 0;

    /* compute forward/residual and apply flow update directly on SoA */
    if (u == g->edge_u[eid])
    {
        d = MIN(g->node_e[u], g->edge_c[eid] - g->edge_f[eid]);
        g->edge_f[eid] += d;
        update.u = g->edge_v[eid];
    }
    else
    {
        d = MIN(g->node_e[u], g->edge_c[eid] + g->edge_f[eid]);
        g->edge_f[eid] -= d;
        update.u = g->edge_u[eid];
    }

    g->node_e[u] -= d;

    size_t index = ctx->count;
    update.new_height = g->node_h[u];
    update.delta = d;
    ctx->plq[index] = update;
    ctx->count++;

    pr("[SCHEDULE PUSH] Thread %zu: %d -> %d delta=%d\n", ctx->tid, u, v, d);
    assert(d >= 0);
}

static void push(thread_ctx_t *ctx, int u, int v, int eid)
{
    _push(ctx, u, v, eid);
}

/* find min height among neighbors with positive residual (sequential view) */
static int _find_min_residual_cap(graph_t *g, int u)
{
    int min_h = INT_MAX;

    int start = g->node_start[u];
    int degree = g->node_degree[u];

    int *edge_u = g->edge_u;
    int *edge_v = g->edge_v;
    int *edge_c = g->edge_c;
    int *edge_f = g->edge_f;
    int *adj = g->adj;
    int *node_h = g->node_h;

    for (int k = 0; k < degree; k++)
    {
        int eid = adj[start + k];
        int eu = edge_u[eid];
        int ev = edge_v[eid];
        int ec = edge_c[eid];
        int ef = edge_f[eid];

        int is_fwd = (u == eu);
        int vid = is_fwd ? ev : eu;

        int residual = is_fwd ? (ec - ef) : (ec + ef);

        if (residual > 0)
            min_h = MIN(min_h, node_h[vid]);
    }
    return min_h;
}

static void relabel(thread_ctx_t *ctx, int u)
{
    graph_t *g = ctx->g;
    int min_h = _find_min_residual_cap(g, u);
    int h = (min_h < INT_MAX) ? min_h + 1 : g->node_h[u] + 1;

    update_t update;
    update.type = 1;
    update.u = u;
    update.delta = 0;
    update.new_height = h;
    ctx->plq[ctx->count++] = update;

    pr("[SCHEDULE RELABEL] Thread %zu: Node %d: h %d -> %d\n", ctx->tid, u, g->node_h[u], h);
}

/* ------------------- Worker ------------------- */
static void _build_update_queue(thread_ctx_t *ctx, int u)
{
    bool_t pushed = 0;
    bool_t can_push = 0;
    int v;
    int i;
    graph_t *g = ctx->g;

    pr("[BUILD] Thread %zu: Node %d start: e=%d, h=%d\n", ctx->tid, u, g->node_e[u], g->node_h[u]);

    int uid = u;
    int start = g->node_start[uid];
    int degree = g->node_degree[uid];

    int *edge_u = g->edge_u;
    int *edge_v = g->edge_v;
    int *edge_c = g->edge_c;
    int *edge_f = g->edge_f;
    int *adj = g->adj;

    for (i = g->node_cur[uid]; i < degree; i++)
    {
        int eid = adj[start + i];

        int eu = edge_u[eid];
        int ev = edge_v[eid];
        int ec = edge_c[eid];
        int ef = edge_f[eid];

        int is_fwd = (uid == eu);
        int vid = is_fwd ? ev : eu;

        int residual = is_fwd ? (ec - ef) : (ec + ef);

        v = vid;
        can_push = (g->node_h[uid] > g->node_h[v]) && (residual > 0);

        pr("[MANUAL PUSH-CHECK] Sanity check: From %d -> %d, residual %d. height u: %d, height v: %d, capacity %d\n",
           uid, v, residual, g->node_h[uid], g->node_h[v], ec);

        if (can_push)
        {
            push(ctx, uid, v, eid);
            pushed = 1;

            if (g->node_e[uid] == 0)
            {
                g->node_cur[uid] = i;
                return;
            }
        }
    }

    if (!pushed)
    {
        relabel(ctx, uid);
        g->node_cur[uid] = 0;
    }
    else if (g->node_e[uid] > 0)
    {
        g->node_cur[uid] = 0;
        size_t index = ctx->count;

        update_t update;
        update.type = 0;
        update.u = uid;
        update.delta = 0;
        update.new_height = g->node_h[uid];
        ctx->plq[index] = update;
        ctx->count++;
    }

    pr("[BUILD] Thread %zu: Node %d end: e=%d, h=%d\n", ctx->tid, uid, g->node_e[uid], g->node_h[uid]);
}

/* ----------------- Batch grab and apply helpers (SoA) ----------------- */

static int grab_excess_batch(graph_t *g, int *local_queue, int max_nodes)
{
    int count = 0;
    pthread_mutex_lock(&g->g_lock);
    while (count < max_nodes && (g->queue_head != g->queue_tail))
    {
        int vid = g->active_queue[g->queue_head];
        g->queue_head = (g->queue_head + 1) % g->queue_size;
        g->node_is_in_queue[vid] = 0;
        local_queue[count++] = vid;
        g->work_counter--;
    }
    pthread_mutex_unlock(&g->g_lock);
    return count;
}

static void _apply_updates(thread_ctx_t *tctx)
{
    int i, k;
    thread_ctx_t *all_thread_contexts = tctx->all_thread_ctx;
    size_t all_thread_contexts_size = tctx->all_thread_ctx_size;
    graph_t *g = tctx->g;

    pthread_mutex_lock(&g->g_lock);

    for (i = 0; i < (int)all_thread_contexts_size; i++)
    {
        thread_ctx_t *thread_ctx = &all_thread_contexts[i];

        for (k = 0; k < (int)thread_ctx->count; k++)
        {
            update_t update = thread_ctx->plq[k];
            if (update.type == 0)
            {
                g->node_e[update.u] += update.delta;
            }
            else if (update.type == 1)
            {
                g->node_h[update.u] = update.new_height;
                g->node_cur[update.u] = 0;
            }
            else
            {
                error("ERROR: unknown update type");
            }

            enter_excess_locked(g, update.u);
        }

        thread_ctx->count = 0;
    }

    pthread_mutex_unlock(&g->g_lock);
}

static inline void sync_and_apply(thread_ctx_t *tctx)
{
    int res = pthread_barrier_wait(tctx->barrier);
    if (res == PTHREAD_BARRIER_SERIAL_THREAD)
    {
        _apply_updates(tctx);
        if (tctx->g->work_counter == 0 && (tctx->g->queue_head == tctx->g->queue_tail))
            tctx->g->done = 1;
    }
    pthread_barrier_wait(tctx->barrier);
}

void *worker(void *arg)
{
    thread_ctx_t *tctx = (thread_ctx_t*)arg;
    graph_t *g = tctx->g;

    int i, n;
    int *local_queue = tctx->inqueue;

    while (1)
    {
        n = grab_excess_batch(g, local_queue, tctx->inqueue_size);
        if (n == 0)
        {
            sync_and_apply(tctx);
            if (g->done)
                break;
            continue;
        }
        for (i = 0; i < n; i++)
        {
            int u = local_queue[i];
            if (u != g->source && u != g->sink)
            {
                _build_update_queue(tctx, u);
            }
        }

        sync_and_apply(tctx);
        if (g->done)
            break;
    }
    return NULL;
}

/* ------------------- Graph Init (SoA) ------------------- */
static graph_t *new_graph(FILE *in, int n, int m)
{
    graph_t *g = xmalloc(sizeof(graph_t));
    g->n = n; g->m = m;

    /* allocate node arrays */
    g->node_h = xcalloc(n, sizeof(int));
    g->node_e = xcalloc(n, sizeof(int));
    g->node_cur = xcalloc(n, sizeof(int));
    g->node_start = xcalloc(n, sizeof(int));
    g->node_degree = xcalloc(n, sizeof(int));
    g->node_is_in_queue = xcalloc(n, sizeof(unsigned char));

    /* allocate SoA edge arrays for 2*m directed edges (forward+reverse) */
    int total_e = 2 * m;
    /* align large arrays for better cache behavior */
    g->edge_u   = xmalloc_aligned(total_e * sizeof(int), 128);
    g->edge_v   = xmalloc_aligned(total_e * sizeof(int), 128);
    g->edge_f   = xmalloc_aligned(total_e * sizeof(int), 128);
    memset(g->edge_f, 0, total_e * sizeof(int));
    g->edge_c   = xmalloc_aligned(total_e * sizeof(int), 128);
    g->edge_rev = xmalloc_aligned(total_e * sizeof(int), 128);

    g->source = 0; g->sink = n - 1;
    g->queue_head = 0;
    g->queue_tail = 0;
    g->queue_size = 0;
    g->work_counter = 0;

    int *deg = xcalloc(n, sizeof(int));
    int *a = xmalloc(m * sizeof(int));
    int *b = xmalloc(m * sizeof(int));
    int *carr = xmalloc(m * sizeof(int));

    pr("Creating graph with %d nodes, %d edges\n", n, m);

    for (int i = 0; i < m; i++)
    {
        a[i] = next_int(); b[i] = next_int(); carr[i] = next_int();
        pr("Edge %d: %d -> %d, capacity=%d\n", i, a[i], b[i], carr[i]);
        deg[a[i]]++; deg[b[i]]++;
    }

    int prefix = 0;
    for (int i = 0; i < n; i++)
    {
        g->node_start[i] = prefix;
        g->node_degree[i] = deg[i];
        prefix += deg[i];
        deg[i] = 0;
    }

    g->adj = xmalloc(prefix * sizeof(int));

    for (int i = 0; i < m; i++)
    {
        int u = a[i], v = b[i];
        int ei = 2 * i, eri = 2 * i + 1;
        g->edge_u[ei] = u; g->edge_v[ei] = v; g->edge_c[ei] = carr[i]; g->edge_f[ei] = 0; g->edge_rev[ei] = eri;
        g->edge_u[eri] = v; g->edge_v[eri] = u; g->edge_c[eri] = carr[i]; g->edge_f[eri] = 0; g->edge_rev[eri] = ei;
        g->adj[g->node_start[u] + deg[u]++] = ei;
        g->adj[g->node_start[v] + deg[v]++] = eri;
    }

    for (int i = 0; i < n; i++)
        pr("Node %d: start=%d, degree=%d\n", i, g->node_start[i], g->node_degree[i]);

    free(deg); free(a); free(b); free(carr);
    return g;
}

static void init_mutexes(graph_t *g){ pthread_mutex_init(&g->g_lock, NULL); pthread_cond_init(&g->cv, NULL); }
static void destroy_mutexes(graph_t *g){ pthread_mutex_destroy(&g->g_lock); pthread_cond_destroy(&g->cv); }

static void init_preflow(graph_t *g)
{
    int s = g->source;
    g->node_h[s] = g->n;
    g->node_e[s] = 0;

    for (int k = 0; k < g->node_degree[s]; k++)
    {
        int eid = g->adj[g->node_start[s] + k];
        if (g->edge_u[eid] == g->source) g->node_e[s] += g->edge_c[eid];
    }
    pr("Source node h=%d, e=%d\n", g->node_h[s], g->node_e[s]);

    for (int k = 0; k < g->node_degree[s]; k++)
    {
        int eid = g->adj[g->node_start[s] + k];
        if (g->edge_u[eid] == g->source && g->edge_c[eid] > 0)
        {
            int v = g->edge_v[eid];
            int f = g->edge_c[eid];       /* push full capacity */
            g->edge_f[eid] = f;           /* record flow */
            g->node_e[s] -= f;
            g->node_e[v] += f;            /* assign excess to neighbor */
            pr("[INIT PUSH] %d -> %d | pushed %d, v.e=%d\n", s, v, f, g->node_e[v]);
            enter_excess(g, v);
        }
    }
}

/* ------------------- Worker Init ------------------- */
static void init_workers(preflow_context_t *algo)
{
    graph_t *g = algo->g;
    pthread_t *threads = algo->threads;
    int threadcount = algo->threadcount;
    int inqueue_batch_size = 12;

    thread_ctx_t *ctxs = xcalloc(threadcount, sizeof(thread_ctx_t));
    algo->thread_ctxs = ctxs;
    g->active_workers = threadcount;
    g->done = 0;

    for (int i = 0; i < threadcount; i++)
    {
        update_t *plq = xcalloc(g->m, sizeof(update_t));
        ctxs[i].g = g;
        ctxs[i].plq = plq;
        ctxs[i].barrier = algo->barrier;
        ctxs[i].tid = i;
        ctxs[i].count = 0;
        ctxs[i].all_thread_ctx = ctxs;
        ctxs[i].all_thread_ctx_size = threadcount;
        ctxs[i].inqueue_size = inqueue_batch_size;
        ctxs[i].inqueue = xcalloc(inqueue_batch_size, sizeof(int));
        pthread_create(&threads[i], NULL, worker, &ctxs[i]);
    }
}

static void join_workers(pthread_t *threads, int threadcount){ for (int i = 0; i < threadcount; i++) pthread_join(threads[i], NULL); }
static void free_ctx(preflow_context_t *algo){ for (int i = 0; i < algo->threadcount; i++){ free(algo->thread_ctxs[i].plq); free(algo->thread_ctxs[i].inqueue);} free(algo->thread_ctxs); }

static void free_graph(graph_t *g){
    free(g->node_h);
    free(g->node_e);
    free(g->node_cur);
    free(g->node_start);
    free(g->node_degree);
    free(g->node_is_in_queue);

    free(g->edge_u);
    free(g->edge_v);
    free(g->edge_f);
    free(g->edge_c);
    free(g->edge_rev);

    free(g->adj);
    free(g->active_queue);
    free(g);
}

static void preflow(preflow_context_t *algo)
{
    graph_t *g = algo->g;
    init_mutexes(g);
    g->queue_size = 2 * g->m;
    g->active_queue = xmalloc(g->queue_size * sizeof(int));
    init_preflow(g);
    pthread_barrier_init(algo->barrier, NULL, algo->threadcount);
    init_workers(algo);
    join_workers(algo->threads, algo->threadcount);
    algo->result = g->node_e[g->sink];
    pr("Max flow computed: %d\n", algo->result);
    free_ctx(algo);
    pthread_barrier_destroy(algo->barrier);
    destroy_mutexes(g);
}

/* ------------------- Main ------------------- */
int main(int argc, char *argv[])
{
    int tc = 8;
    pthread_t threads[tc];
    pthread_barrier_t barrier;
    progname = argv[0];

    int n = next_int();
    int m = next_int();
    next_int(); next_int(); /* skip extras */
    pr("Read n=%d, m=%d\n", n, m);

    graph_t *g = new_graph(stdin, n, m);

    preflow_context_t algo;
    algo.g = g; algo.threadcount = tc; algo.threads = threads; algo.barrier = &barrier;

    preflow(&algo);
    printf("f = %d\n", algo.result);

    free_graph(g);
    return 0;
}
