#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <string.h>

/* ---------------------------- */
/*  Program Constants & Types   */
/* ---------------------------- */

/* Configuration Macros */
#define CACHE_ALIGN_POWER8 128
#define CACHE_ALIGN_X86 64
#define CACHE_ALIGN_POWER8_SMALL 16

/* This is best on my Home Pc (Ryzen 6 core, 12 logical CPU's) */
#define TC 6
#define NODES_PER_BATCH 16
#define CACHE_PADDING CACHE_ALIGN_POWER8

#define PRINT 0 /* enable/disable prints. */
#define MIN(a, b) (((a) <= (b)) ? (a) : (b))
#define MAX(a, b) (((a) >= (b)) ? (a) : (b))
#define BATCH_SIZE(N, THREADCOUNT) ((N / (THREADCOUNT * NODES_PER_BATCH)) > 1 ? (N / (THREADCOUNT * NODES_PER_BATCH)) : 1)
#define BUFSIZE (1 << 16)

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct adj_t adj_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct thread_ctx_t thread_ctx_t;
typedef struct update_t update_t;

typedef struct xedge_t xedge_t;
struct xedge_t {
	int		u;	/* one of the two nodes.	*/
	int		v;	/* the other. 			*/
	int		c;	/* capacity.			*/
};

typedef enum
{
	PUSH = 0,
	RELABEL = 1
} update_type_t;

/* ---------------------------- */
/*  Debug / Print Macros        */
/* ---------------------------- */

#if PRINT
#define pr(...)                       \
	do                                \
	{                                 \
		fprintf(stderr, __VA_ARGS__); \
	} while (0)
#else
#define pr(...) /* no effect at all */
#endif

/* ---------------------------- */
/*  Algorithm Structs           */
/* ---------------------------- */

struct adj_t
{
	edge_t *e;
	node_t *neighbor;
	int8_t dir;
};

struct edge_t
{
	node_t *u;	  /* one of the two nodes.	*/
	node_t *v;	  /* the other. 			*/
	int c;	  /* capacity.			*/
	atomic_int f; /* flow > 0 if from u to v.	*/
};

struct node_t
{
	int id;
	int h, cur, degree;
	bool is_in_queue;
	atomic_int e;
	node_t *next;
	adj_t *edges;
} __attribute__((aligned(CACHE_PADDING)));

struct graph_t
{
	int n;		/* nodes.			*/
	int m;		/* edges.			*/
	node_t *v;		/* array of n nodes.		*/
	edge_t *e;		/* array of m edges.		*/
	node_t *s;		/* source.			*/
	node_t *t;		/* sink.			*/
	node_t *excess; /* nodes with e > 0 except s,t.	*/

	pthread_mutex_t g_lock;
	bool done; /* algorithm done flag */
	int work_counter;
};

struct update_t
{
	char type; /* type = 0 is push; type = 1 is relabel*/
	node_t *u;
	int delta;
	int new_height;
	graph_t *g;
} __attribute__((aligned(CACHE_PADDING)));

struct preflow_context_t
{
	graph_t *g;
	pthread_t *threads;
	pthread_barrier_t barrier;
	thread_ctx_t *thread_ctxs;
	int threadcount;
	int result;
} __attribute__((aligned(CACHE_PADDING)));

struct thread_ctx_t
{
	graph_t *g;
	update_t *pending_updates;
	int pending_updates_size;

	thread_ctx_t *all_thread_ctx;
	int all_thread_ctx_size;

	node_t **inqueue;
	int inqueue_size; // capacity of inqueue array

	pthread_barrier_t *barrier;
} __attribute__((aligned(CACHE_PADDING)));

/* ---------------------------- */
/*  Memory/ Helper Functions    */
/* ---------------------------- */

static void *xmalloc(size_t s)
{
	void *p = malloc(s);
	if (p == NULL)
	{
		fprintf(stderr, "error: out of memory: malloc(%zu) failed\n", s);
		exit(1);
	}
	return p;
}

static void *xcalloc(size_t n, size_t s)
{
	void *p = calloc(n, s);
	if (p == NULL)
	{
		fprintf(stderr, "error: out of memory: calloc(%zu,%zu) failed\n", n, s);
		exit(1);
	}
	return p;
}

/* Fast integer input */
static char buf[BUFSIZE];
static int idx = 0, size = 0;

static char read_char()
{
	if (idx >= size)
	{
		size = fread(buf, 1, BUFSIZE, stdin);
		idx = 0;
		if (size == 0)
			return EOF;
	}
	return buf[idx++];
}

static int next_int()
{
	int x = 0;
	char c = 0;

	do
	{
		c = read_char();
		if (c == EOF)
			return EOF;
	} while (c < '0' || c > '9');

	do
	{
		x = x * 10 + (c - '0');
		c = read_char();
	} while (c >= '0' && c <= '9');

	return x;
}

/* ---------------------------- */
/*  Global Queue Helpers        */
/* ---------------------------- */

static void enter_excess(graph_t *g, node_t *v)
{
	if (!v->is_in_queue)
	{
		v->is_in_queue = 1;
		v->next = g->excess;
		g->excess = v;
		g->work_counter++;
	}
}

static void init_push(graph_t *g, node_t *u, node_t *v, edge_t *e)
{
	//pr("push from %d to %d: ", (int)(u->id), (int)(v->id));
	//pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);

	int d;
	if (u == e->u)
	{
		d = MIN(atomic_load(&u->e), e->c - atomic_load(&e->f));
		atomic_fetch_add(&e->f, d);
	}
	else
	{
		d = MIN(atomic_load(&u->e), e->c + atomic_load(&e->f));
		atomic_fetch_sub(&e->f, d);
	}

	//pr("pushing %d\n", d);

	atomic_fetch_sub(&u->e, d);
	atomic_fetch_add(&v->e, d);

	enter_excess(g, v);
}

/* ---------------------------- */
/*  Preflow Algorithm Helpers   */
/* ---------------------------- */

static int push(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e, int8_t dir)
{
	if (u->h != v->h + 1) // enforce valid push
		return 0;

	int u_excess = atomic_load(&u->e);
	int f = atomic_load(&e->f);
	int d = MIN(u_excess, (e->c - (dir * f)));

	atomic_fetch_add(&e->f, dir * (int32_t)d);
	atomic_fetch_sub(&u->e, d);
	atomic_fetch_add(&v->e, d);

// #ifdef PRINT
// 	if (d != 0)
// 	{
// 		pr("push from %d to %d: ", (int)(u->id), (int)(v->id));
// 		pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);
// 		pr("pushing %d\n", d);
// 	}
// #endif

	return d;
}

static int find_min_residual_cap(graph_t *g, node_t *u)
{
    int i, residual, local_min;
    adj_t *a;
    edge_t *e;
    node_t *v;

    // Initialize the local min to a large value
    local_min = INT32_MAX;
	
    for (i = 0; i < u->degree; i++) {
        a = &u->edges[i];
        e = a->e;
        v = a->neighbor;

        residual = e->c - (a->dir * atomic_load_explicit(&e->f, memory_order_acquire));
        
        // Update the local min based on the condition
        if (residual > 0) {
            local_min = MIN(local_min, v->h);
        }

        // Print debug information
        pr("Thread %d: Processing edge %d with residual %d (local_min = %d)\n", omp_get_thread_num(), i, residual, local_min);

    }

    // Return the final reduced min value
    return local_min;
}

static void relabel(thread_ctx_t *ctx, node_t *u)
{
	int min_h = find_min_residual_cap(ctx->g, u);
	int new_h = (min_h < INT32_MAX) ? (min_h + 1) : (u->h + 1);
	//int new_h = (min_h < INT32_MAX) ? (min_h + (u->degree / 2) + 1) : (u->h + 1);
	update_t update = {
		.type = RELABEL,
		.u = u,
		.delta = 0,
		.new_height = new_h,
		.g = ctx->g};
	ctx->pending_updates[ctx->pending_updates_size++] = update;
}

static void build_update_queue(thread_ctx_t *ctx, node_t *u)
{
int i, degree;
int d;
adj_t *a;
edge_t *e;
node_t *v;
bool pushed = false;

i = u->cur;
degree = u->degree;
	while (i < degree)
	{
		a = &u->edges[i];
		e = a->e;
		v = a->neighbor;

		d = push(ctx, u, v, e, a->dir);

		if (d > 0)
		{
			pushed = true;
			ctx->pending_updates[ctx->pending_updates_size++] = (update_t){
				.type = PUSH,
				.u = v,
				.delta = d,
				.g = ctx->g};
		}

		if (atomic_load(&u->e) == 0)
		{
			u->cur = i;  // Preserve current arc index
			return;
		}

		++i;
	}

	if (!pushed)
	{
		relabel(ctx, u);  // Relabel only if no push occurred
		u->cur = 0; // Reset arc index after relabel
	}
	else if (atomic_load(&u->e) > 0)
	{
		ctx->pending_updates[ctx->pending_updates_size++] =
			(update_t){.type = PUSH, .u = u, .delta = 0, .g = ctx->g};
	}
}

/* ---------------------------- */
/*  Workers / Thread Functions  */
/* ---------------------------- */

static void _apply_updates(thread_ctx_t *ctx)
{
	int i, k, all_thread_contexts_size;
	thread_ctx_t *all_thread_contexts;
	thread_ctx_t *thread_ctx;
	update_t update;
	all_thread_contexts = ctx->all_thread_ctx;
	all_thread_contexts_size = ctx->all_thread_ctx_size;

	for (i = 0; i < all_thread_contexts_size; i++)
	{
		thread_ctx = &all_thread_contexts[i];
		for (k = 0; k < thread_ctx->pending_updates_size; k++)
		{
			update = thread_ctx->pending_updates[k];
			if (update.type == RELABEL)
				update.u->h = update.new_height;
			enter_excess(ctx->g, update.u);
		}
		thread_ctx->pending_updates_size = 0;
	}
}

static int grab_excess_batch(graph_t *g, node_t **local_queue, int max_nodes)
{
	int count = 0;
	node_t *v;

	pthread_mutex_lock(&g->g_lock);
	while (count < max_nodes && g->excess)
	{
		v = g->excess;
		g->excess = v->next;
		v->next = NULL;
		v->is_in_queue = 0;

		local_queue[count++] = v;
		g->work_counter--;
	}
	pthread_mutex_unlock(&g->g_lock);

	return count; // number of nodes grabbed
}

static inline void sync_and_apply(thread_ctx_t *tctx)
{
	int res = pthread_barrier_wait(tctx->barrier);
	if (res == PTHREAD_BARRIER_SERIAL_THREAD)
	{
		_apply_updates(tctx);
		if (tctx->g->work_counter == 0 && tctx->g->excess == NULL)
			tctx->g->done = 1;
	}
	pthread_barrier_wait(tctx->barrier);
}

void *worker(void *arg)
{
	thread_ctx_t *tctx = (thread_ctx_t *)arg;
	graph_t *g = tctx->g;

	int i, n;
	node_t **local_queue = tctx->inqueue;
	memset(local_queue, 0, sizeof(node_t*) * tctx->inqueue_size);
	node_t *u;

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

		for (i = 0; i < n; i += 1)
		{
			u = local_queue[i];
			if (u && u != g->s && u != g->t)
			{
				build_update_queue(tctx, u);
			}
		}

		sync_and_apply(tctx);
		if (g->done)
			break;
	}
	return NULL;
}

static void init_workers(preflow_context_t *algo)
{
	graph_t *g = algo->g;
	pthread_t *threads = algo->threads;
	int threadcount = algo->threadcount;
	int inqueue_batch_size = BATCH_SIZE(g->n, threadcount);

	thread_ctx_t *g_pending_updates = (thread_ctx_t *)xcalloc(threadcount, sizeof(thread_ctx_t));
	algo->thread_ctxs = g_pending_updates;
	g->done = 0;

	for (int i = 0; i < threadcount; i += 1)
	{
		update_t *thread_out_q = (update_t *)xcalloc(g->m, sizeof(update_t));
		g_pending_updates[i].g = g;
		g_pending_updates[i].pending_updates = thread_out_q;
		g_pending_updates[i].pending_updates_size = 0;

		g_pending_updates[i].all_thread_ctx = g_pending_updates;
		g_pending_updates[i].all_thread_ctx_size = algo->threadcount;

		// Allocate local inqueue
		g_pending_updates[i].inqueue_size = inqueue_batch_size;
		g_pending_updates[i].inqueue = (node_t **)xcalloc(inqueue_batch_size, sizeof(node_t *));

		g_pending_updates[i].barrier = &algo->barrier;

		if (pthread_create(&threads[i], NULL, worker, &g_pending_updates[i]) != 0)
		{
			fprintf(stderr, "error: pthread_create failed\n");
			exit(1);
		}
	}
}

static void join_workers(preflow_context_t *algo)
{
	int i;
	int tc = algo->threadcount;
	for (i = 0; i < tc; i++)
		pthread_join(algo->threads[i], NULL);
}

/* ---------------------------------- */
/*  Preflow Algorithm Initialization  */
/* ---------------------------------- */

static graph_t* new_graph(int n, int m, int s, int t, xedge_t* xedges) {
	(void)s;
	(void)t;
    graph_t* g = xmalloc(sizeof(graph_t));
    g->n = n;
    g->m = m;

    g->v = xcalloc(n, sizeof(node_t));
    g->e = xcalloc(m, sizeof(edge_t));
    g->s = &g->v[s];
    g->t = &g->v[t];
	g->excess = NULL;

    int* deg = xcalloc(n, sizeof(int));

    // Count degrees for adjacency allocation
    for (int i = 0; i < m; i++) {
        deg[xedges[i].u]++;
        deg[xedges[i].v]++;
    }

    // Allocate adjacency arrays
    for (int i = 0; i < n; i++) {
        g->v[i].edges = xcalloc(deg[i], sizeof(adj_t));
        g->v[i].degree = deg[i];
        g->v[i].cur = 0;
        g->v[i].id = i;
        atomic_init(&g->v[i].e, 0);
        deg[i] = 0; // reset for insertion
    }

    // Create edge structs and fill adjacency arrays
    for (int i = 0; i < m; i++) {
        int u_idx = xedges[i].u;
        int v_idx = xedges[i].v;
        int c = xedges[i].c;

        edge_t* e = &g->e[i];
        e->u = &g->v[u_idx];
        e->v = &g->v[v_idx];
        e->c = c;
        atomic_init(&e->f, 0);

        g->v[u_idx].edges[deg[u_idx]].e = e;
        g->v[u_idx].edges[deg[u_idx]].neighbor = &g->v[v_idx];
        g->v[u_idx].edges[deg[u_idx]].dir = 1;
        deg[u_idx]++;

        g->v[v_idx].edges[deg[v_idx]].e = e;
        g->v[v_idx].edges[deg[v_idx]].neighbor = &g->v[u_idx];
        g->v[v_idx].edges[deg[v_idx]].dir = -1;
        deg[v_idx]++;
    }

    free(deg);
    return g;
}

static void free_ctx(preflow_context_t *algo)
{
	int i;
	for (i = 0; i < algo->threadcount; i += 1)
	{
		free(algo->thread_ctxs[i].pending_updates);
		free(algo->thread_ctxs[i].inqueue);
	}
	free(algo->thread_ctxs);
}

static void free_graph(graph_t *g)
{
	for (int i = 0; i < g->n; i++)
	{
		free(g->v[i].edges);
	}
	free(g->v);
	free(g->e);
	free(g);
}

static void init_preflow(graph_t *g)
{
	node_t *s = g->s;

	s->h = g->n;
	atomic_store(&s->e, 0);
	g->work_counter = 0;

	for (int i = 0; i < s->degree; i++)
	{
		adj_t *a = &s->edges[i];
		edge_t *e = a->e;
		atomic_fetch_add(&s->e, e->c);
	}

	for (int i = 0; i < s->degree; i++)
	{
		adj_t *a = &s->edges[i];
		edge_t *e = a->e;

		/* other */
		node_t *nbr = (s == e->u) ? e->v : e->u;
		init_push(g, s, nbr, e);
	}
}

static void setup(preflow_context_t *algo_ctx)
{
	pthread_mutex_init(&algo_ctx->g->g_lock, NULL);

	init_preflow(algo_ctx->g);
	pthread_barrier_t barrier;
	algo_ctx->barrier = barrier;
	pthread_barrier_init(&algo_ctx->barrier, NULL, algo_ctx->threadcount);
}

static void teardown(preflow_context_t *algo_ctx)
{
	free_ctx(algo_ctx);
	pthread_barrier_destroy(&algo_ctx->barrier);
	pthread_mutex_destroy(&algo_ctx->g->g_lock);
}


static void _preflow(preflow_context_t *algo_ctx)
{
	setup(algo_ctx);
	init_workers(algo_ctx);
	join_workers(algo_ctx);

	algo_ctx->result = atomic_load(&algo_ctx->g->t->e);

	teardown(algo_ctx);
}


int preflow(int n, int m, int s, int t, xedge_t* xedges)
{
    graph_t* g = new_graph(n, m, s, t, xedges);

    preflow_context_t algo_ctx;
    //pthread_barrier_t *barrier = malloc(sizeof(pthread_barrier_t));

    pthread_t *threads = malloc(sizeof(pthread_t) * TC);
    if (!threads)
    {
        fprintf(stderr, "error: malloc failed for threads\n");
        exit(1);
    }

    algo_ctx.g = g;
    algo_ctx.threadcount = TC;
    algo_ctx.threads = threads;

    // 3. Run the old preflow logic
    _preflow(&algo_ctx);

    // 4. Retrieve result
    int result = algo_ctx.result;

    // 5. Teardown and cleanup
    free_graph(g);
	free(threads);

    return result;
}

/* ---------------------------------- */
/*  Main 						      */
/* ---------------------------------- */

int main(int argc, char *argv[])
{
	(void)argc;
	(void)argv;
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

/*
- Use BFS / Relabel wave (?) to make relabelling faster.
*/
