#if defined(__powerpc__)
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include "pthread_barrier.h"

/* ---------------------------- */
/*  Program Constants & Types   */
/* ---------------------------- */

/* Configuration Macros */
#define CACHE_ALIGN_POWER8 128
#define CACHE_ALIGN_X86 64

/* This is best on my Home Pc (Ryzen 6 core, 12 logical CPU's) */
#define TC 6
#define NODES_PER_BATCH 10
#define CACHE_PADDING CACHE_ALIGN_X86

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
	uint32_t c;	  /* capacity.			*/
	atomic_int f; /* flow > 0 if from u to v.	*/
};

struct node_t
{
	uint32_t id;
	uint8_t h, cur, degree;
	bool is_in_queue;
	atomic_int e;
	node_t *next;
	adj_t *edges;
} __attribute__((aligned(CACHE_PADDING)));

struct graph_t
{
	uint32_t n;		/* nodes.			*/
	uint32_t m;		/* edges.			*/
	node_t *v;		/* array of n nodes.		*/
	edge_t *e;		/* array of m edges.		*/
	node_t *s;		/* source.			*/
	node_t *t;		/* sink.			*/
	node_t *excess; /* nodes with e > 0 except s,t.	*/

	pthread_mutex_t g_lock;
	bool done; /* algorithm done flag */
	uint8_t work_counter;
};

struct update_t
{
	char type; /* type = 0 is push; type = 1 is relabel*/
	node_t *u;
	uint32_t delta;
	uint8_t new_height;
	graph_t *g;
} __attribute__((aligned(CACHE_PADDING)));

struct preflow_context_t
{
	graph_t *g;
	pthread_t *threads;
	pthread_barrier_t *barrier;
	thread_ctx_t *thread_ctxs;
	uint8_t threadcount;
	uint32_t result;
} __attribute__((aligned(CACHE_PADDING)));

struct thread_ctx_t
{
	graph_t *g;
	update_t *pending_updates;
	uint8_t pending_updates_size;

	thread_ctx_t *all_thread_ctx;
	uint8_t all_thread_ctx_size;

	node_t **inqueue;
	uint8_t inqueue_size; // capacity of inqueue array

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

static uint32_t next_int()
{
	uint32_t x = 0;
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
	pr("push from %d to %d: ", (int)(u->id), (int)(v->id));
	pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);

	uint32_t d;
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

	pr("pushing %d\n", d);

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

	uint32_t u_excess = atomic_load(&u->e);
	uint32_t f = atomic_load(&e->f);
	uint32_t d = MIN(u_excess, (e->c - (dir * f)));

	atomic_fetch_add(&e->f, dir * (int32_t)d);
	atomic_fetch_sub(&u->e, d);
	atomic_fetch_add(&v->e, d);

#ifdef PRINT
	if (d != 0)
	{
		pr("push from %d to %d: ", (int)(u->id), (int)(v->id));
		pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);
		pr("pushing %d\n", d);
	}
#endif

	return d;
}

static uint8_t find_min_residual_cap(graph_t *g, node_t *u)
{
	uint8_t i, residual, min_h;
	adj_t *a;
	edge_t *e;
	node_t *v;

	min_h = UINT8_MAX;
	for (i = 0; i < u->degree; i++)
	{
		a = &u->edges[i];
		e = a->e;
		v = a->neighbor;
		residual = e->c - (a->dir * atomic_load_explicit(&e->f, memory_order_relaxed));
		if (residual > 0)
			min_h = MIN(min_h, v->h);
	}
	return min_h;
}

static void relabel(thread_ctx_t *ctx, node_t *u)
{
	uint8_t min_h = find_min_residual_cap(ctx->g, u);
	uint8_t new_h = (min_h < UINT8_MAX) ? (min_h + 1) : (u->h + 1);

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
	uint8_t i, degree;
	uint32_t d;
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
			u->cur = i;
			return;
		}

		++i;
	}

	if (!pushed)
	{
		relabel(ctx, u);
		u->cur = 0; // reset arc index after relabel
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
	uint8_t i, k, all_thread_contexts_size;
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

static uint8_t grab_excess_batch(graph_t *g, node_t **local_queue, uint32_t max_nodes)
{
	uint8_t count = 0;
	node_t *v;
	__transaction_atomic
	{
		while (count < max_nodes && g->excess)
		{
			v = g->excess;
			g->excess = v->next;
			v->next = NULL;
			v->is_in_queue = 0;

			local_queue[count++] = v;
			g->work_counter--;
		}
	}
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

	uint8_t i, n;
	node_t *local_queue[tctx->inqueue_size]; // reuse preallocated inqueue
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

		g_pending_updates[i].barrier = algo->barrier;

		if (pthread_create(&threads[i], NULL, worker, &g_pending_updates[i]) != 0)
		{
			fprintf(stderr, "error: pthread_create failed\n");
			exit(1);
		}
	}
}

static void join_workers(preflow_context_t *algo)
{
	uint8_t i;
	uint8_t tc = algo->threadcount;
	for (i = 0; i < tc; i++)
		pthread_join(algo->threads[i], NULL);
}

/* ---------------------------------- */
/*  Preflow Algorithm Initialization  */
/* ---------------------------------- */

static graph_t *new_graph(FILE *in, int n, int m)
{
	graph_t *g = xmalloc(sizeof(graph_t));
	g->n = n;
	g->m = m;
	g->v = xcalloc(n, sizeof(node_t));
	g->e = xcalloc(m, sizeof(edge_t));
	g->s = &g->v[0];
	g->t = &g->v[n - 1];
	g->excess = NULL;

	// read edges and count degrees
	int *deg = xcalloc(n, sizeof(int));
	int *a = xmalloc(m * sizeof(int));
	int *b = xmalloc(m * sizeof(int));
	int *c = xmalloc(m * sizeof(int));

	for (int i = 0; i < m; i++)
	{
		a[i] = next_int();
		b[i] = next_int();
		c[i] = next_int();
		deg[a[i]]++;
		deg[b[i]]++;
	}

	// allocate adjacency arrays
	for (int i = 0; i < n; i++)
	{
		g->v[i].edges = xcalloc(deg[i], sizeof(adj_t));
		g->v[i].degree = deg[i];
		g->v[i].cur = 0;
		g->v[i].id = i;
		deg[i] = 0; // reset for filling
		atomic_init(&g->v[i].e, 0);
	}

	// create edge structs and hook into arrays
	for (int i = 0; i < m; i++)
	{
		edge_t *e = &g->e[i];
		e->u = &g->v[a[i]];
		e->v = &g->v[b[i]];
		e->c = c[i];
		atomic_init(&e->f, 0);

		g->v[a[i]].edges[deg[a[i]]].e = e;
		g->v[a[i]].edges[deg[a[i]]].neighbor = &g->v[b[i]];
		g->v[a[i]].edges[deg[a[i]]].dir = 1;
		deg[a[i]]++;

		g->v[b[i]].edges[deg[b[i]]].e = e;
		g->v[b[i]].edges[deg[b[i]]].neighbor = &g->v[a[i]];
		g->v[b[i]].edges[deg[b[i]]].dir = -1;
		deg[b[i]]++;
	}

	free(deg);
	free(a);
	free(b);
	free(c);

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
	pthread_barrier_init(algo_ctx->barrier, NULL, algo_ctx->threadcount);
}

static void teardown(preflow_context_t *algo_ctx)
{
	free_ctx(algo_ctx);
	pthread_barrier_destroy(algo_ctx->barrier);
	pthread_mutex_destroy(&algo_ctx->g->g_lock);
}

static void preflow(preflow_context_t *algo_ctx)
{
	setup(algo_ctx);
	init_workers(algo_ctx);
	join_workers(algo_ctx);

	algo_ctx->result = atomic_load(&algo_ctx->g->t->e);

	teardown(algo_ctx);
}

/* ---------------------------------- */
/*  Main 						      */
/* ---------------------------------- */
int main(int argc, char *argv[])
{
	uint8_t tc; /* algorith threadcount */
	FILE *in;	/* input file set to stdin	*/
	graph_t *g; /* undirected graph. 		*/
	uint32_t f; /* output from preflow.		*/
	uint32_t n; /* number of nodes.		*/
	uint32_t m; /* number of edges.		*/
	preflow_context_t algo;

	tc = TC;
	pthread_t threads[tc];
	pthread_barrier_t barrier;

	in = stdin;

	n = next_int();
	m = next_int();

	/* skip C and P from the 6railwayplanning lab in EDAF05 */
	next_int();
	next_int();

	g = new_graph(in, n, m);

	fclose(in);

	algo.g = g;
	algo.threadcount = tc;
	algo.threads = threads;
	algo.barrier = &barrier;

	preflow(&algo);
	f = algo.result;

	printf("f = %d\n", f);

	free_graph(g);

	return 0;
}

/*
- Use BFS / Relabel wave (?) to make relabelling faster.
*/