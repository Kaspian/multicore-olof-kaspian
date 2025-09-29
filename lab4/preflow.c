#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <limits.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

#include "pthread_barrier.h"

/* Lab4
Idea: Make excess and residuals atomic, so pushing can be done entirely during phase 1.
Phase 2 then only has to handle relabels. Makes sequential part of the program much smaller.
*/

/* ---------------------------- */
/*  Program Constants & Types   */
/* ---------------------------- */

/* Configurations / Settings */

#define CACHE_ALIGN_POWER8 128
#define CACHE_ALIGN_X86 64
#define MAX_NODE_HEIGHT 255 /* Used for determining sizes of the integers, TODO: Make dynamic */

/* This is best on my Home Pc (Ryzen 6 core, 12 logical CPU's) */
/*
#define TC 12
#define NODES_PER_BATCH 8
*/

/* My Laptop (Ryzen 7 7730U, 8 core, 16 logical CPU's) */
#define TC 16
#define NODES_PER_BATCH 8
#define CACHE_PADDING CACHE_ALIGN_X86

/* Possible POWER8 Configs - Test
#define CACHE_ALIGN_PADDING CACHE_ALIGN_POWER8
#define TC 8
#define NODES_PER_BATCH 4
OR
#define TC 8
#define NODES_PER_BATCH 8
-- Continue Tweaking these
*/

#define PRINT 0 /* enable/disable prints. */
#define MIN(a, b) (((a) <= (b)) ? (a) : (b))
#define MAX(a, b) (((a) >= (b)) ? (a) : (b))
#define BATCH_SIZE(N, THREADCOUNT) ((N / (THREADCOUNT * NODES_PER_BATCH)) > 1 ? (N / (THREADCOUNT * NODES_PER_BATCH)) : 1)

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct list_t list_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct thread_ctx_t thread_ctx_t;
typedef struct update_t update_t;
typedef enum
{
	PUSH = 0,
	RELABEL = 1
} update_type_t;

/* ---------------------------- */
/*  Global Variables / Debugging */
/* ---------------------------- */

static char *progname;

/* ---------------------------- */
/*  Debug / Print Macros        */
/* ---------------------------- */

/* Debug Macros */
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

struct node_t
{
	uint8_t h, cur, degree;
	bool is_in_queue;
	atomic_int e;
	node_t *next;
	edge_t **edges;
} __attribute__((aligned(CACHE_PADDING))); // or 128 for POWER8

struct edge_t
{
	node_t *u;	  /* one of the two nodes.	*/
	node_t *v;	  /* the other. 			*/
	uint32_t c;	  /* capacity.			*/
	atomic_int f; /* flow > 0 if from u to v.	*/
};

struct graph_t
{
	uint32_t n;		/* nodes.			*/
	uint32_t m;		/* edges.			*/
	node_t *v;		/* array of n nodes.		*/
	edge_t *e;		/* array of m edges.		*/
	node_t *s;		/* source.			*/
	node_t *t;		/* sink.			*/
	node_t *excess; /* nodes with e > 0 except s,t.	*/

	pthread_mutex_t g_lock; /* global lock to ensure nodes coming in and out of excess pool is consistent */
	bool done;				/* algorithm done flag */
	uint8_t work_counter;
};

#if PRINT
static int id(graph_t *g, node_t *v)
{
	return v - g->v;
}
#endif

struct preflow_context_t
{
	graph_t *g;
	pthread_t *threads;
	pthread_barrier_t *barrier;
	thread_ctx_t *thread_ctxs;
	uint8_t threadcount;
	uint32_t result;					   // Will be equal to the sinks final excess
} __attribute__((aligned(CACHE_PADDING))); // or 128 for POWER8

struct update_t
{
	char type; /* type = 0 is push; type = 1 is relabel*/
	node_t *u;
	uint8_t new_height;

	graph_t *g;
} __attribute__((aligned(CACHE_PADDING))); // or 128 for POWER8

struct thread_ctx_t
{
	graph_t *g;
	update_t *plq;
	uint8_t count;
	uint8_t tid;

	thread_ctx_t *all_thread_ctx;
	uint8_t all_thread_ctx_size;

	node_t **inqueue;
	uint8_t inqueue_size; // capacity of inqueue array

	pthread_barrier_t *barrier;
} __attribute__((aligned(CACHE_PADDING)));

/* ---------------------------- */
/*  Memory/ Helper Functions    */
/* ---------------------------- */

void error(const char *fmt, ...)
{
	va_list ap;
	char buf[BUFSIZ];

	va_start(ap, fmt);
	vsprintf(buf, fmt, ap);

	if (progname != NULL)
		fprintf(stderr, "%s: ", progname);

	fprintf(stderr, "error: %s\n", buf);
	exit(1);
}

static void *xmalloc(size_t s)
{
	void *p;
	p = malloc(s);

	if (p == NULL)
		error("out of memory: malloc(%zu) failed", s);

	return p;
}

static void *xcalloc(size_t n, size_t s)
{
	void *p;
	p = xmalloc(n * s);
	memset(p, 0, n * s);

	return p;
}

static inline __attribute__((always_inline)) char next_char()
{
	static char buf[1 << 22]; // 4MB
	static int pos = 0, len = 0;
	if (pos >= len)
	{
		len = fread(buf, 1, sizeof(buf), stdin);
		pos = 0;
		if (len == 0)
			return EOF;
	}
	return buf[pos++];
}

static inline __attribute__((always_inline)) int next_int()
{
	int x = 0;
	char c;
	do
	{
		c = next_char();
		if (c == EOF)
			return -1;
	} while ((unsigned)(c - '0') > 9u);
	do
	{
		x = x * 10 + (c - '0');
		c = next_char();
	} while ((unsigned)(c - '0') <= 9u);
	return x;
}

static node_t *other(node_t *u, edge_t *e)
{
	if (u == e->u)
		return e->v;
	else
		return e->u;
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
		g->work_counter++; // increment work counter
	}
}

/* ---------------------------- */
/*  Preflow Algorithm Helpers   */
/* ---------------------------- */

static int push(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e)
{
	if (u->h != v->h + 1) // enforce valid push
		return 0;

	uint32_t u_excess = atomic_load(&u->e);
	uint32_t f = atomic_load(&e->f);
	uint32_t d = (u == e->u) ? MIN(u_excess, e->c - f)
							 : MIN(u_excess, e->c + f);

	if (u == e->u)
		atomic_fetch_add(&e->f, d);
	else
		atomic_fetch_sub(&e->f, d);

	atomic_fetch_sub_explicit(&u->e, d, memory_order_relaxed);
	atomic_fetch_add_explicit(&v->e, d, memory_order_relaxed);

	return d;
}

static inline uint16_t _find_min_residual_cap(graph_t *g, node_t *u)
{
	uint16_t min_h = UINT16_MAX;
	for (uint8_t i = u->cur; i < u->degree; i++)
	{ // start at cur
		edge_t *e = u->edges[i];
		int f = atomic_load_explicit(&e->f, memory_order_relaxed);
		int residual = (u == e->u) ? (e->c - f) : (e->c + f);
		if (residual <= 0)
			continue;

		node_t *v = (u == e->u) ? e->v : e->u;
		if ((uint16_t)(v->h + 1) == u->h)
			return v->h; // earliest possible
		if (v->h < min_h)
			min_h = v->h;
	}
	return min_h;
}

static uint8_t _relabel(int min_h, node_t *u)
{
	if (min_h < UINT8_MAX)
		return min_h + 1;
	else
		return u->h + 1;
}

static void relabel(thread_ctx_t *ctx, node_t *u)
{
	uint8_t new_h = _relabel(_find_min_residual_cap(ctx->g, u), u);

	// Initialize update struct in one go
	update_t update = {.type = RELABEL, .u = u, .new_height = new_h, .g = ctx->g};
	ctx->plq[ctx->count++] = update;
}

static void _build_update_queue(thread_ctx_t *ctx, node_t *u)
{
	uint8_t i, degree;
	uint32_t d, excess;
	edge_t *e;
	node_t *v;
	bool pushed = false;

	i = u->cur;
	degree = u->degree;
	while (i < degree)
	{
		e = u->edges[i];
		v = (u == e->u) ? e->v : e->u;
		d = push(ctx, u, v, e);

		if (d > 0)
		{
			pushed = true;
			ctx->plq[ctx->count++] = (update_t){.type = PUSH, .u = v, .g = ctx->g};
		}
		if (atomic_load_explicit(&u->e, memory_order_relaxed) == 0)
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
	else if (atomic_load_explicit(&u->e, memory_order_relaxed) > 0)
	{
		update_t update = (update_t){.type = PUSH, .u = u, .g = ctx->g};
		ctx->plq[ctx->count++] = update;
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
		for (k = 0; k < thread_ctx->count; k++)
		{
			update = thread_ctx->plq[k];
			update.u->h = (update.type == RELABEL) ? update.new_height : update.u->h;
			enter_excess(ctx->g, update.u);
		}
		thread_ctx->count = 0;
	}
}

static uint8_t grab_excess_batch(graph_t *g, node_t **local_queue, uint32_t max_nodes)
{
	uint8_t count = 0;
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

		if (n > 0)
		{
			for (i = 0; i < n; i++)
			{
				u = local_queue[i];
				if (u && u != g->s && u != g->t)
					_build_update_queue(tctx, u);
			}
		}

		// Barrier once per round
		int res = pthread_barrier_wait(tctx->barrier);
		if (res == PTHREAD_BARRIER_SERIAL_THREAD)
		{
			_apply_updates(tctx);
			if (g->work_counter == 0 && g->excess == NULL)
				g->done = 1;
		}

		pthread_barrier_wait(tctx->barrier); // ensure all see updates

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

	thread_ctx_t *g_plq = (thread_ctx_t *)xcalloc(threadcount, sizeof(thread_ctx_t));
	algo->thread_ctxs = g_plq; // <== add this
	g->done = 0;

	for (int i = 0; i < threadcount; i += 1)
	{
		// TODO: Memory Bug?
		// TODO: Way too much memory allocated.
		update_t *thread_out_q = (update_t *)xcalloc(g->n, sizeof(update_t));
		g_plq[i].g = g;
		g_plq[i].plq = thread_out_q;
		// Hacky fixa snyggt sen
		g_plq[i].barrier = algo->barrier;
		g_plq[i].tid = i;
		g_plq[i].count = 0;

		g_plq[i].all_thread_ctx = g_plq;
		g_plq[i].all_thread_ctx_size = algo->threadcount;

		// Allocate local inqueue
		g_plq[i].inqueue_size = inqueue_batch_size;
		g_plq[i].inqueue = (node_t **)xcalloc(inqueue_batch_size, sizeof(node_t *));

		if (pthread_create(&threads[i], NULL, worker, &g_plq[i]) != 0)
		{
			error("pthread_create");
			exit(1);
		}
	}
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
		g->v[i].edges = xcalloc(deg[i], sizeof(edge_t *));
		g->v[i].degree = deg[i];
		g->v[i].cur = 0;
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

		g->v[a[i]].edges[deg[a[i]]++] = e;
		g->v[b[i]].edges[deg[b[i]]++] = e;
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
		free(algo->thread_ctxs[i].plq);		// Free per-thread update queue
		free(algo->thread_ctxs[i].inqueue); // Free per-thread inqueue
	}
	free(algo->thread_ctxs); // Free thread_ctx array itself
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
		edge_t *e = s->edges[i];
		atomic_fetch_add_explicit(&s->e, e->c, memory_order_relaxed);
	}

	for (int i = 0; i < s->degree; i++)
	{
		edge_t *e = s->edges[i];
		// Inline the logic of init_push here since it's now removed
		node_t *v = other(s, e);
		uint32_t d;
		if (s == e->u)
		{
			d = MIN(atomic_load(&s->e), e->c - atomic_load(&e->f)); // plain int, no atomic
			atomic_fetch_add(&e->f, d);
		}
		else
		{
			d = MIN(atomic_load(&s->e), e->c + atomic_load(&e->f));
			atomic_fetch_sub(&e->f, d);
		}
		atomic_fetch_sub_explicit(&s->e, d, memory_order_relaxed);
		atomic_fetch_add_explicit(&v->e, d, memory_order_relaxed);
		enter_excess(g, v);
	}
}

static void setup(preflow_context_t *algo_ctx)
{
	pthread_mutex_init(&algo_ctx->g->g_lock, NULL);

	init_preflow(algo_ctx->g);
	pthread_barrier_init(algo_ctx->barrier, NULL, algo_ctx->threadcount);
	init_workers(algo_ctx);
}

static void teardown(preflow_context_t *algo_ctx)
{
	free_ctx(algo_ctx); // Now safe to free thread contexts
	pthread_barrier_destroy(algo_ctx->barrier);
	pthread_mutex_destroy(&algo_ctx->g->g_lock);
}

static void preflow(preflow_context_t *algo_ctx)
{
	setup(algo_ctx);

	for (int i = 0; i < algo_ctx->threadcount; i += 1)
		pthread_join(algo_ctx->threads[i], NULL);

	algo_ctx->result = algo_ctx->g->t->e;
	teardown(algo_ctx);
}

/* ---------------------------------- */
/*  Main 						      */
/* ---------------------------------- */
int main(int argc, char *argv[])
{
	uint8_t tc;				/* algorith threadcount */
	FILE *in;				/* input file set to stdin	*/
	graph_t *g;				/* undirected graph. 		*/
	uint32_t f;				/* output from preflow.		*/
	uint32_t n;				/* number of nodes.		*/
	uint32_t m;				/* number of edges.		*/
	preflow_context_t algo; /* algorithm context - struct wrapper for algorithm setup and attributes */

	tc = TC;
	pthread_t threads[tc]; /* array of threads */
	pthread_barrier_t barrier;

	progname = argv[0]; /* name is a string in argv[0]. */
	in = stdin;			/* same as System.in in Java.	*/

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
See: https://courses.csail.mit.edu/6.884/spring10/projects/viq_velezj_maxflowreport.pdf
*/
