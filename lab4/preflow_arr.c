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

#include "pthread_barrier.h"

/* ---------------------------- */
/*  Program Constants & Types   */
/* ---------------------------- */

#define PRINT 0 /* enable/disable prints. */
#define MIN(a, b) (((a) <= (b)) ? (a) : (b))
#define MAX(a, b) (((a) >= (b)) ? (a) : (b))

typedef int bool_t;

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct list_t list_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct thread_ctx_t thread_ctx_t;
typedef struct node_header_t node_header_t;
typedef struct update_t update_t;
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

#define MAX_NODES_COUNT 1000
#define NEIGHBOR_FRAME_COUNT 15/* TODO: Adjust this later, or have it be set dynamically */
#define PACKET_SIZE 64
#define SZ(x) PACKET_SIZE*x;

#define FRAME_SIZE SZ(NEIGHBOR_FRAME_COUNT) + SZ(1);
/* Structure for whole graph is:
 * [[FRAME_PACKET], [FRAME_PACKET]... ]
 *
 * With FP:s having the internal structure:
 * {[NODE_HEADER_T], [NEIGHOR_FRAME], [NEIGHBOR_FRAME]...}
 * 
 * The width of the FP should be: SZ(NEIGHBOR_FRAME_COUNT) + SZ(1)
 * */

struct node_header_t /* 64 Bytes Node Frame Header */
{
  /* Enforce atomicity? */
  uint32_t e;
  uint32_t h;
  uint32_t nbr_of_neighbors;
  uint32_t g_idx;
};

struct node_neighbor_packet_t /* 64 Bytes Neighbor Frame Packet */
{
  uint32_t flow_to; /* Enforce this front of the struct, maybe not necessary */
  uint32_t neighbor_h;
  uint32_t neighbor_g_idx;
  uint32_t edge_capacity; /* Maybe useful later */
};

struct node_t /* Size is FRAME_SIZE defined above, make sure cache aligned */
{
  node_header_t node_header;
  node_neighbor_packet_t neighbors[NEIGHBOR_FRAME_COUNT];
};

struct graph_t
{
  uint32_t n;
  uint32_t m;
  node_frame_t *nodes; /* Size: FRAME_SIZE(MAX_NODES_COUNT) */
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
	int threadcount;
	int result; // Will be equal to the sinks final excess
};

/* These updates are to be shuffled into a separate list for any changes that can't be added to the local slice / is part of nodes outside the threads access */
struct update_t
{
  uint32_t g_idx;
  uint32_t val;
};

struct thread_ctx_t
{
  /* This is a slice of the global array of all nodes */
  node_frame_t *nodes_ptr;
  size_t start_slice;
  size_t end_slice;
	update_t *oob_updates; /* How to make this storing of updates efficienctly? TODO */
	size_t oob_updates_count;

  /* TODO: Re-visit termination logic */
	size_t tid;
	thread_ctx_t *all_thread_ctx;
	size_t all_thread_ctx_size;
	bool_t did_work;

	pthread_barrier_t *barrier;
};

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

static int next_int()
{
	int x;
	int c;

	x = 0;
	while (isdigit(c = getchar()))
		x = 10 * x + c - '0';

	return x;
}

/* ---------------------------- */
/*  Global Queue Helpers        */
/* ---------------------------- */

static void init_push(graph_t* g)
{
  /* Function is REQUIRED, but no code is like it was before.
   * Rewrite the whole thing.
  */
}

/* ---------------------------- */
/*  Preflow Algorithm Helpers   */
/* ---------------------------- */

static void push(node_frame_t *graph_slice, size_t idx)
{
  /* Function is REQUIRED, but no code is like it was before.
   * Rewrite the whole thing.
  */
}

static int find_min_residual_cap(thread_ctx_t *ctx, size_t idx)
{
  /* Function is REQUIRED, but no code is like it was before.
   * Rewrite the whole thing.
  */
}

static int relabel(int min_h, node_t *u)
{
	if (min_h < INT_MAX)
		return min_h + 1;
	else
		return u->h + 1;
}

static void relabel(thread_ctx_t *ctx, node_t *u)
{
	graph_t *g = ctx->g;
	int min_h = _find_min_residual_cap(g, u);
	int h = _relabel(min_h, u);

	update_t update;
	update.type = 1; // 1 == RELABEL
	update.u = u;
	update.delta = 0; // delta not needed for relabel?
	update.new_height = h;

	ctx->plq[ctx->count++] = update;

	/* CONTINUE FROM HERE LATER */

	// enter_excess(g, u);
}

static void _build_update_queue(thread_ctx_t *ctx, node_t *u)
{
	bool_t pushed = 0;
	bool_t can_push = 0;
	list_t *p;
	edge_t *e;
	node_t *v;
	int b;

	for (int i = u->cur; i < u->degree; i++)
	{
		edge_t *e = u->edges[i];
		node_t *v = (u == e->u) ? e->v : e->u;
		int b = (u == e->u) ? 1 : -1;

		bool_t can_push = (u->h > v->h) && (b * e->f < e->c);

		if (can_push)
		{
			push(ctx, u, v, e);
			pushed = 1;

			if (u->e == 0)
			{
				u->cur = i; // resume from here next time
				return;
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

		size_t index = ctx->count;

		update_t update;
		update.type = 0;
		update.u = u;
		update.delta = 0;
		update.new_height = u->h;
		ctx->plq[index] = update;
		ctx->count++;
	}
}

/* ---------------------------- */
/*  Workers / Thread Functions  */
/* ---------------------------- */
static int get_opt_thread_count(void)
{
	long nprocs = sysconf(_SC_NPROCESSORS_ONLN); /* POSIX, so won't work on every device */
	if (nprocs > 0)
	{
		pr("Detecting max online threads to use: %d threads", (int)nprocs);
		return (int)nprocs;
	}
	else
	{
		pr("_SC_NPROCESSORS_ONLN macro unavailable, fallback to using 4 threads");
		return 4;
	}
}

static void _apply_updates(thread_ctx_t *ctx)
{
	int i;
	int k;
	thread_ctx_t *all_thread_contexts = ctx->all_thread_ctx;
	size_t all_thread_contexts_size = ctx->all_thread_ctx_size;

	// Acquire the global graph lock once for all updates
	pthread_mutex_lock(&ctx->g->g_lock);

	for (i = 0; i < all_thread_contexts_size; i++)
	{
		thread_ctx_t *thread_ctx = &all_thread_contexts[i];
		update_t update;

		for (k = 0; k < thread_ctx->count; k++)
		{
			update = thread_ctx->plq[k];
			if (update.type == 0)
			{
				pr("We are updating %d, with delta: %d", id(thread_ctx->g, update.u), update.delta);
				update.u->e += update.delta;
			}
			else if (update.type == 1)
			{
				update.u->h = update.new_height;
				update.u->cur = 0; // reset current arc after relabel
			}
			else
			{
				error("ERROR");
			}

			enter_excess_locked(ctx->g, update.u);
		}
		thread_ctx->count = 0;
	}

	// Release the global graph lock after all updates
	pthread_mutex_unlock(&ctx->g->g_lock);
}

void *worker(void *arg)
{
	thread_ctx_t *tctx = (thread_ctx_t *)arg;
	graph_t *g = tctx->g;

	node_t *u = NULL;
	while (1)
	{
		tctx->inqueue_count = 0;
		tctx->inqueue_index = 0;

		pthread_mutex_lock(&g->g_lock);
		for (int i = 0; i < tctx->inqueue_size; i++)
		{
			node_t *n = leave_excess_locked(g);
			if (!n)
				break;
			tctx->inqueue[tctx->inqueue_count++] = n;
		}
		pthread_mutex_unlock(&g->g_lock);

		bool_t done = g->done;

		if (done == 1)
		{
			pr("The loop finishes.");
			break;
		}

		bool_t local_work = 0;

		while (tctx->inqueue_index < tctx->inqueue_count)
		{
			node_t *u = tctx->inqueue[tctx->inqueue_index++];
			if (u)
			{
				local_work = 1;
				_build_update_queue(tctx, u);
			}
		}
		tctx->did_work = local_work;

		int res = pthread_barrier_wait(tctx->barrier);
		if (res == PTHREAD_BARRIER_SERIAL_THREAD)
		{
			bool_t algo_done = 1;
			for (int i = 0; i < tctx->all_thread_ctx_size; i++)
			{
				if (tctx->all_thread_ctx[i].did_work != 0)
				{
					algo_done = 0;
					break;
				}
			}
			if (algo_done && g->excess == NULL)
			{
				g->done = 1;
			}
			else
			{
				_apply_updates(tctx);
			}
		}
		// second sync so all threads see updated g->done
		pthread_barrier_wait(tctx->barrier);
	}
	return NULL;
}

static void init_workers(preflow_context_t *algo)
{
	graph_t *g = algo->g;
	pthread_t *threads = algo->threads;
	int threadcount = algo->threadcount;
	int inqueue_batch_size = MAX(g->n / 8, 1);

	thread_ctx_t *g_plq = (thread_ctx_t *)xcalloc(threadcount, sizeof(thread_ctx_t));
	algo->thread_ctxs = g_plq; // <== add this
	g->active_workers = threadcount;
	g->done = 0;

	for (int i = 0; i < threadcount; i += 1)
	{
		// TODO: Memory Bug?
		// TODO: Way too much memory allocated.
		update_t *thread_out_q = (update_t *)xcalloc(g->m, sizeof(update_t));
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
		g_plq[i].inqueue_count = 0;
		g_plq[i].inqueue_index = 0;

		if (pthread_create(&threads[i], NULL, worker, &g_plq[i]) != 0)
		{
			error("pthread_create");
			exit(1);
		}
	}
}

static void join_workers(pthread_t *threads, int threadcount)
{
	for (int i = 0; i < threadcount; i += 1)
		pthread_join(threads[i], NULL);
}

/* ---------------------------------- */
/*  Preflow Algorithm Initialization  */
/* ---------------------------------- */

void init_mutexes(graph_t *g)
{
	pthread_mutex_init(&g->g_lock, NULL);
	pthread_cond_init(&g->cv, NULL);
}

void destroy_mutexes(graph_t *g)
{
	pthread_mutex_destroy(&g->g_lock);
	pthread_cond_destroy(&g->cv);
}

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
	}

	// create edge structs and hook into arrays
	for (int i = 0; i < m; i++)
	{
		edge_t *e = &g->e[i];
		e->u = &g->v[a[i]];
		e->v = &g->v[b[i]];
		e->c = c[i];
		e->f = 0;

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
	for (int i = 0; i < algo->threadcount; i += 1)
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
	s->e = 0;

	// First compute source excess = sum of outgoing capacities
	for (int i = 0; i < s->degree; i++)
	{
		edge_t *e = s->edges[i];
		if (s == e->u)
		{
			s->e += e->c;
		}
		else
		{
			s->e += e->c;
		}
	}

	// Then push that flow to all neighbors
	for (int i = 0; i < s->degree; i++)
	{
		edge_t *e = s->edges[i];
		init_push(g, s, other(s, e), e);
	}
}

static void setup(preflow_context_t *algo_ctx)
{
	graph_t *g = algo_ctx->g;

	init_mutexes(g);
	init_preflow(g);
	pthread_barrier_init(algo_ctx->barrier, NULL, algo_ctx->threadcount);
	init_workers(algo_ctx);
}

static void teardown(preflow_context_t *algo_ctx)
{
	graph_t *g = algo_ctx->g;

	free_ctx(algo_ctx); // Now safe to free thread contexts
	pthread_barrier_destroy(algo_ctx->barrier);
	destroy_mutexes(algo_ctx->g);
}

static void preflow(preflow_context_t *algo_ctx)
{
	setup(algo_ctx);

	join_workers(algo_ctx->threads, algo_ctx->threadcount);
	// UPDATE GRAPHS LOOP

	algo_ctx->result = algo_ctx->g->t->e;
	teardown(algo_ctx);
}

/* ---------------------------------- */
/*  Main 						      */
/* ---------------------------------- */
int main(int argc, char *argv[])
{
	int tc;					/* algorith threadcount */
	FILE *in;				/* input file set to stdin	*/
	graph_t *g;				/* undirected graph. 		*/
	int f;					/* output from preflow.		*/
	int n;					/* number of nodes.		*/
	int m;					/* number of edges.		*/
	preflow_context_t algo; /* algorithm context - struct wrapper for algorithm setup and attributes */

	tc = 8;
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
Algorithm above works. It is slower mainly because of the global contention for the enter excess/
leave excess, where threads are fighting for grabbing from/putting back to the global queue. Improvements are
- Have in/out-queues in the individual threads so they can grab 'more' work at once and only give back after processing it
through.
- Use BFS / Relabel wave (?) to make relabelling faster.
See: https://courses.csail.mit.edu/6.884/spring10/projects/viq_velezj_maxflowreport.pdf
*/
