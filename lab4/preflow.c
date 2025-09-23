#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <limits.h>
#include <stdatomic.h>

#include "pthread_barrier.h"

/* Lab4 
Idea: Make excess and residuals atomic, so pushing can be done entirely during phase 1.
Phase 2 then only has to handle relabels. Makes sequential part of the program much smaller.
*/

/* ---------------------------- */
/*  Program Constants & Types   */
/* ---------------------------- */

/* Configuration (adapt to optimal for power) */

/* This is best on my Home Pc (Ryzen 6 core, 12 logical CPU's) */
#define TC 12
#define NODES_PER_BATCH 8

/*
Possible POWER8 Configs - Test
#define TC 8
#define NODES_PER_BATCH 4
or
#define TC 8
#define NODES_PER_BATCH 8
*/

/*
*/

#define PRINT 0 /* enable/disable prints. */
#define MIN(a, b) (((a) <= (b)) ? (a) : (b))
#define MAX(a, b) (((a) >= (b)) ? (a) : (b))
#define BATCH_SIZE(N, THREADCOUNT)  ((N / (THREADCOUNT * NODES_PER_BATCH)) > 1 ? (N / (THREADCOUNT * NODES_PER_BATCH)) : 1)

typedef int bool_t;

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct list_t list_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct thread_ctx_t thread_ctx_t;
typedef enum { PUSH = 0, RELABEL = 1 } update_type_t;

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
	int h;		  /* height.			*/
	atomic_int e;		  /* excess flow.			*/
	node_t *next; /* with excess preflow.		*/

	int cur;		// current-arc index into edges[]
	edge_t **edges; // adjacency array
	int degree;

	int is_in_queue; /* flag to keep track of whether already in the excess queue */ // TODO: Atomic?
	pthread_mutex_t n_lock;															 /* individual lock for each node */
};

struct edge_t
{
	node_t *u; /* one of the two nodes.	*/
	node_t *v; /* the other. 			*/
	atomic_int f;	   /* flow > 0 if from u to v.	*/
	int c;	   /* capacity.			*/
};

struct graph_t
{
	int n;			/* nodes.			*/
	int m;			/* edges.			*/
	node_t *v;		/* array of n nodes.		*/
	edge_t *e;		/* array of m edges.		*/
	node_t *s;		/* source.			*/
	node_t *t;		/* sink.			*/
	node_t *excess; /* nodes with e > 0 except s,t.	*/

	pthread_mutex_t g_lock; /* global lock to ensure nodes coming in and out of excess pool is consistent */
	int done;				/* algorithm done flag */
  	atomic_int work_counter;
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

typedef struct
{
	int type; /* type = 0 is push; type = 1 is relabel*/
	node_t *u;
	int delta;
	int new_height;

	graph_t *g;
} update_t;

struct thread_ctx_t
{
	graph_t *g;
	update_t *plq;
	size_t count;
	size_t tid;

	thread_ctx_t *all_thread_ctx;
	size_t all_thread_ctx_size;

	node_t **inqueue;
	int inqueue_size;  // capacity of inqueue array
	int inqueue_count; // number of valid nodes in inqueue
	int inqueue_index; // next index to pop

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

/* REQUIRE: The global lock (g->g_lock) MUST be held before calling this function.
Otherwise, use the safe enter_excess helper.*/
static void enter_excess_locked(graph_t *g, node_t *v)
{
	if(!v->is_in_queue)
	{
		v->is_in_queue = 1;
		v->next = g->excess;
		g->excess = v;
    	atomic_fetch_add(&g->work_counter, 1); // increment work counter
	}
}

static void enter_excess(graph_t *g, node_t *v)
{
	pthread_mutex_lock(&g->g_lock);
	enter_excess_locked(g, v);
	pthread_mutex_unlock(&g->g_lock);
}

/* REQUIRE: The global lock (g->g_lock) MUST be held before calling this function. */
static void *leave_excess_locked(graph_t *g)
{
	node_t *v = g->excess;
	if(v)
	{
		v->is_in_queue = 0;
		g->excess = v->next;
		v->next = NULL;
    	atomic_fetch_sub(&g->work_counter, 1); // decrement work counter
	}
	return v;
}

static node_t *leave_excess(graph_t *g)
{
	pthread_mutex_lock(&g->g_lock);
	node_t *v = leave_excess_locked(g);
	pthread_mutex_unlock(&g->g_lock);
	return v;
}

static void init_push(graph_t *g, node_t *u, node_t *v, edge_t *e)
{
    pr("push from %d to %d: ", id(g, u), id(g, v));
    pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);

    int d;
    
	if(u == e->u)
	{
        d = MIN(u->e, e->c - e->f); // plain int, no atomic
        e->f += d;
    }
	else
	{
        d = MIN(u->e, e->c + e->f);
        e->f -= d;
    }
    pr("pushing %d\n", d);

    u->e -= d;
    v->e += d;

	enter_excess(g, v);
}

/* ---------------------------- */
/*  Preflow Algorithm Helpers   */
/* ---------------------------- */

static int _push(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e)
{
    if (u->h != v->h + 1)  // enforce valid push
        return 0;
    int u_excess = atomic_load(&u->e);
    int f 		 = atomic_load(&e->f);
    int d = (u == e->u) ? MIN(u_excess, e->c - f)
                        : MIN(u_excess, e->c + f);

    if (u == e->u) atomic_fetch_add(&e->f, d);
    else           atomic_fetch_sub(&e->f, d);

    atomic_fetch_sub(&u->e, d);
    atomic_fetch_add(&v->e, d);

    return d;
}

static int push(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e)
{
	graph_t *g = ctx->g;
	int push = _push(ctx, u, v, e);
	if(push != 0) {
		pr("push from %d to %d: ", id(g, u), id(g, v));
		pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);
		pr("pushing %d\n", push);
	}
	return push;
}

static int _find_min_residual_cap(graph_t *g, node_t *u)
{
	int min_h = INT_MAX;
	for (int i = 0; i < u->degree; i++)
	{
		edge_t *e = u->edges[i];
    	int f = atomic_load(&e->f);

		int residual = (u == e->u) ? (e->c - f) : (e->c + f);
		node_t *v = (u == e->u) ? e->v : e->u;
		
		if (residual > 0)
			min_h = MIN(min_h, v->h);
	}
	return min_h;
}

static int _relabel(int min_h, node_t *u)
{
	if (min_h < INT_MAX)
		return min_h + 1;
	else
		return u->h + 1;
}

static void relabel(thread_ctx_t *ctx, node_t *u)
{
    int min_h = _find_min_residual_cap(ctx->g, u);
    int new_h = _relabel(min_h, u);

    // Initialize update struct in one go
    update_t update = { .type = RELABEL, .u = u, .delta = 0, .new_height = new_h, .g = ctx->g };
    ctx->plq[ctx->count++] = update;
}

static void _build_update_queue(thread_ctx_t *ctx, node_t *u)
{
	int i, nbr_of_neighbors, d;
	edge_t* e;
	node_t* v;
    bool_t pushed = 0;

	i = u->cur;
	nbr_of_neighbors = u->degree;
	while(i < nbr_of_neighbors)
	{
		e = u->edges[i];
		v = (u == e->u) ? e->v : e->u;
		d = push(ctx, u, v, e);

		if(d > 0)
		{
			pushed = 1;
			ctx->plq[ctx->count++] = (update_t){ .type=PUSH, .u=v, .delta=d, .g=ctx->g };
		}
		if(atomic_load(&u->e) == 0)
		{
			u->cur = i;
			return;
		}
		++i;
	}

    if(!pushed)
    {
		relabel(ctx, u);
        u->cur = 0; // reset arc index after relabel
    } else if(atomic_load(&u->e) > 0)
	{
		update_t update = (update_t){ .type = PUSH, .u=u, .delta=d, .g=ctx->g };
        ctx->plq[ctx->count++] = update;
	}
}

/* ---------------------------- */
/*  Workers / Thread Functions  */
/* ---------------------------- */

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
        update.u->cur = 0;
      }
			if (update.type == 1)
			{
				update.u->h = update.new_height;
				update.u->cur = 0; // reset current arc after relabel
			}

			enter_excess_locked(ctx->g, update.u);
		}
		thread_ctx->count = 0;
	}

	// Release the global graph lock after all updates
	pthread_mutex_unlock(&ctx->g->g_lock);
}

static int grab_excess_batch(graph_t *g, node_t **local_queue, int max_nodes) {
    int count = 0;

    pthread_mutex_lock(&g->g_lock);
    while (count < max_nodes && g->excess) {
        node_t *v = g->excess;
        g->excess = v->next;
        v->next = NULL;
        v->is_in_queue = 0;

        local_queue[count++] = v;

        atomic_fetch_sub(&g->work_counter, 1);
    }
    pthread_mutex_unlock(&g->g_lock);

    return count;  // number of nodes grabbed
}

void *worker(void *arg)
{
    thread_ctx_t *tctx = (thread_ctx_t *)arg;
    graph_t *g = tctx->g;

    node_t *local_queue[tctx->inqueue_size]; // reuse preallocated inqueue
    int n;

    while (1)
    {
        // Grab a batch of nodes from the global excess queue
        n = grab_excess_batch(g, local_queue, tctx->inqueue_size);
        if (n == 0) {
            // nothing to do this round, wait at barrier
            tctx->did_work = 0;
            int res = pthread_barrier_wait(tctx->barrier);
            if (res == PTHREAD_BARRIER_SERIAL_THREAD) {
                _apply_updates(tctx);
                if (atomic_load(&g->work_counter) == 0 && g->excess == NULL) {
                    g->done = 1;
                }
            }
            pthread_barrier_wait(tctx->barrier);
            if (g->done) break;
            continue;
        }

        // Process the batch locally
        bool_t local_work = 0;
        for (int i = 0; i < n; i++) {
            node_t *u = local_queue[i];
            if (u && u != g->s && u != g->t) {
                local_work = 1;
                _build_update_queue(tctx, u);
            }
        }
        tctx->did_work = local_work;

        // Sync and apply updates
        int res = pthread_barrier_wait(tctx->barrier);
        if (res == PTHREAD_BARRIER_SERIAL_THREAD) {
            _apply_updates(tctx);
            if (atomic_load(&g->work_counter) == 0 && g->excess == NULL) {
                g->done = 1;
            }
        }
        pthread_barrier_wait(tctx->barrier);
        if (g->done) break;
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
	for(i = 0; i < algo->threadcount; i += 1)
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
  	atomic_store(&g->work_counter, 0);

	for (int i = 0; i < s->degree; i++)
	{
		edge_t *e = s->edges[i];
		atomic_fetch_add(&s->e, e->c);
	}

	for (int i = 0; i < s->degree; i++)
	{
		edge_t *e = s->edges[i];
		init_push(g, s, other(s, e), e);
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
	int tc;					/* algorith threadcount */
	FILE *in;				/* input file set to stdin	*/
	graph_t *g;				/* undirected graph. 		*/
	int f;					/* output from preflow.		*/
	int n;					/* number of nodes.		*/
	int m;					/* number of edges.		*/
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
Algorithm above works. It is slower mainly because of the global contention for the enter excess/
leave excess, where threads are fighting for grabbing from/putting back to the global queue. Improvements are
- Have in/out-queues in the individual threads so they can grab 'more' work at once and only give back after processing it
through.
- Use BFS / Relabel wave (?) to make relabelling faster.
See: https://courses.csail.mit.edu/6.884/spring10/projects/viq_velezj_maxflowreport.pdf
*/
