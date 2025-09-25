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
#define TC 12
#define NODES_PER_BATCH 8
#define CACHE_PADDING CACHE_ALIGN_X86 

/* My Laptop (Ryzen 7 7730U, 8 core, 16 logical CPU's) */
/*
#define TC 16
#define NODES_PER_BATCH 16
#define CACHE_PADDING CACHE_ALIGN_X86 
*/

/* Possible POWER8 Configs - Test
#define CACHE_ALIGN_PADDING CACHE_ALIGN_POWER8
#define TC 8
#define NODES_PER_BATCH 4
OR
#define TC 8
#define NODES_PER_BATCH 8
-- Continue Tweaking these
*/

#define DIAGNOSTICS 1
#define PRINT 1 /* enable/disable prints. */
#define MIN(a, b) (((a) <= (b)) ? (a) : (b))
#define MAX(a, b) (((a) >= (b)) ? (a) : (b))
#define BATCH_SIZE(N, THREADCOUNT)  ((N / (THREADCOUNT * NODES_PER_BATCH)) > 1 ? (N / (THREADCOUNT * NODES_PER_BATCH)) : 1)
#define BUFSIZE (1<<16)

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct adj_t adj_t;
typedef struct list_t list_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct thread_ctx_t thread_ctx_t;
typedef struct update_t update_t;
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

#if DIAGNOSTICS
#define pr_diag(...)                       \
	do                                \
	{                                 \
		fprintf(stderr, __VA_ARGS__); \
	} while (0)
#else
#define pr_diag(...) /* no effect at all */
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

struct node_t
{
	uint32_t id;
    uint8_t h, cur, degree;
    bool is_in_queue;
    atomic_int e;
    node_t *next;
    adj_t *edges;
} __attribute__((aligned(CACHE_PADDING))); // or 128 for POWER8

struct edge_t
{
	node_t *u; /* one of the two nodes.	*/
	node_t *v; /* the other. 			*/
	uint32_t c;	   /* capacity.			*/
	atomic_int f;	   /* flow > 0 if from u to v.	*/
};

struct graph_t
{
	uint32_t n;			/* nodes.			*/
	uint32_t m;			/* edges.			*/
	node_t *v;		/* array of n nodes.		*/
	edge_t *e;		/* array of m edges.		*/
	node_t *s;		/* source.			*/
	node_t *t;		/* sink.			*/

	node_t **active_nodes; /* sparse array with pointers to nodes instead of using excess */
	uint32_t n_active;
	// node_t *excess; /* nodes with e > 0 except s,t.	*/

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
	uint32_t result; // Will be equal to the sinks final excess
} __attribute__((aligned(CACHE_PADDING))); // or 128 for POWER8

struct update_t
{
	char type; /* type = 0 is push; type = 1 is relabel*/
	node_t *u;
	uint32_t delta;
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
	uint8_t inqueue_size;  // capacity of inqueue array
	uint8_t inqueue_count; // number of valid nodes in inqueue
	uint8_t inqueue_index; // next index to pop

	bool did_work;
	pthread_barrier_t *barrier;
	uint8_t *owned;
	
	#ifdef DIAGNOSTICS
	uint64_t owned_pushes;
	uint64_t atomic_pushes;
	#endif
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

/* Quickly optimizing next_int as well */
static char buf[BUFSIZE];
static int idx = 0, size = 0;

static char read_char()
{
  if (idx >= size) {
      size = fread(buf, 1, BUFSIZE, stdin);
      idx = 0;
      if (size == 0) return EOF;
  }
  return buf[idx++];
}

static uint32_t next_int()
{
	uint32_t x = 0;
  char c = 0;

  do {
    c = read_char();
    if (c == EOF) return EOF;
  } while (c < '0' || c > '9');

  do {
    x = x * 10 + (c - '0');
    c = read_char();
  } while (c >= '0' && c <= '9');

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
 	if(!v->is_in_queue)
	{
		v->is_in_queue = 1;
        g->active_nodes[g->n_active++] = v;
        g->work_counter++;
	} 
}

static void init_push(graph_t *g, node_t *u, node_t *v, edge_t *e)
{
    pr("push from %d to %d: ", id(g, u), id(g, v));
    pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);

    uint32_t d;
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

static int push_owned(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e, int8_t dir)
{
	if (u->h != v->h + 1)  // enforce valid push
		return 0;

	uint32_t u_excess = u->e;
	uint32_t f 	      = e->f;
	uint32_t d1 = MIN(u_excess, e->c - f);
	uint32_t d2 = MIN(u_excess, e->c + f);
	uint32_t d = (dir == 1) ? d1 : d2;

	if (dir == 1) e->f += d;
	else 		  e->f -= d;

	u->e -= d;
	v->e += d;

	  #ifdef PRINT
	if(d != 0) {
		pr("push from %d to %d: ", id(ctx->g, u), id(ctx->g, v));
		pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);
		pr("pushing %d\n", d);
	}
  #endif

  return d;
}

static int push_atomic(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e, int8_t dir)
{
  if (u->h != v->h + 1)  // enforce valid push
        return 0;

  uint32_t u_excess = atomic_load(&u->e);
  uint32_t f 		    = atomic_load(&e->f);
                      //: MIN(u_excess, e->c + f);
  uint32_t d1 = MIN(u_excess, e->c - f);
  uint32_t d2 = MIN(u_excess, e->c + f);
  uint32_t d  = (dir == 1) ? d1 : d2;

  if (dir == 1) atomic_fetch_add(&e->f, d);
  else          atomic_fetch_sub(&e->f, d);

  atomic_fetch_sub(&u->e, d);
  atomic_fetch_add(&v->e, d);

  #ifdef PRINT
	if(d != 0) {
		pr("push from %d to %d: ", id(ctx->g, u), id(ctx->g, v));
		pr("f = %d, c = %d, so ", atomic_load(&e->f), e->c);
		pr("pushing %d\n", d);
	}
  #endif

  return d;
}

static uint8_t _find_min_residual_cap(graph_t *g, node_t *u)
{
  uint8_t i, f, residual, min_h;
  adj_t  *a;
  edge_t *e;
  node_t *v;
	
  min_h = UINT8_MAX;
	for (i = 0; i < u->degree; i++)
	{
    a = &u->edges[i];
    // a = &u->edges[i];
    e = a->e;
    v = a->neighbor;
    //e = u->edges[i];
    f = atomic_load_explicit(&e->f, memory_order_relaxed);
    //f = atomic_load(&e->f);
    residual  = e->c - (a->dir * f);
    //residual  = (a->dir == 0) ? (e->c - f) : (e->c + f);
    //residual  = (u == e->u) ? e->c - f : e->c + f;
    //v         = (u == e->u) ? e->v : e->u;

    if (residual > 0)
      min_h = MIN(min_h, v->h);
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
  uint8_t min_h = _find_min_residual_cap(ctx->g, u);
  uint8_t new_h = _relabel(min_h, u);

  // Initialize update struct in one go
  update_t update = { .type = RELABEL, .u = u, .delta = 0, .new_height = new_h, .g = ctx->g };
  ctx->plq[ctx->count++] = update;
}

static void _build_update_queue(thread_ctx_t *ctx, node_t *u)
{
    uint8_t i = u->cur;
    uint8_t degree = u->degree;
    bool pushed = false;

    while (i < degree)
    {
        adj_t *a = &u->edges[i];
        edge_t *e = a->e;
        node_t *v = a->neighbor;
        uint32_t d;

        if (ctx->owned[v->id]) {
			#ifdef DIAGNOSTICS
			ctx->owned_pushes++;
			#endif
            d = push_owned(ctx, u, v, e, a->dir);
        } else {
			#ifdef DIAGNOSTICS
			ctx->atomic_pushes++;
			#endif
            d = push_atomic(ctx, u, v, e, a->dir);
        }

        if (d > 0) {
            pushed = true;
            ctx->plq[ctx->count++] = (update_t){ .type=PUSH, .u=v, .delta=d, .g=ctx->g };
        }

        if (atomic_load(&u->e) == 0) {
            u->cur = i;
            return;
        }

        i++;
    }

	if (atomic_load(&u->e) > 0 && pushed)
		ctx->plq[ctx->count++] = (update_t){ .type=PUSH, .u=u, .delta=0, .g=ctx->g };
	else if (!pushed)
		relabel(ctx, u);
}

/* ---------------------------- */
/*  Workers / Thread Functions  */
/* ---------------------------- */

static void _apply_updates(thread_ctx_t *ctx)
{
    thread_ctx_t *all_thread_contexts = ctx->all_thread_ctx;
    uint8_t n_threads = ctx->all_thread_ctx_size;

    for (uint8_t t = 0; t < n_threads; t++)
    {
        thread_ctx_t *tctx = &all_thread_contexts[t];

        for (uint32_t k = 0; k < tctx->count; k++)
        {
            update_t *update = &tctx->plq[k];
            
            // Apply relabel
            if (update->type == RELABEL)
            {
                update->u->h = update->new_height;
            }

            // Only add node back to global active queue if it still has excess
            // and is not already in the queue
            if (update->u->e > 0 && !update->u->is_in_queue)
            {
                enter_excess(ctx->g, update->u);
            }
        }

        // Reset this thread's PLQ
        tctx->count = 0;
    }
}

static uint8_t grab_excess_batch(graph_t *g, node_t **local_queue, uint32_t max_nodes, uint8_t *owned)
{
    uint8_t count = 0;

    pthread_mutex_lock(&g->g_lock);
    uint32_t n = (g->n_active < max_nodes) ? g->n_active : max_nodes;

    for (uint32_t i = 0; i < n; i++) {
        node_t *v = g->active_nodes[i];
        if (!owned[v->id]) {   // claim ownership
            local_queue[count++] = v;
            owned[v->id] = 1;
            v->is_in_queue = 0;
            g->work_counter--;  // decrement when taken
        }
    }

    for (uint32_t i = count; i < g->n_active; i++)
        g->active_nodes[i - count] = g->active_nodes[i];

    g->n_active -= count;
    pthread_mutex_unlock(&g->g_lock);

    return count;
}

void *worker(void *arg)
{
    thread_ctx_t *tctx = (thread_ctx_t *)arg;
    graph_t *g = tctx->g;
    node_t *local_queue[tctx->inqueue_size];
    uint8_t n_local = 0;

    while (1)
    {
        // Phase 1: process local queue
        if (n_local > 0)
        {
            node_t *u = local_queue[--n_local]; // pop last node
            tctx->owned[u->id] = 1;             // claim ownership
            if (u != g->s && u != g->t)
            {
                _build_update_queue(tctx, u);
				tctx->owned[u->id] = 0;
                tctx->did_work = true;
            }
        }

        // Barrier 1: synchronize threads
        int res = pthread_barrier_wait(tctx->barrier);

        // Serial thread applies updates and checks termination
        if (res == PTHREAD_BARRIER_SERIAL_THREAD)
        {
            _apply_updates(tctx);

            bool all_idle = true;
            for (int i = 0; i < tctx->all_thread_ctx_size; i++)
            {
                if (tctx->all_thread_ctx[i].did_work)
                {
                    all_idle = false;
                    break;
                }
            }

            if (all_idle && g->n_active == 0)
                g->done = 1;

            for (int i = 0; i < tctx->all_thread_ctx_size; i++)
                tctx->all_thread_ctx[i].did_work = false;
        }

        // Barrier 2: all threads see updates / g->done
        pthread_barrier_wait(tctx->barrier);

        // Termination check
        if (g->done)
            break;

        // Phase 2: grab new work from global queue
        if (n_local < tctx->inqueue_size)
        {
            uint8_t grabbed = grab_excess_batch(g, local_queue + n_local,
                                                tctx->inqueue_size - n_local,
                                                tctx->owned);
            n_local += grabbed;
        }
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

		g_plq[i].owned = xcalloc(g->n, sizeof(uint8_t));

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
	g->active_nodes = xcalloc(n, sizeof(node_t*));
	g->n_active = 0;
	g->s = &g->v[0];
	g->t = &g->v[n - 1];
	//g->excess = NULL;

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
	for(i = 0; i < algo->threadcount; i += 1)
	{
		free(algo->thread_ctxs[i].plq);		// Free per-thread update queue
		free(algo->thread_ctxs[i].inqueue); // Free per-thread inqueue
		free(algo->thread_ctxs[i].owned);
	}
	free(algo->thread_ctxs); // Free thread_ctx array itself
}

static void free_graph(graph_t *g)
{
	for (int i = 0; i < g->n; i++)
	{
		free(g->v[i].edges);
	}
	free(g->active_nodes);
	free(g->v);
	free(g->e);
	free(g);
}

static void init_preflow(graph_t *g)
{
	node_t *s = g->s;

	s->h = g->n;
	s->e = 0;
  g->work_counter = 0;

	for (int i = 0; i < s->degree; i++)
	{
    adj_t  *a = &s->edges[i];
    edge_t *e = a->e;
		//edge_t *e = s->edges[i];
		atomic_fetch_add(&s->e, e->c);
	}

	for (int i = 0; i < s->degree; i++)
	{
		adj_t  *a = &s->edges[i];
		edge_t *e = a->e;
			//edge_t *e = s->edges[i];
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

	#ifdef DIAGNOSTICS
	uint64_t total_owned = 0, total_atomic = 0;
	for (int t = 0; t < algo_ctx->threadcount; t++) {
		total_owned  += algo_ctx->thread_ctxs[t].owned_pushes;
		total_atomic += algo_ctx->thread_ctxs[t].atomic_pushes;
	}

	pr_diag("Owned pushes:  %lu\n", total_owned);
	pr_diag("Atomic pushes: %lu\n", total_atomic);
	double ratio = (total_owned + total_atomic) ? 
				(double) total_owned / (total_owned + total_atomic) : 0.0;
	pr_diag("Owned push ratio: %.2f%%\n", ratio * 100.0);
	#endif

	algo_ctx->result = algo_ctx->g->t->e;
	teardown(algo_ctx);
}

/* ---------------------------------- */
/*  Main 						      */
/* ---------------------------------- */
int main(int argc, char *argv[])
{
	uint8_t tc;					/* algorith threadcount */
	FILE *in;				/* input file set to stdin	*/
	graph_t *g;				/* undirected graph. 		*/
	uint32_t f;					/* output from preflow.		*/
	uint32_t n;					/* number of nodes.		*/
	uint32_t m;					/* number of edges.		*/
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
