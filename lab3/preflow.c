#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <limits.h>

#include "pthread_barrier.h"

/* ---------------------------- */
/*  Program Constants & Types   */
/* ---------------------------- */

#define PRINT 0 /* enable/disable prints. */
#define MIN(a, b) (((a) <= (b)) ? (a) : (b))

typedef int bool_t;

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct list_t list_t;
typedef struct preflow_context_t preflow_context_t;
typedef struct thread_ctx_t thread_ctx_t;

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

#if PRINT
static int id(graph_t *g, node_t *v)
{
	return v - g->v;
}
#endif

/* ---------------------------- */
/*  Algorithm Structs           */
/* ---------------------------- */

struct list_t
{
	edge_t *edge;
	list_t *next;
};

struct node_t
{
	int h;		  /* height.			*/
	int e;		  /* excess flow.			*/
	list_t *edge; /* adjacency list.		*/
	node_t *next; /* with excess preflow.		*/

	int is_in_queue; /* flag to keep track of whether already in the excess queue */ // TODO: Atomic?
	pthread_mutex_t n_lock;															 /* individual lock for each node */
};

struct edge_t
{
	node_t *u; /* one of the two nodes.	*/
	node_t *v; /* the other. 			*/
	int f;	   /* flow > 0 if from u to v.	*/
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
	pthread_cond_t cv;		/* cond variable to make sure threads sleep when they're done with their job and there isn't more work to do */
	int done;				/* algorithm done flag */
	int active_workers;		/* enumerate for amount of currently active threads */
};

struct preflow_context_t
{
	graph_t *g;
	pthread_t *threads;
	pthread_barrier_t barrier;
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

static void add_edge(node_t *u, edge_t *e)
{
	list_t *p;

	p = xmalloc(sizeof(list_t));
	p->edge = e;
	p->next = u->edge;
	u->edge = p;
}

static void connect(node_t *u, node_t *v, int c, edge_t *e)
{
	e->u = u;
	e->v = v;
	e->c = c;

	add_edge(u, e);
	add_edge(v, e);
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
	if (v == g->s || v == g->t)
		return;
	if (!v->is_in_queue)
	{
		v->is_in_queue = 1;
		v->next = g->excess;
		g->excess = v;
		pthread_cond_signal(&g->cv);
	}
}

static void enter_excess(graph_t *g, node_t *v)
{
	enter_excess_locked(g, v);
}

/* REQUIRE: The global lock (g->g_lock) MUST be held before calling this function. */
static void *leave_excess_locked(graph_t *g)
{
	node_t *v = g->excess;
	if (v)
	{
		v->is_in_queue = 0;
		g->excess = v->next;
		v->next = NULL;
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

static void _init_push(node_t *u, node_t *v, edge_t *e, bool_t *u_is_active, bool_t *v_is_active)
{
	int d; /* remaining capacity of the edge. */
	int u_excess_after_push, v_excess_after_push;

	if (u == e->u)
	{
		d = MIN(u->e, e->c - e->f);
		e->f += d;
		pr("d: %d ::", d);
	}
	else
	{
		d = MIN(u->e, e->c + e->f);
		e->f -= d;
		pr("d: -%d ::", d);
	}
	pr("pushing %d\n", d);

	u->e -= d;
	v->e += d;

	// Save these to prevent potential races between unlocking and checking below
	u_excess_after_push = u->e;
	v_excess_after_push = v->e;

	/* the following are always true. */
	assert(d >= 0);
	assert(u->e >= 0);
	assert(abs(e->f) <= e->c);

	*u_is_active = (u_excess_after_push > 0);
	*v_is_active = (v_excess_after_push == d);

	// *v_is_active = (v_excess_after_push > 0); Potentially better (?)? == d should be fine, because any node that
	// is pushed to which isn't at zero excess SHOULD already be in the active queue, and adding it back will just mean extra
	// redundancy. But maybe it's safer?
}

static void init_push(graph_t *g, node_t *u, node_t *v, edge_t *e)
{
	pr("push from %d to %d: ", id(g, u), id(g, v));
	pr("f = %d, c = %d, so ", e->f, e->c);

	bool_t u_is_active, v_is_active;
	_init_push(u, v, e, &u_is_active, &v_is_active);

	if (u_is_active)
		enter_excess(g, u);
	if (v_is_active)
		enter_excess(g, v);
}

/* REQUIRE: Caller does not hold g->g_lock.
 * This function locks/unlocks global lock internally.
 */
static node_t *_get_next_active_node(graph_t *g)
{
	node_t *u = NULL;

	pthread_mutex_lock(&g->g_lock);
	while (1)
	{
		u = leave_excess_locked(g);
		if (u != NULL)
			break;

		g->active_workers--;
		if (g->active_workers == 0 && g->excess == NULL)
		{
			// last worker, no nodes left = algorithm done
			g->done = 1;
			pthread_cond_broadcast(&g->cv);
			pthread_mutex_unlock(&g->g_lock);
			return NULL;
		}

		while ((u = leave_excess_locked(g)) == NULL && !g->done)
			pthread_cond_wait(&g->cv, &g->g_lock);

		g->active_workers++;
		if (g->done)
		{
			pthread_mutex_unlock(&g->g_lock);
			return NULL;
		}

		if (u != NULL)
			break; // we got work after wakeup
	}

	pthread_mutex_unlock(&g->g_lock);
	return u;
}

/* ---------------------------- */
/*  Preflow Algorithm Helpers   */
/* ---------------------------- */

static void _push(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e)
{
	int d; /* remaining capacity of the edge. */
	int u_excess_after_push, v_excess_after_push;

	if (u == e->u)
	{
		d = MIN(u->e, e->c - e->f);
		e->f += d;
		pr("d: %d ::", d);
	}
	else
	{
		d = MIN(u->e, e->c + e->f);
		e->f -= d;
		pr("d: -%d ::", d);
	}
	pr("pushing %d\n", d);
	u->e -= d;

	/* --- */
	size_t index = ctx->count;

	update_t update;
	update.type = 0;
	update.u = u;
	update.delta = d;
	update.new_height = 0;
	ctx->plq[index] = update;
	ctx->count++;

	/* the following are always true. */
	assert(d >= 0);
	assert(u->e >= 0);
	assert(abs(e->f) <= e->c);
}

static void push(thread_ctx_t *ctx, node_t *u, node_t *v, edge_t *e)
{
	graph_t *g = ctx->g;

	pr("push from %d to %d: ", id(g, u), id(g, v));
	pr("f = %d, c = %d, so ", e->f, e->c);

	_push(ctx, u, v, e);

	// if (u_is_active)
	// 	enter_excess(g, u);
	// if (v_is_active) /* LIST OF V */
	// 	enter_excess(g, v);
}

/* REQUIRE: Caller does not hold u->n_lock or v->n_lock.
 * This function locks/unlocks edge nodes internally.
 */
static int _find_min_residual_cap(graph_t *g, node_t *u)
{
	int min_h = INT_MAX;
	list_t *p;
	edge_t *e;
	node_t *v;

	for (p = u->edge; p != NULL; p = p->next)
	{
		e = p->edge;

		v = other(u, e);
		int rf;
		if (u == e->u)
			rf = e->c - e->f; // residual capacity from u -> v
		else
			rf = e->c + e->f; // residual capacity from u <- v

		if (rf > 0)
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
	graph_t *g = ctx->g;
	int min_h = _find_min_residual_cap(g, u);
	int h = _relabel(min_h, u);

	update_t update;
	update.type = 1;
	update.u = u;
	update.delta = 0;
	update.new_height = h;
	size_t index = ctx->count;
	ctx->plq[index] = update;
	ctx->count++;

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

	for (p = u->edge; p != NULL; p = p->next)
	{
		e = p->edge;
		if (u == e->u)
		{
			v = e->v;
			b = 1;
		}
		else
		{
			v = e->u;
			b = -1;
		}

		can_push = (u->h > v->h) && (b * e->f < e->c);

		if (can_push)
		{
			push(ctx, u, v, e);
			pushed = 1;
		}
	}

	if (!pushed)
	{
		relabel(ctx, u);
	}
	else if (u->e > 0)
	{

		size_t index = ctx->count;

		update_t update;
		update.type = 0;
		update.u = u;
		update.delta = 0;
		update.new_height = 0;
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

	for (i = 0; i < all_thread_contexts_size; i++)
	{
		thread_ctx_t thread_ctx = all_thread_contexts[i];
		update_t update;
		for (k = 0; k < thread_ctx.count; k++)
		{
			update = thread_ctx.plq[k];
			if (update.type == 0)
			{
				update.u->e += update.delta;
			}
			else if (update.type == 1)
			{
				update.u->h += update.new_height;
			}
			else
			{
				error("ERROR");
			}

			enter_excess(thread_ctx.g, update.u);
		}
		thread_ctx.count = 0;
	}
}

void *worker(void *arg)
{
	thread_ctx_t *tctx = (thread_ctx_t *)arg;
	graph_t *g = tctx->g;

	node_t *u = NULL;
	while (1)
	{
		u = _get_next_active_node(g);
		if (!u)
			break;

		_build_update_queue(tctx, u);
		pthread_barrier_wait(tctx->barrier);

		if (tctx->tid == 0) // Special Thread 0
		{
			/* TODO NEXT: We need to store the amount of threads
			in the tctx? It's getting confusing.*/
			_apply_updates(tctx);
		}

		pthread_barrier_wait(tctx->barrier);
		// BARRIER STOP:
		// THREADS WAIT ON A COND VARIABLE, SAME AS BEFORE
	}
	return NULL;
}

static void init_workers(preflow_context_t *algo)
{
	graph_t *g = algo->g;
	pthread_t *threads = algo->threads;
	int threadcount = algo->threadcount;

	thread_ctx_t *g_plq = (thread_ctx_t *)xcalloc(threadcount, sizeof(thread_ctx_t));
	g->active_workers = threadcount;
	g->done = 0;

	for (int i = 0; i < threadcount; i += 1)
	{
		// TODO: Memory Bug?
		// TODO: Way too much memory allocated.
		update_t *thread_out_q = (update_t *)xcalloc(g->n, sizeof(update_t));
		g_plq[i].g = g;
		g_plq[i].plq = thread_out_q;
		// Hacky fixa snyggt sen
		g_plq[i].barrier = &algo->barrier;
		g_plq[i].tid = i;

		g_plq[i].all_thread_ctx = g_plq;
		g_plq[i].all_thread_ctx_size = algo->threadcount;

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
	graph_t *g;
	node_t *u;
	node_t *v;
	int i;
	int a;
	int b;
	int c;

	g = xmalloc(sizeof(graph_t));

	g->n = n;
	g->m = m;

	g->v = xcalloc(n, sizeof(node_t));
	g->e = xcalloc(m, sizeof(edge_t));

	g->s = &g->v[0];
	g->t = &g->v[n - 1];
	g->excess = NULL;

	for (i = 0; i < m; i += 1)
	{
		a = next_int();
		b = next_int();
		c = next_int();
		u = &g->v[a];

		v = &g->v[b];
		connect(u, v, c, g->e + i);
	}

	return g;
}

static void free_ctx(thread_ctx_t *ctx, int threadcount)
{
	for (int i = 0; i < threadcount; i += 1)
	{
		free(ctx[i].plq);
	}
	free(ctx);
}

static void free_graph(graph_t *g)
{
	int i;
	list_t *p;
	list_t *q;

	for (i = 0; i < g->n; i += 1)
	{
		p = g->v[i].edge;
		while (p != NULL)
		{
			q = p->next;
			free(p);
			p = q;
		}
	}
	free(g->v);
	free(g->e);
	free(g);
}

static void init_preflow(graph_t *g)
{
	list_t *p;
	edge_t *e;
	node_t *s = g->s;

	s->h = g->n;
	s->e = 0;

	for (p = s->edge; p != NULL; p = p->next)
	{
		e = p->edge;
		s->e += e->c;
	}

	for (p = s->edge; p != NULL; p = p->next)
		init_push(g, s, other(s, p->edge), p->edge);
}

static void wait_for_finish(graph_t *g)
{
	pthread_mutex_lock(&g->g_lock);
	while (!g->done)
		pthread_cond_wait(&g->cv, &g->g_lock);
	pthread_mutex_unlock(&g->g_lock);
}

static void setup(preflow_context_t *algo_ctx)
{
	graph_t *g = algo_ctx->g;

	init_preflow(g);
	init_mutexes(g);
	pthread_barrier_init(&algo_ctx->barrier, NULL, algo_ctx->threadcount);
	init_workers(algo_ctx);
}

static void teardown(preflow_context_t *algo_ctx)
{
	graph_t *g = algo_ctx->g;

	destroy_mutexes(g);
	int ret = pthread_barrier_destroy(&algo_ctx->barrier);
	join_workers(algo_ctx->threads, algo_ctx->threadcount);
}

static void preflow(preflow_context_t *algo_ctx)
{
	setup(algo_ctx);

	// UPDATE GRAPHS LOOP
	wait_for_finish(algo_ctx->g);

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

	tc = get_opt_thread_count();
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
	algo.barrier = barrier;

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
