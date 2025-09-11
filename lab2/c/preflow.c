/* This is an implementation of the preflow-push algorithm, by
 * Goldberg and Tarjan, for the 2021 EDAN26 Multicore programming labs.
 *
 * It is intended to be as simple as possible to understand and is
 * not optimized in any way.
 *
 * You should NOT read everything for this course.
 *
 * Focus on what is most similar to the pseudo code, i.e., the functions
 * preflow, push, and relabel.
 *
 * Some things about C are explained which are useful for everyone
 * for lab 3, and things you most likely want to skip have a warning
 * saying it is only for the curious or really curious.
 * That can safely be ignored since it is not part of this course.
 *
 * Compile and run with: make
 *
 * Enable prints by changing from 1 to 0 at PRINT below.
 *
 * Feel free to ask any questions about it on Discord
 * at #lab0-preflow-push
 *
 * A variable or function declared with static is only visible from
 * within its file so it is a good practice to use in order to avoid
 * conflicts for names which need not be visible from other files.
 *
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

#define B 8
#define PRINT 0 /* enable/disable prints. */

/* the funny do-while next clearly performs one iteration of the loop.
 * if you are really curious about why there is a loop, please check
 * the course book about the C preprocessor where it is explained. it
 * is to avoid bugs and/or syntax errors in case you use the pr in an
 * if-statement without { }.
 *
 */

#if PRINT
#define pr(...)                       \
	do                                \
	{                                 \
		fprintf(stderr, __VA_ARGS__); \
	} while (0)
#else
#define pr(...) /* no effect at all */
#endif

#define MIN(a, b) (((a) <= (b)) ? (a) : (b))

/* introduce names for some structs. a struct is like a class, except
 * it cannot be extended and has no member methods, and everything is
 * public.
 *
 * using typedef like this means we can avoid writing 'struct' in
 * every declaration. no new type is introduded and only a shorter name.
 *
 */

typedef struct graph_t graph_t;
typedef struct node_t node_t;
typedef struct edge_t edge_t;
typedef struct list_t list_t;

struct list_t
{
	edge_t *edge;
	list_t *next;
};

struct node_t
{
	int in_queue;
	int h;		  /* height.			*/
	int e;		  /* excess flow.			*/
	list_t *edge; /* adjacency list.		*/
	node_t *next; /* with excess preflow.		*/
	pthread_mutex_t n_lock;
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
    
	pthread_mutex_t g_lock;
	pthread_cond_t  cv;
    int done;
	int active_workers;
};

/* a remark about C arrays. the phrase above 'array of n nodes' is using
 * the word 'array' in a general sense for any language. in C an array
 * (i.e., the technical term array in ISO C) is declared as: int x[10],
 * i.e., with [size] but for convenience most people refer to the data
 * in memory as an array here despite the graph_t's v and e members
 * are not strictly arrays. they are pointers. once we have allocated
 * memory for the data in the ''array'' for the pointer, the syntax of
 * using an array or pointer is the same so we can refer to a node with
 *
 * 			g->v[i]
 *
 * where the -> is identical to Java's . in this expression.
 *
 * in summary: just use the v and e as arrays.
 *
 * a difference between C and Java is that in Java you can really not
 * have an array of nodes as we do. instead you need to have an array
 * of node references. in C we can have both arrays and local variables
 * with structs that are not allocated as with Java's new but instead
 * as any basic type such as int.
 *
 */

static char *progname;

#if PRINT

static int id(graph_t *g, node_t *v)
{
	/* return the node index for v.
	 *
	 * the rest is only for the curious.
	 *
	 * we convert a node pointer to its index by subtracting
	 * v and the array (which is a pointer) with all nodes.
	 *
	 * if p and q are pointers to elements of the same array,
	 * then p - q is the number of elements between p and q.
	 *
	 * we can of course also use q - p which is -(p - q)
	 *
	 * subtracting like this is only valid for pointers to the
	 * same array.
	 *
	 * what happens is a subtract instruction followed by a
	 * divide by the size of the array element.
	 *
	 */

	return v - g->v;
}
#endif

void error(const char *fmt, ...)
{
	/* print error message and exit.
	 *
	 * it can be used as printf with formatting commands such as:
	 *
	 *	error("height is negative %d", v->h);
	 *
	 * the rest is only for the really curious. the va_list
	 * represents a compiler-specific type to handle an unknown
	 * number of arguments for this error function so that they
	 * can be passed to the vsprintf function that prints the
	 * error message to buf which is then printed to stderr.
	 *
	 * the compiler needs to keep track of which parameters are
	 * passed in integer registers, floating point registers, and
	 * which are instead written to the stack.
	 *
	 * avoid ... in performance critical code since it makes
	 * life for optimizing compilers much more difficult. but in
	 * in error functions, they obviously are fine (unless we are
	 * sufficiently paranoid and don't want to risk an error
	 * condition escalate and crash a car or nuclear reactor
	 * instead of doing an even safer shutdown (corrupted memory
	 * can cause even more damage if we trust the stack is in good
	 * shape)).
	 *
	 */

	va_list ap;
	char buf[BUFSIZ];

	va_start(ap, fmt);
	vsprintf(buf, fmt, ap);

	if (progname != NULL)
		fprintf(stderr, "%s: ", progname);

	fprintf(stderr, "error: %s\n", buf);
	exit(1);
}

static int next_int()
{
	int x;
	int c;

	/* this is like Java's nextInt to get the next integer.
	 *
	 * we read the next integer one digit at a time which is
	 * simpler and faster than using the normal function
	 * fscanf that needs to do more work.
	 *
	 * we get the value of a digit character by subtracting '0'
	 * so the character '4' gives '4' - '0' == 4
	 *
	 * it works like this: say the next input is 124
	 * x is first 0, then 1, then 10 + 2, and then 120 + 4.
	 *
	 */

	x = 0;
	while (isdigit(c = getchar()))
		x = 10 * x + c - '0';

	return x;
}

static void *xmalloc(size_t s)
{
	void *p;

	/* allocate s bytes from the heap and check that there was
	 * memory for our request.
	 *
	 * memory from malloc contains garbage except at the beginning
	 * of the program execution when it contains zeroes for
	 * security reasons so that no program should read data written
	 * by a different program and user.
	 *
	 * size_t is an unsigned integer type (printed with %zu and
	 * not %d as for int).
	 *
	 */

	p = malloc(s);

	if (p == NULL)
		error("out of memory: malloc(%zu) failed", s);

	return p;
}

static void *xcalloc(size_t n, size_t s)
{
	void *p;

	p = xmalloc(n * s);

	/* memset sets everything (in this case) to 0. */
	memset(p, 0, n * s);

	/* for the curious: so memset is equivalent to a simple
	 * loop but a call to memset needs less memory, and also
	 * most computers have special instructions to zero cache
	 * blocks which usually are used by memset since it normally
	 * is written in assembler code. note that good compilers
	 * decide themselves whether to use memset or a for-loop
	 * so it often does not matter. for small amounts of memory
	 * such as a few bytes, good compilers will just use a
	 * sequence of store instructions and no call or loop at all.
	 *
	 */

	return p;
}

static void add_edge(node_t *u, edge_t *e)
{
	list_t *p;

	/* allocate memory for a list link and put it first
	 * in the adjacency list of u.
	 *
	 */

	p = xmalloc(sizeof(list_t));
	p->edge = e;
	p->next = u->edge;
	u->edge = p;
}

static void connect(node_t *u, node_t *v, int c, edge_t *e)
{
	/* connect two nodes by putting a shared (same object)
	 * in their adjacency lists.
	 *
	 */

	e->u = u;
	e->v = v;
	e->c = c;

	add_edge(u, e);
	add_edge(v, e);
}

/*
static void lock_edge_nodes(edge_t *a)
{
       if (a->u < a->v)
       {
               pthread_mutex_lock(&a->u->n_lock);
               pthread_mutex_lock(&a->v->n_lock);
       }
       else
       {
               pthread_mutex_lock(&a->v->n_lock);
               pthread_mutex_lock(&a->u->n_lock);
       }
}

static void unlock_edge_nodes(edge_t *a)
{
       if (a->u < a->v)
       {
               pthread_mutex_unlock(&a->v->n_lock);
               pthread_mutex_unlock(&a->u->n_lock);
       }
       else
       {
               pthread_mutex_unlock(&a->u->n_lock);
               pthread_mutex_unlock(&a->v->n_lock);
       }
}

*/

static void lock_edge_nodes(edge_t *a)
{
       if (a->u < a->v)
       {
               pthread_mutex_lock(&a->u->n_lock);
               pthread_mutex_lock(&a->v->n_lock);
       }
       else
       {
               pthread_mutex_lock(&a->v->n_lock);
               pthread_mutex_lock(&a->u->n_lock);
       }
}

static void unlock_edge_nodes(edge_t *a)
{
       if (a->u < a->v)
       {
               pthread_mutex_unlock(&a->v->n_lock);
               pthread_mutex_unlock(&a->u->n_lock);
       }
       else
       {
               pthread_mutex_unlock(&a->u->n_lock);
               pthread_mutex_unlock(&a->v->n_lock);
       }
}


static node_t *other(node_t *u, edge_t *e)
{
	if (u == e->u)
		return e->v;
	else
		return e->u;
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

static void enter_excess(graph_t *g, node_t *v)
{
	/* put v at the front of the list of nodes
	 * that have excess preflow > 0.
	 *
	 * note that for the algorithm, this is just
	 * a set of nodes which has no order but putting it
	 * it first is simplest.
	 *
	 */

    if (v == g->s || v == g->t) return;

    pthread_mutex_lock(&g->g_lock);
    if (!v->in_queue) {
        v->in_queue = 1;
        v->next = g->excess;
        g->excess = v;
        pthread_cond_signal(&g->cv);
    }
    pthread_mutex_unlock(&g->g_lock);
}

static node_t *leave_excess_no_lock(graph_t *g)
{
	// TODO: Obviously change this whole pattern - this is a quick fix.
    node_t* v = g->excess;
    if (v) {
        g->excess = v->next;
        v->next = NULL;
        v->in_queue = 0;
    }
    return v;
}

static node_t *leave_excess(graph_t *g)
{
	/* take any node from the set of nodes with excess preflow
	 * and for simplicity we always take the first.
	 *
	 */

    pthread_mutex_lock(&g->g_lock);
    node_t* v = g->excess;
    if (v) {
        g->excess = v->next;
        v->next = NULL;
        v->in_queue = 0;
    }
    pthread_mutex_unlock(&g->g_lock);
    return v;
}

static void push(graph_t *g, node_t *u, node_t *v, edge_t *e)
{
	int d; /* remaining capacity of the edge. */

	pr("push from %d to %d: ", id(g, u), id(g, v));
	pr("f = %d, c = %d, so ", e->f, e->c);

	lock_edge_nodes(e);
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
	unlock_edge_nodes(e);

	/* the following are always true. */

	assert(d >= 0);
	assert(u->e >= 0);
	assert(abs(e->f) <= e->c);

	if (u->e > 0)
	{
		/* still some remaining so let u push more. */
		enter_excess(g, u);
	}

	if (v->e == d)
	{
		/* since v has d excess now it had zero before and
		 * can now push.
		 *
		 */

		enter_excess(g, v);
	}
}

static void relabel(graph_t *g, node_t *u)
{
    int min_h = INT_MAX;
    list_t *p;
    edge_t *e;
    node_t *v;

    /* Iterate over all neighbors to find the minimum height with residual capacity */
    for (p = u->edge; p != NULL; p = p->next)
    {
        e = p->edge;

        /* Lock both endpoints to safely read flow and height */
        lock_edge_nodes(e);
        v = other(u, e);

        int rescap;
        if (u == e->u)
            rescap = e->c - e->f;  // residual capacity from u -> v
        else
            rescap = e->c + e->f;  // residual capacity from u <- v

        if (rescap > 0)
            min_h = MIN(min_h, v->h);

        unlock_edge_nodes(e);
    }

    /* Now set u->h safely */
    pthread_mutex_lock(&u->n_lock);
    if (min_h < INT_MAX)
        u->h = min_h + 1;  // standard relabel
    else
        u->h += 1;          // fallback if no residual neighbors
    pthread_mutex_unlock(&u->n_lock);

    /* Re-insert u into the excess worklist */
    enter_excess(g, u);      // make sure enter_excess checks in_queue
}

void* worker(void* arg) {
    graph_t* g = (graph_t*)arg;
    node_t* u = NULL;
    int can_push;
	int t_done = 0;

    while (1) {
		u = leave_excess(g);
		if(u == NULL) {
			pthread_mutex_lock(&g->g_lock);
			g->active_workers--;
			if (g->active_workers == 0 && g->excess == NULL) {
				g->done = 1;
				pthread_cond_broadcast(&g->cv);
				pthread_mutex_unlock(&g->g_lock);
				return NULL;
			}

			while ((u = leave_excess_no_lock(g)) == NULL && !g->done) {
				pthread_cond_wait(&g->cv, &g->g_lock);
			}

			if (g->done) {
				pthread_mutex_unlock(&g->g_lock);
				return NULL;
			}

			g->active_workers++;
			pthread_mutex_unlock(&g->g_lock);

			if (u == NULL) continue; // woke up but no work (done == 1)
		}

        // --- do push/relabel on node u ---
        can_push = 0;
        list_t* p;
        edge_t* e;
        node_t* v;
        int b;

        for (p = u->edge; p != NULL; p = p->next) {
            e = p->edge;
            if (u == e->u) { v = e->v; b = 1; } 
            else { v = e->u; b = -1; }
			pthread_mutex_lock(&u->n_lock);
			int uh = u->h;
			pthread_mutex_unlock(&u->n_lock);

			pthread_mutex_lock(&v->n_lock);
			int vh = v->h;
			pthread_mutex_unlock(&v->n_lock);

			lock_edge_nodes(e);
			int ef = e->f;
			unlock_edge_nodes(e);
			
			can_push = (uh > vh) && (b * ef < e->c);

            if (can_push) {
                push(g, u, v, e);
                break; // push at most once
            }
        }

        if (!can_push) {
            relabel(g, u);
		}
    }
}


void init_mutexes(graph_t *g)
{
	pthread_mutex_init(&g->g_lock, NULL);
	pthread_cond_init(&g->cv, NULL);
	g->done = 0;

	for(int i = 0; i < g->n; i+=1)
	{
		pthread_mutex_init(&g->v[i].n_lock, NULL);
	}
}

void destroy_mutexes(graph_t *g)
{
	pthread_mutex_destroy(&g->g_lock);
	pthread_cond_destroy(&g->cv);
	
	for(int i = 0; i < g->n; i+=1)
	{
		pthread_mutex_destroy(&g->v[i].n_lock);
	}
}

int preflow(graph_t *g)
{
	/* Initialize the graph lock and the individual nodes' locks */
	init_mutexes(g);

	/* --- Start of algorithm - kept sequential for now. */
	node_t *s;
	node_t *u;
	node_t *v;
	edge_t *e;
	list_t *p;
	int b;

	s = g->s;
	s->h = g->n;
	
	// Initialize sinks excess capacity to be the sum of outgoing capacities
	s->e = 0;
	for (p = s->edge; p != NULL; p = p->next) {
    	e = p->edge;
		if (s == e->u)
			s->e += e->c;
		else
			s->e += e->c;
	}

	/* start by pushing as much as possible (limited by
	 * the edge capacity) from the source to its neighbors.
	 *
	 */

	p = s->edge;
	while (p != NULL) {
		e = p->edge;
		push(g, s, other(s,e), e);
		p = p->next;
	}

	/* then loop until only s and/or t have excess preflow. */

	/* --- End of Start of algorithm, below is multi-threaded improvements */

	int threadcount = 4; // TODO: Optimal amount is device specific
	pthread_t threads[threadcount];
	g->active_workers = threadcount;
	g->done = 0;

	for(int i = 0; i < threadcount; i++) {
		if (pthread_create(&threads[i], NULL, worker, g) != 0) {
		    perror("pthread_create");
		    exit(1);
		}
	}

	pthread_mutex_lock(&g->g_lock);
	while(!g->done) {
		pthread_cond_wait(&g->cv, &g->g_lock);
	}
	pthread_mutex_unlock(&g->g_lock);

	/* --- After algorithm finish, join all created threads and destroy locks */
	for (int i = 0; i < threadcount; i++) {
		pthread_join(threads[i], NULL);
	}

	pr("\n algorithm finished - all threads inactive, no more active nodes\n");
	destroy_mutexes(g);
	return g->t->e;
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


int main(int argc, char *argv[])
{
	FILE *in;	/* input file set to stdin	*/
	graph_t *g; /* undirected graph. 		*/
	int f;		/* output from preflow.		*/
	int n;		/* number of nodes.		*/
	int m;		/* number of edges.		*/

	progname = argv[0]; /* name is a string in argv[0]. */

	in = stdin; /* same as System.in in Java.	*/

	n = next_int();
	m = next_int();

	/* skip C and P from the 6railwayplanning lab in EDAF05 */
	next_int();
	next_int();

	g = new_graph(in, n, m);

	fclose(in);

	f = preflow(g);

	printf("f = %d\n", f);

	free_graph(g);

	return 0;
}
