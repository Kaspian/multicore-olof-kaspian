import scala.util._
import java.util.Scanner
import java.io._
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await,ExecutionContext,Future,Promise}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.io._

case class Flow(f: Int)
case class Debug(debug: Boolean)
case class Control(control:ActorRef)
case class Source(n: Int)

case class Activated(self: ActorRef)
case class Push(amount: Int, fromH: Int, edge: Edge)
case class PushReply(amount: Int)

case object Discharge
case object Deactivated
case object Print
case object Start
case object Excess
case object Maxflow
case object Sink
case object Hello

class Edge(var u: ActorRef, var v: ActorRef, var c: Int) {
	var	f = 0
}

class Node(val index: Int) extends Actor {
	var	e = 0;				/* excess preflow. 						*/
	var	h = 0;				/* height. 							*/
	var k = 0;
	var activated = false;

	var cur = 0                       // current-arc index (0 .. degree-1)
	
	var	control:ActorRef = null		/* controller to report to when e is zero. 			*/
	var	source:Boolean	= false		/* true if we are the source.					*/
	var	sink:Boolean	= false		/* true if we are the sink.					*/
	var	edge: List[Edge] = Nil		/* adjacency list with edge objects shared with other nodes.	*/
	var	debug = false			/* to enable printing.						*/
	
	def min(a:Int, b:Int) : Int = { if (a < b) a else b }

	def deg: Int = edge.length
	def nextEdge(i: Int) = if (i + 1 < deg) i + 1 else 0

	def id: String = "@" + index;

	def other(a:Edge, u:ActorRef) : ActorRef = { if (u == a.u) a.v else a.u }

	def status: Unit = { if (debug) println(id + " e = " + e + ", h = " + h) }

	def enter(func: String): Unit = { if (debug) { println(id + " enters " + func); status } }
	def exit(func: String): Unit = { if (debug) { println(id + " exits " + func); status } }

	def relabel : Unit = {

		enter("relabel")

		h += 1

		exit("relabel")
	}

	def pushAll : Unit = {
		var i = cur
		var fullCycle = false

		while (e > 0 && !fullCycle) {
			val a = edge(i)
			val neighbor = other(a, self)
			var delta = 0;


			if(a.u.equals(self)) {
				delta = min(e, a.c - a.f)
				a.f += delta
			} else {
				delta = min(e, a.c + a.f)
				a.f -= delta
			}
			delta = delta.abs
			e -= delta.abs


			if(delta != 0) {
				k += 1

				neighbor ! Push(delta, h, a)
			}
			
			
			i = nextEdge(i)
			fullCycle = (i == cur)
		}
		cur = i	
	}
			


	def receive = {

	case Debug(debug: Boolean)	=> this.debug = debug

	case Print => status

	case Excess => { 
		sender ! Flow(e) /* send our current excess preflow to actor that asked for it. */ }

	case edge:Edge => { this.edge = edge :: this.edge /* put this edge first in the adjacency-list. */ }

	case Control(control:ActorRef)	=> this.control = control

	case Sink	=> { sink = true}

	case Source(n:Int) => {
	  h = n
	  source = true

	  for (a <- edge) {
		e -= a.c
	    val neighbor = other(a, self)
		if (a.u.equals(self)) {
			a.f = a.c
		} else {
			a.f = -a.c
		}

		neighbor ! Push(a.c, h, a) // neighbor gains excess
	    }
	  
	}

	case Push(amount, fromH, edge) => {
		if (fromH > h) {
			e+= amount

			if(source || sink) {
				control ! Flow(e)
			}

			sender ! PushReply(-1)

			if (!activated) {activated = true; control ! Activated(self)}
		} else {
			if(edge.u.equals(self)) {
				edge.f += amount
			} else {
				edge.f -= amount
			}
			sender ! PushReply(amount)
			
		}
	}

	case PushReply(amount) => {
		if(amount == -1) {
			
		} else {
			e += amount
			k -= 1
		}

		if(!source) assert(e >= 0)
		if (e == 0 || sink || source) {
			if(debug){
				print(id + "deactivated" + e)
			}
			activated = false;
			
		} else if (k <= 0) {
			if(debug){
				println(id +"relabel time" + e)
			}
			relabel
			k = 0
			control ! Activated(self)

		} else {
			if(debug){
				println(id +"try again"+ e)
			}
			k =0
			control ! Activated(self)
		}
	}

	case Discharge => {
		if(source || sink) {
			control ! Deactivated
		} else {
			pushAll
		}
	}

	case _		=> {
		println("" + index + " received an unknown message" + _) }

		assert(false)
	}

}


class Preflow extends Actor
{
	var	s	= 0;			/* index of source node.					*/
	var	t	= 0;			/* index of sink node.					*/
	var	n	= 0;			/* number of vertices in the graph.				*/
	var sourceFlow = 0;
	var sinkFlow = 0;
	var	edge:Array[Edge]	= null	/* edges in the graph.						*/
	var	node:Array[ActorRef]	= null	/* vertices in the graph.					*/
	var	ret:ActorRef 		= null	/* Actor to send result to.					*/

	def receive = {

	case node:Array[ActorRef]	=> {
		this.node = node
		n = node.size
		s = 0
		t = n-1
		for (u <- node)
			u ! Control(self)
		node(t) ! Sink
		node(s) ! Source(n)
	}

	case edge:Array[Edge] => this.edge = edge

	case Flow(f:Int) => {
		if (sender.equals(node(t))) {
			sinkFlow = f
		} else if(sender.equals(node(s))) {
			sourceFlow = f
		}
		if(sinkFlow + sourceFlow == 0) {
			ret ! sinkFlow
		}

	}

	case Maxflow => {
		ret = sender
		
		//node(t) ! Excess	/* ask sink for its excess preflow (which certainly still is zero). */
		
	}

	case Activated(who) => {
		who ! Discharge
	}
	}
}

object main extends App {
	implicit val t = Timeout(100 seconds);

	val	begin = System.currentTimeMillis()
	val system = ActorSystem("Main")
	val control = system.actorOf(Props[Preflow], name = "control")

	var	n = 0;
	var	m = 0;
	var	edge: Array[Edge] = null
	var	node: Array[ActorRef] = null

	val	s = new Scanner(System.in);

	n = s.nextInt
	m = s.nextInt

	/* next ignore c and p from 6railwayplanning */
	s.nextInt
	s.nextInt

	node = new Array[ActorRef](n)

	for (i <- 0 to n-1)
		node(i) = system.actorOf(Props(new Node(i)), name = "v" + i)

	edge = new Array[Edge](m)

	for (i <- 0 to m-1) {

		val u = s.nextInt
		val v = s.nextInt
		val c = s.nextInt

		edge(i) = new Edge(node(u), node(v), c)

		node(u) ! edge(i)
		node(v) ! edge(i)
	}

	control ! node
	control ! edge

	val flow = control ? Maxflow
	val f = Await.result(flow, t.duration)

	println("f = " + f)

	system.stop(control);
	system.terminate()

	val	end = System.currentTimeMillis()

	println("t = " + (end - begin) / 1000.0 + " s")
}
