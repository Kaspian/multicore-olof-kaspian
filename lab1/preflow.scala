import scala.util._
import java.util.Scanner
import java.io._
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await,ExecutionContext,Future,Promise}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.io._

case class Flow(f: Int)
case class Debug(debug: Boolean)
case class Control(control:ActorRef)
case class Source(n: Int)
case class ExcessFlow(e: Int)
case class Push(delta:Int,height:Int,a:Edge)
case class PushReply(excessBack:Int)

case object Print
case object Start
case object Excess
case object Maxflow
case object Sink
case object Hello
case object Activate

class Edge(var u: ActorRef, var v: ActorRef, var c: Int) {
	var	f = 0
}

class Node(val index: Int) extends Actor {
	var	e = 0;				/* excess preflow. 						*/
	var	h = 0;				/* height. 							*/
	var k = 0;         //Unanswered pushes
	var shufflesDone = 0;
	var	control:ActorRef = null		/* controller to report to when e is zero. 			*/
	var	source:Boolean	= false		/* true if we are the source.					*/
	var	sink:Boolean	= false		/* true if we are the sink.					*/
	var rejected = false
	//var	edge: List[Edge] = Nil	/* adjacency list with edge objects shared with other nodes.	*/
	var edge: mutable.ArrayDeque[Edge] = mutable.ArrayDeque.empty
	var pushedNodes: mutable.HashSet[ActorRef] = mutable.HashSet.empty
	var	debug = false			/* to enable printing.						*/

	
	def min(a:Int, b:Int) : Int = { if (a < b) a else b }

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


	def receive = {

	case Debug(debug: Boolean)	=> this.debug = debug

	case Print => status

	case Excess => { sender ! Flow(e) /* send our current excess preflow to actor that asked for it. */ }

	//case edge:Edge => { this.edge = edge :: this.edge /* put this edge first in the adjacency-list. */ }
	case edge: Edge => { this.edge.prepend(edge) }

	case Control(control:ActorRef)	=> this.control = control

	case Sink	=> { sink = true }

	case Source(n:Int)	=> { 
		h = n 
		source = true

		for(a <- edge){  /* Loop through all edges and push capacity */
				e-= a.c
				other(a,self) ! Push(a.c,h,a)
			}
			control ! ExcessFlow(e) /*Message controller with negative flow */
		}

	case Push(delta:Int,height:Int,a:Edge) => {
		if(height>h){ //Push accepted
			if (self == a.u) a.f += delta else a.f -= delta //Update flow of edge depending on u or v node
			e += delta
			
			if(sink || source) control ! ExcessFlow(delta) //Message flow to controller if source or sink

			sender ! PushReply(0) //Send 0 PushReply to confirm that Push was accepted
			self ! Activate 

		}else{ //Push rejected
			sender ! PushReply(delta) //PushReply to confirm rejected
		}
	}

	case PushReply(amount:Int) => {
		e += amount
		k -= 1
		
		if(amount > 0){ 
			rejected = true
		}else{
			pushedNodes += sender
		}
		k -= 1

		if(k == 0) {
			if(rejected){ //reshuffle to not start att same edge every time
				edge.append(edge.removeHead())
				rejected = false
				shufflesDone +=1
				if(shufflesDone >= edge.length){ //If reshuffled through whole list, relabel
					relabel
					shufflesDone = 0
				}
				self ! Activate
			} else { //Reset succesful pushes
				pushedNodes.clear()
			}
		}
	}	

	case Activate => {

		if(!(sink || source) && e > 0){
			for(a <- edge if e > 0){
				val neighbor = other(a, self)
				if(!(pushedNodes.contains(neighbor))){
					var res = if (self == a.u) a.c + a.f
								else a.c - a.f
					var delta = min(e, res)
					if(delta > 0) {
						k += 1
						e -= delta
						neighbor ! Push(delta,h,a)
					}
				}
			}
		}

	} 


	case _		=> {
		println("" + index + " received an unknown message" + _)

		assert(false)
	}
 	}
}


class Preflow extends Actor
{
	var	s	= 0;			/* index of source node.					*/
	var	t	= 0;			/* index of sink node.					*/
	var	n	= 0;			/* number of vertices in the graph.				*/
	var	edge:Array[Edge]	= null	/* edges in the graph.						*/
	var	node:Array[ActorRef]	= null	/* vertices in the graph.					*/
	var	ret:ActorRef 		= null	/* Actor to send result to.					*/
	var totalExcess = 0;
	var debug = true

	def enter(func: String): Unit = { if (debug) { println("Control enters " + func)}}
	def exit(func: String): Unit = { if (debug) { println("Control exits " + func)}}

	def receive = {

	case node:Array[ActorRef]	=> {
		this.node = node
		n = node.size
		s = 0
		t = n-1
		for (u <- node)
			u ! Control(self)
	}

	case edge:Array[Edge] => this.edge = edge

	case Flow(f:Int) => {
		ret ! f			/* somebody (hopefully the sink) told us its current excess preflow. */
	}

	case Maxflow => {
		ret = sender

		node(s) ! Source(n)
		node(t) ! Sink
		node(s) ! Print
		//node(t) ! Excess	/* ask sink for its excess preflow (which certainly still is zero). */
		/*if(debug)*/ //sender ! 666; /* 666 for debugging purposes*/
	}

	case ExcessFlow(e:Int)  => {
		if(debug) println(totalExcess)
		totalExcess += e
		if(totalExcess == 0) node(t) ! Excess
	} 
	}
}

object main extends App {
	implicit val t = Timeout(10 seconds);

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
