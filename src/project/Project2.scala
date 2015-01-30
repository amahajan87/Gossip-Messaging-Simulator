package project

import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext._
import scala.math._
import java.security._
import akka.routing._
import akka.io._
import scala.util.Random

case object Done
case object StartNode
case class Done(sumEstimate: Double)

case object GossipMsg
case object GossipMsgSend
case object GossipMsgRcvd

case object PushSumMsgSend
case class PushSumMsgRcvd(s: Double, w: Double)

case class NetworkRefs(myNodes: Array[ActorRef], i: Int)
case class Start(nodeNos: Int, topology: String, algorithm: String)



object MainActor extends App{

		//def main(args: Array[String]){
		val system = ActorSystem("ActorSystem")
		val master = system.actorOf(Props(new Master), name = "Master")
		val nodeNos = args(0).toInt
		val topology = args(1)
		val algorithm = args(2)
		print("****Main********" + "   Nodes: " + nodeNos) 
		println("   Topology: " + topology + "   Algorithm: " + algorithm)
		master ! Start(nodeNos, topology, algorithm)
		//}

		
	//************************************Master***********************************		
	class Master extends Actor {
		
		var doneNodes = 0
		var pracSum: Double = 0
		var theorySum = 0
		var startTime: Long = _
		var nodeRefs: Array[ActorRef] = _
		var doneNodesArray: Array[Int] = _ 
		var _nodeNos: Int = 0
			  
		def receive = {
			case Start(nodeNos, topology, algorithm) =>
			    _nodeNos = pow(sqrt(nodeNos).toInt + 1,2).toInt
				println("Edited Number of nodes : " + _nodeNos)
			  	nodeRefs = new Array[ActorRef](_nodeNos)
			  	doneNodesArray = new Array[Int](_nodeNos)
				//Start all the nodes
				for (i <- 0 until nodeRefs.length){
					theorySum = theorySum + i
					nodeRefs(i) = system.actorOf(Props(new NetworkNode(self)), name = "node" + i)
					}
	
				createTopology(topology)
	
				startTime = System.currentTimeMillis
				var startNode = _nodeNos/2
				
				if (algorithm.equals("push-sum")) {
					println("*********PushSum********* " + nodeRefs(startNode))
					nodeRefs(startNode) ! PushSumMsgSend
					}
				else if (algorithm.equals("gossip")){
					println("*********Gossip********* " + nodeRefs(startNode))
					//nodeRefs(startNode) ! GossipMsgRcvd
					nodeRefs(startNode) ! GossipMsg
					}
				
			case Done =>
				doneNodes += 1
				//doneNodesArray(sender.path.name.substring(4,sender.path.name.length()).toInt) = 1
				//for(i <- 0 until _nodeNos)
				//  print(doneNodesArray(i))
				//println(" ")
				if (doneNodes == _nodeNos){
					println("*********Convergence*********")
					println("Runtime : " + (System.currentTimeMillis - startTime))
					println("****************Shutting Down****************")
					//context.system.shutdown
					}
				
			case Done(sumNode) =>
				doneNodes += 1
				pracSum = pracSum + sumNode
				//println("Practical sum for node " + sender + " : " + sumNode)
				if (doneNodes == _nodeNos){
					println("*********Convergence*********")
					println("Practical Sum " + pracSum/_nodeNos + "  Theoretical Sum : " + theorySum/_nodeNos)
					println("Runtime Milliseconds : " + (System.currentTimeMillis - startTime))
					context.system.shutdown
					}
	
				//End receive		
			}

		//****************createTopology****************		
		def createTopology(topology: String){
	
			//Create Line Topology
			if(topology.equals("line")){
				for (i <- 0 until nodeRefs.length){
					if (i == 0){
						var myNodes = new Array[ActorRef](1)
						myNodes(0) = nodeRefs(i+1)
						nodeRefs(i) ! NetworkRefs(myNodes, i)
						}
					else if (i == nodeRefs.length-1){
						var myNodes = new Array[ActorRef](1)
						myNodes(0) = nodeRefs(i-1)
						nodeRefs(i) ! NetworkRefs(myNodes, i)
						}
					else {
						var myNodes = new Array[ActorRef](2)
						myNodes(0) = nodeRefs(i-1)
						myNodes(1) = nodeRefs(i+1)
						nodeRefs(i) ! NetworkRefs(myNodes, i)
						}
					}
				}//End Line Topology
			
			//Create Full Topology
			if(topology.equals("full")){
				for (i <- 0 until nodeRefs.length){
					var myNodes = new Array[ActorRef](nodeRefs.length)
					myNodes = nodeRefs
					nodeRefs(i) ! NetworkRefs(myNodes, i)
					}
				}//End Full Topology

			//Create 2D Topology
			if(topology.equals("2D")){
				var temp = sqrt(_nodeNos).toInt
				for (i <- 0 until nodeRefs.length){
					if (i < temp){
						if (i == 0) { 
							var myNodes = new Array[ActorRef](2) 
							myNodes(0) = nodeRefs(i + 1)
							myNodes(1) = nodeRefs(i + temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						else if (i == temp - 1){
							var myNodes = new Array[ActorRef](2) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						else { 
							var myNodes = new Array[ActorRef](3) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + 1)
							myNodes(2) = nodeRefs(i + temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						}
					else if (i >= _nodeNos - temp){
						if (i == _nodeNos - temp) { 
							var myNodes = new Array[ActorRef](2) 
							myNodes(0) = nodeRefs(i + 1)
							myNodes(1) = nodeRefs(i - temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						else if (i == _nodeNos - 1){
							var myNodes = new Array[ActorRef](2) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i - temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						else { 
							var myNodes = new Array[ActorRef](3) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + 1)
							myNodes(2) = nodeRefs(i - temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						}
					else {
						if (i % temp == 0) {
							var myNodes = new Array[ActorRef](3)
							myNodes(0) = nodeRefs(i + 1)
							myNodes(1) = nodeRefs(i - temp)
							myNodes(2) = nodeRefs(i + temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						else if ((i+1) % temp == 0) {
							var myNodes = new Array[ActorRef](3)
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i - temp)
							myNodes(2) = nodeRefs(i + temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						else{
							var myNodes = new Array[ActorRef](4)
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + 1)
							myNodes(2) = nodeRefs(i - temp)
							myNodes(3) = nodeRefs(i + temp)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							}
						}
					//Thread.sleep(2000)
					}//end for loop
				}//End 2D Topology

			//Create Imperfect 2D Topology
			if(topology.equals("imp2D")){
				var temp = sqrt(_nodeNos).toInt
				var random = 0
				for (i <- 0 until nodeRefs.length){
					random = Random.nextInt(_nodeNos)
					if (i < temp){
						if (i == 0) { 
							var myNodes = new Array[ActorRef](3) 
							myNodes(0) = nodeRefs(i + 1)
							myNodes(1) = nodeRefs(i + temp)
							myNodes(2) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						else if (i == temp - 1){
							var myNodes = new Array[ActorRef](3) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + temp)
							myNodes(2) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						else { 
							var myNodes = new Array[ActorRef](4) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + 1)
							myNodes(2) = nodeRefs(i + temp)
							myNodes(3) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						}
					else if (i >= _nodeNos - temp){
						if (i == _nodeNos - temp) { 
							var myNodes = new Array[ActorRef](3) 
							myNodes(0) = nodeRefs(i + 1)
							myNodes(1) = nodeRefs(i - temp)
							myNodes(2) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						else if (i == _nodeNos - 1){
							var myNodes = new Array[ActorRef](3) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i - temp)
							myNodes(2) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						else { 
							var myNodes = new Array[ActorRef](4) 
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + 1)
							myNodes(2) = nodeRefs(i - temp)
							myNodes(3) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						}
					else {
						if (i % temp == 0) {
							var myNodes = new Array[ActorRef](4)
							myNodes(0) = nodeRefs(i + 1)
							myNodes(1) = nodeRefs(i - temp)
							myNodes(2) = nodeRefs(i + temp)
							myNodes(3) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						else if ((i+1) % temp == 0) {
							var myNodes = new Array[ActorRef](4)
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i - temp)
							myNodes(2) = nodeRefs(i + temp)
							myNodes(3) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						else{
							var myNodes = new Array[ActorRef](5)
							myNodes(0) = nodeRefs(i - 1)
							myNodes(1) = nodeRefs(i + 1)
							myNodes(2) = nodeRefs(i - temp)
							myNodes(3) = nodeRefs(i + temp)
							myNodes(4) = nodeRefs(random)
							nodeRefs(i) ! NetworkRefs(myNodes, i)
							//println ("i/ random node" + i + "  :  " + random)
							}
						}
					//Thread.sleep(2000)
					}//end for loop
				}//End Imp 2D Topology
			
			//Sleep for 5 seconds to allow nodes to be created
			Thread.sleep(1000) 
			println("****************Nodes have been created****************")
			}//End createTopology
	
	}//End Master Class
	  
	
	//************************************NetworkNode***********************************
	class NetworkNode(master: ActorRef) extends Actor {
	
		var myNodes: Array[ActorRef] = _
		var s:Double  = 0
		var w:Double = 1
		var msgSent = 0
		var msgRcvd = 0
		var next: Int = _
		var noChange = 0
		var s_w_Ratio:Double = 0 
		var powVal = pow(10,-10)
		var status = 0
		var temp: Double = 0
		val log = Logging(context.system, this)
		var sqrtNodes: Int = 0
		var sendIndex: Int = 0
		var rcvdIndex: Int = 0
		var _nodeNos: Int = 0
		
		def receive = {
	
			case StartNode =>
				println("Started" + self.path)
	
			case GossipMsg =>
				if (sender != self && (sender.path.name != "Master")){
					msgRcvd += 1
					sendIndex = sender.path.name.substring(4,sender.path.name.length()).toInt
					rcvdIndex = self.path.name.substring(4,self.path.name.length()).toInt
					//println ("*******Done********* " + "  " + sendIndex + "  " + rcvdIndex + "  " + sqrtNodes)
					log.info("\n" + (sendIndex/sqrtNodes).toInt + "," + (sendIndex % sqrtNodes) + "," + (sendIndex % sqrtNodes) + "," + sender.path.name + "," + self.path.name + "," + System.currentTimeMillis() + 
							 "\n" +	(rcvdIndex/sqrtNodes).toInt + "," + (rcvdIndex % sqrtNodes) + "," + (rcvdIndex % sqrtNodes) + "," + sender.path.name + "," + self.path.name + "," + System.currentTimeMillis())			
				}

				if (status == 0) {
				  master ! Done
				  status = 1
				  //println ("Done " + self)
				}

				if (msgRcvd < 10)	{
					next = Random.nextInt(myNodes.size)
					myNodes(next) ! GossipMsg
				  	//context.setReceiveTimeout(1.seconds)
					} 
				
			case ReceiveTimeout =>
			  	//println ("Msg Self " + self.path.name)
				self ! GossipMsg
				
			case PushSumMsgSend =>
				next = Random.nextInt(myNodes.size)
				s = s / 2
				w = w / 2
				myNodes(next) ! PushSumMsgRcvd(s, w)
	
			case PushSumMsgRcvd(s_Rcvd, w_Rcvd) =>
				msgRcvd += 1
				s = s + s_Rcvd
				w = w + w_Rcvd
				temp = s_w_Ratio - (s/w)
				if (temp < powVal)	noChange += 1
				else noChange = 0
				s_w_Ratio = s / w
				self ! PushSumMsgSend
				if(noChange == 3 && status == 0) {
					status = 1
					//println("Total messages for node " + self + " : " + msgRcvd)
					master ! Done(s_w_Ratio)
					}
	
			case NetworkRefs(receivedNodes, i) =>
			  	//println("Topology Created For Node : " + self)
				myNodes = receivedNodes
				//println("****myNodes.size*****" + myNodes.size)
				s = i
				_nodeNos = pow(sqrt(nodeNos).toInt + 1,2).toInt
				sqrtNodes = sqrt(nodeNos).toInt + 1

			
			/*
			case GossipMsgSend =>
				msgSent += 1
				next = Random.nextInt(myNodes.size)
				myNodes(Random.nextInt(myNodes.size)) ! GossipMsgRcvd
				if (msgSent <= 20){
					self ! GossipMsgSend
					}
			case GossipMsgRcvd =>
				if (msgRcvd < 1) {
					//println("****************Start Sending***************" + self)
					self ! GossipMsgSend
					master ! Done
				}
				//println("****************Received***************" + msgRcvd + "       " + self)
				msgRcvd += 1
			 */
				
		}//End receive
	
	}// End NetworkNode Class
	

}//End MainActor



