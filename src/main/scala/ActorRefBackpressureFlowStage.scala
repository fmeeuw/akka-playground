import akka.actor.{ActorRef, Status, Terminated}
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

object ActorRefBackpressureFlowStage {
  case object StreamInit
  case object StreamAck
  case object StreamCompleted
  case class StreamFailed(ex: Throwable)
  case class StreamElementIn[A](element: A)
  case class StreamElementOut[A](element: A)
}

/**
  * Sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
  * First element is always `StreamInit`, then stream is waiting for acknowledgement message
  * `ackMessage` from the given actor which means that it is ready to process
  * elements. It also requires `ackMessage` message after each stream element
  * to make backpressure work. Stream elements are wrapped inside `StreamElementIn(elem)` messages.
  *
  * The target actor can emit elements at any time by sending a `StreamElementOut(elem)` message, which will
  * be emitted downstream when there is demand.
  *
  * If the target actor terminates the stage will fail with a WatchedActorTerminatedException.
  * When the stream is completed successfully a `StreamCompleted` message
  * will be sent to the destination actor.
  * When the stream is completed with failure a `StreamFailed(ex)` message will be send to the destination actor.
  */
class ActorRefBackpressureFlowStage[In, Out](mkActor: () => ActorRef) extends GraphStage[FlowShape[In, Out]] {

  import ActorRefBackpressureFlowStage._

  val in: Inlet[In] = Inlet("ActorFlowIn")
  val out: Outlet[Out] = Outlet("ActorFlowOut")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    //Used as sender, when sending messages from this graphStage.
    implicit var thisStageActor: ActorRef = ActorRef.noSender
    var flowActor: ActorRef = ActorRef.noSender
    var firstPullReceived: Boolean = false
    var pullOnFirstPullReceived: Boolean = false

    override def preStart(): Unit = {

      def messageHandler(receive: (ActorRef, Any)): Unit = {
        receive match {
          case (_, StreamAck) =>
            if(firstPullReceived) {
              if (!isClosed(in) && !hasBeenPulled(in)) {
                pull(in)
              }
            } else {
              pullOnFirstPullReceived = true
            }

          case (_, StreamElementOut(elemOut)) =>
            val elem = elemOut.asInstanceOf[Out]
            emit(out, elem)

          case (_, Terminated(targetRef)) =>
            failStage(new WatchedActorTerminatedException("ActorRefBackpressureFlowStage", targetRef))

          case (actorRef, unexpected) =>
            failStage(new IllegalStateException(s"Unexpected message: `$unexpected` received from actor `$actorRef`."))
        }
      }

      flowActor = mkActor()
      thisStageActor = getStageActor(messageHandler).ref
      stageActor.watch(flowActor)
      flowActor ! StreamInit
    }

    setHandler(in, new InHandler {

      override def onPush(): Unit = {
        val elementIn = grab(in)
        flowActor ! StreamElementIn(elementIn)
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        flowActor ! StreamFailed(ex)
        super.onUpstreamFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        flowActor ! StreamCompleted
        super.onUpstreamFinish()
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if(!firstPullReceived) {
          firstPullReceived = true
          if(pullOnFirstPullReceived) {
            if (!isClosed(in) && !hasBeenPulled(in)) {
              pull(in)
            }
          }
        }

      }

      override def onDownstreamFinish(): Unit = {
        flowActor ! StreamCompleted
        super.onDownstreamFinish()
      }
    })

  }

  override def shape: FlowShape[In, Out] = FlowShape(in, out)

}