import akka.actor.{ActorRef, Terminated}
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

object ActorRefBackpressureFlowStage {
  case object StreamInit
  case object StreamAck
  case object StreamCompleted
  case class StreamFailed(ex: Throwable)
  case class StreamElementIn[A](element: A)
  case class StreamElementOut[A](element: A)
  case class StreamElementOutWithAck[A](element: A)
}

/**
  * Sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
  * First element is always `StreamInit`, then stream is waiting for acknowledgement message
  * `ackMessage` from the given actor which means that it is ready to process
  * elements. It also requires `ackMessage` message after each stream element
  * to make backpressure work. Stream elements are wrapped inside `StreamElementIn(elem)` messages.
  *
  * The target actor can emit elements at any time by sending a `StreamElementOut(elem)` message, which will
  * be emitted downstream when there is demand. There is also a StreamElementOutWithAck(elem), that combines the
  * StreamElementOut and StreamAck message in one.
  *
  * If the target actor terminates the stage will fail with a WatchedActorTerminatedException.
  * When the stream is completed successfully a `StreamCompleted` message
  * will be sent to the destination actor.
  * When the stream is completed with failure a `StreamFailed(ex)` message will be send to the destination actor.
  */
class ActorRefBackpressureFlowStage[In, Out](private val flowActor: ActorRef) extends GraphStage[FlowShape[In, Out]] {

  import ActorRefBackpressureFlowStage._

  val in: Inlet[In] = Inlet("ActorFlowIn")
  val out: Outlet[Out] = Outlet("ActorFlowOut")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var firstAckReceived: Boolean = false
    var firstPullReceived: Boolean = false
    var expectingAck: Boolean = false

    def stageActorReceive(messageWithSender: (ActorRef, Any)): Unit = {
      def onAck(): Unit = {
        firstAckReceived = true
        expectingAck = false
        pullIfNeeded()
        completeStageIfNeeded()
      }

      def onElementOut(elemOut: Any): Unit = {
        val elem = elemOut.asInstanceOf[Out]
        emit(out, elem)
      }

      messageWithSender match {
        case (_, StreamAck) =>
          onAck()

        case (_, StreamElementOut(elemOut)) =>
          onElementOut(elemOut)

        case (_, StreamElementOutWithAck(elemOut)) =>
          onElementOut(elemOut)
          onAck()

        case (_, Terminated(targetRef)) =>
          failStage(new WatchedActorTerminatedException("ActorRefBackpressureFlowStage", targetRef))

        case (actorRef, unexpected) =>
          failStage(new IllegalStateException(s"Unexpected message: `$unexpected` received from actor `$actorRef`."))
      }
    }
    private lazy val self = getStageActor(stageActorReceive)

    override def preStart(): Unit = {
      //initialize stage actor and watch flow actor.
      self.watch(flowActor)
      tellFlowActor(StreamInit)
      expectingAck = true
    }

    setHandler(in, new InHandler {

      override def onPush(): Unit = {
        val elementIn = grab(in)
        tellFlowActor(StreamElementIn(elementIn))
        expectingAck = true
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        self.unwatch(flowActor)
        tellFlowActor(StreamFailed(ex))
        super.onUpstreamFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        if(!expectingAck) {
          unwatchAndSendCompleted()
          super.onUpstreamFinish()
        }
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        firstPullReceived = true
        pullIfNeeded()
      }

      override def onDownstreamFinish(): Unit = {
        unwatchAndSendCompleted()
        super.onDownstreamFinish()
      }
    })

    private def pullIfNeeded(): Unit = {
      if(firstAckReceived && firstPullReceived && !hasBeenPulled(in)) {
        tryPull(in)
      }
    }

    private def completeStageIfNeeded(): Unit = {
      if(isClosed(in)) {
        self.unwatch(flowActor)
        tellFlowActor(StreamCompleted)
        this.completeStage() //Complete stage when in is closed, this might happen if onUpstreamFinish is called when still expecting an ack.
      }
    }

    private def unwatchAndSendCompleted(): Unit = {
      self.unwatch(flowActor)
      tellFlowActor(StreamCompleted)
    }

    private def tellFlowActor(message: Any): Unit = {
      flowActor.tell(message, self.ref)
    }
  }

  override def shape: FlowShape[In, Out] = FlowShape(in, out)

}