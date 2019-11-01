import ActorRefBackpressureFlowStage._
import akka.actor.{ActorRef, Terminated}
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.util.Failure

class ActorRefBackpressureFlowLinearStage[In, Out](private val flowActor: ActorRef) extends GraphStage[FlowShape[In, Out]] {

  val in: Inlet[In] = Inlet("ActorFlowLinearIn")
  val out: Outlet[Out] = Outlet("ActorFlowLinearOut")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var expectingAck: Boolean = false

    def stageActorReceive(messageWithSender: (ActorRef, Any)): Unit = {
      messageWithSender match {
        case (_, elemOut: Out) =>
          val elem = elemOut.asInstanceOf[Out]
          emit(out, elem)
          completeStageIfNeeded()
          expectingAck = false

        case (actorRef, Failure(cause)) =>
          terminateActorAndFailStage(new RuntimeException(s"Exception during processing by actor $actorRef: ${cause.getMessage}", cause))

        case (_, Terminated(targetRef)) =>
          failStage(new WatchedActorTerminatedException("ActorRefBackpressureFlowStage", targetRef))

        case (actorRef, unexpected) =>
          terminateActorAndFailStage(new IllegalStateException(s"Unexpected message: `$unexpected` received from actor `$actorRef`."))
      }
    }

    private lazy val self = getStageActor(stageActorReceive)

    override def preStart(): Unit = {
      //initialize stage actor and watch flow actor.
      self.watch(flowActor)
    }

    setHandler(in, new InHandler {

      override def onPush(): Unit = {
        val elementIn = grab(in)
        tellFlowActor(elementIn)
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
        if(!hasBeenPulled(in)) {
          tryPull(in)
        }
      }

      override def onDownstreamFinish(): Unit = {
        unwatchAndSendCompleted()
        super.onDownstreamFinish()
      }
    })

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

    private def terminateActorAndFailStage(ex: Throwable): Unit = {
      self.unwatch(flowActor)
      tellFlowActor(StreamFailed(ex))
      failStage(ex)
    }
  }

  override def shape: FlowShape[In, Out] = FlowShape(in, out)
}
