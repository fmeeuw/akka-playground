
import ActorRefBackpressureFlowStage._
import akka.actor.{ActorSystem, PoisonPill}
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, WatchedActorTerminatedException}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ActorRefBackpressureFlowStageTest extends TestKit(ActorSystem("ActorFlowTest"))
  with WordSpecLike
  with Matchers
  with Eventually {


  val settings: ActorMaterializerSettings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)
    .withFuzzing(true)

  implicit val materializer: Materializer = ActorMaterializer(settings)
  val expectTimeout: FiniteDuration = 5.seconds



  "The ActorFlow" should {

    "do the first pull when first the StreamAck is received, followed by an upstream pull." in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>

        //Input buffer requests 1 before source is actually pulled, therefore sending first element already to clear requests.
        sourceProbe.sendNext(1)
        sourceProbe.pending shouldEqual 0


        actorProbe.expectMsg(StreamInit)
        intercept[AssertionError] {
          sourceProbe.expectRequest()
        }

        actorProbe.lastSender ! StreamAck
        intercept[AssertionError] {
          sourceProbe.expectRequest()
        }

        sinkProbe.request(1)
        sourceProbe.expectRequest() shouldEqual 1
      }
    }

    "do the first pull when first the upstream pull is received, followed by a StreamAck." in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>

        //Input buffer requests 1 before source is actually pulled, therefore sending first element already to clear requests.
        sourceProbe.sendNext(1)
        sourceProbe.pending shouldEqual 0

        actorProbe.expectMsg(StreamInit)
        intercept[AssertionError] {
          sourceProbe.expectRequest()
        }

        sinkProbe.request(1)
        intercept[AssertionError] {
          sourceProbe.expectRequest()
        }

        actorProbe.lastSender ! StreamAck
        sourceProbe.expectRequest() shouldEqual 1
      }
    }

    "pull after a StreamElementIn message is acknowledged." in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>

        //Input buffer requests 1 before source is actually pulled, therefore sending first element already to clear requests.
        sourceProbe.sendNext(1)
        sourceProbe.pending shouldEqual 0

        //first pull after StreamAck+upstream pull
        sinkProbe.request(1)
        actorProbe.expectMsg(StreamInit)
        actorProbe.lastSender ! StreamAck

        //Send next element already to clear pending requests.
        sourceProbe.sendNext(2)
        sourceProbe.pending shouldEqual 0


        actorProbe.expectMsg(expectTimeout, StreamElementIn(1))
        intercept[AssertionError] {
          sourceProbe.expectRequest()
        }

        actorProbe.lastSender ! StreamAck
        sourceProbe.expectRequest() shouldEqual 1
      }
    }

    "send a StreamFailed message to the actor, when upstream fails." in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>
        actorProbe.expectMsg(StreamInit)
        actorProbe.lastSender ! StreamAck

        sourceProbe.sendError(new IllegalStateException("BOOM"))
        actorProbe.expectMsgPF() {
          case StreamFailed(ex) if ex.isInstanceOf[IllegalStateException] =>
        }
      }
    }

    "send a StreamCompleted message to the flow actor, when upstream completes." in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>
        actorProbe.expectMsg(StreamInit)
        actorProbe.lastSender ! StreamAck

        sourceProbe.sendComplete()
        actorProbe.expectMsg(StreamCompleted)
      }
    }

    "send a streamCompleted message to the flow actor, when downstream finishes." in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>
        actorProbe.expectMsg(StreamInit)
        actorProbe.lastSender ! StreamAck

        sinkProbe.cancel()
        actorProbe.expectMsg(StreamCompleted)
      }
    }

    "fail the stage, when the flow actor terminates." in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>
        sinkProbe.expectSubscription()
        actorProbe.ref ! PoisonPill

        val receivedError = sinkProbe.expectError()
        receivedError shouldBe a[WatchedActorTerminatedException]
        receivedError.getMessage shouldEqual new WatchedActorTerminatedException("ActorRefBackpressureFlowStage", actorProbe.ref).getMessage
      }
    }

    "work correctly when the flow actor replies on each StreamElementIn element with a StreamElementOut, regardless whether the StreamAck is before or after the StreamElementOut. (linear)" in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>
        actorProbe.expectMsg(StreamInit)
        actorProbe.lastSender ! StreamAck

        receiveAckAndEmit(actorProbe, sourceProbe, sinkProbe)(1, 101)
        receiveEmitAndAck(actorProbe, sourceProbe, sinkProbe)(2, 102)
        receiveAckAndEmit(actorProbe, sourceProbe, sinkProbe)(3, 103)
        receiveEmitAndAck(actorProbe, sourceProbe, sinkProbe)(4, 104)
        receiveAckAndEmit(actorProbe, sourceProbe, sinkProbe)(5, 105)
        receiveEmitAndAck(actorProbe, sourceProbe, sinkProbe)(6, 106)
      }
    }

    "work correctly, when the  when the flow actor sends multiple StreamElementOut messages without StreamElementIn messages received. (detached)" in {
      withFixture { (actorProbe, sourceProbe, sinkProbe) =>
        actorProbe.expectMsg(StreamInit)
        actorProbe.lastSender ! StreamAck

        emit(actorProbe, sinkProbe)(101)
        emit(actorProbe, sinkProbe)(102)
        emit(actorProbe, sinkProbe)(103)
        receiveAckAndEmit(actorProbe, sourceProbe, sinkProbe)(1, 104)
        emit(actorProbe, sinkProbe)(105)
        emit(actorProbe, sinkProbe)(106)
        emit(actorProbe, sinkProbe)(107)
        receiveEmitAndAck(actorProbe, sourceProbe, sinkProbe)(1, 108)
      }
    }

    def receiveAckAndEmit(actorProbe: TestProbe, sourceProbe: TestPublisher.Probe[Int], sinkProbe: TestSubscriber.Probe[Int])(elementIn: Int, elementOut: Int): Unit = {
      receiveAndAck(actorProbe, sourceProbe, sinkProbe)(elementIn)
      emit(actorProbe, sinkProbe)(elementOut)
    }

    def receiveEmitAndAck(actorProbe: TestProbe, sourceProbe: TestPublisher.Probe[Int], sinkProbe: TestSubscriber.Probe[Int])(elementIn: Int, elementOut: Int): Unit = {
      sinkProbe.request(1)
      sourceProbe.sendNext(elementIn)
      actorProbe.expectMsg(expectTimeout, StreamElementIn(elementIn))
      emit(actorProbe, sinkProbe)(elementOut)
      actorProbe.lastSender ! StreamAck
    }

    def receiveAndAck(actorProbe: TestProbe, sourceProbe: TestPublisher.Probe[Int], sinkProbe: TestSubscriber.Probe[Int])(elementIn: Int): Unit = {
      sinkProbe.request(1)
      sourceProbe.sendNext(elementIn)
      actorProbe.expectMsg(expectTimeout, StreamElementIn(elementIn))
      actorProbe.lastSender ! StreamAck
    }

    def emit(actorProbe: TestProbe, sinkProbe: TestSubscriber.Probe[Int])(elementOut: Int): Unit = {
      sinkProbe.request(1)
      actorProbe.lastSender ! StreamElementOut(elementOut)
      sinkProbe.expectNext(expectTimeout) shouldEqual elementOut
    }

    def withFixture(f: (TestProbe, TestPublisher.Probe[Int], TestSubscriber.Probe[Int]) => Unit): Unit = {
      val actorProbe = TestProbe()
      val (sourceProbe, sinkProbe) = TestSource.probe[Int]
        .via(new ActorRefBackpressureFlowStage[Int, Int](() => actorProbe.ref))
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()
      f(actorProbe, sourceProbe, sinkProbe)
    }

  }

}



