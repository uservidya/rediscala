package redis.bench

import java.net.InetSocketAddress


import akka.actor._
import akka.io._
import akka.io.Tcp.Command
import akka.io.TcpPipelineHandler.{Init, WithinActorContext}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Await, Future, Promise}

import org.specs2.mutable.{SpecificationLike, Tags}
import org.specs2.time.NoTimeConversions
import akka.testkit.TestKit
import scala.collection.mutable
import redis.{RedisCommands, Operation}
import redis.protocol.{RedisProtocolReply, RedisReply}
import akka.io.Tcp.Connected
import akka.io.Tcp.Register
import akka.io.Tcp.CommandFailed
import akka.actor.Terminated
import redis.commands.Transactions

import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.compat.Platform


class SentinelClientWorker(stages: PipelineStage[PipelineContext, ByteString, ByteString, RedisReply, ByteString],
                           workerDescription: String = "Sentinel Client Worker") extends Actor with ActorLogging with Stash {

  val tcp = akka.io.IO(Tcp)(context.system)

  /* Current open requests */
  val promises = mutable.Queue[Promise[RedisReply]]()

  def receive = {
    case h: ConnectToHost ⇒
      tcp ! Tcp.Connect(h.address)

    case Connected(remoteAddr, localAddr) ⇒
      val init =
        TcpPipelineHandler.withLogger(log,
          stages >>
            new TcpReadWriteAdapter >>
            new BackpressureBuffer(lowBytes = 10000, highBytes = 100000, maxBytes = 10000000))

      val handler = context.actorOf(TcpPipelineHandler.props(init, sender, self).withDeploy(Deploy.local))
      context watch handler

      sender ! Register(handler)
      unstashAll()
      context.become(connected(init, handler))

    case CommandFailed(cmd: Command) ⇒
      context.stop(self) // Bit harsh at the moment, but should trigger reconnect and probably do better next time...

    case _ ⇒ stash()
  }

  def handleResponses(init: Init[WithinActorContext, ByteString, RedisReply], connection: ActorRef): Receive = {
    case init.Event(data) ⇒
      val pr = promises.dequeue()
      pr.success(data)

    case Terminated(`connection`) ⇒
      promises.foreach(_.failure(new Exception("TCP Actor disconnected")))
      context.stop(self)
  }

  def connected(init: Init[WithinActorContext, ByteString, RedisReply], connection: ActorRef): Receive = handleResponses(init, connection) orElse {
    case Operation(request, promise) =>
      promises.enqueue(promise)
      connection ! init.command(request)

    /*
    case o: Operation[ByteString, RedisReply] ⇒
      promises.enqueue(o.promise)
      connection ! init.Command(o.command)

    case o: StreamedOperation[ByteString, RedisReply] ⇒
      promises.enqueue(o.promise)
      o.stream |>>> Iteratee.foreach(x ⇒ connection ! init.Command(x))
*/
    case BackpressureBuffer.HighWatermarkReached ⇒
      context.become(highWaterMark(init, connection))
  }

  def highWaterMark(init: Init[WithinActorContext, ByteString, RedisReply], connection: ActorRef): Receive = handleResponses(init, connection) orElse {
    case BackpressureBuffer.LowWatermarkReached ⇒
      unstashAll()
      context.become(connected(init, connection))
    case _: Operation ⇒ stash()
  }
}

/* Worker commands */
case class ConnectToHost(address: InetSocketAddress)


trait HasByteOrder extends PipelineContext {
  def byteOrder: java.nio.ByteOrder
}

class RedisStage extends PipelineStage[PipelineContext, ByteString, ByteString, RedisReply, ByteString] {
  override def apply(ctx: PipelineContext): PipePair[ByteString, ByteString, RedisReply, ByteString] = new PipePair[ByteString, ByteString, RedisReply, ByteString] {
    val commandPipeline = {
      cmd: ByteString => {
        ctx.singleCommand(cmd)
      }
    }

    @tailrec
    private def decodeReplies(bs: ByteString, acc: mutable.Queue[Left[RedisReply, ByteString]]): ByteString = {
      if (bs.nonEmpty) {
        val r = RedisProtocolReply.decodeReply(bs)
        if (r.nonEmpty) {
          acc enqueue (Left(r.get._1))
          //onReceivedReply(r.get._1)
          decodeReplies(r.get._2, acc)
        } else {
          bs
        }
      } else {
        bs
      }
    }

    var bufferRead: ByteString = ByteString.empty

    val eventPipeline: (ByteString) => Iterable[Left[RedisReply, ByteString]] = {
      dataByteString: ByteString => {
        val acc = mutable.Queue[Left[RedisReply, ByteString]]()
        bufferRead = decodeReplies(bufferRead ++ dataByteString, acc).compact
        //println(dataByteString.utf8String)
        //ctx.singleEvent(protocol.Status(ByteString("PONG")))
        acc.toIterable
      }
    }
  }
}

case class RedisClient(host: String = "localhost", port: Int = 6379)(implicit system: ActorSystem) extends RedisCommands with Transactions {

  val redisStage = new RedisStage()

  val redisConnection: ActorRef = system.actorOf(Props(classOf[SentinelClientWorker], redisStage, "").withDispatcher("rediscala.rediscala-client-worker-dispatcher2"))

  redisConnection ! ConnectToHost(new InetSocketAddress(host, port))

  def send(request: ByteString): Future[Any] = {
    val promise = Promise[RedisReply]()
    redisConnection ! Operation(request, promise)
    promise.future
  }

  // will disconnect from the server
  def disconnect() {
    system stop redisConnection
  }

}


class RedisWithAkkaPipeline extends TestKit(ActorSystem()) with SpecificationLike with Tags with NoTimeConversions {

  sequential

  implicit val ec = ExecutionContext.Implicits.global

  import redis.Converter._

  "aa" should {
    "one test" in {

      val timeOut = 10 seconds

      val redis = RedisClient()

      Thread.sleep(2000)
      Await.result(redis.ping(), timeOut) mustEqual "PONG"

      val n = 200000
      for (i <- 1 to 10) yield {
        //redis.set("i", "0")
        val ops = n * i / 10
        timed(s"ping $ops times (run $i)", ops) {
          val results = for (_ <- 1 to ops) yield {
            redis.ping()
            //redis.incr("i")
            //redis.set("mykey", "myvalue") //.map(x => println(x))
            //redis.get("mykey").map(x => println(x))
          }
          Await.result(Future.sequence(results), FiniteDuration(30, "s"))
        }
        Platform.collectGarbage()

      }
      true mustEqual true // TODO remove that hack for spec2

    } tag ("benchmark")
  }


  def timed(desc: String, n: Int)(benchmark: ⇒ Unit) {
    println("* " + desc)
    val start = System.currentTimeMillis
    benchmark
    val stop = System.currentTimeMillis
    val elapsedSeconds = (stop - start) / 1000.0
    val opsPerSec = n / elapsedSeconds

    println(s"* - number of ops/s: $opsPerSec ( $n ops in $elapsedSeconds)")
  }
}
