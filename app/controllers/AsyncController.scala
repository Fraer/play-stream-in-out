package controllers

import javax.inject._

import akka.actor.ActorSystem
import play.api.mvc._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

import akka.stream.ThrottleMode
import play.api.mvc.BodyParser
import play.api.libs.streams._
import akka.util.ByteString
import akka.stream.scaladsl._
import play.api.libs.json.{Reads, Writes}


/**
 * This controller creates an `Action` that demonstrates how to write
 * simple asynchronous code in a controller. It uses a timer to
 * asynchronously delay sending a response for 1 second.
 *
 * @param cc standard controller components
 * @param actorSystem We need the `ActorSystem`'s `Scheduler` to
 * run code after a delay.
 * @param exec We need an `ExecutionContext` to execute our
 * asynchronous code.  When rendering content, you should use Play's
 * default execution context, which is dependency injected.  If you are
 * using blocking operations, such as database or network access, then you should
 * use a different custom execution context that has a thread pool configured for
 * a blocking API.
 */
@Singleton
class AsyncController @Inject()(cc: ControllerComponents,
                                actionBuilder: DefaultActionBuilder,
                                actorSystem: ActorSystem)(implicit exec: ExecutionContext)
  extends AbstractController(cc) {

  /**
   * Creates an Action that returns a plain text message after a delay
   * of 1 second.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/message`.
   */
  def message = Action.async {
    getFutureMessage(1.second).map { msg => Ok(msg) }
  }

  private def getFutureMessage(delayTime: FiniteDuration): Future[String] = {
    val promise: Promise[String] = Promise[String]()
    actorSystem.scheduler.scheduleOnce(delayTime) {
      promise.success("Hi!")
    }(actorSystem.dispatcher) // run scheduled tasks using the actor system's dispatcher
    promise.future
  }

  // curl -N --header "Content-Type: text/csv" --request POST --data-binary "@username.csv" http://localhost:9000/test

  val sourceBodyParser = BodyParser("streamed") { _ =>
    Accumulator.source[ByteString].map(Right.apply)
  }

  def test = Action.async(sourceBodyParser){ request =>

    println(s"contents: ${request.body}")

    val ff = Flow[ByteString]
      // We split by the new line character, allowing a maximum of 1000 characters per line
      .via(Framing.delimiter(ByteString("\n"), 1000, allowTruncation = true))
      // Turn each line to a String and split it by commas
      .map(_.utf8String.trim.split(",").toSeq)

    val newSource = request.body
      .via(ff)
      .map{ prePart =>
        println(s"processing: ${prePart}")
        prePart
      }
      .mapConcat{identity}
      .grouped(4)
      .mapAsyncUnordered(2) { group =>
        println(s"processing group of ${group.size} parts")
        Future.successful(group.mkString(","))
      }
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .map{ part =>
        println(s"----> ${part}")
        part
      }
      .filter(_.nonEmpty)

    Future.successful(Results.Ok.chunked(newSource))
  }

}