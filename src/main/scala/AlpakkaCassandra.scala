import java.nio.file.FileSystems
import java.security.KeyFactory
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

import AlpakkaCassandra.system.dispatcher
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSink}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.alpakka.googlecloud.pubsub.{PubSubMessage, PublishRequest, ReceivedMessage}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Source}
import akka.util.ByteString
import com.datastax.driver.core.{PreparedStatement, Row}

import scala.concurrent.Future
import scala.util.matching.Regex
// import akka.actor.scaladsl.Behaviors
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.Sink
import com.datastax.driver.core.{Cluster, Session, SimpleStatement}


case class People(id: String, name: String, age: Integer)

object AlpakkaCassandra extends App {
//  def transform(row: Row): People = {
//    println(s"Got a people from cassandra table : [$row]")
//    People(
//      row.getString("id"),
//      row.getString("name"),
//      row.getInt("age")
//    )
//  }
//
  val cluster = Cluster.builder()
    .addContactPoint("localhost")
    .withPort(9042)
    .build()
  implicit val session: Session = cluster
    .connect("DataCassandraExamples")

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
//
//  val stmt = new SimpleStatement(s"SELECT * FROM People").setFetchSize(20)
//  CassandraSource(stmt).map(transform).runWith(Sink.seq).foreach(println)
//
//
//  val s = Source(List(People("222", "Aaron Lesli", 25), People("980", "Rose Wesley", 33),
//    People("111", "Sonya Louren", 14)))
//  val fs = FileSystems.getDefault
  val regex = "([0-9]+),([ a-zA-Z]+),([0-9]+)".r
//  val src1 = Directory.ls(
//      fs.getPath("C:\\Users\\mtselikina\\IdeaProjects\\CassandraProject\\src\\main\\scala\\people")
//    )
//    .flatMapConcat(FileIO.fromPath(_))
//    .via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = 1024))
//    .map(msg => msg.utf8String)
//    .map{
//      case regex(id, name, age) => People(id, name, age.toInt)
//    }
//  val preparedStatement = session.prepare(s"INSERT INTO People (id, name, age) VALUES (?, ?, ?)")
//  val statementBinder = (people: People, statement: PreparedStatement) =>
//    statement.bind(people.id, people.name, people.age)
//  val sink = CassandraSink[People](parallelism = 2, preparedStatement, statementBinder)
//  val result = s.runWith(sink)
//  result.map(res => println(res))
//
//  val src = Source(List(People("001", "Clara Zeppelin", 35), People("002", "Slava Vandervudsen", 18)))
//  val preparedStatementS = session.prepare(s"INSERT INTO People (id, name, age) VALUES (?, ?, ?)")
//  val statementBinderS = (people: People, statement: PreparedStatement) =>
//    statement.bind(people.id, people.name, people.age)
//  val flow = CassandraFlow.createWithPassThrough[People](parallelism = 2, preparedStatementS, statementBinderS)
//  val resultS = src1.via(flow).runWith(Sink.foreach(println))
  //resultS.map(res => println(res))

  //session.close()
  //cluster.close()


  val privateKey = {
    val pk = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCxwdLoCIviW0BsREeKzi" +
      "qiSgzl17Q6nD4RhqbB71oPGG8h82EJPeIlLQsMGEtuig0MVsUa9MudewFuQ/XHWtxnueQ3I900EJm" +
      "rDTA4ysgHcVvyDBPuYdVVV7LE/9nysuHb2x3bh057Sy60qZqDS2hV9ybOBp2RIEK04k/hQDDqp+Lx" +
      "cnNQBi5C0f6aohTN6Ced2vvTY6hWbgFDk4Hdw9JDJpf8TSx/ZxJxPd3EA58SgXRBuamVZWy1IVpFO" +
      "SKUCr4wwMOrELu9mRGzmNJiLSqn1jqJlG97ogth3dEldSOtwlfVI1M4sDe3k1SnF1+IagfK7Wda5h" +
      "PbMdbh2my3EMGY159ktbtTAUzJejPQfhVzk84XNxVPdjN01xN2iceXSKcJHzy8iy9JHb+t9qIIcYk" +
      "ZPJrBCyphUGlMWE+MFwtjbHMBxhqJNyG0TYByWudF+/QRFaz0FsMr4TmksNmoLPBZTo8zAoGBAKZI" +
      "vf5XBlTqd/tR4cnTBQOeeegTHT5x7e+W0mfpCo/gDDmKnOsF2lAwj/F/hM5WqorHoM0ibno+0zUb5" +
      "q6rhccAm511h0LmV1taVkbWk4UReuPuN+UyVUP+IjmXjagDle9IkOE7+fDlNb+Q7BHl2R8zm1jZjE" +
      "DwM2NQnSxQ22+/"
    val pk1 = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDXHDuRq7rjFaz4" +
      "WSkgVEO/iuL+U0xKEAROlTEaouI2rj+3usiJBjsvxoWFGPmIeDuWuOGeoalmb8Z/" +
      "mMEjhn/zTgoBmNhNzLRS4d6uYAlOdoiAgdwKXB8fR/5NvviR7a1IW2ZsEW+Ujvq5" +
      "0HWmm4bet2YP1B/eGAzJKQ2lRahnEROQw6ylApMrYMbqmYV/r+U67FF6ljGjnfRM" +
      "db+v/ZGZCS5UqTth5XnGZnwBbeeh++K7ZWLjojF2Gtu7iyh9lrmeL1BnHiki7f/f" +
      "CAh80ccVaLBxiR0tC2qjdrlsJm8mjq0H2PZMKhDYuyGH1fpMMTcFbuOtozwSyQ22" +
      "XxL3VP+tAgMBAAECggEAO6TQ4YmvxoI+lRfHvvaOWGBOozz+dLSvNQjJ9jw+o/G5" +
      "qhxouRGYvJz7bZWors3Dm/2EVYM2oLgSXlPBDbQf6xryzFGQfDXrs+V75HatbeuQ" +
      "2qznEZpMaNSB7xWp/3Ba8SqioglNmm/wli/2Ry1tfnh0nKPw+BhMB0TSS0aetIC1" +
      "/RVydaG86vgtOW4QzrsCKRnSdE2o/5dGTbofMFWxWA8hrwTAjnBzOr0apNE17p69" +
      "K1zxRXwjr5uggK2lm7MpBecIwqTIHwCl5w637LnuXa/N9/wm1E4ud48Ewt+7dqWH" +
      "EO9JPVJDzIfjP3nupxVdsTeeFo5gjmp2MDDoxVFp0wKBgQDrZAT4TbXCFebJ/Id2" +
      "lsYSLbRvBsWmKaXk/eDwrOCCQtWPBulWXmSdF22140aM0YrlwrVw7uMV40O7qocn" +
      "sQupgjCm3rb292/E9muD15op7jfb5AMXglep51Y+x0nT2Rb1MqX4wTtZBPr83y6Q" +
      "PRwauWCqjFBPrIWU/VeTWhsbZwKBgQDp8adcK5FyGYaGCjUMqmhUQEfNoOptYYXA" +
      "VGL3bJeNcR36zZlbFuqh4KX8yZB9bHwQdfL5UzjMIows8hVVmMe3NMgjr/ReOBOw" +
      "5If+AAmbtRjCPcxA30kTIdXZt+UMMs5mBjwKG5Eb0501RT1PCbUnP9RxqbBsgvXm" +
      "z65xF+FzywKBgFdHYsBesAIi9Mv4IpJKHW8oGzr/m9Gcp7JcoEcdErG57k12Fpa7" +
      "sTq+MSO7/bDBEWtX4sbZDMJU3gx7klmZP+W+LCPzeIvh+0Ngy0S7cKITfgq+ZJcW" +
      "8UJCUKw+YJV4bRbFjNzLaSH8wsN4TV+WCRGvorQaCr1ADtfLh+lPA7YJAoGAZjwU" +
      "x1LC3PoKr8kGJeLSdP8iYdpQmDxmFwbJuRbbpBQq5c+zlPsOzm8+Gpp7alReY1Mo" +
      "O40C5TKzz3B7okyB+q42SGI8iHA5KrS0OWwKxuD9UOVwvWjWpmXC7sJOdmY9jJhs" +
      "5H3njCGOMhlwEXpMkDp0vLdLQiB5FcTslr+45sECgYARnxo4YmTQ3K443rCu2/oq" +
      "2yPMwb/BNIskzr8m+o5TKu8neZWlT+A52qRC+p7GlCxKq0MOne+FE/K/yAaR/Py8" +
      "4EXsw1+RkL4XR7slx3RCaoINbslZuKziRn/hp+8n6vCy7vYXMQzIXP3u0C/2OQWU" +
      "MGUIfo7KvFkRNeMPeNs72g=="
    val kf = KeyFactory.getInstance("RSA")
    val encodedPv = Base64.getDecoder.decode(pk1)
    val keySpecPv = new PKCS8EncodedKeySpec(encodedPv)
    kf.generatePrivate(keySpecPv)
  }
  val clientEmail = "owner-904@my-spark-project-270614.iam.gserviceaccount.com"
  val projectId = "my-spark-project-270614"
  val apiKey = "AIzaSyCfiOg3oAYlQ1ZNeMg9MnRSqx18DfU-5xU"
  val topic = "topic1"
  val subscription = "subscription1"

  val publishMessage =
    PubSubMessage(messageId = "1", data = new String(Base64.getEncoder.encode("456,Whitesnow,33".getBytes)))
  val publishRequest = PublishRequest(scala.collection.immutable.Seq(publishMessage))

  val source: Source[PublishRequest, NotUsed] = Source.single(publishRequest)

  val publishFlow: Flow[PublishRequest, Seq[String], NotUsed] =
    GooglePubSub.publish(projectId, apiKey, clientEmail, privateKey, topic)

  //val publishedMessageIds: Future[Seq[Seq[String]]] = source.via(publishFlow).runWith(Sink.seq)

  val subscriptionSource: Source[ReceivedMessage, NotUsed] =
    GooglePubSub.subscribe(projectId, apiKey, clientEmail, privateKey, subscription)

  val src = subscriptionSource
    .map { message =>
      new String(Base64.getDecoder.decode(message.message.data.getBytes))
    }
    .map{
          case regex(id, name, age) =>
            println(name)
            People(id, name, age.toInt)
    }


  val preparedStatementS = session.prepare(s"INSERT INTO People (id, name, age) VALUES (?, ?, ?)")
  val statementBinderS = (people: People, statement: PreparedStatement) =>
    statement.bind(people.id, people.name, people.age)
  val flow = CassandraFlow.createWithPassThrough[People](parallelism = 2, preparedStatementS, statementBinderS)
  val resultS = src.via(flow).runWith(Sink.foreach(println))
}


