import java.net.InetSocketAddress

import com.datastax.driver.core._

object Cassandra extends App{
  val cluster = Cluster.builder()
    .addContactPoint("localhost")
    .withPort(9042)
    .build()
  val session = cluster.connect("DataCassandraExamples")
  val prepStatement = session.prepare("select * from People where id = ?")
  val bound = prepStatement.bind()
  bound.setString(0, "123")
  session.execute(bound).forEach(println)
  session.close()
  cluster.close()
}
