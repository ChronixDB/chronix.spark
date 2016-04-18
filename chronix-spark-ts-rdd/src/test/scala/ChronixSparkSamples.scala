import de.qaware.chronix.spark.api.java.ChronixSparkContext
import de.qaware.chronix.spark.api.java.TestConfiguration._
import org.apache.solr.client.solrj.{SolrQuery, SolrServerException}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}


object ChronixSparkSamples {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME)
    try {
      val sc: JavaSparkContext = new JavaSparkContext(conf)
      try {
        val csc: ChronixSparkContext = new ChronixSparkContext(sc)
        val query: SolrQuery = new SolrQuery(
          "metric:\"MXBean(java.lang:type=Memory).NonHeapMemoryUsage.used\"")
        val result = csc.queryChronixChunks(query, ZK_HOST)
        val timeSeries = result.take(5)
        println(timeSeries)
      }
      catch {
        case e: SolrServerException => {
          throw new RuntimeException(e)
        }
      } finally {
        if (sc != null) sc.close()
      }
    }
  }
}