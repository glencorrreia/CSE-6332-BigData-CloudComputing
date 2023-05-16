import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

@SerialVersionUID(123L)
case class Matrix_M ( i: Long, j: Long, v: Double )
      extends Serializable {}

@SerialVersionUID(123L)
case class Matrix_N ( j: Long, k: Long, w: Double )
      extends Serializable {}



object Multiply {

  def main ( args: Array[String] ) {





    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)
    val M = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                Matrix_M(a(0).toLong,a(1).toLong,a(2).toDouble)})
    val N = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                Matrix_N(a(0).toLong,a(1).toLong,a(2).toDouble)})
    
    val M_Indexj = M.map(M => (M.j,(M.i,M.v)))
    val N_Indexj = N.map(N => (N.j,(N.k,N.w)))
    val Join_MN = M_Indexj.join(N_Indexj)
    
    val reduce = Join_MN.map({case (j,((i,v),(k,w))) => ((i,k),v*w)}).reduceByKey(_ + _)
    val final_MN = reduce.map({case ((i,k),v) => (i,k,v)})
    reduce.sortByKey(true).collect().foreach(println)
    reduce.sortByKey(true).saveAsTextFile(args(2))
    sc.stop()
    


    
  }
}

