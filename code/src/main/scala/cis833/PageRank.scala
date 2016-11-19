package cis833

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PageRank {
  val epsilon = .1
  val iterations = 10

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]") // only set locally
    val sc = new SparkContext(conf)
    val path = args(0)
    
    val base = loadWikipedia(sc, path)
    val links = generateLinkGraph(base)
    val count = links.count.toDouble
    
    println(s"corpus size: ${base.count}")
    println(s"vertices size: $count")
    
    val init = links.map { case (title,outgoing) => (title,(1/count)) }
    var output:RDD[(String,Double)] = init
    
    for(i <- 0 until iterations) {
      val input = output.join(links)
      
      output = input
        .flatMap{ case (title,(prY,outgoing)) => {
          outgoing.map(out => out -> prY / outgoing.size.toDouble)
        }}
        .reduceByKey(_ + _)
        .map{ case (y,incomingLinksSum) => y -> (epsilon / count + (1 - epsilon) * incomingLinksSum)}
        
      val scoreSum = output.map(_._2).sum
      output = output.map(x => (x._1,x._2/scoreSum))
    }
    
    sc.parallelize(output.map(x => (x._1, x._2 *10000)).sortBy(_._2, false).take(10).toSeq).saveAsTextFile(s"pageRank_${System.nanoTime}.out")
  }
  
  /**
   * Loads the file path of a Wikipedia set, one line = one page, and returns
   * the titles and the bodies.
   */
  def loadWikipedia(sc: SparkContext, path: String) : RDD[(String,String)] = {
    sc.textFile(path).flatMap { x => {
      val title = x.substring(x.indexOf("<title>")+7, x.indexOf("</title>"))
      val body = x.substring(x.indexOf("<text>")+6, x.indexOf("</text>"))
      
      if(title.indexOf(':') == -1)
        Some(title,body)
      else
        None
    }}
  }

  /**
   * Takes an RDD of titles and bodies, and parses the links from the body, and 
   * ensures that the link destinations are valid pages in our local corpus.
   */
  def generateLinkGraph(baseCorpus: RDD[(String,String)]) : RDD[(String,Iterable[String])] = {
    val getLinks = "\\[(.*?)\\]".r
    
    baseCorpus.flatMap{ case (title,body) => {
      val links = getLinks.findAllMatchIn(body).map(_.matched).toList
      
      links.filter(_.indexOf(':') == -1).map{ link => link match {
        case x if x.indexOf('#') >= 0 => x.substring(0, x.indexOf('#')).replace("[", "").replace("]", "")
        case x if x.indexOf('|') >= 0 => x.substring(0, x.indexOf('|')).replace("[", "").replace("]", "")
        case _ => link.replace("[", "").replace("]", "")
      }}
      .distinct
      .map(x => (x -> (1,title))) ++ List((title -> (0,title)))
    }}
    .groupByKey // group by P
    .flatMap { case (key,list) => {
      if(list.toList.contains((0,key))) // valid page
        list.map(x => (x._2,key))
            .filter{ case (a,b) => a != b } // remove (p,p)
      else
        None
    }}
    .groupByKey
   }
}