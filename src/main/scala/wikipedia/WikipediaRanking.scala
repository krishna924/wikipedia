package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}



object WikipediaRanking {

  // val wikiRDD = sc.parallelize(text)

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setAppName("Wikiasg").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  //val wiki1 : RDD[String] = sc.textFile("WikipediaData.filePath")

  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)


  /** Returns the number of articles on which the language `lang` occurs.
    *  Hint1: consider using method `aggregate` on RDD[T].
    *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {

    rdd.filter(_.mentionsLanguage(lang)).count().toInt

    /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
    //rdd.aggregate(1)(sqop : (rdd.text) => mentionsLanguage(rdd.text))
    //langs.exists()
  }


  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {

    //     rdd.flatMap(a => List(langs,occurrencesOfLang(langs(a),rdd))).collect().toList

    langs.map(lang => (lang, occurrencesOfLang(lang,rdd))).sortBy(_._2).reverse

  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {

    //rdd.flatMap(_.text.split(" ")).groupBy(x => if (x.contains(langs))  )
    //rdd.flatMap(x => if ((mentionsLanguage(x.text)) List(langs, x.text)))

    //langs.map(lang => (if (mentionsLanguage(lang)) List(lang,rdd.text) ) ).groupByKey().mapVlaues(_.toList)

    val intialmap = rdd.flatMap( x => {

      val  langlist = langs.filter(lang => x.text.split(" ").contains(lang))

      langlist.map(y => (y,x))

    })
    intialmap.groupByKey()

  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *   Hint: method mapValues on PairRDD could be useful for this part.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {

    index.mapValues(_.size).collect().sortBy(_._2).reverse.toList
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {

    /*    val initalmap = rdd.map(a => {

          val numblang = langs.filter(lang1 => a.text.split(" ").contains(lang1))

            numblang.map(x => (x,1))

        } )*/

    rdd.flatMap( x => {

      val  langlist = langs.filter(lang => x.text.split(" ").contains(lang))

      langlist.map(a => (a,1))

    }).reduceByKey(_ + _).collect().toList
  }



  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    // println(rankLangs(langs, wikiRdd))

    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result

  }
}

