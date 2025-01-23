import org.apache.spark.sql.SparkSession
import utils.StringUtils.longestCommonSubstring
import utils.SparkSessionProvider.spark

object Exercise1 {
  def main(args: Array[String]): Unit = {

    import spark.implicits._
    println("Spark session started...")


    // 2. Read CSV into DataFrame, specifying options
    // val studentsDF = spark.read
    //  .option("quote", "\"")
    //  .option("delimiter", ",")
    //  .option("header", "true")
    //  .csv("data/students.csv")    // Path to your CSV
    //  .toDF("ID", "Name", "Password", "Gene")

    // 3. Convert DataFrame to Dataset of Tuples or Case Class
    // as[(String, String, String, String)] => a Dataset of 4-field tuples
    //val studentsDS = studentsDF.as[(String, String, String, String)]

    val students = spark
    .read
    .option("quote", "\"")
    .option("delimiter", ",")
    .csv(s"data/exercise1/students.csv")
    .toDF("ID", "Name", "Password", "Gene")
    .as[(String, String, String, String)] //this converts dataFrame to a Dataset of Tuples or case classes

    val students2 = spark
    .read
    .option("quote", "\"")
    .option("delimiter", ",")
    .csv(s"data/exercise1/students.csv")
    .toDF("ID", "Name", "Password", "Gene")
    .as[(String, String, String, String)]

    students
    .joinWith(students2, students.col("ID") =!= students2.col("ID")) //results in DF with structure Dataset[(Row, Row)], where t._1: row from students, t._2: corresponding row from students2
    .map(t => (t._1._1, longestCommonSubstring(t._1._4, t._2._4))) //map is applied to each pair of rows, t._1._1: ID from first row, t._1._4/t._2._4: Gene from first/second row; results in Dataset[(String, String)], where each element is (ID, lgS)
    .groupByKey(t => t._1) //results in KeyValueGroupedDataset[String, (String, String)], key = ID, values are tuples of the form (ID, substring)
    .mapGroups{ case (key, iterator) => (key, iterator //mapGroups transformation processes each groups of substrings for a specific students
    .map(t => t._2) //Converts the iterator of tuples (ID, substring) into an iterator of substrings.
    .reduce((a,b) => { if (a.length > b.length) a else b })) } //finds the longest common substring by comparing the length, results in Dataset[(String, String)], each element is (ID, substring)
    .toDF("ID", "Substring")
    .show()

    students
    .joinWith(students2, students.col("ID") =!= students2.col("ID"))
    .map(t => (t._1._1, longestCommonSubstring(t._1._4, t._2._4)))
    .groupByKey(t => t._1)
    .mapGroups{ case (key, iterator) => 
    // Find the longest common substring
    val longestSubstring = iterator.maxBy(_._2.length) //find the longtest substring
    (key, longestSubstring._2)}
    .toDF("ID", "Substring")
    .show()

    // Optional: Print a message
    println("Waiting 30 seconds before stopping Spark...")

    // Sleep for 300 seconds (30,000 milliseconds)
    Thread.sleep(300000)

    // 7. Stop Spark
    spark.stop()
  }
}