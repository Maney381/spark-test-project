error id: _empty_/Exercise3.main().courses.
file://<WORKSPACE>/spark-test-project/src/main/scala/Exercise3.scala
empty definition using pc, found symbol in pc: _empty_/Exercise3.main().courses.
semanticdb not found
|empty definition using fallback
non-local guesses:
	 -

Document text:

```scala
import utils.StringUtils.longestCommonSubstring
import utils.SparkSessionProvider.spark

object Exercise3 {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    import spark.implicits._ // Needed for Dataset transformations and implicits
    import org.apache.spark.sql.Encoders // Needed if using .as with specific types
    
    spark.sparkContext.setLogLevel("ERROR")
    println("Spark session started...")

    val students = spark
    .read
    .option("header", "true") // Skip the header row
    .option("quote", "\"")
    .option("delimiter", ",")
    .csv(s"data/exercise2/students.csv")
    .toDF("ID","Name","Semester","Supervisor")
    .as[(String, String, String, String)] //this converts dataFrame to a Dataset of Tuples or case classes

    val enrollments = spark
    .read
    .option("header", "true") // Skip the header row
    .option("quote", "\"")
    .option("delimiter", ",")
    .csv(s"data/exercise2/enrollments.csv")
    .toDF("StudentID", "CourseID", "Credits")
    .as[(String, String, String)]

    val courses = spark
    .read
    .option("header", "true") // Skip the header row
    .option("quote", "\"")
    .option("delimiter", ",")
    .csv(s"data/exercise2/courses.csv")
    .toDF("ID", "Title", "Teacher", "Topic")
    .as[(String, String, String, String)]

    // Write a Spark transformation pipeline that starts with the students Dataset,
    // then filters all those students that are supervised by “Prof. Dumbledore”, maps
    // these students to their ID and Semester, and finally displays the results in tabular
    // form on the standard output.
    val result1a = students
    .filter(col("Supervisor") === "Prof. Dumbledore")
    .map(t => (t._1, t._3))
    .toDF("ID", "Semester")
    .show()

    val result2a = students
    .filter(col("Supervisor") === "Prof. Dumbledore")
    .select(col("ID"), col("Semester"))
    .show()

    // Translate the following SQL query into a Spark transformation pipeline
    // SELECT *
    //     FROM {
    //     SELECT DISTINCT Title
    //     FROM courses
    //     WHERE Teacher = "Prof. Snape"
    // } INTERSECT {
    //     SELECT DISTINCT Title
    //     FROM courses
    //     WHERE Teacher = "Prof. Moody"
    //     }
    // ORDER BY Title

    val titles_snape = courses
    .filter(col("Teacher") === "Prof. Snape")
    .select(col("Title"))
    .distinct()

    val titles_moody = courses
    .filter(col("Teacher") === "Prof. Moody")
    .select(col("Title"))
    .distinct()

    val result2a = titles_snape
    .intersect(titles_moody)
    .sort(col("Title"))
    .show()

    // Optional: Print a message
    println("Waiting 30 seconds before stopping Spark...")

    // Sleep for 300 seconds (30,000 milliseconds)
    Thread.sleep(300000)

    // 7. Stop Spark
    spark.stop()
  }
}

```

#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/Exercise3.main().courses.