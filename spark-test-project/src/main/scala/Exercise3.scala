import utils.StringUtils.longestCommonSubstring
import utils.SparkSessionProvider.spark

object Exercise2 {
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
    val result = students
    .filter(s => s._4 === "Prof. Dumbledore")
    .map(t => (t._1, t._3))
    .show()

    // Translate the following SQL query into a Spark transformation pipeline. 5 points
    // SELECT *
    // FROM {
    //     SELECT DISTINCT Title
    //     FROM courses
    //     WHERE Teacher = "Prof. Snape"
    // } INTERSECT {
    //     SELECT DISTINCT Title
    //     FROM courses
    //     WHERE Teacher = "Prof. Moody"
    // }
    // ORDER BY Title
    val result_2 = courses
    .joinWith(courses)
    .map(t => (t._1._2, t._1._4, t._2._4))
    .filter(t => (t._2 ==  "Prof. Snape"))
    .filter(t => (t._3 ==  "Prof. Moody"))
    .distinct()
    .sort(t._1)
    .show()


    // Optional: Print a message
    println("Waiting 30 seconds before stopping Spark...")

    // Sleep for 300 seconds (30,000 milliseconds)
    Thread.sleep(300000)

    // 7. Stop Spark
    spark.stop()
  }
}