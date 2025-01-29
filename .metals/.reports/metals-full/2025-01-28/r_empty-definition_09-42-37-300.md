error id: filter.
file://<WORKSPACE>/spark-test-project/src/main/scala/Exercise4.scala
empty definition using pc, found symbol in pc: filter.
empty definition using semanticdb
|empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/students/filter.
	 -org/apache/spark/sql/functions/students/filter#
	 -org/apache/spark/sql/functions/students/filter().
	 -spark/implicits/students/filter.
	 -spark/implicits/students/filter#
	 -spark/implicits/students/filter().
	 -students/filter.
	 -students/filter#
	 -students/filter().
	 -scala/Predef.students.filter.
	 -scala/Predef.students.filter#
	 -scala/Predef.students.filter().

Document text:

```scala
import utils.StringUtils.longestCommonSubstring
import utils.SparkSessionProvider.spark

object Exercise4 {
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



    // For administrative purposes, we need a list of all students that did not finish their
    // studies in 10 semesters (value of Semester > 10) with their supervisors. Because
    // this is very sensitive information, all student names need to be anonymized via
    // hash() call on their Name values. The result should have the schema (HashedName,
    // Supervisor) and be copied into a local variable in the driver program. Write a
    // transformation pipeline that solves the request. 
    val result1a = students
    .filter(s => s._3.toInt > 10)
    .map(s => (hash(s._2), s._4))
    .toDF("HashedName", "Supervisor")
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

empty definition using pc, found symbol in pc: filter.