import utils.StringUtils.longestCommonSubstring
import utils.SparkSessionProvider.spark

object Exercise2 {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    import spark.implicits._ // Needed for Dataset transformations and implicits
    import org.apache.spark.sql.Encoders // Needed if using .as with specific types
    
    spark.sparkContext.setLogLevel("ERROR")
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

    // students.show()
    // enrollments.show()
    // courses.show()

    // removes all those enrollments that grand less than 3 credits, then multiplies
    // the credits by 2/3 in order to transform them into SWS s, and finally displays
    // the results in tabular form with schema (StudentID, CourseID, SWS) on the
    // standard output
    val result_1a = enrollments
    .filter($"Credits".cast("int") < 3)
    .map(t => (t._1, t._2, t._3.toInt * 2/3))
    .toDF("ID", "CourseID", "SWS")
    .show()

    // Alternative solution:
    // val result_1b = enrollments
    // .filter(e => Integer.valueOf(e._3) >= 3)
    // .map(e => (e._1, e._2, Integer.valueOf(e._3) * 2/3))
    // .toDF("ID", "CourseID", "SWS")
    // .show()


    // Translate the following SQL query into a Spark transformation pipeline. 4 points
    // SELECT Semester, COUNT(ID) AS Students
    // FROM students
    // WHERE Semester <= 10
    // GROUP BY Semester;
    val result_2a = students
    .filter($"Semester".cast("int") <= 10)
    .groupBy($"Semester")
    .count()
    .toDF("Semester","Students")
    .show()

    // alternative solution
    val result_2b = students
    .groupByKey(s => s._3)
    .mapGroups((key, iterator) => (key, iterator.size)) //use () for inline functions and {} for multiline functions
    //.filter(s => Integer.valueOf(s._1) <= 10)
    .filter(s => s._1.toInt <= 10)
    .toDF("Semester", "Students")
    .show()

    // Rank all teachers by the number of enrollments that they got for all their courses.
    // You can assume that teachers have unique names in courses.Teacher and that
    // the enrollments file stores all enrollments for all courses ever given. Teacher that
    // never gave a course do not exist in the files and will not appear in the ranking.
    // Report a list of (Teacher, SumEnrollments) that is sorted by SumEnrollments.
    val result_3a = enrollments
    .joinWith(courses, enrollments.col("CourseID") === courses.col("ID"))
    .groupByKey(t => t._2._3) //group by key = teacher
    .mapGroups{ case (key, iterator) =>
        val SumEnrollments = iterator.size
        (key, SumEnrollments) }
    .toDF("Teacher", "SumEnrollments")
    .sort("SumEnrollments")
    .show()

    // alternative solution
    val result_3b = enrollments
    .joinWith(courses, col("CourseID") === col("ID"))
    .map(s => (s._1._1, s._1._2, s._1._3, s._2._2, s._2._3, s._2._4)) //results in Dataset([String, .., String]), (StudentID, CourseID, Credits, Title, Teacher, Topic)
    .groupByKey(row => row._5)
    .mapGroups((key, iterator) => (key, iterator.size))
    .toDF("Teacher", "SumEnrollments")
    .as[(String, Int)]
    .sort(col("SumEnrollments"))
    .show()

    // Optional: Print a message
    println("Waiting 30 seconds before stopping Spark...")

    // Sleep for 300 seconds (30,000 milliseconds)
    Thread.sleep(300000)

    // 7. Stop Spark
    spark.stop()
  }
}