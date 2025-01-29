import utils.StringUtils.longestCommonSubstring
import utils.SparkSessionProvider.spark
import org.apache.spark.sql.functions._
import spark.implicits._ // Needed for Dataset transformations and implicits
import org.apache.spark.sql.Encoders // Needed if using .as with specific types
import org.apache.spark.sql.functions.col


object Exercise4 {
  def main(args: Array[String]): Unit = {

    
    
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

    def hash(value: String): Int = value.hashCode

    val result1a = students
    .filter(s => s._3.toInt > 10)
    .map(s => (hash(s._2), s._4))
    .toDF("HashedName", "Supervisor")
    .show() //for saving use: .collect()

    // use dataframe operations
    //val result1b = students
    //.filter(col("Semester").cast("int") > 10) // Filter for Semester > 10
    //.select(hash(col("Name")).alias("HashedName"), col("Supervisor")) // Hash Name
    //.collect() // Collect result to the driver 


    //  The following SQL query searches for supervisors that supervise PhD students
    // (Semester < 0), but never taught a course. Translate the SQL query into a Spark
    // transformation pipeline that writes the result to the console of the driver. 5 points
    // (SELECT Supervisor
    // FROM students
    // WHERE Semester < 0)
    // except
    // (SELECT Supervisor
    // FROM students, courses
    // WHERE Supervisor = Teacher)

    //this gives me the names of all the professors that supervise PHD students
    val transformation1 = students
    .filter($"semester".cast("int") < 0)
    .map(t => t._4)
    .distinct()
    
    // this gives me the name of all the professors which tought a course
    val transformation2 = students
    .joinWith(courses, col("Supervisor") === col("Teacher"))
    .map(t => t._1._4)
    .distinct()

    // this gives me all the proffessors that supervise PHD students and have not tought a course
    val result2a = transformation1
    .except(transformation2)
    .show()

    // Supervisors who supervise PhD students (Semester < 0)
    // val transformation1 = students
    // .filter(col("Semester").cast("int") < 0)
    // .select(col("Supervisor"))
    // .distinct()

    // // Supervisors who taught a course
    // val transformation2 = students
    // .join(courses, col("Supervisor") === col("Teacher"))
    // .select(col("Supervisor"))
    // .distinct()

    // // Supervisors who supervise PhD students but never taught a course
    // val result2a = transformation1
    // .except(transformation2)

    // // Display the result on the console
    // result2a.show()


    // Optional: Print a message
    println("Waiting 30 seconds before stopping Spark...")

    // Sleep for 2min/1200 seconds (120,000 milliseconds)
    Thread.sleep(120000)

    // 7. Stop Spark
    spark.stop()
}
}
