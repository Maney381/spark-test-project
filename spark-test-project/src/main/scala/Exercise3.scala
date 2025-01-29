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

    val result1b = students
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
    .filter(col("Teacher") === "Prof. Snape") // .filter(c => c._3 == "Prof. Snape")
    .select(col("Title"))                     // .map(c => c._2)
    .distinct()

    val titles_moody = courses
    .filter(col("Teacher") === "Prof. Moody") // .filter(c => c._3 == "Prof. Moody")
    .select(col("Title"))                     // .map(c => c._2)
    .distinct() 

    val result2a = titles_snape
    .intersect(titles_moody)
    .sort()                                  // .sort(col("..."))
    .show()
    
    // Implement a join between the students and the enrollments dataset as a Spark
    // batch job without using Spark’s joinWith() transformation. The result should
    // be a Dataset with two columns: one column containing the student record and
    // one column containing the enrollment record. Join on students.ID = enroll-
    // ments.StudentID and finalize the pipeline with some action of your choice. Do
    // not implement the join as a broadcast join or any map-side join, because both
    // relations are large. 5 points
    // Hint: Remember how reduce-side joins work in the MapReduce framework

    // val result3a = students
    // .union(enrollments)
    // .filter(t._1 === t._5) //keep only the rows where ID == StudentID
    // .groupByKey(t => t._1)
    // .mapGroups(case (key, iterator) => (key, iterator
    // .map(iter => (t._1, t._2, t._3))))

    // Maps the data into a tuple structure: (studentID, 1, Array(...student fields...))
    // because union combines both datasets into one by stacking rows together
    val emapped = enrollments.map(t => (t._1, 2, Array(t._1, t._2, t._3)))
    val smapped = students.map(t => (t._1, 1, Array(t._1, t._2, t._3, t._4)))

    val union = smapped
    .union(emapped)
    .groupByKey(t => t._1) //this now groups all of the rows by the student_id
    .mapGroups((key, iterator) => {
        val group = iterator.toList

        val side1 = group.filter(t => t._2 == 1).map(t => t._3)
        val side2 = group.filter(t => t._2 == 2).map(t => t._3)


        // 1. for each element t in side1, apply the funtion in flatMap
        // 2. For the given t from side1, iterate over all elements s in side2 and create a tuple (t,s)
        // 3. flatMap() Flattens the collection of tuples (t, s) created for each element of side1 into a single collection
        side1.flatMap(t => side2.map(s => (t, s)))
    })
    .flatMap(t => t) //Ensures all elements from all groups are flattened into a single dataset
    .show()
    
    //how to use mapGroups:
    //.mapGroups { case (key, iterator) =>
    //   val group = iterator.toList
    //   val result = group.map(record => s"Key: $key, Value: $record")
    //   result
    
    // .mapGroups((key, iterator) => {
    // val group = iterator.toList
    // val result = group.map(record => s"Key: $key, Value: $record")
    // result
    // })
}




    // Optional: Print a message
    println("Waiting 30 seconds before stopping Spark...")

    // Sleep for 300 seconds (30,000 milliseconds)
    Thread.sleep(300000)

    // 7. Stop Spark
    spark.stop()
  }
}