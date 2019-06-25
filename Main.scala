package myproject

import org.apache.spark.sql.{SparkSession}

object Main extends App {

  val spark = SparkSession.builder()
    .appName("example")
    .config("spark.master", "local")
    .getOrCreate()

  val file1 = spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs://nodo1:9000/scalaApp/assessments.csv")

  file1.createOrReplaceTempView("file1")

  val file2 = spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs://nodo1:9000/scalaApp/studentAssessment.csv")

  file2.createOrReplaceTempView("file2")

  val file3 = spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs://nodo1:9000/scalaApp/studentInfo.csv")

  file3.createOrReplaceTempView("file3")

  val file4 = spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs://nodo1:9000/scalaApp/studentRegistration.csv")

  file4.createOrReplaceTempView("file4")

  val file5 = spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs://nodo1:9000/scalaApp/studentVle.csv")

  file5.createOrReplaceTempView("file5")

  val file6 = spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs://nodo1:9000/scalaApp/vle.csv")

  file6.createOrReplaceTempView("file6")

  val file7 = spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs://nodo1:9000/scalaApp/courses.csv")

  file7.createOrReplaceTempView("file7")

  //INNER JOIN DE LAS TABLAS QUE COMPONEN EL DATASET

  val join1 = file1.join(file2, Seq("id_assessment"), "inner").drop("is_banked", "date_submitted", "date")

  val join2 = join1.join(file3, Seq("id_student", "code_module", "code_presentation"), "inner").drop("imd_band", "num_of_prev_attempts", "studied_credits")

  val join3 = join2.join(file4, Seq("id_student", "code_module", "code_presentation"), "inner").drop("date_unregistration")

  val join4 = join3.join(file5, Seq("id_student", "code_module", "code_presentation"), "inner").drop("date")

  val join5 = join4.join(file6, Seq("id_site", "code_module", "code_presentation"), "inner").drop("week_from")

  val join_final = join5.join(file7, Seq("code_module", "code_presentation"), "inner")

  join_final.createOrReplaceTempView("table")

  join1.createOrReplaceTempView("tableChart2")

  join2.createOrReplaceTempView("tableChartMain")

  // INICIALIZACION VARIABLES

  var query = s""

  var assessment_type = ""
  var weight = ""
  var weight_symbol = ""
  var score = ""
  var score_symbol = ""
  var score_num = ""
  var gender = ""
  var region = ""
  var education = ""
  var age = ""
  var disability = ""
  var result = ""

  var registration = ""
  var registration_symbol = ""
  var registration_num = ""
  var clicks = ""
  var clicks_symbol = ""
  var clicks_num = ""
  var activity = ""
  var week = ""
  var length = ""
  var length_symbol = ""
  var length_num = ""

  // VARIABLE FILTRO QUERYS/CHARTS

  var filter = args(0)

  if (filter == "assessment") {

    // VARIABLES INTERFAZ QUERY ASSESSMENT

    assessment_type = args(1)
    if (assessment_type == s"""'undefined'""") {assessment_type = "null"}


    weight = args(2)
    weight_symbol = weight match {
      case "less" => "<="
      case "greater" => ">="
      case "undefined" => "null"
    }

    score = args(3)
    score_symbol = score match {
      case "less" => "<="
      case "greater" => ">="
      case "undefined" => "="
    }

    score_num = args(4)
    if (score_num == s"""undefined""") {score_num = "null"}

    gender = args(5)
    if (gender == s"""'undefined'""") {gender = "null"}

    region = args(6)
    if (region == s"""'undefined'""") {
      region = "null"
    } else if (region == s"""'London'""") {
      region = s"""'London Region'"""
    } else if (region == s"""'Yorkshire'""") {
      region = s"""'Yorkshire Region'"""
    } else if (region == s"""'NorthRe'""") {
      region = s"""'North Region'"""
    } else if (region == s"""'NorthWe'""") {
      region = s"""'North Western Region'"""
    } else if (region == s"""'SouthRe'""") {
      region = s"""'South Region'"""
    } else if (region == s"""'SouthEa'""") {
      region = s"""'South East Region'"""
    } else if (region == s"""'SouthWe'""") {
      region = s"""'South West Region'"""
    } else if (region == s"""'West'""") {
      region = s"""'West Midlands Region'"""
    } else if (region == s"""'EastAn'""") {
      region = s"""'East Anglian Region'"""
    } else if (region == s"""'EastMid'""") {
      region = s"""'East Midlands Region'"""
    }

    education = args(7)
    if (education == s"""'undefined'""") {
      education = "null"
    } else if (education == s"""'HE'""") {
      education = s"""'HE Qualification'"""
    } else if(education == s"""'A'""") {
      education = s"""'A Level or Equivalent'"""
    } else if (education == s"""'LowerA'""") {
      education = s"""'Lower Than A Level'"""
    } else if (education == s"""'Post'""") {
      education = s"""'Post Graduate Qualification'"""
    }

    age = args(8)
    if (age == s"""'undefined'""") { age = "null" }

    disability = args(9)
    if (disability == s"""'undefined'""") { disability = "null" }

    result = args(10)
    if (result == s"""'undefined'""") { result = "null" }

    // QUERY ASSESSMENTS

    if (weight_symbol == s"null") {

      query = s"SELECT * FROM table WHERE (assessment_type= $assessment_type OR $assessment_type is null)" +
        s"AND (score $score_symbol $score_num OR $score_num is null)" +
        s"AND (gender = $gender OR $gender is null)" +
        s"AND (region = $region OR $region is null)" +
        s"AND (highest_education = $education OR $education is null)" +
        s"AND (age_band = $age OR $age is null)" +
        s"AND (disability = $disability OR $disability is null)" +
        s"AND (final_result = $result OR $result is null)"

    } else {

      query = s"SELECT * FROM table WHERE (assessment_type= $assessment_type OR $assessment_type is null)" +
        s"AND (weight $weight_symbol 20)" +
        s"AND (score $score_symbol $score_num OR $score_num is null)" +
        s"AND (gender = $gender OR $gender is null)" +
        s"AND (region = $region OR $region is null)" +
        s"AND (highest_education = $education OR $education is null)" +
        s"AND (age_band = $age OR $age is null)" +
        s"AND (disability = $disability OR $disability is null)" +
        s"AND (final_result = $result OR $result is null)"
    }

  } else if (filter == "course") {

    // VARIABLES INTERFAZ QUERY COURSE

    gender = args(1)
    if (gender == s"""'undefined'""") {gender = "null"}

    region = args(2)
    if (region == s"""'undefined'""") {
      region = "null"
    } else if (region == s"""'London'""") {
      region = s"""'London Region'"""
    } else if (region == s"""'Yorkshire'""") {
      region = s"""'Yorkshire Region'"""
    } else if (region == s"""'NorthRe'""") {
      region = s"""'North Region'"""
    } else if (region == s"""'NorthWe'""") {
      region = s"""'North Western Region'"""
    } else if (region == s"""'SouthRe'""") {
      region = s"""'South Region'"""
    } else if (region == s"""'SouthEa'""") {
      region = s"""'South East Region'"""
    } else if (region == s"""'SouthWe'""") {
      region = s"""'South West Region'"""
    } else if (region == s"""'West'""") {
      region = s"""'West Midlands Region'"""
    } else if (region == s"""'EastAn'""") {
      region = s"""'East Anglian Region'"""
    } else if (region == s"""'EastMid'""") {
      region = s"""'East Midlands Region'"""
    }

    education = args(3)
    if (education == s"""'undefined'""") {
      education = "null"
    } else if (education == s"""'HE'""") {
      education = s"""'HE Qualification'"""
    } else if(education == s"""'A'""") {
      education = s"""'A Level or Equivalent'"""
    } else if (education == s"""'LowerA'""") {
      education = s"""'Lower Than A Level'"""
    } else if (education == s"""'Post'""") {
      education = s"""'Post Graduate Qualification'"""
    }

    age = args(4)
    if (age == s"""'undefined'""") { age = "null" }

    disability = args(5)
    if (disability == s"""'undefined'""") { disability = "null" }

    result = args(6)
    if (result == s"""'undefined'""") { result = "null" }

    registration = args(7)
    registration_symbol = registration match {
      case "greater" => "<="
      case "less" => ">="
      case "undefined" => "="
    }

    registration_num = s"""-""" + args(8)
    if (registration_num == s"""-undefined""") {registration_num = "null"}

    clicks = args(9)
    clicks_symbol = clicks match {
      case "less" => "<="
      case "greater" => ">="
      case "undefined" => "="
    }

    clicks_num = args(10)
    if (clicks_num == s"""undefined""") {clicks_num = "null"}

    activity = args(11)
    if (activity == s"""'undefined'""") { activity = "null" }

    week = args(12)
    if (week == s"""undefined""") {week = "null"}

    length = args(13)
    length_symbol = length match {
      case "less" => "<="
      case "greater" => ">="
      case "undefined" => "="
    }

    length_num = args(14)
    if (length_num == s"""undefined""") {length_num = "null"}

    // QUERY COURSE

    query = s"SELECT * FROM table WHERE (gender= $gender OR $gender is null)" +
      s"AND (region = $region OR $region is null)" +
      s"AND (highest_education = $education OR $education is null)" +
      s"AND (age_band = $age OR $age is null)" +
      s"AND (disability = $disability OR $disability is null)" +
      s"AND (final_result = $result OR $result is null)" +
      s"AND (date_registration $registration_symbol $registration_num OR $registration_num is null)" +
      s"AND (sum_click $clicks_symbol $clicks_num OR $clicks_num is null)" +
      s"AND (activity_type = $activity OR $activity is null)" +
      s"AND (week_to = $week OR $week is null)" +
      s"AND (module_presentation_length $length_symbol $length_num OR $length_num is null)"

  } else if (filter == "chart1") {

    query = s"SELECT * FROM tableChartMain"   //QUERY CHART1

  } else if (filter == "chart2") {

    query = s"SELECT * FROM tableChart2"      //QUERY CHART2

  } else if (filter == "chart3") {

    query = s"SELECT * FROM tableChartMain"   //QUERY CHART3

  } else if (filter == "chart4") {

    query = s"SELECT * FROM tableChartMain"   //QUERY CHART4

  }

  val sqlDF = spark.sql(query)      //ENVIO QUERY

  sqlDF.show()

  sqlDF.write.json("hdfs://nodo1:9000/output/output.json")    //ESCRITURA SALIDA QUERY EN HDFS

  spark.stop()

}
