// --- Accidents ---

// Create a custom class to represent an accident
case class Accident(date: String, heure: String, vehicule_type: String, latitude: String, longitude: String)

// Read CSV datas and put onto RDD
val roadAccidents_rawDatas = sc.textFile("file:/home/cloudera/workspace/project/datas/accidents/accidentologie-paris.csv")
//Debug
//roadAccidents_rawDatas.collect()

// Split each columns separated by ";"
val roadAccidents_splittedDatas = roadAccidents_rawDatas.map(_.split(";"))
//Debug
//roadAccidents_splittedDatas.collect()

// Map raw datas with Accident class
val roadAccidents_mappedDatas = roadAccidents_splittedDatas.map(a => Accident(a(0), a(1), a(10), a(28), a(28)))
//Debug
//roadAccidents_mappedDatas.collect()

// Convert RDD to DataFrame
val roadAccidents_df = roadAccidents_mappedDatas.toDF()
//Debug
//roadAccidents_df.show()
//roadAccidents_df.printSchema()

// Split location onto latitude and longitude and round position
val roadAccidents_cleaned_df = roadAccidents_df.withColumn("_position", split($"latitude", ", ")).withColumn("latitude", round($"_position".getItem(0), 2)).withColumn("longitude", round($"_position".getItem(1), 2)).drop("_position").select($"date" as "date", $"heure" as "heure", $"vehicule_type" as "vehicule_type", concat($"latitude", lit(", "), $"longitude") as "position")
//Debug
//roadAccidents_cleaned_df.show()
//roadAccidents_cleaned_df.printSchema()

// Persist cleaned dataframe to sql table
roadAccidents_cleaned_df.registerTempTable("accidents")
//Debug
//sqlContext.sql("select * from accidents").show()


// --- Speedcams ---


// Create a custom class to represent a speedcam
case class Speedcam(speed: String, latitude: String, longitude: String)

// Read CSV datas and put onto RDD
val speedcams_rawDatas = sc.textFile("file:/home/cloudera/workspace/project/datas/speedcams/F-speedcam.csv")
//Debug
//speedcams_rawDatas.collect()

// Split each columns separated by ";"
val speedcams_splittedDatas = speedcams_rawDatas.map(_.split(" "))
//Debug
//speedcams_splittedDatas.collect()

// Map raw datas with Accident class
val speedcams_mappedDatas = speedcams_splittedDatas.map(s => Speedcam(s(2), s(0), s(1)))
//Debug
//speedcams_mappedDatas.collect()

// Convert RDD to DataFrame
val speedcams_df = speedcams_mappedDatas.toDF()
//Debug
//speedcams_df.show()
//speedcams_df.printSchema()

// Clean values and round position
val removeArobase = udf((s : String) => s.replaceAll("@", ""))
val removeComma = udf((s : String) => s.replaceAll(",", ""))
val isAccident = udf((s : String) => if (s == null || s.isEmpty) 0 else 1)

val speedcams_cleaned_df = speedcams_df.withColumn("latitude", removeComma($"latitude")).withColumn("longitude", removeComma($"longitude")).withColumn("speed", removeArobase($"speed")).withColumn("latitude", round($"latitude", 2)).withColumn("longitude", round($"longitude", 2)).select($"speed" as "speed", concat($"latitude", lit(", "), $"longitude") as "position")
//Debug
//speedcams_cleaned_df.show()
//speedcams_cleaned_df.printSchema()

// Persist cleaned dataframe to sql table
speedcams_cleaned_df.registerTempTable("speedcams")
//Debug
//sqlContext.sql("select * from speedcams").show()

// Join Accidents table and Speedcams table with locations
val speedcams_join_accidents_df = speedcams_cleaned_df.alias("s").join(roadAccidents_cleaned_df.alias("a"), speedcams_cleaned_df("position") === roadAccidents_cleaned_df("position"), "left_outer").select($"s.speed", $"s.position", $"a.date", $"a.heure", $"a.vehicule_type", isAccident($"a.vehicule_type") as "is_accident")

speedcams_join_accidents_df.registerTempTable("speedcams_join_accidents")
//Debug
//sqlContext.sql("select * from speedcams_join_accidents where vehicule_type <> 'null'").show()

val trainingDataTable = sqlContext.sql("SELECT speed, position, vehicule_type, date, heure FROM speedcams_join_accidents")

// Training datas
val trainingData = trainingDataTable.map {
	val features = Array[Double](row(3), row(4), row(5))
	LabeledPoint(row(2), features)
}