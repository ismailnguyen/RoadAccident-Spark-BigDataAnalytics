//Nombre d'accidents total (4386 lignes)
val nombre_accidents_near_speedcam = sqlContext.sql("SELECT COUNT(*) as count FROM speedcams_join_accidents")

//Nombre d'accidents près de radars (1880 lignes)
val nombre_accidents_near_speedcam = sqlContext.sql("SELECT COUNT(*) as count FROM speedcams_join_accidents WHERE is_accident= 1")

//Nombre d'accidents loin de radars (2506 lignes)
val nombre_accidents_near_speedcam = sqlContext.sql("SELECT COUNT(*) as count FROM speedcams_join_accidents WHERE is_accident = 0")


//-----------------------------------------------------------------------------------------
//				TRI selon type de véhicule
//-----------------------------------------------------------------------------------------
// Classement des accidents par type de véhicule et par vitesse de limitation du radar, trié par nombre d'accidents décroissant
sqlContext.sql("SELECT speed, vehicule_type, COUNT(vehicule_type) as count FROM speedcams_join_accidents WHERE is_accident = 1 GROUP BY vehicule_type, speed ORDER BY count DESC")

// Nombre d'accidents chez les Véhicules légers en fonction de la limitation de vitesse du radar
sqlContext.sql("SELECT speed, vehicule_type, COUNT(vehicule_type) as count FROM speedcams_join_accidents WHERE is_accident = 1 AND vehicule_type = 'VL' GROUP BY vehicule_type, speed ORDER BY count DESC")

// Nombre d'accidents chez les motos en fonction de la limitation de vitesse du radar
sqlContext.sql("SELECT speed, vehicule_type, COUNT(vehicule_type) as count FROM speedcams_join_accidents WHERE is_accident = 1 AND (vehicule_type like '%50%' OR vehicule_type like 'Cyclo' OR vehicule_type like '%125%') GROUP BY vehicule_type, speed ORDER BY count DESC")


// Nombre d'accidents chez les Poids lourds en fonction de la limitation de vitesse du radar
sqlContext.sql("SELECT speed, vehicule_type, COUNT(vehicule_type) as count FROM speedcams_join_accidents WHERE is_accident = 1 AND vehicule_type like '%PL%' GROUP BY vehicule_type, speed ORDER BY count DESC")

//-----------------------------------------------------------------------------------------
//				TRI selon limitation de vitesse du radar
//-----------------------------------------------------------------------------------------

// Classement du nombre d'accidents en fonction du type de véhicule sur des radars limités à 50km/h
sqlContext.sql("SELECT speed, vehicule_type, COUNT(vehicule_type) as count FROM speedcams_join_accidents WHERE is_accident = 1 AND speed = 50 GROUP BY vehicule_type, speed ORDER BY count DESC")

// Classement du nombre d'accidents en fonction du type de véhicule sur des radars limités à 70km/h
sqlContext.sql("SELECT speed, vehicule_type, COUNT(vehicule_type) as count FROM speedcams_join_accidents WHERE is_accident = 1 AND speed = 70 GROUP BY vehicule_type, speed ORDER BY count DESC")

// Classement du nombre d'accidents en fonction du type de véhicule sur des radars limités à 90km/h
sqlContext.sql("SELECT speed, vehicule_type, COUNT(vehicule_type) as count FROM speedcams_join_accidents WHERE is_accident = 1 AND speed = 90 GROUP BY vehicule_type, speed ORDER BY count DESC")