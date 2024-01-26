import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.math._

object BikeGraph extends App{
  //define class
  case class Station(stationId: String, name: String, long: Float, lat: Float)
  case class Trip(startStationName: String, endStationName: String,
                  dateStartMillis: Long, dateEndMillis: Long, dateSpendMinute: Long,
                  start_lat: Float, start_lng: Float, end_lat: Float, end_lng: Float )

  // Fonction pour convertir une ligne en Trip
  def parseLine(line: String): Option[Trip] = {
    val row = line.split(",")
    if (row.length >= 4) {
      try {
        val format = new java.text.SimpleDateFormat("yyyy-mm-dd HH:mm:ss")
        val dateStartMillis = format.parse(row(2)).getTime()
        val dateEndMillis = format.parse(row(3)).getTime()
        val timeTrip = dateEndMillis - dateStartMillis
        val timeTripMinutes = timeTrip / (60 * 1000)
        val start_lat = row(8).toFloat
        val start_lng = row(9).toFloat
        val end_lat = row(10).toFloat
        val end_lng = row(11).toFloat
        Some(Trip(row(4), row(6), dateStartMillis, dateEndMillis, timeTripMinutes, start_lat, start_lng, end_lat, end_lng))
      } catch {
        case _: Throwable => None
      }
    } else {
      None
    }
  }
  
  /**
   * 1.1 Préparation des données
   */

  //Read file
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)

  val DATASET_PATH = "/Users/melvin/Documents/ESIR/ESIR3/S9/DMB_/DMB_TP/TP_BIKE/"
  val rddFromFile = sc.textFile(DATASET_PATH +"citibike-tripdata.csv")
  val tripRDD: RDD[Trip] = rddFromFile.flatMap(parseLine)

  /**
   * 1.2 - Création du premier graphes
   */

  // Construire l'RDD de nœuds (stations) et l'RDD d'arêtes (trajets)
  val stationsRDD: RDD[(VertexId, Station)] = tripRDD.flatMap(trip => Seq(
    (hash(trip.startStationName), Station(trip.startStationName, trip.startStationName, trip.start_lat, trip.start_lng)),
    (hash(trip.endStationName), Station(trip.endStationName, trip.endStationName, trip.end_lat, trip.end_lng))
  ))

  val tripsRDD: RDD[Edge[Trip]] = tripRDD.map(trip =>
    Edge(hash(trip.startStationName), hash(trip.endStationName), trip)
  )

  // Construire le graphe
  val bikeGraph: Graph[Station, Trip] = Graph(stationsRDD, tripsRDD)

  // Définir la fonction de hachage pour les nœuds
  def hash(name: String): Long = {
    name.hashCode.toLong & 0xFFFFFFFFL
  }

  println("1.2 : création du graphe")
  // Afficher les nœuds du graphe
  bikeGraph.vertices.collect().foreach(println)
  // Afficher les arêtes du graphe
  bikeGraph.edges.collect().foreach(println)
  println("------------------------------------------------------------")


  /**
   * 2 - Calcul du degré
   * 2.1 - Extrayez le sous-graphe dont les intervalles temporelles de ces trajets (relations) existent
   * entre 05-12- 2021 et 25-12-2021. (subgraph)
   */
  val dateStartref = "05-12-2021 00:00:00"
  val dateEndRef = "25-12-2021 00:00:00"
  val format = new java.text.SimpleDateFormat("yyyy-mm-dd HH:mm:ss")
  val dateStartRefMillis = format.parse(dateStartref).getTime()
  val dateEndRefMillis = format.parse(dateEndRef).getTime()

  val subgraph = bikeGraph.subgraph(trip => trip.attr.dateStartMillis >= dateStartRefMillis && trip.attr.dateEndMillis <= dateEndRefMillis)

  println("2.1 : Subgraph")
  // Afficher les nœuds du sous-graphe
  subgraph.vertices.collect().foreach(println)
  // Afficher les arêtes du sous-graphe
  subgraph.edges.collect().foreach(println)
  println("------------------------------------------------------------")

  /**
   * 2.2 - Calculez le nombre total de trajets entrants de chaque station et affichez les 10 stations
   * ayant plus de trajets entrants. (aggregateMessages)
   */

  val inDegreeRDD: RDD[(VertexId, Int)] = bikeGraph.inDegrees
  val sortedInDegreeRDD = inDegreeRDD.sortBy(_._2, ascending = false)

  println("2.2 : Nombre de trajet entrant")
  // Afficher les 10 stations avec le plus de trajets entrants
  sortedInDegreeRDD.take(10).foreach { case (stationId, inDegree) =>
    println(s"Station $stationId a $inDegree trajets entrants.")
  }
  println("------------------------------------------------------------")

  /**
   * 2.2 - Calculez le nombre total de trajets sortant de chaque station et affichez les 10 stations
   * ayant plus de trajets sortant. (aggregateMessages)
   */
  println("2.2 : Nombre de trajet sortant")
  val outDegreeRDD: RDD[(VertexId, Int)] = bikeGraph.outDegrees
  val sortedOutDegreeRDD = outDegreeRDD.sortBy(_._2, ascending = false)

  println("2.2 : Nombre de trajet sortant")
  // Afficher les 10 stations avec le plus de trajets sortants
  sortedOutDegreeRDD.take(10).foreach { case (stationId, outDegree) =>
    println(s"Station $stationId a $outDegree trajets sortants.")
  }
  println("------------------------------------------------------------")


  /**
   * 3.1 - Trouvez et affichez la station la plus proche de la station JC013 tel que la distance du trajet (relation) entre les deux stations soit minimale. (aggregateMessages)
   */
  val jcStationId = 2672553465L
  def getDistKilometers(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val R = 6371.0  // Rayon moyen de la Terre en kilomètres

    val dLat = toRadians(lat2 - lat1)
    val dLon = toRadians(lon2 - lon1)

    val a = pow(sin(dLat / 2), 2) + cos(toRadians(lat1)) * cos(toRadians(lat2)) * pow(sin(dLon / 2), 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))

    R * c
  }

  // Obtenez tous les triplets du graphe
  val allTriplets = bikeGraph.triplets

  // Filtrer les triplets où la station source est JC013
  val tripsFromJC013 = allTriplets.filter(triplet => triplet.srcId == jcStationId)

  // Calculer la distance minimale pour chaque paire unique
  val minDistanceTripsFromJC013 = tripsFromJC013
    .map(triplet => ((triplet.srcId, triplet.dstId), triplet))
    .reduceByKey((trip1, trip2) => {
      if (trip1.attr.dateStartMillis < trip2.attr.dateStartMillis) trip1
      else trip2
    })
    .map { case (_, minDistanceTrip) => minDistanceTrip }

  private var short_dist = 10000D
  private var startNameStaion = ""
  private var endNameStaion = ""

  // Calculer et afficher la distance pour chaque trajet unique
  minDistanceTripsFromJC013.foreach { triplet =>

    val distance = getDistKilometers(
      triplet.srcAttr.lat, triplet.srcAttr.long,
      triplet.dstAttr.lat, triplet.dstAttr.long
    )
    if(short_dist>distance && distance>0.0){
      short_dist = distance
      startNameStaion = triplet.srcAttr.name
      endNameStaion = triplet.dstAttr.name
    }
  }
  println(s"Distance minimale entre ${startNameStaion} et ${endNameStaion}: $short_dist kilomètres")

  println("------------------------------------------------------------")

  /**
  * 3.2 - Trouvez et affichez la station la plus proche de la station JC013 tel que la durée du trajet (relation) entre les deux stations soit minimale. (aggregateMessage)
  */

  private var minTime = 100000L
  private var startNameStaionTime = ""
  private var endNameStaionTime = ""

  // Chercher la valeur minimum
  tripsFromJC013.foreach { triplet =>
    val tripAttr: Trip = triplet.attr

    if(minTime>tripAttr.dateSpendMinute && tripAttr.dateSpendMinute>0){
      minTime = tripAttr.dateSpendMinute
      startNameStaionTime = triplet.srcAttr.name
      endNameStaionTime = triplet.dstAttr.name
    }
  }

  println(s"Distance minimale entre ${startNameStaionTime} et ${endNameStaionTime}: $minTime minutes")

  println("------------------------------------------------------------")


}