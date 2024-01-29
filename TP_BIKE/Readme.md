# TP BIKE

Le but de ce tp est d'utiliser <strong>Spark</strong> ainsi que la librairie <strong>graphX</strong>. Pour cela nous allons travailler en <strong>Scala</strong>, sur un jeu de données qui représente des trajets entre plusieurs stations de vélo.

## 1 - Préparation des données
### 1.1) Load des data.

```scala
val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
val sc = new SparkContext(sparkConf)

val DATASET_PATH = "/Users/melvin/Documents/ESIR/ESIR3/S9/DMB_/DMB_TP/TP_BIKE/"
val rddFromFile = sc.textFile(DATASET_PATH +"citibike-tripdata.csv")
val tripRDD: RDD[Trip] = rddFromFile.flatMap(parseLine)

```

La dernière ligne va appeler une méthode qui va permettre de mettre en ordre les données de notre trajet. Par exemple, dans cette méthode on va récupérer les informations essentielles à notre trajet et, par exemple, calculer le temps du trajet. Mais avant cela, il va falloir créer des classes pour respecter le fonctionnement de Scala. Dans notre cas, on crée 2 classes <strong>Station</strong> et <strong>Trip</strong>.

```scala
case class Station(stationId: String, name: String, long: Float, lat: Float)
case class Trip(startStationName: String, endStationName: String,
                dateStartMillis: Long, dateEndMillis: Long, dateSpendMinute: Long,
                start_lat: Float, start_lng: Float, end_lat: Float, end_lng: Float )
```

```scala
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
```
### 1.2) Création du graphe 

On va maintenant créer un graphe dont les noeuds représentent des stations de vélos et les relations représentent des trajets de vélo entre deux stations. Pour cela on va créer les RDD des stations et des trips. Dans notre cas un trip sera composé de la station de départ, de la station d'arrivée et du trip. Tandis qu'une station contient un objet/class station avec les informations indiquées plus haut.

```scala
// Construire l'RDD de nœuds (stations) et l'RDD d'arêtes (trajets)
val stationsRDD: RDD[(VertexId, Station)] = tripRDD.flatMap(trip => Seq(
(hash(trip.startStationName), Station(trip.startStationName, trip.startStationName, trip.start_lat, trip.start_lng)),
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
```

## 2 - Calcul du degré
### 2.1) Afficher tous les trajets entre le 05-12-2021 et 25-12-2021
```scala
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
```
On vient tout d'abord transformer nos dates pour pouvoir les comparer à celle de nos données. Ensuite, on vient simplement faire un subgraph en ajoutant nos conditions sur les dates. 

#### 2.2.1) Les 10 stations ayant le plus de trajets entrant
Pour cela, on vient trier le tableau pour ensuite calculer le nombre de trajet pour chaque station pour lequel il est en station de départ. Afin de faciliter les calculs et les comparaisons, les noms des stations ont été hashé lors de la création de nos RDD.

```
Station : 639314359 a 2375 trajets entrants.
Station : 1354324787 a 2026 trajets entrants.
Station : 1549718166 a 1677 trajets entrants.
Station : 3758876234 a 1518 trajets entrants.
Station : 3808540238 a 1352 trajets entrants.
Station : 1624497230 a 1296 trajets entrants.
Station : 926534974 a 1296 trajets entrants.
Station : 3815922404 a 1261 trajets entrants.
Station : 3815963900 a 1206 trajets entrants.
Station : 1008954473 a 1126 trajets entrants.
```

```
Station : Grove St PATH a 2375 trajets entrants.
Station : Hoboken Terminal - River St & Hudson Pl a 2026 trajets entrants.
Station : Hoboken Terminal - Hudson St & Hudson Pl a 1677 trajets entrants.
Station : Sip Ave a 1518 trajets entrants.
Station : Hamilton Park a 1352 trajets entrants.
Station : South Waterfront Walkway - Sinatra Dr & 1 St a 1296 trajets entrants.
Station : City Hall - Washington St & 1 St a 1296 trajets entrants.
Station : Newport PATH a 1261 trajets entrants.
Station : Newport Pkwy a 1206 trajets entrants.
Station : Hoboken Ave at Monmouth St a 1126 trajets entrants.
```

#### 2.2.2) Les 10 stations ayant le plus de trajets entrant
Pour cela, on vient trier le tableau pour ensuite calculer le nombre de trajet pour lequel il est en station d'arrivée pour chaque station. Afin de faciliter les calculs et les comparaisons, les noms des stations ont été hashé lors de la création de nos RDD.

```
Station : 639314359 a 2358 trajets sortants.
Station : 1354324787 a 2133 trajets sortants.
Station : 3758876234 a 1671 trajets sortants.
Station : 1549718166 a 1652 trajets sortants.
Station : 3815922404 a 1303 trajets sortants.
Station : 3808540238 a 1294 trajets sortants.
Station : 926534974 a 1248 trajets sortants.
Station : 1624497230 a 1241 trajets sortants.
Station : 3815963900 a 1213 trajets sortants.
Station : 2672553465 a 1089 trajets sortants.
```

```
Station : Grove St PATH a 2358 trajets sortants.
Station : Hoboken Terminal - River St & Hudson Pl a 2133 trajets sortants.
Station : Sip Ave a 1671 trajets sortants.
Station : Hoboken Terminal - Hudson St & Hudson Pl a 1652 trajets sortants.
Station : Newport PATH a 1303 trajets sortants.
Station : Hamilton Park a 1294 trajets sortants.
Station : City Hall - Washington St & 1 St a 1248 trajets sortants.
Station : South Waterfront Walkway - Sinatra Dr & 1 St a 1241 trajets sortants.
Station : Newport Pkwy a 1213 trajets sortants.
Station : Marin Light Rail a 1089 trajets sortants.
```

## 3 - Proximité entre les stations
### 3.1) Station la plus proche en km
La première étape est de récupérer tous les trajets que l'on a. On va ensuite filtrer et faire en sorte de récupérer que les trajets dont la station de départ est celle de l'id <strong>JC013</strong>. On remarque que certains trajets étaient les mêmes (avec les mêmes stations d'arrivées), alors on décide d'avoir des trajets uniques pour gagner en performance. De plus, au même moment, on vient calculer la distance pour chaque trajet entre les 2 stations. On stocke ensuite le résultat dans un nouveau dataframe. Enfin on vient parcourir le dataframe afin de récupérer la plus petite valeur et l'afficher. Évidemement lors de la récupération de la valeur, on enlève les valeurs égales à 0 car il pourrait y avoir un trajet dont la station d'arrivée est la même que celle de départ. Voici le résultat obtenu :

```
Distance minimale entre Marin Light Rail et City Hall: 0.14957127656780697 kilomètres
```

### 3.2) Station la plus proche en temps 
La première étape est de récupérer tous les trajets que l'on a. On va ensuite filtrer et faire en sorte de récupérer uniquement les trajets dont la station de départ est celle de l'id <strong>JC013</strong>. Etant donné que dès le début, on calcule le temps que mettait chaque trajet, on vient juste parcourir les trajets afin de récupérer la plus petite valeur en terme de temps et l'afficher. Évidemement lors de la récupération de la valeur, on enlève les valeurs égales à 0 car il pourrait y avoir un trajet dont la station d'arrivée est la même que celle de départ. Voici le résultat obtenu :

```
Distance minimale entre Marin Light Rail et Grand St: 1 minutes
```
