from pyspark.sql import SparkSession
#from graphframes import GraphFrame
from graphframes import *

# Créer une session Spark
spark = SparkSession.builder.appName("BikeGraph").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Partie 1 - Préparation des données
bike_data = spark.read.csv("../data/citibike-tripdata.csv", header=True, inferSchema=True)

stations_df = bike_data.select("start_station_id", "start_station_name", "start_lat", "start_lng").distinct()

# Renommer les colonnes pour correspondre aux attentes de GraphFrame
stations_df = stations_df.withColumnRenamed("start_station_id", "id") \
                         .withColumnRenamed("start_station_name", "name") \
                         .withColumnRenamed("start_lat", "lat") \
                         .withColumnRenamed("start_lng", "lng")

# Sélectionner les colonnes pertinentes pour créer les trajets
trips_df = bike_data.select("start_station_id", "end_station_id").distinct()

# Renommer les colonnes pour correspondre aux attentes de GraphFrame
trips_df = trips_df.withColumnRenamed("start_station_id", "src") \
                   .withColumnRenamed("end_station_id", "dst")

# Créer un GraphFrame à partir des DataFrames de stations et de trajets
graph = GraphFrame(stations_df, trips_df)

# Afficher le schéma du graphe
graph.printSchema()

# Afficher les sommets du graphe (stations)
graph.vertices.show()

# Afficher les arêtes du graphe (trajets)
graph.edges.show()

# PARTIE 2
# 1. Extrayez le sous-graphe dont les intervalles temporelles de ces trajets (relations) existent entre 05-12-2021 et 25-12-2021.
subgraph = graph.filterEdges("date >= '2021-12-05' AND date <= '2021-12-25'")

# 2. Calculez le nombre total de trajets entrants et sortants de chaque station
incoming_edges = subgraph.groupBy("dst").count().withColumnRenamed("dst", "id").withColumnRenamed("count", "incoming_count")
outgoing_edges = subgraph.groupBy("src").count().withColumnRenamed("src", "id").withColumnRenamed("count", "outgoing_count")

# Agréger les données pour obtenir le nombre total de trajets entrants et sortants pour chaque station
total_trips = incoming_edges.join(outgoing_edges, "id", "outer").na.fill(0)

# Afficher les 10 stations ayant plus de trajets sortants
top_outgoing_stations = total_trips.orderBy(col("outgoing_count").desc()).limit(10)

# Afficher les 10 stations ayant plus de trajets entrants
top_incoming_stations = total_trips.orderBy(col("incoming_count").desc()).limit(10)

# Afficher les résultats
print("Top 10 stations avec plus de trajets sortants:")
top_outgoing_stations.show()

print("\nTop 10 stations avec plus de trajets entrants:")
top_incoming_stations.show()

# PARTIE 3
# 1. Trouver et afficher la station la plus proche de JC013 en termes de distance
closest_station_by_distance = graph.aggregateMessages(
    source="id",
    edge=["distance"],
    sendToSrc="min(dst, distance) as closest_station",
    collect=set
).filter("id = 'JC013'").select("id", "closest_station").show()

# 2. Trouver et afficher la station la plus proche de JC013 en termes de durée de trajet
closest_station_by_duration = graph.aggregateMessages(
    source="id",
    edge=["duration"],
    sendToSrc="min(dst, duration) as closest_station",
    collect=set
).filter("id = 'JC013'").select("id", "closest_station").show()

# Arrêter la session Spark
spark.stop()