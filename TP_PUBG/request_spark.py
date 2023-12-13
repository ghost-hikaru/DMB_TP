from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import avg, count, col

# Créer une session Spark
spark = SparkSession.builder.appName("pubg").getOrCreate()

# 1. Chargez le jeu de données. (voir textFile)
pubg_data = spark.read.csv("../data/echantillon.csv", header=True, inferSchema=True)

# 2. Pour chaque partie, obtenez uniquement le nom du joueur et son nombre d’éliminations ou sa position. (voir map)
def get_player_position():
    player_position = pubg_data.select('player_name', 'team_placement').rdd.collectAsMap();
    result = pd.DataFrame(list(player_position.items()), columns=['player_name', 'placement'])
    print(result)

# 3. Obtenez la moyenne de la position de chaque joueur, ainsi que le nombre de  parties concernées. (voir reduceByKey ou groupByKey)
def average_position():
    # Grouper les données par joueur
    grouped_data = pubg_data.groupBy("player_name")
    # Calculer la moyenne de la position et le nombre de parties pour chaque joueur
    avg_postion = grouped_data.agg(avg("team_placement").alias("average_position"),count("team_placement").alias("number_of_games"))
    # Afficher les résultats
    avg_postion.show()

# 4. Obtenez les 10 meilleurs joueurs selon la position. (voir sortBy)
def sort_by():
    sorted_data = pubg_data.orderBy("team_placement")
    # Afficher les dix meilleurs joueurs
    top_players = sorted_data.select("player_name", "team_placement").limit(10)
    top_players.show()

# 5. Certains joueurs n’ayant joué qu’une partie, nous souhaitions ne garder que ceux ayant au moins 4 parties. (voir filter)
def filter_player():
    filtered_result = (
        pubg_data.groupBy("player_name")
            .agg(avg("team_placement").alias("average_position"), count("team_placement").alias("number_of_games"))
            .filter(col("number_of_games") >= 4)
            .orderBy("average_position")
            .select("player_name", "average_position", "number_of_games")
            .limit(10)
    )
    filtered_result.show()

# 6. Si vous observez un joueur particulier, traitez-le de la manière appropriée.
def filtered_player_without_null():
    filtered_result_without_null = (
        pubg_data.groupBy("player_name")
            .agg(avg("team_placement").alias("average_position"), count("team_placement").alias("number_of_games"))
            .filter(col("number_of_games") >= 4)
            .na.drop()
            .orderBy("average_position")
            .select("player_name", "average_position", "number_of_games")
            .limit(10)
    )
    filtered_result_without_null.show()

# Partie 4 - Score des joueurs
# Définir une fonction pour calculer le score
def calculate_score(assists, damage, eliminations, placement):
    return 50 * assists + damage + 100 * eliminations + (1000 - (placement - 1) * 10)

def scores():
    # Enregistrer la fonction comme une UDF (User Defined Function)
    calculate_score_udf = spark.udf.register("calculate_score", calculate_score)

    # Appliquer la fonction pour calculer le score
    scored_data = (
        pubg_data.withColumn("score",
             calculate_score_udf(col("player_assists"), col("player_dmg"), col("player_kills"),
                                 col("team_placement"))

             )
    )

    # Grouper les données par joueur et calculer la somme du score
    result_score = (
        scored_data.groupBy("player_name")
            .agg(avg("score").alias("average_score"), count('score').alias('number_of_games'))
            .orderBy("average_score", ascending=False)
            .select("player_name", "average_score","number_of_games")
            .limit(10)
    )

    # Afficher les résultats
    result_score.show()

# MAIN
print("Partie 3 - Les meilleurs joueurs \n")
print("Question 2 :\n")
get_player_position()
print("\n")
print("Question 3 :\n")
average_position()
print("\n")
print("Question 4 :\n")
sort_by()
print("\n")
print("Question 5 :\n")
filter_player()
print("\n")
print("Question 6 :\n")
filtered_player_without_null()
print("\n")
print("Partie 4 - Score des joueurs \n")
scores()
print("\n")
print("Fin des données \n")
# Arrêter la session Spark
spark.stop()