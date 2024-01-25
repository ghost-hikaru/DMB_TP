import numpy as np
from pyspark.sql.functions import col, desc, current_date, year
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import pandas as pd
import seaborn as sns

# Créer une session Spark
spark = SparkSession.builder.appName("lefooteuh").getOrCreate()

# 1. Chargez le jeu de données. (voir textFile)
players_data = spark.read.csv("../data/2022-2023 Football Player Stats.csv", header=True, inferSchema=True, sep=";")


# Question 1 : Qui sont les joueurs ayant marqué le plus de but ?
def sort_byGoals():
    print(players_data.printSchema())
    sorted_data = players_data.orderBy(desc("Goals"))
    # Afficher les dix meilleurs joueurs
    top_players = sorted_data.select("Player", "Goals", "Squad", "Pos", "Nation").limit(10)
    top_players.show()


# Question 2 : Qui est l'attaquant le plus efficace
# Filtrer les attaquants ayant joué la moitié de la saison (environ 20 matchs == 20 * 90 = 1800 minutes)

def sort_byBestFinisher(minimum_minute):
    filtered_data = players_data.filter((col("Pos").contains("FW") & (col("Min") > minimum_minute)))
    weights = {
        "Goals": 1,
        "SoT%": 0.8,
        "ShoPK": 0.3,
    }

    total_weight = sum(weights.values())
    # Normalisation
    normalized_weights = {stat: weight / total_weight for stat, weight in weights.items()}

    # Calcule de la somme pondérée des statistiques pour chaque joueur
    for stat, weight in normalized_weights.items():
        filtered_data = filtered_data.withColumn(f"Weighted_{stat}", col(stat) * weight)

    # Calcul du score global pondéré pour chaque joueur
    best_finisher_score = sum(col(f"Weighted_{stat}") for stat in normalized_weights.keys())

    # Ajout d'une colonne avec le score pondéré
    result_data = filtered_data.withColumn("WeightedSum", best_finisher_score)

    # Tri du DataFrame en fonction du score pondéré, du plus élevé au plus bas
    return result_data.orderBy("WeightedSum", ascending=False)


def getTopTenBestFinisher():
    sorted_result = sort_byBestFinisher(1200)
    top_players = sorted_result.select("Player", "Squad", "Pos", "Nation").limit(10)
    top_players.show()


def ageCorrBestFinisher():
    sorted_result = sort_byBestFinisher(500)
    sorted_result = sorted_result.withColumn("playerAge", year(current_date()) - col("Born"))
    # Convertissez le DataFrame PySpark en un DataFrame Pandas
    pandas_df = sorted_result.select("playerAge", "WeightedSum").toPandas()
    # Tracez le graphique de dispersion avec une ligne de régression
    plt.figure()
    plt.scatter(pandas_df["playerAge"], pandas_df["WeightedSum"], alpha=0.5)
    plt.title("Corrélation entre l'âge et la performance")
    plt.xlabel("Âge du joueur")
    plt.ylabel("Performance pondérée")
    plt.grid(True)

    # Ajoutez une ligne de régression
    z = np.polyfit(pandas_df["playerAge"], pandas_df["WeightedSum"], 1)
    p = np.poly1d(z)
    plt.plot(pandas_df["playerAge"], p(pandas_df["playerAge"]), color='red')

    # Affichez le graphique
    plt.show()


def competitionCorrBestFinisher():
    sorted_result = sort_byBestFinisher(500)
    plt.figure(figsize=(12, 6))  # Ajustez la taille selon vos préférences

    # Utilisez la fonction scatter() pour créer le diagramme de dispersion
    sns.scatterplot(x='WeightedSum', y='Comp', data=sorted_result.toPandas(),
                    palette='viridis')
    sns.boxplot(x='WeightedSum', y='Comp', data=sorted_result.toPandas(), palette='Set2', width=0.5)
    # Ajoutez des titres et des labels
    plt.title('Corrélation entre Performance et Championnat')
    plt.xlabel('Performance')
    plt.ylabel('Championnat')

    # Affichez le diagramme
    plt.show()


# Question 3 : Est-ce que l'âge et le championnat affecte l'efficacité du buteur ?

print("Ces données sont basés sur la saison 2022-2023 mais ont l'air incomplète (pas la saison complète)")
print("Question 1 : Quels sont les joueurs ayant marqué le plus de but")
sort_byGoals()

print("Question 2 : Qui sont les buteur les plus efficaces ?")
getTopTenBestFinisher()

print("Question 3 : Est ce que l'âge et le championnat affecte l'efficacité du buteur ?")
ageCorrBestFinisher()
competitionCorrBestFinisher()