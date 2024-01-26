import numpy as np
from pyspark.sql.functions import col, desc, current_date, year
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import seaborn as sns

spark = SparkSession.builder.appName("footballPerformanceAnalysis").getOrCreate()

players_data = spark.read.csv("../data/2022-2023 Football Player Stats.csv", header=True, inferSchema=True, sep=";")


# Question 1 : Qui sont les joueurs ayant marqué le plus de but ?
def sort_byGoals():
    sorted_data = players_data.orderBy(desc("Goals"))
    # Affichage des dix meilleurs joueurs
    top_players = sorted_data.select("Player", "Goals", "Squad", "Pos", "Nation").limit(10)
    top_players.show()


# Question 2 : Qui est l'attaquant le plus efficace

def sort_byBestFinisher(minimum_minute):
    filtered_data = players_data.filter((col("Pos").contains("FW") & (col("Min") > minimum_minute)))
    weights = {
        "Goals": 1,
        "SoT%": 0.8,
        "ShoPK": 0.3,
        "Assists": 0.7
    }

    total_weight = sum(weights.values())
    # Normalisation
    normalized_weights = {stat: weight / total_weight for stat, weight in weights.items()}

    # Calcul de la somme pondérée des statistiques pour chaque joueur
    for stat, weight in normalized_weights.items():
        filtered_data = filtered_data.withColumn(f"Weighted_{stat}", col(stat) * weight)

    # Calcul du score global pondéré pour chaque joueur
    best_finisher_score = sum(col(f"Weighted_{stat}") for stat in normalized_weights.keys())

    # Ajout d'une colonne avec le score pondéré
    result_data = filtered_data.withColumn("AttackingPerformance", best_finisher_score)

    # Tri du DataFrame en fonction du score pondéré, du plus élevé au plus bas
    return result_data.orderBy("AttackingPerformance", ascending=False)


def getTopTenBestFinisher():
    sorted_result = sort_byBestFinisher(1200)
    top_players = sorted_result.select("Player", "Squad", "Pos", "Nation").limit(10)
    top_players.show()


# Question 3 : Est-ce que l'âge et le championnat affecte l'efficacité d'un attaquant ?
def ageCorrBestFinisher():
    sorted_result = sort_byBestFinisher(500)
    sorted_result = sorted_result.withColumn("playerAge", year(current_date()) - col("Born"))
    pandas_df = sorted_result.select("playerAge", "AttackingPerformance").toPandas()
    plt.figure()
    plt.scatter(pandas_df["playerAge"], pandas_df["AttackingPerformance"], alpha=0.5)
    plt.title("Corrélation entre l'âge et la performance")
    plt.xlabel("Âge du joueur")
    plt.ylabel("Performance pondérée")
    plt.grid(True)

    z = np.polyfit(pandas_df["playerAge"], pandas_df["AttackingPerformance"], 1)
    p = np.poly1d(z)
    plt.plot(pandas_df["playerAge"], p(pandas_df["playerAge"]), color='red')

    plt.show()


def competitionCorrBestFinisher():
    sorted_result = sort_byBestFinisher(500)
    plt.figure(figsize=(12, 6))  # Ajustez la taille selon vos préférences

    sns.scatterplot(x='AttackingPerformance', y='Comp', data=sorted_result.toPandas(),
                    palette='viridis')
    sns.boxplot(x='AttackingPerformance', y='Comp', data=sorted_result.toPandas(), palette='Set2', width=0.5)
    plt.title('Corrélation entre Performance et Championnat')
    plt.xlabel('Performance')
    plt.ylabel('Championnat')

    plt.show()


print("Ces données sont basés sur la saison 2022-2023 mais ont l'air incomplète (pas la saison complète)")
print("Question 1 : Quels sont les joueurs ayant marqué le plus de but")
sort_byGoals()

print("Question 2 : Qui sont les attaquants les plus efficaces ?")
getTopTenBestFinisher()

print("Question 3 : Est ce que l'âge et le championnat affectent l'efficacité d'un attaquant ?")
ageCorrBestFinisher()
competitionCorrBestFinisher()
