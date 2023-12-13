from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("pubg").getOrCreate()

# 1. Chargez le jeu de données. (voir textFile)

# 2. Pour chaque partie, obtenez uniquement le nom du joueur et son nombre d’éliminations ou sa position. (voir map)

# 3. Obtenez la moyenne des éliminations ou de la position de chaque joueur, ainsi que le nombre de  parties concernées. (voir reduceByKey ou groupByKey)

# 4. Obtenez les 10 meilleurs joueurs selon les éliminations ou la position. (voir sortBy)

# 5. Certains joueurs n’ayant joué qu’une partie, nous souhaitions ne garder que ceux ayant au moins 4 parties. (voir filter)

# 6. Si vous observez un joueur particulier, traitez-le de la manière appropriée.

# 7. En partageant avec vos camarades qui ont exploré l’autre condition, donnez votre avis sur l’affirmation de départ.