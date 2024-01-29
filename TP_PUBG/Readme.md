# TP PUBG
A l'aide de la commande WC, on peut remarquer que nous avons 13 849 288 lignes dans le fichier CSV.

# 3 - Les meilleurs joueurs
7. Tableau en fonction de la position
```
+---------------+----------------+---------------+
|    player_name|average_position|number_of_games|
+---------------+----------------+---------------+
|       ChanronG|             9.0|              4|
|  JustTuatuatua|           10.75|              4|
|       dman4771|            11.5|              4|
|         KBSDUI|            12.0|              4|
|      TemcoEwok|           13.25|              4|
|     PapaNuntis|           13.25|              4|
|        Dcc-ccD|            14.5|              4|
|China_huangyong|           21.75|              4|
|   siliymaui125|           22.75|              4|
|      crazyone8|           23.25|              4|
+---------------+----------------+---------------+
```

Tableau en fonction des kills effectués
```
+--------------+-------------+---------------+
|   player_name|average_kills|number_of_games|
+--------------+-------------+---------------+
|LawngD-a-w-n-g|          2.2|              5|
|  siliymaui125|          2.0|              4|
|       Dcc-ccD|         1.75|              4|
|      dman4771|         1.75|              4|
|     NerdyMoJo|          1.5|              4|
|   Roobydooble|          1.0|              4|
|    PapaNuntis|          1.0|              4|
| JustTuatuatua|         0.75|              4|
|       GenOrgg|          0.5|              4|
|      ChanronG|          0.5|              4|
+--------------+-------------+---------------+
```


Si on compare ces 2 tableaux, on peut remarquer que les joueurs sont similaires. On peut donc en déduire que pour effectuer des Top1, il faut faire des dégâts et ne pas rester cacher dans la carte.

# 4 - Score des joueurs
Tableau des 10 meilleurs joueurs :

```
+-------------+-------------+---------------+
|  player_name|average_score|number_of_games|
+-------------+-------------+---------------+
|     gogolnyg|      13282.0|              1|
|    651651646|       9156.0|              1|
| appar1008611|       8819.0|              1|
|   EsNmToging|       8016.0|              1|
|      Kinmmpp|       6629.0|              1|
|     motoMepp|       6405.0|              1|
|  LiliTusfdfs|       6332.0|              1|
|asdkmiojfdioe|       6028.0|              1|
| babyylaowang|       6009.0|              1|
|     MoGu1314|       5941.0|              1|
+-------------+-------------+---------------+
```


On peut en déduite que le 1er a du être 1er de son classement et faire énormement de dégâts et/ou d'assit afin de se hisser aussi haut. 

# Partie 5 - Persistance
Étant donné que nous disposons d'un fichier CSV relativement important de 2 Go, le choix du mode de persistance doit tenir compte de la taille de la mémoire disponible et du besoin d'optimiser les performances.

Dans le cas d'un fichier de cette taille, l'option MEMORY_ONLY pourrait entraîner une utilisation importante de la mémoire. Cependant, il peut être intéressant d'essayer d'utiliser MEMORY_ONLY_SER (sérialisation en mémoire) car cela peut réduire l'utilisation de la mémoire par rapport à MEMORY_ONLY, bien que cela puisse entraîner une légère augmentation du temps de traitement dû à la nécessité de désérialiser les données lors de leur utilisation.
