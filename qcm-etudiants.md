# QCM — Spark ClimaCity Paris (Sessions 1 à 4)

**Version étudiant** — sans corrections  
**Formateur :** voir [`qcm-test.md`](qcm-test.md) (corrigé)

**Projet :** Vélib' Paris · PySpark · Delta Lake · Structured Streaming  
**Références :** [`rapport.md`](rapport.md) · notebooks `Spark_DIA3_Session_1` à `4`

> **Mode d'emploi :** une seule bonne réponse par question (sauf indication contraire).  
> Cochez la lettre choisie sur la grille de réponses en fin de document.

**Nom :** _________________________________  
**Date :** _________________________________

---

## Session 1 — API RDD

### Q1
Qu'affiche `print(mon_rdd)` sur un RDD PySpark non encore exécuté ?

- A) Les 10 premières lignes du fichier  
- B) Une représentation du type `PythonRDD[…]` sans les données  
- C) Le nombre exact de lignes du fichier  
- D) Le plan d'exécution SQL formaté  

### Q2
En Spark, quand une transformation comme `filter()` ou `map()` est-elle réellement exécutée ?

- A) Dès l'appel de la transformation  
- B) Au moment d'une action (`take`, `collect`, `count`, …)  
- C) À la création du `SparkContext`  
- D) Uniquement lors d'un `spark.stop()`  

### Q3
Quelle est la différence principale entre les partitions d'un RDD (`getNumPartitions()`) et `spark.sql.shuffle.partitions` ?

- A) Ce sont exactement la même chose  
- B) Les partitions RDD concernent les données en entrée ; `shuffle.partitions` concerne les étapes de shuffle (jointures, agrégations)  
- C) `shuffle.partitions` ne s'applique qu'au mode streaming  
- D) Les partitions RDD ne peuvent pas être modifiées  

### Q4
Pourquoi utilise-t-on `operative.lower() == "true"` lors du parsing CSV ?

- A) Pour convertir automatiquement en entier  
- B) Parce que les valeurs CSV sont du texte et peuvent varier en casse (`True`, `TRUE`, …)  
- C) Pour supprimer les lignes sans station_id  
- D) Pour partitionner le RDD par arrondissement  

### Q5
Dans le pipeline Session 1, une station est considérée « quasi vide » si :

- A) `velos_total == 0`  
- B) `taux_occupation < 0.10`  
- C) `bornettes_libres == capacite`  
- D) `operative == False`  

### Q6
Sur Mac Apple Silicon, le warning « Please install psutil » pendant un shuffle est souvent dû à :

- A) L'absence du package Delta Lake  
- B) Un conflit d'architecture entre JVM (x86) et workers Python / `psutil` (arm64)  
- C) Un fichier CSV trop volumineux  
- D) L'utilisation de `collect()` sur un gros RDD  

### Q7
Quelle action est **déconseillée** sur un très gros RDD ?

- A) `take(10)`  
- B) `collect()`  
- C) `count()`  
- D) `filter()`  

### Q8
`sc.textFile("fichier.csv")` produit initialement un RDD de type :

- A) `RDD[dict]`  
- B) `RDD[str]` (une ligne = une chaîne)  
- C) `DataFrame`  
- D) `RDD[int]`  

---

## Session 2 — DataFrame & Parquet

### Q9
À quoi sert `df.explain(mode="formatted")` ?

- A) À afficher les 20 premières lignes sans troncature  
- B) À inspecter le plan physique / logique d'exécution Spark  
- C) À écrire le DataFrame en Parquet  
- D) À créer une vue temporaire SQL  

### Q10
Quel avantage majeur du format Parquet par rapport au CSV ?

- A) Stockage par lignes, plus lisible à l'œil nu  
- B) Stockage par colonnes, compression et lecture sélective des colonnes  
- C) Pas besoin de schéma  
- D) Compatible uniquement avec l'API RDD  

### Q11
Le **predicate pushdown** avec Parquet permet à Spark de :

- A) Ignorer des colonnes entières non utilisées  
- B) Sauter des blocs de données selon des filtres, avant de tout charger  
- C) Convertir automatiquement CSV en JSON  
- D) Désactiver le shuffle  

### Q12
Par rapport à Pandas, un DataFrame Spark se distingue surtout par :

- A) L'exécution sur une seule machine, toujours en mémoire  
- B) Le calcul distribué et l'évaluation paresseuse (lazy)  
- C) L'absence de schéma  
- D) L'impossibilité de faire des jointures  

---

## Session 3 — Spark SQL, fenêtres & Delta Lake

### Q13
À quoi sert le package `delta-spark` ?

- A) À remplacer PySpark par une API plus rapide  
- B) À ajouter des transactions ACID, historique et MERGE au-dessus de Parquet  
- C) À lire uniquement des fichiers CSV  
- D) À exécuter du Python sur les workers sans JVM  

### Q14
Quel dossier distingue une table Delta d'un simple dossier Parquet ?

- A) `_metadata/`  
- B) `_delta_log/`  
- C) `_checkpoint/`  
- D) `_spark_conf/`  

### Q15
Un `LEFT ANTI JOIN` entre `A` et `B` retourne :

- A) Toutes les lignes de A et B, même sans correspondance  
- B) Les lignes de A qui **n'ont pas** de correspondance dans B  
- C) Uniquement les lignes où A et B matchent  
- D) Le produit cartésien de A et B  

### Q16
À quoi sert `DATE_TRUNC('hour', horodatage)` dans la jointure Vélib' × météo ?

- A) À supprimer les lignes sans température  
- B) À aligner les deux sources sur la même granularité temporelle (ici l'heure)  
- C) À partitionner le disque par mois  
- D) À activer le time travel Delta  

### Q17
En Spark SQL, `LAG(col, 1) OVER (PARTITION BY station ORDER BY horodatage)` permet de :

- A) Compter le nombre total de stations  
- B) Récupérer la valeur de la ligne **précédente** dans la fenêtre  
- C) Supprimer les doublons  
- D) Écrire en mode streaming  

### Q18
`ROW_NUMBER() OVER (PARTITION BY heure ORDER BY taux DESC)` sert surtout à :

- A) Calculer une moyenne mobile  
- B) Attribuer un rang (1, 2, 3…) à chaque ligne dans un groupe  
- C) Fusionner deux tables Delta  
- D) Lire une version antérieure d'une table  

### Q19
Pour relire une table Delta **telle qu'elle était** à la version 3, on utilise :

- A) `.option("versionAsOf", 3)`  
- B) `.mode("overwrite")`  
- C) `MERGE INTO … WHEN NOT MATCHED`  
- D) `repartition(3)`  

### Q20
La commande `DESCRIBE HISTORY` sur une table Delta permet de :

- A) Lister les versions et opérations (INSERT, MERGE, …)  
- B) Supprimer les anciennes partitions  
- C) Arrêter une requête streaming  
- D) Convertir Parquet en CSV  

### Q21
`MERGE INTO` sur une table Delta est particulièrement utile pour :

- A) Lire un fichier JSON ligne par ligne  
- B) Faire des upserts (mettre à jour les lignes existantes, insérer les nouvelles)  
- C) Remplacer le driver Python  
- D) Désactiver le watermark  

### Q22
Une vue temporaire créée avec `createOrReplaceTempView("ma_vue")` :

- A) Est persistée sur disque après `spark.stop()`  
- B) Existe uniquement pour la durée de la `SparkSession`  
- C) Remplace définitivement une table Delta  
- D) Ne peut pas être interrogée en SQL  

---

## Session 4 — Structured Streaming

### Q23
Quelle affirmation sur un DataFrame `readStream` est **fausse** ?

- A) On ne peut pas appeler `.count()` directement dessus comme en batch  
- B) Il représente un flux de données potentiellement infini  
- C) Il peut être écrit avec `writeStream`  
- D) Il est matérialisé entièrement en mémoire dès sa création  

### Q24
À quoi sert le **checkpoint** dans une requête `writeStream` ?

- A) À compresser les fichiers Parquet  
- B) À mémoriser l'état (offsets, watermark…) pour reprendre après redémarrage  
- C) À remplacer Delta Lake  
- D) À afficher les résultats dans Jupyter  

### Q25
Le **watermark** sur une colonne temporelle sert à :

- A) Accélérer le chargement du CSV initial  
- B) Définir le retard maximal toléré avant de fermer une fenêtre d'agrégation  
- C) Chiffrer les données en transit  
- D) Forcer l'exécution immédiate de toutes les transformations  

### Q26
Pour une agrégation fenêtrée en streaming, quel `outputMode` est adapté pour n'émettre que les **nouvelles lignes définitives** ?

- A) `complete` uniquement  
- B) `append` (avec watermark)  
- C) `ignore`  
- D) `batch`  

### Q27
Dans Session 4, `foreachBatch` est préféré à une simple agrégation lorsque :

- A) On veut uniquement afficher 10 lignes en console  
- B) On a besoin d'une logique métier complexe avec état entre micro-batchs (ex. alertes)  
- C) On n'a pas besoin de Delta Lake  
- D) Le simulateur est arrêté  

### Q28
Quelle est la différence entre une fenêtre **glissante** `window(col, "10 minutes", "2 minutes")` et une fenêtre **basculante** `window(col, "15 minutes")` ?

- A) La glissante avance par pas de 2 min ; la basculante est découpée en blocs fixes de 15 min sans chevauchement  
- B) La basculante ne fonctionne qu'en batch  
- C) La glissante ne nécessite pas de watermark  
- D) Il n'y a aucune différence  

### Q29
Le **driver** Spark, c'est :

- A) Le processus qui exécute les tâches sur chaque partition  
- B) Le processus qui planifie le travail, envoie les tâches aux workers et assemble les résultats  
- C) Un fichier Parquet sur disque  
- D) Le simulateur `simulateur_flux.py`  

### Q30
Pourquoi le simulateur `scripts/simulateur_flux.py` doit-il tourner **en parallèle** du notebook Session 4 ?

- A) Parce que Spark ne peut pas lire de JSON  
- B) Parce que `readStream` consomme un flux continu de nouveaux fichiers dans `stream_input`  
- C) Parce qu'il remplace `spark.stop()`  
- D) Parce qu'il crée la `SparkSession`  

### Q31
L'erreur `Cannot start query with name fenetres_arrondissement as a query with that name is already active` signifie :

- A) Delta Lake n'est pas installé  
- B) Une requête streaming du même nom tourne déjà dans la session  
- C) Le watermark est trop court  
- D) Le Parquet consolidé est manquant  

### Q32
Pourquoi appeler `spark.stop()` en fin de notebook ?

- A) Pour relancer automatiquement le simulateur  
- B) Pour libérer la JVM, la mémoire et arrêter les requêtes streaming résiduelles  
- C) Pour activer le time travel  
- D) Pour convertir les RDD en DataFrame  

---

## Session transversale (bonus)

### Q33
Dans le projet ClimaCity, le fichier `disponibilite_consolidee.parquet` (Session 2 §2.8) est un prérequis pour :

- A) Session 1 uniquement  
- B) Session 4 (simulateur de flux et streaming)  
- C) Aucune session  
- D) Uniquement l'API RDD  

### Q34
Delta Lake + checkpoint en streaming garantissent surtout :

- A) Des requêtes plus lentes mais plus jolies  
- B) Des écritures fiables et une reprise cohérente après incident  
- C) La suppression automatique des données anciennes  
- D) Le remplacement de Java par Python  

---

## Barème

| Session | Questions | Points (1 pt / question) |
|---|---|---|
| Session 1 | Q1–Q8 | 8 |
| Session 2 | Q9–Q12 | 4 |
| Session 3 | Q13–Q22 | 10 |
| Session 4 | Q23–Q32 | 10 |
| Bonus | Q33–Q34 | 2 |
| **Total** | **34 questions** | **/ 34** |

---

## Grille de réponses

| Q | A/B/C/D | Q | A/B/C/D | Q | A/B/C/D | Q | A/B/C/D |
|---|---|---|---|---|---|---|---|
| 1 | | 9 | | 17 | | 25 | |
| 2 | | 10 | | 18 | | 26 | |
| 3 | | 11 | | 19 | | 27 | |
| 4 | | 12 | | 20 | | 28 | |
| 5 | | 13 | | 21 | | 29 | |
| 6 | | 14 | | 22 | | 30 | |
| 7 | | 15 | | 23 | | 31 | |
| 8 | | 16 | | 24 | | 32 | |
| | | | | 33 | | 34 | |

**Score :** ______ / 34
