# Rapport — Spark ClimaCity Paris (Sessions 1–4)

Notes et explications des notebooks Spark du projet ClimaCity Paris.

| Notebook | Thème | Sections rapport |
|---|---|---|
| [`Spark_DIA3_Session_1.ipynb`](Spark_DIA3_Session_1.ipynb) | API RDD | [§1–§10](#session-1--api-rdd) |
| [`Spark_DIA3_Session_2.ipynb`](Spark_DIA3_Session_2.ipynb) | DataFrame, Parquet | [§11–§12](#session-2--dataframe--parquet) |
| [`Spark_DIA3_Session_3.ipynb`](Spark_DIA3_Session_3.ipynb) | Spark SQL, fenêtres, Delta Lake | [§13–§33](#session-3--spark-sql-bases) · [§39 ACID](#session-3--delta-lake-écriture-merge-time-travel) |
| [`Spark_DIA3_Session_4.ipynb`](Spark_DIA3_Session_4.ipynb) | Structured Streaming | [§34–§38](#session-4--structured-streaming) |

**Référence complémentaire :** [`MEM-02SPARK_Window-Functions.md`](MEM-02SPARK_Window-Functions.md) — catalogue et syntaxe SQL des fonctions de fenêtrage (`OVER`, `WINDOW w`, `LAG`, `ROW_NUMBER`, etc.).

**QCM (Sessions 1–4) :** [`qcm-etudiants.md`](qcm-etudiants.md) (sans corrigé) · [`qcm-test.md`](qcm-test.md) (formateur) — [notes §40–§47](#annexes--notes-qcm-sessions-14) · [Python §48](#annexes--python-rappels)

**Accès rapide :** [Session 1](#session-1--api-rdd) · [Session 2](#session-2--dataframe--parquet) · [Session 3 SQL](#session-3--spark-sql-bases) · [Session 3 fenêtres](#session-3--fenêtres-analytiques-spark-sql) · [Session 3 Delta](#session-3--delta-lake-écriture-merge-time-travel) · [Session 4](#session-4--structured-streaming) · [QCM](#annexes--notes-qcm-sessions-14) · [Python](#annexes--python-rappels) · [Parcours pipeline](#parcours-du-pipeline-liens-entre-sections)

## Sommaire

<a id="session-1--api-rdd"></a>

### Session 1 — API RDD

1. [Chargement d'un CSV avec `textFile()`](#1-chargement-dun-csv-avec-textfile)
2. [Partitions RDD vs `spark.sql.shuffle.partitions`](#2-partitions-rdd-vs-sparksqlshufflepartitions)
3. [Filtrage d'un RDD avec `filter()` (en-tête)](#3-filtrage-dun-rdd-avec-filter-en-tête)
4. [Affichage d'un RDD : `PythonRDD[26]`](#4-affichage-dun-rdd-pythonrdd26)
5. [Déballage de liste (`horodatage, capacite, ... = champs`)](#5-déballage-de-liste-horodatage-capacite--champs)
6. [Conversion booléenne (`operative.lower() == "true"`)](#6-conversion-booléenne-operativelower--true)
7. [Suppression des lignes malformées (`x is not None`)](#7-suppression-des-lignes-malformées-x-is-not-none)
8. [Filtrage par taux d'occupation (`< 0.10`)](#8-filtrage-par-taux-doccupation--010)
9. [Formatage d'un en-tête de tableau (`<40` / `>12`)](#9-formatage-dun-en-tête-de-tableau-40--12)
10. [Mac Apple Silicon — Java arm64 et warning `psutil`](#10-mac-apple-silicon--java-arm64-et-warning-psutil)

<a id="session-2--dataframe--parquet"></a>

### Session 2 — DataFrame & Parquet

11. [Plan d'exécution : `df.explain(mode="formatted")`](#11-plan-dexécution--dfexplainmodeformatted)
12. [Pourquoi Parquet plutôt que CSV ou JSON ?](#12-pourquoi-parquet-plutôt-que-csv-ou-json)

<a id="session-3--spark-sql-bases"></a>

### Session 3 — Spark SQL (bases)

13. [Delta Lake et le package `delta-spark`](#13-delta-lake-et-le-package-delta-spark)
14. [Que sont les JARs Delta ?](#14-que-sont-les-jars-delta)
15. [Résumé d'une table : `df.count()` et `len(df.columns)`](#15-résumé-dune-table--dfcount-et-lendfcolumns)
16. [Spark SQL : distribution des statuts par arrondissement](#16-spark-sql--distribution-des-statuts-par-arrondissement)
17. [`SHOW VIEWS` — vérifier qu'une vue temporaire est enregistrée](#17-show-views--vérifier-quune-vue-temporaire-est-enregistrée)
18. [`nullable = true` dans un schéma Spark](#18-nullable--true-dans-un-schéma-spark)
19. [Alias SQL : que signifie `d.station_id` ?](#19-alias-sql--que-signifie-dstation_id-)
20. [`LEFT ANTI JOIN` — exclure les lignes qui matchent](#20-left-anti-join--exclure-les-lignes-qui-matchent)
21. [`show(truncate=False)` — afficher le texte complet](#21-showtruncatefalse--afficher-le-texte-complet)
22. [`DATE_TRUNC` — aligner Velib et météo à l'heure](#22-date_trunc--aligner-velib-et-météo-à-lheure)
23. [`nb_snapshots` — nombre d'observations, pas de bornes vides](#23-nb_snapshots--nombre-dobservations-pas-de-bornes-vides)
24. [Alias `d` et `m` — jointure Velib × météo](#24-alias-d-et-m--jointure-velib--météo)
25. [Inspecter le format de `horodatage` avant `TO_TIMESTAMP`](#25-inspecter-le-format-de-horodatage-avant-to_timestamp)

<a id="session-3--fenêtres-analytiques-spark-sql"></a>

### Session 3 — Fenêtres analytiques (Spark SQL)

26. [Spark SQL `OVER` / `WINDOW w` — LAG, LEAD, moyenne mobile](#26-spark-sql-over--window-w--lag-lead-moyenne-mobile)
27. [`ROW_NUMBER()` — classement par heure](#27-row_number--classement-par-heure)
28. [Moyenne cumulée et delta entre snapshots (`ROWS UNBOUNDED PRECEDING`)](#28-moyenne-cumulée-et-delta-entre-snapshots-rows-unbounded-preceding)
29. [`Window.currentRow` (API PySpark) — équivalent de `ROWS BETWEEN`](#29-windowcurrentrow-api-pyspark--équivalent-de-rows-between)

<a id="session-3--delta-lake-écriture-merge-time-travel"></a>

### Session 3 — Delta Lake (écriture, MERGE, time travel)

30. [Simulation batch MERGE — décaler un `horodatage` string](#30-simulation-batch-merge--décaler-un-horodatage-string)
31. [`F.col("mois") == 1` — filtrer sur janvier](#31-fcolmois--1--filtrer-sur-janvier)
32. [`MERGE INTO` en Spark SQL — chemin absolu Delta](#32-merge-into-en-spark-sql--chemin-absolu-delta)
33. [`DESCRIBE HISTORY` et time travel (`versionAsOf`)](#33-describe-history-et-time-travel-versionasof)
39. [Transactions ACID — pourquoi Delta Lake plutôt que Parquet seul ?](#39-transactions-acid--pourquoi-delta-lake-plutôt-que-parquet-seul)

<a id="session-4--structured-streaming"></a>

### Session 4 — Structured Streaming

34. [Simulateur de flux + cellule de vérification Session 4](#34-simulateur-de-flux--cellule-de-vérification-session-4)
35. [Sink console PySpark vs simulation Python pure (§2.4)](#35-sink-console-pyspark-vs-simulation-python-pure-24)
36. [Driver vs workers — rôles dans Spark](#36-driver-vs-workers--rôles-dans-spark)
37. [Delta Spark — à quoi ça sert en Session 4 ?](#37-delta-spark--à-quoi-ça-sert-en-session-4)
38. [Sink Delta des fenêtres glissantes (`writeStream`)](#38-sink-delta-des-fenêtres-glissantes-writestream)

<a id="annexes--notes-qcm-sessions-14"></a>

### Annexes — Notes QCM (Sessions 1–4)

| QCM | Question (résumé) | Note |
|---|---|---|
| Q8 | `textFile()` → type de RDD | [§40](#40-notes-qcm--textfile-et-rddstr) |
| Q11 | predicate pushdown Parquet | [§41](#41-notes-qcm--predicate-pushdown-et-parquet) |
| Q16 | `DATE_TRUNC` jointure Vélib' × météo | [§42](#42-notes-qcm--date_trunc-et-jointure-velib-meteo) |
| Q17 | `LAG(...) OVER` | [§43](#43-notes-qcm--lag-over-et-fenetre-analytique) |
| Q18 | `ROW_NUMBER() OVER` | [§44](#44-notes-qcm--row_number-over-et-classement) |
| Q19 | time travel `versionAsOf` | [§45](#45-notes-qcm--versionasof-et-time-travel-delta) |
| Q20 | `DESCRIBE HISTORY` | [§46](#46-notes-qcm--describe-history-et-versions-delta) |
| Q21 | `MERGE INTO` upserts | [§47](#47-notes-qcm--merge-into-et-upserts-delta) |

40. [Notes QCM — `textFile` et `RDD[str]`](#40-notes-qcm--textfile-et-rddstr)
41. [Notes QCM — predicate pushdown et Parquet](#41-notes-qcm--predicate-pushdown-et-parquet)
42. [Notes QCM — `DATE_TRUNC` et jointure Vélib' × météo](#42-notes-qcm--date_trunc-et-jointure-velib-meteo)
43. [Notes QCM — `LAG(...) OVER` et fenêtre analytique](#43-notes-qcm--lag-over-et-fenetre-analytique)
44. [Notes QCM — `ROW_NUMBER() OVER` et classement](#44-notes-qcm--row_number-over-et-classement)
45. [Notes QCM — `versionAsOf` et time travel Delta](#45-notes-qcm--versionasof-et-time-travel-delta)
46. [Notes QCM — `DESCRIBE HISTORY` et versions Delta](#46-notes-qcm--describe-history-et-versions-delta)
47. [Notes QCM — `MERGE INTO` et upserts Delta](#47-notes-qcm--merge-into-et-upserts-delta)

<a id="annexes--python-rappels"></a>

### Annexes — Python (rappels)

48. [Générateurs Python — `yield` vs `return`](#48-générateurs-python--yield-vs-return)

> Les questions Q1–Q7, Q9–Q10, Q12–Q15 et Q22–Q34 sont couvertes par les sections thématiques ci-dessus (sans note QCM dédiée pour l'instant).

## Parcours du pipeline (liens entre sections)

<a id="parcours-du-pipeline-liens-entre-sections"></a>

### Session 1 — RDD

```
[1] textFile(historique_stations.csv)     →  RDD[str] brut
         ↓
[3] filter(ligne != entete)               →  data_rdd
         ↓
[5][6] map(parse_ligne)                   →  dict typé (ETL)
         ↓
[7] filter(x is not None)                →  clean_rdd
         ↓
[8] filter / map (taux d'occupation)      →  step3
         ↓
reduceByKey / sortBy / take               →  top 10 [9]
```

| Étape notebook | Section rapport |
|---|---|
| `sc.textFile()` | [§1 Chargement](#1-chargement-dun-csv-avec-textfile) · [§40 QCM `textFile`](#40-notes-qcm--textfile-et-rddstr) |
| `getNumPartitions()` / `repartition()` | [§2 Partitions](#2-partitions-rdd-vs-sparksqlshufflepartitions) |
| `filter` en-tête | [§3 Filtrage en-tête](#3-filtrage-dun-rdd-avec-filter-en-tête) |
| `print(rdd)` lazy | [§4 PythonRDD](#4-affichage-dun-rdd-pythonrdd26) |
| `parse_ligne()` | [§5 Déballage](#5-déballage-de-liste-horodatage-capacite--champs) · [§6 Booléen](#6-conversion-booléenne-operativelower--true) |
| `filter(None)` | [§7 Malformées](#7-suppression-des-lignes-malformées-x-is-not-none) |
| `filter(taux)` | [§8 Taux d'occupation](#8-filtrage-par-taux-doccupation--010) |
| affichage top 10 | [§9 Format tableau](#9-formatage-dun-en-tête-de-tableau-40--12) |
| Section 0 (config Mac) | [§10 Java arm64 / psutil](#10-mac-apple-silicon--java-arm64-et-warning-psutil) |

### Session 2 — DataFrame & Parquet

| Étape notebook | Section rapport |
|---|---|
| jointure Vélib' × météo + `explain()` | [§11 Plan d'exécution](#11-plan-dexécution--dfexplainmodeformatted) |
| lecture / écriture Parquet partitionné | [§12 Parquet vs CSV/JSON](#12-pourquoi-parquet-plutôt-que-csv-ou-json) · [§41 QCM pushdown](#41-notes-qcm--predicate-pushdown-et-parquet) |

### Session 3 — Spark SQL, fenêtres & Delta

| Étape notebook | Section rapport |
|---|---|
| config Delta, vues SQL | [§13–§18](#13-delta-lake-et-le-package-delta-spark) |
| jointures Velib × météo | [§19–§25](#19-alias-sql--que-signifie-dstation_id-) · [§42 QCM `DATE_TRUNC`](#42-notes-qcm--date_trunc-et-jointure-velib-meteo) |
| `LAG` / `LEAD` / moyenne mobile | [§26 Fenêtres SQL](#26-spark-sql-over--window-w--lag-lead-moyenne-mobile) · [§43 QCM `LAG`](#43-notes-qcm--lag-over-et-fenetre-analytique) · [MEM-02](MEM-02SPARK_Window-Functions.md) |
| `ROW_NUMBER`, cumul, delta | [§27](#27-row_number--classement-par-heure) · [§44 QCM `ROW_NUMBER`](#44-notes-qcm--row_number-over-et-classement) · [§28](#28-moyenne-cumulée-et-delta-entre-snapshots-rows-unbounded-preceding) |
| batch + `MERGE INTO` | [§30–§32](#30-simulation-batch-merge--décaler-un-horodatage-string) · [§39 ACID](#39-transactions-acid--pourquoi-delta-lake-plutôt-que-parquet-seul) · [§47 QCM `MERGE INTO`](#47-notes-qcm--merge-into-et-upserts-delta) |
| time travel | [§33](#33-describe-history-et-time-travel-versionasof) · [§45 QCM `versionAsOf`](#45-notes-qcm--versionasof-et-time-travel-delta) · [§46 QCM `DESCRIBE HISTORY`](#46-notes-qcm--describe-history-et-versions-delta) |

### Session 4 — Structured Streaming

| Étape notebook | Section rapport |
|---|---|
| simulateur + vérification JSON | [§34 Simulateur Session 4](#34-simulateur-de-flux--cellule-de-vérification-session-4) |
| sink console PySpark vs Python | [§35 Console vs Python pur](#35-sink-console-pyspark-vs-simulation-python-pure-24) |
| Section 0 — driver, workers, Mac ARM | [§36 Driver vs workers](#36-driver-vs-workers--rôles-dans-spark) · [§10 Java arm64](#10-mac-apple-silicon--java-arm64-et-warning-psutil) |
| fenêtres glissantes + sink Delta | [§38 Sink Delta fenêtres](#38-sink-delta-des-fenêtres-glissantes-writestream) · [§37 Delta Spark](#37-delta-spark--à-quoi-ça-sert-en-session-4) · [§39 ACID](#39-transactions-acid--pourquoi-delta-lake-plutôt-que-parquet-seul) |
| checkpoint, watermark, `foreachBatch` | [§38](#38-sink-delta-des-fenêtres-glissantes-writestream) · [§36](#36-driver-vs-workers--rôles-dans-spark) |
| fin de session (`spark.stop()`) | [§34](#34-simulateur-de-flux--cellule-de-vérification-session-4) |

---

<a id="1-chargement-dun-csv-avec-textfile"></a>

# 1. Chargement d'un CSV avec Spark (`textFile`)

## Question

Comment Spark récupère le fichier CSV lors de l'appel suivant ?

```python
raw_rdd = sc.textFile(str(HISTORIQUE_STATIONS_CSV))
```

---

## Réponse

`sc.textFile(str(HISTORIQUE_STATIONS_CSV))` ne « lit » pas immédiatement le CSV en mémoire. Spark construit d'abord un **plan de calcul** : un RDD de chaînes de caractères, où **une ligne du fichier = un élément**.

---

## 1. Ce que fait l'appel

- `HISTORIQUE_STATIONS_CSV` est un objet `Path` Python (ex. `historique_stations.csv`).
- `str(...)` le convertit en chemin texte que la JVM Spark comprend.
- `sc.textFile(...)` demande à Spark : « prépare-moi un RDD à partir de ce fichier texte ».

À ce stade : **aucune ligne n'est lue**, **aucun comptage**, **aucun affichage**. C'est une **transformation paresseuse** (*lazy*).

> **Rappel Python :** cette idée de « préparer un calcul sans tout exécuter tout de suite » ressemble aux **générateurs** (`yield`). Voir [§48](#48-générateurs-python--yield-vs-return).

---

## 2. Ce que Spark fait en coulisse

Spark (via Hadoop) traite le chemin comme une **source de fichiers** :

1. **Résolution du chemin** — chemin absolu sur le disque local (`file:///Users/.../historique_stations.csv`).
2. **Découpe en partitions** — le gros fichier est découpé en blocs (souvent ~128 Mo par défaut en local). Chaque bloc devient une **partition** du RDD.
3. **Création du RDD** — type `RDD[str]` : chaque élément = une ligne brute, sans parsing CSV.

Spark ne décode pas les colonnes (`date`, `capacity`, etc.). Il voit juste des lignes de texte.

---

## 3. Quand la lecture a vraiment lieu

La lecture disque ne se déclenche que lors d'une **action**, par exemple :

- `raw_rdd.count()` → parcourt toutes les partitions pour compter
- `raw_rdd.take(5)` → lit le minimum pour ramener 5 lignes au driver
- `raw_rdd.collect()` → lit tout (à éviter sur un gros fichier)

Sans action, Spark ne touche pas au fichier.

---

## 4. Parallélisme (mode `local[*]`)

En `local[*]`, Spark utilise plusieurs cœurs :

- chaque partition peut être lue/traitée par une **tâche** sur un thread ;
- sur ~400 Mo / 5 M lignes, plusieurs partitions travaillent en parallèle lors du `count()`.

C'est pour ça que le premier `count()` peut prendre du temps : c'est la **première vraie lecture** du fichier.

---

## 5. Différence avec Pandas

| | `sc.textFile()` | `pd.read_csv()` |
|---|---|---|
| Moment de lecture | à la première **action** | **immédiat** |
| Format | lignes texte brutes | tableau typé en mémoire |
| Échelle | conçu pour gros volumes / cluster | limité par la RAM |
| Parsing CSV | à faire ensuite (`map`, `filter`…) | fait par Pandas |

---

## 6. Synthèse

`textFile(str(HISTORIQUE_STATIONS_CSV))` signifie : **« enregistre ce CSV comme source distribuée de lignes texte »**, et non « charge le CSV ».

Le flux est le suivant :

```
textFile(chemin)  →  RDD[str] (plan, instantané)
       ↓
count() / take()  →  lecture disque + traitement parallèle
       ↓
résultat remonté au notebook
```

> **QCM (Q8) :** `sc.textFile("fichier.csv")` produit initialement un **`RDD[str]`** — une ligne du fichier = une chaîne brute, pas encore un dictionnaire ni un DataFrame. Voir [§40](#40-notes-qcm--textfile-et-rddstr).

C'est le principe central de Spark : **transformations paresseuses** (construire le plan) vs **actions** (exécuter le plan).

---

<a id="2-partitions-rdd-vs-sparksqlshufflepartitions"></a>

# 2. Partitions RDD vs `spark.sql.shuffle.partitions`

## Question

Pourquoi n'utilise-t-on pas `spark.conf.get("spark.sql.shuffle.partitions", 16)` dans la cellule qui observe les partitions du RDD ?

```python
print(f"Nombre de partitions : {raw_rdd.getNumPartitions()}")

raw_rdd_8p = raw_rdd.repartition(8)
print(f"Apres repartition(8) : {raw_rdd_8p.getNumPartitions()} partitions")
```

---

## Réponse

`spark.conf.get("spark.sql.shuffle.partitions", 16)` n'est **pas le bon levier** pour cette cellule : ce n'est **pas la même chose** que `getNumPartitions()` / `repartition(8)`.

---

## 1. Deux mondes de partitions différents

| | Partitions du **RDD d'entrée** (`textFile`) | `spark.sql.shuffle.partitions` |
|---|---|---|
| **Quand ?** | À la **lecture** du fichier | Après un **shuffle** (join, `groupBy`, `reduceByKey`…) |
| **API** | RDD (`sc.textFile`, `repartition`) | SQL / DataFrame surtout |
| **Contrôlé par** | taille du fichier, découpage Hadoop (~128 Mo/bloc), nombre de fichiers | config Spark SQL |
| **Ce qu'on observe ici** | `raw_rdd.getNumPartitions()` → ex. **12** | n'intervient **pas** à ce stade |

La cellule porte sur le **découpage initial** de `historique_stations.csv` en partitions de lecture. Ce découpage est calculé par Spark/Hadoop à partir du fichier sur disque — pas par `spark.sql.shuffle.partitions`.

---

## 2. Ce que ferait `spark.conf.get(...)` ici ?

Seulement **lire une valeur de config** (dans le notebook elle vaut **8**, pas 16, car `SHUFFLE_PARTS = 8` est défini à la création de la session).

Ça ne :

- ne change **pas** le nombre de partitions de `raw_rdd` ;
- ne remplace **pas** `getNumPartitions()` ;
- ne s'applique **pas** tant qu'il n'y a pas eu de shuffle.

Pour aligner un RDD sur cette config, il faudrait explicitement faire `raw_rdd.repartition(valeur_lue)` — ce que la cellule fait déjà en dur avec `repartition(8)`.

---

## 3. Pourquoi `repartition(8)` en dur ?

1. **Pédagogie RDD** — montrer qu'on contrôle le parallélisme **explicitement** sur l'API bas niveau.
2. **Cohérence locale** — `8` correspond à `SHUFFLE_PARTS = 8` défini en Section 0, mais pour un **autre usage** (shuffles plus tard).
3. **Lecture du plan** — `getNumPartitions()` renvoie le plan du RDD sans relire le fichier ; c'est adapté à l'exercice « combien de partitions avant/après repartition ? ».

---

## 4. Synthèse

- **`getNumPartitions()`** → « combien de morceaux pour **lire** ce CSV ? »
- **`spark.sql.shuffle.partitions`** → « combien de morceaux **après un mélange/redistribution** (shuffle) ? »

Les confondre serait trompeur dans cette cellule. `spark.sql.shuffle.partitions` devient pertinent plus tard, lors de `groupBy`, `join` ou `reduceByKey` — pas au moment du simple `textFile()`.

---

<a id="3-filtrage-dun-rdd-avec-filter-en-tête"></a>

# 3. Filtrage d'un RDD avec `filter()` (en-tête)

> Notebook : `Spark_DIA3_Session_1.ipynb` — Partie 1, retrait de l'en-tête CSV  
> Voir aussi : [§1 `textFile`](#1-chargement-dun-csv-avec-textfile) · [§4 `print(RDD)`](#4-affichage-dun-rdd-pythonrdd26)

## Question

Que fait la ligne suivante ?

```python
data_rdd = raw_rdd.filter(lambda line: line != entete)
```

---

## Réponse

Cette ligne **enlève l'en-tête CSV** du RDD : elle crée un **nouveau RDD** (`data_rdd`) qui conserve uniquement les lignes **différentes** de `entete`.

---

## 1. Décomposition (tableau)

| Morceau | Rôle |
|---|---|
| `raw_rdd` | toutes les lignes du fichier (y compris la 1ʳᵉ : noms de colonnes ou 1ʳᵉ observation) |
| `entete` | valeur de `raw_rdd.first()` — la **première ligne** du fichier |
| `.filter(...)` | transformation : ne garde que les lignes qui passent le test |
| `lambda line: line != entete` | pour chaque ligne : `True` → gardée, `False` → rejetée |
| `data_rdd` | nouveau RDD **sans** cette ligne d'en-tête (plan de calcul seulement) |

`raw_rdd` n'est **pas modifié** (immutabilité des RDD).

---

## 2. Exemple concret

```text
raw_rdd :
  "station_id,name,..."   ← entete  →  exclue
  "101,Station A,..."     ← data    →  gardée
  "205,Station B,..."     ← data    →  gardée

data_rdd :
  "101,Station A,..."
  "205,Station B,..."
```

Après ça, `data_rdd` ne contient que des lignes de **données**, prêtes pour le `map` / parsing.

---

## 3. Points importants

- **Lazy** : `filter` ne lit pas encore le fichier ; le calcul partira à la prochaine **action** (`count`, `take`, etc.).
- On filtre par **égalité de chaîne** avec la 1ʳᵉ ligne, **pas** par numéro de ligne. Si la même chaîne réapparaît ailleurs (rare), elle serait aussi retirée.
- **`print(data_rdd)`** affiche une référence RDD (ex. `MapPartitionsRDD[...] at filter`), **pas** les données — c'est normal.

Pour un résultat concret :

- `data_rdd.count()` → nombre de lignes restantes ;
- `data_rdd.take(5)` → aperçu de quelques lignes.

---

## 4. Ce que ça ne fait pas

- **Ne lit pas** le fichier sur le disque tout de suite ;
- **N'affiche pas** les lignes filtrées ;
- **Ne compte pas** les résultats.

Tant qu'il n'y a pas d'**action**, rien n'est exécuté.

---

## 5. Intention pédagogique

Dans un CSV **avec en-tête**, la première ligne ressemble à :

```text
horodatage,capacite,velos_meca,...
```

Le filtre permet de la retirer pour ne garder que les **lignes de données**.

Avec **`historique_stations.csv`**, il n'y a **pas de ligne d'en-tête** : `entete` est déjà une ligne de données (ex. un snapshot de la station Benjamin Godard). Le filtre ne retire donc que les lignes **strictement identiques** à cette première — en pratique très peu, voire une seule.

---

## 6. Schéma du flux

```
raw_rdd  (toutes les lignes)
    │
    │  filter(line != entete)   ← transformation, instantanée
    ▼
data_rdd  (lignes restantes, plan seulement)
    │
    │  count() / take()         ← action : lecture + filtrage effectifs
    ▼
résultat remonté au notebook
```

---

## 7. Synthèse

`filter(lambda line: line != entete)` dit à Spark : **« à partir de maintenant, ne considère que les lignes qui ne sont pas la première »**. Le filtrage réel n'a lieu qu'au moment d'une action — même principe paresseux que pour `textFile()`.

---

<a id="4-affichage-dun-rdd-pythonrdd26"></a>

# 4. Affichage d'un RDD : `PythonRDD[26]`

## Question

Que signifie l'affichage suivant lorsqu'on fait `print(data_rdd)` après un `filter()` ?

```
PythonRDD[26] at RDD at PythonRDD.scala:53
```

---

## Réponse

Spark n'affiche **pas les données** du RDD, mais une **représentation interne** de l'objet RDD côté JVM (Java/Scala). C'est le comportement normal après une **transformation paresseuse**.

---

## 1. Décomposition de la chaîne

**`PythonRDD`** — Type de RDD utilisé en **PySpark** : le moteur de calcul tourne en JVM, Python communique avec lui via **Py4J**.

**`[26]`** — Identifiant interne de cet objet RDD dans la session Spark (numéro unique pour cette branche du plan de calcul). Ce n'est **ni** le nombre de lignes **ni** le nombre de partitions.

**`at RDD at PythonRDD.scala:53`** — Trace indiquant **où** l'objet a été créé dans le code source Spark (fichier `PythonRDD.scala`, ligne 53). Information utile pour les développeurs Spark, pas pour l'analyse de données.

---

## 2. Pourquoi on voit ça

`filter()` est une **transformation paresseuse** : Spark enregistre uniquement le **plan de calcul** (« filtrer les lignes où `line != entete` »), sans exécuter le filtrage.

`print(data_rdd)` affiche donc : **« voici un RDD Python raccordé au plan Spark n°26 »** — pas son contenu.

---

## 3. Comment voir les vraies données

Il faut déclencher une **action** :

| Action | Effet |
|---|---|
| `data_rdd.take(5)` | renvoie 5 premières lignes |
| `data_rdd.count()` | compte les lignes |
| `data_rdd.collect()` | ramène tout au driver (à éviter sur gros volume) |

C'est seulement à ce moment que Spark lit le fichier, applique le filtre et renvoie un résultat concret.

---

## 4. Synthèse

`PythonRDD[26] at RDD at PythonRDD.scala:53` signifie : **un RDD PySpark existe dans le plan de calcul, mais son contenu n'a pas encore été lu ni affiché**. C'est la preuve visuelle que Spark fonctionne en mode **lazy** — le plan est construit, l'exécution attend une action.

---

<a id="5-déballage-de-liste-horodatage-capacite--champs"></a>

# 5. Déballage de liste : `horodatage, capacite, ... = champs`

## Question

Dans `parse_ligne()`, que signifie cette ligne ?

```python
horodatage, capacite, velos_meca, velos_elec, nom_station, coordonnees, operative = champs
```

---

## Réponse

C'est du **déballage de séquence** (*unpacking*) en Python : on affecte en une seule instruction les **7 éléments** de la liste `champs` à **7 variables distinctes**, dans l'ordre.

---

## 1. Équivalence avec l'indexation

C'est équivalent à écrire :

```python
horodatage  = champs[0]
capacite    = champs[1]
velos_meca  = champs[2]
velos_elec  = champs[3]
nom_station = champs[4]
coordonnees = champs[5]
operative   = champs[6]
```

| Variable | Index | Exemple de valeur |
|---|---|---|
| `horodatage` | `champs[0]` | `2020-11-26T12:59Z` |
| `capacite` | `champs[1]` | `35` |
| `velos_meca` | `champs[2]` | `4` |
| `velos_elec` | `champs[3]` | `5` |
| `nom_station` | `champs[4]` | `Benjamin Godard - Victor Hugo` |
| `coordonnees` | `champs[5]` | `48.86598,2.27572` |
| `operative` | `champs[6]` | `True` |

---

## 2. D'où vient `champs` ?

Juste avant, `csv.reader` découpe une ligne brute en champs :

```python
champs = next(csv.reader([line]))
```

Pour la ligne :

```
2020-11-26T12:59Z,35,4,5,Benjamin Godard - Victor Hugo,"48.86598,2.27572",True
```

`champs` devient une **liste de 7 chaînes** — une par colonne du CSV. Le déballage donne un nom lisible à chaque position au lieu de manipuler des indices numériques.

---

## 3. Pourquoi cette syntaxe ?

- **Lisibilité** — `nom_station` est plus clair que `champs[4]`.
- **Concision** — une ligne au lieu de sept.
- **Sécurité implicite** — Python lève une erreur si le nombre d'éléments ne correspond pas (d'où le test `len(champs) != 7` avant le déballage).

---

## 4. Synthèse

`horodatage, capacite, ... = champs` est une **affectation multiple** : Python distribue chaque valeur de la liste dans la variable correspondante, **de gauche à droite**, exactement comme si on écrivait `champs[0]`, `champs[1]`, … `champs[6]` un par un.

C'est une étape classique de préparation des données (ETL).

---

<a id="6-conversion-booléenne-operativelower--true"></a>

# 6. Conversion booléenne : `operative.lower() == "true"`

## Question

Pourquoi utilise-t-on `.lower()` dans cette ligne de `parse_ligne()` ?

```python
"operative": operative.lower() == "true",
```

---

## Réponse

Cette ligne sert à transformer une valeur **texte** en **booléen Python** (`True` ou `False`).

---

## 1. Le problème : les données CSV sont du texte

Dans un fichier CSV, tout arrive sous forme de chaîne (`str`).

Par exemple :

```python
operative = "TRUE"
operative = "True"
operative = "true"
```

Pour Python, ce sont **3 textes différents** :

```
"TRUE" != "True" != "true"
```

Donc si tu fais :

```python
operative == "true"
```

ça marche uniquement pour `operative = "true"`, mais **pas** pour `operative = "TRUE"` ou `operative = "True"`.

---

## 2. À quoi sert `.lower()` ?

`.lower()` transforme tout le texte en **minuscules** :

| Entrée | `operative.lower()` |
|---|---|
| `"TRUE"` | `"true"` |
| `"True"` | `"true"` |
| `"true"` | `"true"` |

Donc :

```python
operative.lower() == "true"
```

revient à dire : **« peu importe comment c'est écrit, si après conversion en minuscules c'est `'true'`, alors c'est vrai »**.

**Exemple :**

```python
operative = "TRUE"
resultat = operative.lower() == "true"   # → True
```

**Avec `FALSE` :**

```python
operative = "FALSE"
operative.lower() == "true"   # "false" == "true" → False
```

---

## 3. Pourquoi c'est utile dans le pipeline Spark Vélib ?

Le CSV peut contenir des variantes :

```
...,TRUE
...,False
...,true
```

Sans conversion, on obtiendrait un **texte** dans le dictionnaire :

```python
{"operative": "TRUE"}   # str — pas exploitable comme booléen
```

Avec la conversion, on obtient un **vrai booléen** :

```python
{"operative": True}      # bool — utilisable dans des filtres
```

Ensuite Spark pourra facilement filtrer les stations ouvertes :

```python
clean_rdd.filter(lambda row: row["operative"] is True)
```

---

## 4. Version encore plus robuste

```python
"operative": operative.strip().lower() == "true"
```

`.strip()` enlève les espaces cachés autour de la valeur :

```
" TRUE ".strip().lower()  →  "true"
```

Cela évite des erreurs fréquentes avec les CSV mal formatés.

---

## 5. Synthèse

| Sans `.lower()` | Avec `.lower()` |
|---|---|
| sensible à la casse (`TRUE` ≠ `true`) | insensible à la casse |
| reste du texte (`str`) | devient un booléen (`bool`) |
| filtres fragiles | filtres fiables en aval |

`operative.lower() == "true"` est une étape classique de **nettoyage** dans un pipeline ETL : normaliser une chaîne avant de la typer.

---

<a id="7-suppression-des-lignes-malformées-x-is-not-none"></a>

# 7. Suppression des lignes malformées : `filter(lambda x: x is not None)`

## Question

Que fait ce code ?

```python
# Supprimer les lignes malformees (None)
clean_rdd = parsed_rdd.filter(lambda x: x is not None)
```

---

## Réponse

Ce code sert à **supprimer les lignes invalides** d'un RDD Spark — celles pour lesquelles `parse_ligne()` n'a pas pu produire un dictionnaire et a renvoyé `None`.

---

## 1. Le contexte avant

Avant, la fonction `parse_ligne()` tente de parser chaque ligne :

```python
def parse_ligne(line: str) -> dict | None:
    try:
        ...
        return {
            "horodatage": horodatage,
            "nom_station": nom_station,
            "velos_meca": velos_meca,
            ...
        }
    except (ValueError, StopIteration):
        return None
```

Le RDD `parsed_rdd` contient donc un **mélange** de lignes valides et invalides :

```python
parsed_rdd = [
    {"nom_station": "Louvre", "velos_meca": 20, ...},
    None,
    {"nom_station": "République", "velos_meca": 15, ...},
    None,
]
```

Les `None` représentent les lignes qui n'ont pas pu être parsées (mauvais format, champ manquant, conversion impossible…).

---

## 2. `.filter()`

En Spark, `filter()` garde uniquement les éléments qui respectent une condition.

Structure générale :

```python
rdd.filter(condition)
```

**Exemple simple :**

```python
rdd = [1, 2, 3, 4, 5]
rdd.filter(lambda x: x > 3)   # → [4, 5]
```

Spark **teste chaque élément** de chaque partition et ne conserve que ceux pour lesquels la condition renvoie `True`.

---

## 3. La fonction `lambda`

```python
lambda x: x is not None
```

est une petite fonction anonyme, équivalente à :

```python
def verifier(x):
    return x is not None
```

Elle prend un élément `x` et retourne :

- `True` → **garder** l'élément ;
- `False` → **supprimer** l'élément.

---

## 4. Que fait `is not None` ?

Cela vérifie que la valeur **existe** (n'est pas `None`).

```python
x = {"nom_station": "Louvre", "velos_meca": 20}
x is not None   # → True  → ligne conservée

x = None
x is not None   # → False → ligne supprimée
```

On utilise `is not None` plutôt que `!= None` car c'est la convention Python pour tester l'absence de valeur.

---

## 5. Exemple complet

**Avant** — `parsed_rdd` contient :

```
[
  {"nom_station": "Louvre",    "velos_meca": 20},
  None,
  {"nom_station": "Bastille",  "velos_meca": 5},
  None,
]
```

**On applique :**

```python
clean_rdd = parsed_rdd.filter(lambda x: x is not None)
```

**Après** — `clean_rdd` contient :

```
[
  {"nom_station": "Louvre",    "velos_meca": 20},
  {"nom_station": "Bastille",  "velos_meca": 5},
]
```

---

## 6. Pourquoi faire ça avant un DataFrame Spark ?

Un DataFrame Spark attend des **lignes structurées homogènes**.

Si tu fais :

```python
spark.createDataFrame(parsed_rdd)
```

avec des `None` au milieu, tu risques :

- des **erreurs d'inférence de schéma** ;
- des **colonnes impossibles à déterminer** ;
- des **lignes vides** dans le résultat.

Le pipeline devient :

```
CSV brut
   │
   ▼
map(parse_ligne)
   │
   ▼
RDD [ dict, None, dict, None ]
   │
   ▼
filter(lambda x: x is not None)
   │
   ▼
RDD propre [ dict, dict ]
   │
   ▼
DataFrame Spark
```

---

## 7. Synthèse

`clean_rdd = parsed_rdd.filter(lambda x: x is not None)` est une étape classique de **nettoyage des données** (*data cleaning*) dans un pipeline ETL Spark : on isole les enregistrements valides avant toute agrégation ou conversion en DataFrame.

---

<a id="8-filtrage-par-taux-doccupation--010"></a>

# 8. Filtrage par taux d'occupation : `lambda r: r["taux_occupation"] < 0.10`

## Question

Que signifie cette ligne ?

```python
step3 = step2.filter(lambda r: r["taux_occupation"] < 0.10)
```

---

## Réponse

Spark construit un **nouveau RDD** `step3` en ne conservant que les enregistrements dont le **taux d'occupation est strictement inférieur à 10 %** (0,10).

---

## 1. Rôle de `filter()`

`filter()` parcourt chaque élément du RDD et **conserve** ceux pour lesquels la condition renvoie `True`, **rejette** les autres.

C'est une **transformation paresseuse** : rien n'est calculé tant qu'une action (`count()`, `take()`, etc.) n'est pas déclenchée.

---

## 2. Rôle de `lambda`

`lambda` définit une **petite fonction anonyme** en une ligne :

```python
lambda r: r["taux_occupation"] < 0.10
```

C'est équivalent à :

```python
def garder_station_peu_occupee(r):
    return r["taux_occupation"] < 0.10
```

| Partie | Signification |
|---|---|
| `lambda` | « voici une fonction sans nom » |
| `r` | un enregistrement du RDD (un `dict` après parsing et calcul du taux) |
| `r["taux_occupation"]` | le taux calculé à l'étape 2 |
| `< 0.10` | condition : taux **inférieur à 10 %** |

Spark appelle cette fonction **une fois par ligne** de `step2`.

---

## 3. Exemple concret

Si `step2` contient :

```python
{"nom_station": "Louvre",     "taux_occupation": 0.08, ...}   # 8 %
{"nom_station": "République", "taux_occupation": 0.35, ...}   # 35 %
{"nom_station": "Bastille",   "taux_occupation": 0.05, ...}   # 5 %
```

Après le filtre :

| Station | Taux | Conservée ? |
|---|---|---|
| Louvre | 0,08 | oui (< 0,10) |
| République | 0,35 | non |
| Bastille | 0,05 | oui |

`step3` ne contient que Louvre et Bastille — des snapshots où la station est **peu occupée**.

---

## 4. Interprétation métier

Un taux d'occupation de **0,08** signifie que **8 % des bornettes sont occupées** (92 % libres).

Le filtre `< 0.10` ne garde que les snapshots où la station est **quasi vide** (moins de 10 % d'occupation).

---

## 5. Schéma du flux

```
step2  [ dict, dict, dict, ... ]
         │
         │  filter(lambda r: r["taux_occupation"] < 0.10)
         │       pour chaque r : True → garder, False → rejeter
         ▼
step3  [ dict, dict, ... ]   ← uniquement taux < 10 %
```

---

## 6. Note sur le notebook

Dans le notebook corrigé, la condition peut aussi être :

```python
step3 = step2.filter(lambda r: 0 <= r["taux_occupation"] <= 1)
```

pour ne garder que les taux **valides** (entre 0 et 100 %), afin de ne pas réduire excessivement le dataset pour la suite du cours.

La logique du `lambda` reste identique — **seule la condition change**.

---

## 7. Synthèse

`lambda r: r["taux_occupation"] < 0.10` dit à Spark : **« pour chaque snapshot, garde-le seulement si moins de 10 % des bornettes sont occupées »**. C'est une étape de **filtrage métier** dans le pipeline ETL, après le calcul du taux d'occupation.

---

<a id="9-formatage-dun-en-tête-de-tableau-40--12"></a>

# 9. Formatage d'un en-tête de tableau : `{'Station':<40}` et `{'Snapshots':>12}`

## Question

Que signifie cette ligne ?

```python
print(f"{'Station':<40} {'Snapshots':>12}")
```

---

## Réponse

Cette ligne affiche un **en-tête de tableau aligné**, avant les lignes de données du top 10 des stations :

```
Station                                  Snapshots
```

---

## 1. C'est une f-string

`f"..."` permet de formater des valeurs entre `{` et `}`.

Ici ce ne sont pas des variables, mais du **texte fixe** avec des **règles d'alignement**.

---

## 2. `{'Station':<40}` — aligné à gauche

| Partie | Signification |
|---|---|
| `'Station'` | le texte à afficher |
| `:` | début du format |
| `<` | alignement **à gauche** |
| `40` | largeur **fixe de 40 caractères** |

`Station` est collé à gauche, puis des espaces remplissent jusqu'à 40 caractères.

---

## 3. `{'Snapshots':>12}` — aligné à droite

| Partie | Signification |
|---|---|
| `'Snapshots'` | le texte |
| `>` | alignement **à droite** |
| `12` | largeur **fixe de 12 caractères** |

`Snapshots` est poussé à droite dans une zone de 12 caractères.

---

## 4. Pourquoi faire ça ?

Les lignes suivantes utilisent le **même format** :

```python
print(f"{nom_station:<40} {count:>12,}")
```

Exemple de rendu :

```
Benjamin Godard - Victor Hugo                  2,022
André Mazet - Saint-André des Arts             1,987
```

- **Noms à gauche** (40 car.) → colonne lisible, noms longs alignés ;
- **Nombres à droite** (12 car.) → chiffres empilés, faciles à comparer.

Sans alignement, l'affichage devient illisible :

```
Benjamin Godard - Victor Hugo 2022
A 5
```

---

## 5. Les symboles d'alignement

| Symbole | Effet |
|---|---|
| `<` | aligné à **gauche** |
| `>` | aligné à **droite** |
| `^` | **centré** |

---

## 6. Synthèse

`print(f"{'Station':<40} {'Snapshots':>12}")` crée un **en-tête de tableau** : « Station » sur 40 caractères à gauche, « Snapshots » sur 12 caractères à droite — pour que les lignes de données en dessous s'alignent proprement.

---

<a id="10-mac-apple-silicon--java-arm64-et-warning-psutil"></a>

# 10. Mac Apple Silicon — Java arm64 et warning `psutil`

## Question

Pourquoi Spark affiche-t-il en boucle ce message, même après `pip install psutil` ?

```
UserWarning: Please install psutil to have better support with spilling
```

Et pourquoi installer OpenJDK 17 via Homebrew ?

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

---

## Réponse

Ce n'est **pas** un problème d'installation de `psutil` dans le venv. Le module est bien présent côté Jupyter. Le warning vient d'une **incompatibilité d'architecture** entre Java et les workers PySpark sur Mac M1/M2/M3.

---

## 1. Ce qui se passe sous le capot

PySpark lance deux types de processus Python :

| Processus | Rôle |
|---|---|
| **Driver** | le notebook Jupyter — exécute vos cellules |
| **Workers** | processus fils lancés par Spark pour traiter les partitions en parallèle |

Lors d'un **shuffle** (`reduceByKey`, `join`, `sortBy`), Spark doit surveiller la mémoire utilisée par chaque worker pour décider si des données doivent être **déversées sur disque** (*spilling*). Pour cela, il s'appuie sur `psutil`, qui lit la consommation mémoire du processus.

---

## 2. Le conflit arm64 / x86_64

Sur Mac Apple Silicon, macOS installe souvent par défaut un Java **x86_64** (Rosetta), par exemple le plugin Oracle 1.8. Or Jupyter et le venv Python tournent en **arm64** natif.

| Composant | Java x86_64 (défaut macOS) | OpenJDK 17 arm64 |
|---|---|---|
| Driver Python (Jupyter) | arm64 | arm64 |
| JVM Spark | **x86_64** (Rosetta) | **arm64** |
| Workers PySpark | **x86_64** | **arm64** |
| `psutil` (bibliothèque native) | arm64 | arm64 |

Les workers PySpark **héritent de l'architecture de la JVM**. Quand Java tourne en x86_64, les workers tournent aussi en x86_64 — alors que `psutil` installé via pip dans le venv arm64 ne fournit qu'une extension native **arm64**.

Résultat :

- le **driver** importe `psutil` sans erreur ;
- les **workers** échouent silencieusement à charger l'extension ;
- Spark affiche le warning « Please install psutil » à chaque opération de shuffle.

---

## 3. La solution : OpenJDK 17 arm64

Homebrew fournit un OpenJDK compilé nativement pour arm64 :

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

En pointant `JAVA_HOME` vers ce JDK **avant** de lancer Jupyter (ou via la Section 0 du notebook), Java, les workers PySpark et `psutil` tournent tous en arm64. Le warning disparaît et Spark gère correctement le spilling mémoire.

> **Attention :** `java_home -v 17` ne trouve pas OpenJDK Homebrew tant qu'il n'est pas symlinké dans `/Library/Java/JavaVirtualMachines/`.

Pour rendre le réglage permanent, ajouter la ligne `export JAVA_HOME=...` dans `~/.zshrc`.

---

## 4. Vérifier que tout est aligné

```bash
java -version          # doit mentionner OpenJDK 17 (arm64)
echo $JAVA_HOME        # doit pointer vers le JDK Homebrew, pas le plugin Oracle
python -c "import psutil; print('psutil OK')"
```

---

## 5. Contournement dans le notebook (Section 0)

Si vous ne pouvez pas changer de Java immédiatement, la **Section 0** de `Spark_DIA3_Session_1.ipynb` crée automatiquement un script `python-arm64` dans le venv :

```python
# extrait de la Section 0
if platform.system() == "Darwin" and platform.machine() == "arm64":
    os.environ["PYSPARK_PYTHON"] = str(_wrapper)       # force arm64 pour les workers
    os.environ["PYSPARK_DRIVER_PYTHON"] = str(_python)
```

Ce wrapper exécute `arch -arm64 python` pour que les workers tournent en arm64 même si Java reste en x86_64. **Ça fonctionne**, mais installer OpenJDK 17 reste la solution propre — une seule architecture pour toute la chaîne Spark.

---

## 6. Synthèse

Le warning `psutil` sur Mac Apple Silicon ne signifie pas que le paquet manque : il signale que les **workers Spark ne peuvent pas charger l'extension native arm64 de psutil** parce qu'ils tournent en x86_64 via Rosetta. Installer **OpenJDK 17 arm64** et définir `JAVA_HOME` aligne Java et Python sur la même architecture — c'est la correction définitive.

---

<a id="11-plan-dexécution--dfexplainmodeformatted"></a>

# 11. Plan d'exécution : `df_joint.explain(mode="formatted")`

> Notebook : `Spark_DIA3_Session_2.ipynb` — §2.6 Jointure Vélib' × Météo

## Question

Que fait cette ligne ?

```python
df_joint.explain(mode="formatted")
```

Et comment lire le résultat affiché ?

---

## Réponse

`explain()` affiche le **plan d'exécution physique** que Spark utilisera pour produire le DataFrame — ici `df_joint`, résultat de la jointure Vélib' × météo. C'est une opération **lazy** : aucun recalcul n'est déclenché, Spark décrit seulement *comment* il compte exécuter la requête.

---

## 1. `explain()` vs `show()`

| Méthode | Rôle | Déclenche le calcul ? |
|---|---|---|
| `show()` | affiche des **lignes de données** | oui (action) |
| `explain()` | affiche la **stratégie d'exécution** | non (diagnostic) |

L'équivalent dans la Spark UI serait de consulter l'onglet **SQL** ou le **DAG** d'un job — mais directement dans le notebook.

---

## 2. Le paramètre `mode="formatted"`

Spark propose plusieurs modes d'affichage :

| Mode | Rendu |
|---|---|
| `"simple"` (défaut) | texte compact, une ligne par opérateur |
| `"formatted"` | arbre indenté, hiérarchie visuelle claire |
| `"codegen"` | détail du code Java généré (debug avancé) |
| `"cost"` | estimation de coût (Spark 3.x+) |

En cours, **`mode="formatted"`** est le plus lisible : on voit quelle opération est **enfant** de quelle autre.

---

## 3. Comment lire l'arbre

Exemple simplifié de ce que vous pouvez voir :

```
== Physical Plan ==
* Project (3)
+- * BroadcastHashJoin Inner (2)
   :- FileScan parquet ...          ← grosse table Vélib'
   +- BroadcastExchange (1)         ← diffusion de la petite table
      +- FileScan csv ...            ← table météo
```

**Sens de lecture :** de bas en haut (feuilles → racine).

| Opérateur | Signification |
|---|---|
| `Scan parquet` / `Scan csv` | lecture des fichiers sources |
| `BroadcastExchange` | Spark envoie une **copie complète** de la petite table à chaque executor |
| `BroadcastHashJoin` | jointure **sans shuffle** de la grosse table |
| `Project` | sélection / projection des colonnes finales |

---

## 4. Ce qu'on cherche dans la §2.6

Dans le notebook, la jointure est écrite ainsi :

```python
df_joint = (
    df_velib_join
    .join(broadcast(df_meteo_join), on="heure_tronquee", how="left")
    .join(broadcast(df_stations), on="nom_station", how="left")
)
```

On s'attend à voir dans le plan :

```
BroadcastHashJoin
BroadcastExchange
```

**Bon signe** — le `broadcast()` a été pris en compte par l'optimiseur Catalyst : la table météo (~17 000 lignes) est répliquée localement sur chaque nœud, évitant un shuffle de la table Vélib' (~690 000 lignes).

**Signe d'alerte** (sur une jointure grosse × grosse) :

```
SortMergeJoin
Exchange (shuffle)
```

Un **shuffle** = redistribution des données entre partitions → coûteux en réseau et en mémoire.

---

## 5. Pourquoi c'est important

Spark ne exécute pas les transformations ligne par ligne comme Pandas. Il construit d'abord un **DAG** (graphe acyclique), puis l'optimiseur **Catalyst** choisit la stratégie la moins coûteuse :

1. **Logical plan** — ce qu'on a écrit (`join`, `filter`, `groupBy`…)
2. **Optimized logical plan** — simplifications (predicate pushdown, etc.)
3. **Physical plan** — ce que `explain()` affiche (`BroadcastHashJoin`, `Scan`, etc.)

Savoir lire `explain()` permet de vérifier qu'une optimisation explicite (`broadcast()`) est bien appliquée — compétence essentielle pour le debug performance Spark.

---

## 6. Synthèse

`df_joint.explain(mode="formatted")` = « Montre-moi l'arbre des opérations physiques que Spark utilisera pour calculer `df_joint`. » Dans la jointure Vélib' × météo, l'objectif est de **confirmer visuellement** la présence de `BroadcastHashJoin` plutôt qu'un shuffle coûteux.

---

<a id="12-pourquoi-parquet-plutôt-que-csv-ou-json"></a>

# 12. Pourquoi Parquet plutôt que CSV ou JSON ?

> Notebook : `Spark_DIA3_Session_2.ipynb` — §2.2 Lecture Parquet, §2.8 Écriture partitionnée

## Question

Pourquoi convertir les données Vélib' en **Parquet** plutôt que de rester en CSV ou JSON pour les analyses Spark ?

---

## Réponse

**Parquet** est beaucoup plus adapté au Big Data que CSV ou JSON : il est conçu pour être **rapide**, **compact** et **optimisé pour les traitements distribués** (Spark, Hadoop, Databricks, etc.).

Dans ce projet, le CSV sert à l'**import initial** (`historique_stations.csv`, ~376 Mo) ; le Parquet sert au **stockage analytique** (`data/velib/parquet/`, `disponibilite_consolidee.parquet`).

---

## Tableau comparatif

| Critère | CSV | JSON | Parquet |
|---|---|---|---|
| Taille du fichier | Grande | Très grande | **Petite** |
| Compression | Non | Non | **Oui** |
| Schéma (types) | Non | Non | **Oui** |
| Lecture par colonnes | Non | Non | **Oui** |
| Très rapide avec Spark | Non | Moyen | **Oui** |

---

## 1. Parquet est un format colonnaire

C'est la plus grande différence.

### CSV — stockage ligne par ligne

```
Alice,Lyon,1200
Bob,Paris,1500
Claire,Lille,1800
```

En mémoire, une ligne = un enregistrement complet :

```
Nom    | Ville  | Salaire
---------------------------
Alice  | Lyon   | 1200
Bob    | Paris  | 1500
Claire | Lille  | 1800
```

Si vous voulez uniquement les salaires, Spark est obligé de lire **aussi** les noms et les villes.

### Parquet — stockage colonne par colonne

```
Nom          Ville         Salaire
-----        -----         -------
Alice        Lyon          1200
Bob          Paris         1500
Claire       Lille         1800
```

Chaque colonne est stockée **séparément** sur disque.

Si vous faites :

```python
df.select("Salaire")
```

Spark lit **uniquement** la colonne `Salaire`.

**Résultat :**

- moins de lecture disque ;
- moins de mémoire utilisée ;
- beaucoup plus rapide.

---

## 2. Compression beaucoup plus efficace

Les valeurs similaires sont regroupées dans une même colonne.

Exemple — une colonne `Ville` :

```
Paris
Paris
Paris
Paris
Paris
Paris
```

Dans un CSV, le mot `Paris` est écrit **6 fois**.

Dans Parquet, il peut être encodé de manière beaucoup plus compacte (dictionnaire, run-length encoding).

Ordre de grandeur typique sur un même jeu de données :

| Format | Taille indicative |
|---|---|
| CSV | 10 Go |
| JSON | 15 Go |
| Parquet | 2–3 Go |

Dans le notebook §2.8, la cellule de comparaison `taille_dossier_mb()` illustre ce rapport sur vos propres fichiers.

---

## 3. Les types sont conservés

Dans un CSV :

```
1200
```

Spark ne sait pas si c'est un **entier**, un **texte** ou une **date** — il doit inférer ou parser manuellement (comme `parse_ligne()` en Session 1).

Dans Parquet, le type est **embarqué dans le fichier** :

| Colonne | Type Parquet |
|---|---|
| `Salaire` | `Integer` |
| `horodatage` | `Timestamp` |
| `taux_occupation` | `Double` |

Pas d'ambiguïté à la lecture : `spark.read.parquet()` restitue directement le bon schéma.

---

## 4. Spark lit beaucoup plus vite

Imaginez une table de **100 colonnes**. Vous voulez uniquement :

```python
df.select("nom_station")
```

| Format | Ce que Spark lit |
|---|---|
| **CSV** | les 100 colonnes, puis ne garde qu'une |
| **Parquet** | **uniquement** la colonne `nom_station` |

Sur des **milliards de lignes**, la différence de temps est énorme.

---

## 5. Optimisé pour Spark : column pruning

Spark applique le **column pruning** (élagage de colonnes) automatiquement sur Parquet.

Exemple :

```python
df.select("nom_station", "taux_occupation")
```

Avec Parquet, le plan physique ne lit que les colonnes demandées :

```
Lecture disque
    │
    ├── nom_station      ✔
    ├── taux_occupation  ✔
    ├── coordonnees      ✘
    ├── velos_meca       ✘
    ├── velos_elec       ✘
    └── …                ✘
```

On ne lit que ce qui est nécessaire — invisible avec un CSV lu via `textFile()`.

---

## 6. Les filtres sont plus rapides (predicate pushdown)

Si vous écrivez :

```python
df.filter(df.annee == 2020)
```

Parquet stocke des **statistiques par bloc** (min, max, nombre de valeurs nulles…). Si un bloc ne contient que des années 2021, Spark peut **sauter ce bloc** sans le lire.

Avec un CSV, il faut parcourir **toutes les lignes** pour vérifier le filtre.

C'est le **predicate pushdown** : le filtre est poussé le plus bas possible dans le plan d'exécution, au plus près du disque.

> **QCM (Q11) :** le predicate pushdown permet de **sauter des blocs de données** selon les filtres, avant de tout charger — pas seulement d'ignorer des colonnes. Voir [§41](#41-notes-qcm--predicate-pushdown-et-parquet).

---

## 7. Exemple concret

Données :

- **1 milliard** de lignes ;
- **50** colonnes ;
- requête : `df.select("prix")`.

### CSV

```
Lire les 50 colonnes
        ↓
Garder uniquement "prix"
```

→ Temps : **long** (I/O inutile sur 49 colonnes).

### Parquet

```
Lire directement la colonne "prix"
```

→ Temps : **beaucoup plus court**.

---

## 8. Lien avec le projet ClimaCity

| Étape | Format | Rôle |
|---|---|---|
| Import brut | CSV (`historique_stations.csv`) | échange, simplicité, compatibilité |
| Stockage intermédiaire | Parquet (`data/velib/parquet/`) | lecture rapide, partitions `annee`/`mois` |
| Table consolidée | Parquet (`disponibilite_consolidee.parquet`) | jointure Vélib' × météo, réutilisation Jour 2+ |

Le pattern standard en production Spark :

1. **Importer** en CSV (ou JSON) une seule fois ;
2. **Convertir** en Parquet partitionné ;
3. **Analyser** exclusivement sur Parquet.

---

## 9. Synthèse

Parquet est privilégié en Big Data parce qu'il :

1. **stocke par colonnes** — ne lit que les colonnes utiles ;
2. **compresse efficacement** — moins d'espace disque et moins de transfert réseau ;
3. **conserve les types** — pas d'inférence ni de parsing manuel ;
4. **permet le column pruning et le predicate pushdown** — Spark ignore colonnes et blocs inutiles ;
5. **est donc beaucoup plus performant** sur de très grands volumes que les formats texte (CSV, JSON).

C'est pour ces raisons que, dans la plupart des projets Spark en production, on utilise le CSV uniquement pour **importer** les données, puis on les convertit rapidement en **Parquet** avant de lancer les analyses.

---

<a id="13-delta-lake-et-le-package-delta-spark"></a>

# 13. Delta Lake et le package `delta-spark`

> Notebook : `Spark_DIA3_Session_3.ipynb` — Spark SQL, Delta Lake, `MERGE INTO`, time travel

## Question

Pourquoi installer `delta-spark` alors que Spark sait déjà lire et écrire du **Parquet** ?

---

## Réponse

Le package **`delta-spark`** permet à Spark de lire, écrire et gérer des tables au format **Delta Lake**.

Delta Lake est une **couche au-dessus de Parquet** qui ajoute des fonctionnalités essentielles pour les pipelines Big Data : transactions, mises à jour, historique et time travel.

> **Approfondissement :** [§39 — Transactions ACID](#39-transactions-acid--pourquoi-delta-lake-plutôt-que-parquet-seul)

---

## Sans `delta-spark` — Parquet seul

Écriture :

```python
df.write.parquet("data/")
```

Relecture :

```python
df = spark.read.parquet("data/")
```

**Limites du Parquet brut :**

- pas d'historique des modifications ;
- pas de transactions ;
- pas d'`UPDATE` ou `DELETE` simples ;
- risque de fichiers **incohérents** si un job échoue en pleine écriture.

---

## Avec `delta-spark` — Delta Lake

Écriture :

```python
df.write.format("delta").save("data/")
```

Relecture :

```python
df = spark.read.format("delta").load("data/")
```

Structure du dossier :

```
data/
│
├── part-0000.parquet
├── part-0001.parquet
├── part-0002.parquet
└── _delta_log/
```

Le dossier **`_delta_log`** est la grande différence : il contient un **journal** de toutes les modifications de la table (versions, métadonnées, opérations).

---

## Les principaux avantages

### 1. Transactions ACID

Si plusieurs jobs écrivent dans la même table en parallèle :

```
Utilisateur A ──┐
                  ├── Table Delta (cohérente)
Utilisateur B ──┘
```

Delta garantit que les données restent **cohérentes**. Avec un simple dossier Parquet, deux écritures simultanées peuvent produire des fichiers **incomplets** ou corrompus.

### 2. UPDATE

En Parquet : il faut souvent **réécrire tout le dataset**.

Avec Delta :

```python
from delta.tables import DeltaTable

table = DeltaTable.forPath(spark, "data")

table.update(
    condition="ville = 'Paris'",
    set={"ville": "'PARIS'"},
)
```

### 3. DELETE

```python
table.delete("age < 18")
```

En Parquet, il faudrait relire tout le fichier, filtrer, puis tout réécrire.

### 4. MERGE (UPSERT)

Très utile pour les **mises à jour incrémentales** (ETL quotidien) :

```python
table.alias("t").merge(
    nouveaux.alias("n"),
    "t.id = n.id",
)
```

Permet de :

- **mettre à jour** les lignes existantes ;
- **insérer** les nouvelles.

C'est l'opération centrale des pipelines ETL en production — utilisée dans la Session 3 (`MERGE INTO`).

### 5. Historique des versions

Chaque modification crée une **nouvelle version** de la table :

```python
table.history().show()
```

### 6. Time Travel

Relire la table **telle qu'elle était** à une version antérieure :

```python
spark.read.format("delta") \
    .option("versionAsOf", 3) \
    .load("data/")
```

Très pratique si une mise à jour a introduit une erreur — on peut revenir en arrière sans restaurer un backup complet.

### 7. Évolution du schéma

Ajout d'une colonne sans réécrire toute la table :

```python
df.write \
  .format("delta") \
  .option("mergeSchema", "true") \
  .save("data/")
```

Delta met à jour automatiquement le schéma dans `_delta_log`.

---

## Pourquoi c'est très utilisé en entreprise

Les données arrivent souvent **chaque jour** :

| Jour | Volume |
|---|---|
| Jour 1 | 100 000 clients |
| Jour 2 | 2 000 nouveaux + 500 modifiés |

| Approche | Stratégie |
|---|---|
| **Parquet** | réécrire souvent une grande partie des données |
| **Delta** | mettre à jour uniquement les lignes concernées via `MERGE` |

---

## Pourquoi installer `delta-spark` ?

Spark gère nativement CSV, JSON, Parquet et ORC — mais **pas** les fonctionnalités avancées de Delta Lake sans la bibliothèque dédiée.

Installation dans le projet :

```bash
pip install delta-spark
```

Configuration dans `Spark_DIA3_Session_3.ipynb` :

```python
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder.appName(APP_NAME).master("local[*]")
).getOrCreate()
```

`configure_spark_with_delta_pip()` ajoute automatiquement les extensions Spark et les JARs Maven nécessaires (`DeltaSparkSessionExtension`, `DeltaCatalog`).

> **Note pratique — interpréteur Python de l'éditeur**
>
> Pour éviter que `delta` soit souligné dans Cursor/VS Code alors que le notebook fonctionne, le projet contient aussi un fichier `/.vscode/settings.json` :
>
> ```json
> {
>   "python.defaultInterpreterPath": "/Users/romain/Desktop/SparkVelib/.venv-spark/bin/python",
>   "python.analysis.extraPaths": [
>     "/Users/romain/Desktop/SparkVelib/.venv-spark/lib/python3.12/site-packages"
>   ]
> }
> ```
>
> Cette configuration ne change pas le code Spark lui-même : elle indique simplement à l'éditeur quel **interpréteur Python** et quel dossier `site-packages` utiliser pour l'analyse statique. Sans cela, l'import `from delta import configure_spark_with_delta_pip` peut être exécuté correctement par Jupyter tout en restant souligné visuellement dans l'éditeur.

---

## Tableau comparatif

| Fonction | Parquet | Delta Lake (`delta-spark`) |
|---|---|---|
| Lecture / écriture | Oui | Oui |
| Compression | Oui | Oui |
| Transactions ACID | Non | Oui |
| UPDATE | Non | Oui |
| DELETE | Non | Oui |
| MERGE (upsert) | Non | Oui |
| Historique des versions | Non | Oui |
| Time Travel | Non | Oui |
| Évolution du schéma | Limitée | Oui |

---

## Lien avec le projet ClimaCity

| Étape | Format | Notebook |
|---|---|---|
| Import brut | CSV | Session 1 |
| Stockage analytique | Parquet | Session 2 §2.8 |
| Persistance transactionnelle | **Delta** | Session 3 (`data/output/delta/`) |

---

## Synthèse

Workflow courant en production :

1. **Importer** les données (CSV, JSON, API…)
2. **Transformer** avec Spark (DataFrame / SQL)
3. **Stocker en Delta Lake** via `delta-spark` — performances proches du Parquet, avec en plus les garanties d'une base de données (transactions, mises à jour, historique, time travel)

Dans ce projet, le Parquet (`disponibilite_consolidee.parquet`) sert de **table analytique** produite au Jour 1 ; Delta Lake prend le relais au **Jour 2** pour les écritures incrémentales, le `MERGE INTO` et le streaming.

---

<a id="14-que-sont-les-jars-delta"></a>

# 14. Que sont les JARs Delta ?

> Notebook : `Spark_DIA3_Session_3.ipynb` — démarrage de la SparkSession Delta

## Question

Quand Spark affiche :

```text
io.delta#delta-spark_2.12;3.2.1
io.delta#delta-storage;3.2.1
```

ou télécharge des dépendances au démarrage, que sont exactement les **JARs Delta** ?

---

## Réponse

Les **JARs Delta** sont les **bibliothèques Java/Scala** dont Spark a besoin pour comprendre et exécuter **Delta Lake** dans la JVM.

Même si l'on code en Python avec :

```python
from delta import configure_spark_with_delta_pip
```

le moteur Spark exécute l'essentiel de son travail côté **JVM** (Java / Scala), pas directement dans l'interpréteur Python.

---

## 1. `delta-spark` Python vs JARs Delta

Il faut distinguer deux couches :

| Élément | Rôle |
|---|---|
| package Python `delta-spark` | interface Python + configuration Spark |
| **JARs Delta** | moteur Delta Lake exécuté par Spark dans la JVM |

Le package Python ne suffit pas à lui seul : il doit aussi faire charger à Spark les bibliothèques Java correspondantes.

---

## 2. Qu'est-ce qu'un JAR ?

Un **JAR** (*Java ARchive*) est l'équivalent, pour l'écosystème Java, d'un paquet compilé.

On peut comparer :

| Écosystème | Format de bibliothèque |
|---|---|
| Python | `.whl` / paquet `pip` |
| Java / Scala | `.jar` |

Spark charge ces fichiers dans sa **JVM** au démarrage.

---

## 3. Pourquoi Spark en a besoin

Quand on demande :

```python
df.write.format("delta").save("data/")
```

ou :

```python
spark.read.format("delta").load("data/")
```

Spark doit savoir :

- lire `_delta_log` ;
- interpréter les versions de table ;
- appliquer `MERGE`, `UPDATE`, `DELETE` ;
- gérer le time travel ;
- maintenir la cohérence transactionnelle.

Toutes ces fonctionnalités sont implémentées dans les **classes Java/Scala** fournies par les JARs Delta.

---

## 4. Ce que signifie le log affiché

Quand Spark affiche :

```text
io.delta#delta-spark_2.12;3.2.1
io.delta#delta-storage;3.2.1
```

cela signifie qu'il a résolu les bibliothèques nécessaires :

| Composant | Rôle |
|---|---|
| `delta-spark_2.12` | implémentation principale de Delta Lake pour Spark |
| `delta-storage` | couche de stockage / journalisation Delta |
| `antlr4-runtime` | dépendance utilitaire utilisée par Delta |

Le suffixe **`_2.12`** correspond à la version de **Scala** utilisée par cette build Spark.

---

## 5. Pourquoi l'erreur arrivait avant

Le message :

```text
ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension
```

signifie exactement :

- Spark cherche la classe Java `DeltaSparkSessionExtension` ;
- mais le JAR qui contient cette classe n'est **pas chargé** dans la JVM.

Autrement dit, Python connaissait peut-être `delta-spark`, mais Spark lui-même ne trouvait pas encore le moteur Delta côté Java.

---

## 6. Le rôle de `configure_spark_with_delta_pip()`

Dans ce projet, on utilise :

```python
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder.appName(APP_NAME).master("local[*]")
).getOrCreate()
```

Cette fonction :

1. ajoute les bons paramètres Spark ;
2. indique quelles dépendances Maven/JAR doivent être chargées ;
3. permet à Spark de récupérer automatiquement les JARs Delta au premier lancement.

Ensuite, Spark peut créer une session pleinement compatible Delta Lake.

---

## 7. Résumé

Les **JARs Delta** sont les bibliothèques Java/Scala qui fournissent à Spark le **vrai moteur Delta Lake**.

- `delta-spark` côté Python = interface et configuration ;
- **JARs Delta** côté JVM = exécution réelle des commandes Delta ;
- sans ces JARs, Spark ne sait pas utiliser `format("delta")` ni `DeltaSparkSessionExtension` ;
- avec eux, Spark peut gérer lecture/écriture Delta, `MERGE`, `DELETE`, historique et time travel.

---

<a id="15-résumé-dune-table--dfcount-et-lendfcolumns"></a>

# 15. Résumé d'une table : `df.count()` et `len(df.columns)`

> Notebook : `Spark_DIA3_Session_3.ipynb` — chargement de `disponibilite_consolidee.parquet`

## Question

Que fait cette ligne ?

```python
print(f"Table consolidée : {df.count():,} lignes  |  {len(df.columns)} colonnes")
```

---

## Réponse

Cette ligne affiche un **résumé rapide** de la table Vélib' chargée depuis le Parquet consolidé produit en Session 2.

Exemple de sortie :

```text
Table consolidée : 690,858 lignes  |  20 colonnes
```

---

## 1. Décomposition de la ligne

| Partie | Signification |
|---|---|
| `print(...)` | affiche du texte dans le notebook |
| `f"..."` | **f-string** : insère des valeurs Python dans le texte |
| `df` | le DataFrame chargé depuis `disponibilite_consolidee.parquet` |
| `df.count()` | **action Spark** : compte le nombre de lignes |
| `:,` | formatage avec séparateur de milliers (`690858` → `690,858`) |
| `len(df.columns)` | nombre de colonnes du DataFrame |
| `\|` | séparateur visuel entre les deux informations |

---

## 2. `df.count()` est une action Spark

Spark fonctionne en **évaluation paresseuse** : tant qu'on ne déclenche pas d'action, rien n'est réellement calculé.

`count()` est une **action** : elle force Spark à parcourir les données (ou le cache) pour compter les lignes.

Dans la cellule du notebook :

```python
df = spark.read.parquet(str(VELIB_CONSOLIDE))
df.cache()
df.count()   # force la mise en cache

print(f"Table consolidée : {df.count():,} lignes  |  {len(df.columns)} colonnes")
```

- le **premier** `count()` matérialise le DataFrame en cache ;
- le **second** `count()` dans le `print()` relit depuis le cache — souvent beaucoup plus rapide.

---

## 3. `len(df.columns)` est instantané

`df.columns` renvoie la liste des noms de colonnes côté **driver** (machine qui exécute le notebook).

`len(df.columns)` compte simplement cette liste — **aucun parcours des données** sur le cluster n'est nécessaire.

---

## 4. Le format `:,`

Dans une f-string :

```python
f"{690858:,}"   # → "690,858"
```

Le `:,` ajoute un séparateur de milliers pour rendre les grands nombres plus lisibles dans les rapports et les sorties notebook.

---

## 5. Synthèse

`print(f"Table consolidée : {df.count():,} lignes  |  {len(df.columns)} colonnes")` répond en une ligne à la question : **« combien de lignes et combien de colonnes contient ma table consolidée ? »** — avant d'afficher le détail du schéma avec `df.printSchema()`.

---

<a id="16-spark-sql--distribution-des-statuts-par-arrondissement"></a>

# 16. Spark SQL : distribution des statuts par arrondissement

> Notebook : `Spark_DIA3_Session_3.ipynb` — premières requêtes SQL sur la vue `disponibilite`

## Question

Que fait cette requête SQL ?

```sql
SELECT
    code_arr,
    statut,
    COUNT(*) AS nb_snapshots,
    ROUND(AVG(taux_occupation), 4) AS taux_moyen,
    ROUND(STDDEV(taux_occupation), 4) AS ecart_type
FROM disponibilite
WHERE code_arr IS NOT NULL
GROUP BY code_arr, statut
ORDER BY code_arr, statut
```

Et pourquoi y a-t-il un `4` dans `ROUND(AVG(taux_occupation), 4)` ?

---

## Réponse

Cette requête produit, pour chaque **arrondissement** (`code_arr`) et chaque **statut** de station (`statut`), un petit résumé statistique du niveau d'occupation.

Elle répond à la question : **« dans chaque arrondissement, combien de snapshots observe-t-on pour chaque statut, et quel est le taux d'occupation moyen associé ? »**

---

## 1. Décomposition de la requête

| Élément SQL | Rôle |
|---|---|
| `FROM disponibilite` | lit la vue temporaire créée à partir du DataFrame Spark |
| `WHERE code_arr IS NOT NULL` | retire les lignes sans arrondissement renseigné |
| `GROUP BY code_arr, statut` | regroupe les données par arrondissement et par statut |
| `COUNT(*)` | compte le nombre de lignes dans chaque groupe |
| `AVG(taux_occupation)` | calcule le taux d'occupation moyen du groupe |
| `STDDEV(taux_occupation)` | mesure la dispersion autour de la moyenne |
| `ORDER BY code_arr, statut` | trie le résultat pour un affichage lisible |

---

## 2. Signification métier des colonnes calculées

Pour un groupe donné, par exemple :

- `code_arr = 15`
- `statut = 'plein'`

Spark calcule :

- **`nb_snapshots`** : combien de snapshots de stations du 15e arrondissement ont le statut `plein` ;
- **`taux_moyen`** : la moyenne des valeurs de `taux_occupation` dans ce groupe ;
- **`ecart_type`** : la variabilité du taux d'occupation dans ce groupe.

Si `taux_moyen` est proche de `1.0`, cela signifie que les stations de ce groupe sont en moyenne très occupées.

Si `ecart_type` est faible, les valeurs sont proches les unes des autres ; s'il est élevé, l'occupation varie davantage selon les snapshots.

---

## 3. Pourquoi `ROUND(..., 4)` ?

En SQL Spark, `ROUND(valeur, n)` signifie :

**« arrondir `valeur` à `n` chiffres après la virgule »**.

Donc :

```sql
ROUND(AVG(taux_occupation), 4)
```

veut dire :

1. calculer la moyenne avec `AVG(taux_occupation)` ;
2. arrondir le résultat final à **4 décimales**.

Exemples :

| Expression | Résultat |
|---|---|
| `AVG(taux_occupation)` | `0.673891234...` |
| `ROUND(AVG(taux_occupation), 4)` | `0.6739` |
| `ROUND(AVG(taux_occupation), 2)` | `0.67` |

Le **`4` n'a aucun rôle statistique** : il ne change pas la logique de `AVG`, il sert seulement à rendre l'affichage plus lisible dans le notebook.

La même logique s'applique à :

```sql
ROUND(STDDEV(taux_occupation), 4)
```

---

## 4. Pourquoi utiliser `STDDEV` ici ?

La moyenne seule ne suffit pas toujours.

Deux groupes peuvent avoir :

- le même `taux_moyen`
- mais une variabilité très différente

Exemple simplifié :

| Groupe | Valeurs | Moyenne | Écart-type |
|---|---|---|---|
| A | `0.50, 0.51, 0.49` | `0.50` | faible |
| B | `0.10, 0.90, 0.50` | `0.50` | élevé |

Dans les deux cas, la moyenne vaut `0.50`, mais le groupe B est beaucoup plus instable.

`STDDEV(taux_occupation)` ajoute donc une information de **dispersion** utile pour l'analyse.

---

## 5. Résultat attendu

Le résultat affiché ressemble à une table du type :

```text
+--------+--------+------------+----------+----------+
|code_arr|statut  |nb_snapshots|taux_moyen|ecart_type|
+--------+--------+------------+----------+----------+
|1       |normal  |...         |0.54      |0.18      |
|1       |plein   |...         |0.97      |0.03      |
|1       |vide    |...         |0.04      |0.02      |
...
```

Chaque ligne résume donc un couple :

- **un arrondissement**
- **un statut**

---

## 6. Synthèse

Cette requête Spark SQL regroupe les snapshots par **arrondissement** et **statut**, puis calcule :

1. le **nombre d'observations** ;
2. le **taux d'occupation moyen** ;
3. l'**écart-type** de l'occupation.

Le `4` dans `ROUND(..., 4)` signifie simplement : **afficher 4 décimales après la virgule**.

---

<a id="17-show-views--vérifier-quune-vue-temporaire-est-enregistrée"></a>

# 17. `SHOW VIEWS` — vérifier qu'une vue temporaire est enregistrée

> Notebook : `Spark_DIA3_Session_3.ipynb` — cellule de vérification avant les requêtes SQL

## Question

Pourquoi la commande suivante ne lève-t-elle pas d'erreur, et à quoi fait-elle référence ?

```python
spark.sql("SHOW VIEWS").show()
```

---

## Réponse

`SHOW VIEWS` est une commande **Spark SQL** (inspirée de Hive/SQL standard) qui liste toutes les **vues temporaires** enregistrées dans la session Spark courante.

Elle n'interroge **aucune donnée** : elle consulte le **catalogue interne de Spark** (`SessionCatalog`), qui mémorise les vues déclarées via `createOrReplaceTempView(...)`. C'est pour cela qu'elle s'exécute sans erreur, même si aucun fichier n'a encore été lu.

---

## Ce qu'elle affiche

```text
+---------+-------------+-----------+
|namespace|     viewName|isTemporary|
+---------+-------------+-----------+
|         |disponibilite|       true|
+---------+-------------+-----------+
```

| Colonne | Signification |
|---|---|
| `namespace` | catalogue / base de données (vide = catalogue par défaut) |
| `viewName` | nom de la vue |
| `isTemporary` | `true` = vue temporaire (disparaît à la fin de la session) |

---

## Quand la vue est-elle créée ?

Dans la cellule précédente :

```python
df.createOrReplaceTempView("disponibilite")
```

Cette instruction dit à Spark : **« enregistre ce DataFrame sous le nom `disponibilite` dans le catalogue SQL »**.

`SHOW VIEWS` sert ensuite à **confirmer** que l'enregistrement a bien eu lieu avant d'écrire les premières requêtes SQL dessus.

---

## Synthèse

| | `SHOW VIEWS` | `df.show()` |
|---|---|---|
| Interroge | le catalogue Spark (métadonnées) | les données du DataFrame |
| Déclenche un calcul Spark ? | non | oui (action) |
| Utile pour | vérifier qu'une vue existe | afficher un aperçu des données |

---

<a id="18-nullable--true-dans-un-schéma-spark"></a>

# 18. `nullable = true` dans un schéma Spark

> Notebook : `Spark_DIA3_Session_3.ipynb` — affichage du schéma avec `printSchema()`

## Question

Que signifie l'indication suivante dans un schéma Spark ?

```text
|-- code_arr: integer (nullable = true)
```

---

## Réponse

`nullable = true` signifie que la colonne **a le droit de contenir des valeurs `NULL`**.

Autrement dit :

- `code_arr` est de type `integer`
- mais Spark autorise l'absence de valeur sur certaines lignes

À l'inverse :

```text
|-- id: integer (nullable = false)
```

signifie que Spark attend une valeur renseignée pour chaque ligne.

---

## Exemple concret

Dans ce projet, après certaines jointures, il peut arriver qu'un snapshot ne trouve pas d'arrondissement correspondant.

On peut alors obtenir :

```text
code_arr = NULL
```

C'est précisément pour cela que la colonne peut apparaître comme :

```text
(nullable = true)
```

---

## Lien avec la requête SQL

Dans la requête sur la distribution des statuts par arrondissement, on a utilisé :

```sql
WHERE code_arr IS NOT NULL
```

Cette condition sert à **exclure les lignes où l'arrondissement manque** avant le `GROUP BY`.

---

## Synthèse

| Valeur | Signification |
|---|---|
| `nullable = true` | la colonne peut contenir `NULL` |
| `nullable = false` | la colonne ne doit pas contenir `NULL` |

---

<a id="19-alias-sql--que-signifie-dstation_id-"></a>

# 19. Alias SQL : que signifie `d.station_id` ?

> Notebook : `Spark_DIA3_Session_3.ipynb` — requête SQL avec `disponibilite_ts d`

## Question

Que signifie une écriture comme :

```sql
d.station_id
```

---

## Réponse

`d.station_id` désigne simplement la colonne **`station_id`** de la table ou sous-requête à laquelle on a donné l'alias **`d`**.

Dans la requête, on écrit :

```sql
FROM disponibilite_ts d
```

Cela veut dire exactement :

```sql
FROM disponibilite_ts AS d
```

Autrement dit :

- `disponibilite_ts` = nom complet de la table ou sous-requête ;
- `d` = nom court temporaire utilisé dans le reste de la requête.

Donc :

```sql
d.station_id
```

est équivalent à :

```sql
disponibilite_ts.station_id
```

Le `d` ne change **ni les données**, ni le **résultat** de la requête : il sert uniquement à donner un nom plus court à `disponibilite_ts`.

---

## Pourquoi utiliser un alias ?

Les alias servent à :

- **raccourcir** l'écriture ;
- **améliorer la lisibilité** ;
- **éviter les ambiguïtés** quand plusieurs tables ont des colonnes du même nom.

Par exemple, au lieu d'écrire plusieurs fois :

```sql
disponibilite_ts.station_id
disponibilite_ts.ts
disponibilite_ts.nom_station
```

on peut écrire plus simplement :

```sql
d.station_id
d.ts
d.nom_station
```

La requête devient plus compacte, surtout quand le nom de table est long.

Dans la requête du notebook, on a par exemple :

```sql
FROM disponibilite_ts d
LEFT ANTI JOIN jours_feries jf
    ON TO_DATE(d.ts) = jf.date_ferie
```

Ici :

- `d` = alias de `disponibilite_ts`
- `jf` = alias de `jours_feries`

Donc :

- `d.ts` = colonne `ts` de `disponibilite_ts`
- `jf.date_ferie` = colonne `date_ferie` de `jours_feries`

---

## Sans alias

La même idée, écrite sans alias, serait plus longue :

```sql
SELECT disponibilite_ts.station_id
FROM disponibilite_ts
LEFT ANTI JOIN jours_feries
    ON TO_DATE(disponibilite_ts.ts) = jours_feries.date_ferie
```

Le résultat est le même, mais l'écriture est plus lourde.

On ajoute donc `d` pour avoir un **nom court temporaire** dans la requête, pas pour modifier le calcul.

---

## Synthèse

| Écriture | Signification |
|---|---|
| `d.station_id` | colonne `station_id` de la table aliasée `d` |
| `jf.date_ferie` | colonne `date_ferie` de la table aliasée `jf` |

Les alias SQL sont donc simplement des **noms courts temporaires** pour référencer plus facilement les tables dans une requête. Ils améliorent surtout la lisibilité et n'ont aucun impact sur les données retournées.

---

<a id="20-left-anti-join--exclure-les-lignes-qui-matchent"></a>

# 20. `LEFT ANTI JOIN` — exclure les lignes qui matchent

> Notebook : `Spark_DIA3_Session_3.ipynb` — exclusion des jours fériés dans une requête SQL

## Question

Que signifie :

```sql
LEFT ANTI JOIN jours_feries jf
```

---

## Réponse

`LEFT ANTI JOIN` signifie :

**« garder uniquement les lignes de gauche qui n'ont aucune correspondance dans la table de droite »**.

Dans la requête du notebook :

```sql
FROM disponibilite_ts d
LEFT ANTI JOIN jours_feries jf
    ON TO_DATE(d.ts) = jf.date_ferie
```

cela veut dire :

- on part de `disponibilite_ts` ;
- on compare chaque date de snapshot à la table `jours_feries` ;
- si la date correspond à un jour férié, la ligne est **exclue** ;
- si elle ne correspond à aucun jour férié, la ligne est **conservée**.

Autrement dit :

**on garde les disponibilités sauf celles qui tombent un jour férié**.

---

## Intuition

`LEFT ANTI JOIN` est un **filtre par absence de correspondance**.

Comparaison rapide :

| Jointure | Effet |
|---|---|
| `INNER JOIN` | garde les lignes qui matchent |
| `LEFT JOIN` | garde toutes les lignes de gauche, avec complément à droite si ça match |
| `LEFT ANTI JOIN` | garde seulement les lignes de gauche qui **ne matchent pas** |

---

## Exemple simple

Supposons :

### Table `disponibilite_ts`

| date |
|---|
| `2023-05-01` |
| `2023-05-02` |
| `2023-05-08` |

### Table `jours_feries`

| date_ferie |
|---|
| `2023-05-01` |
| `2023-05-08` |

Avec :

```sql
LEFT ANTI JOIN jours_feries jf
ON TO_DATE(d.ts) = jf.date_ferie
```

le résultat garde uniquement :

| date |
|---|
| `2023-05-02` |

car `2023-05-01` et `2023-05-08` existent dans la table `jours_feries`.

---

## Pourquoi l'utiliser ici ?

Dans ce notebook, l'objectif est d'étudier les stations en rupture **hors jours fériés**.

`LEFT ANTI JOIN` est très adapté pour cela car il exprime directement :

**« retire de `disponibilite_ts` toutes les lignes dont la date apparaît dans `jours_feries` »**

Sans cette jointure, il faudrait écrire une logique plus longue avec `NOT IN` ou une `LEFT JOIN` suivie d'un test `IS NULL`.

---

## Synthèse

| Expression | Signification |
|---|---|
| `LEFT ANTI JOIN` | garder les lignes de gauche sans correspondance à droite |
| dans ce projet | exclure les snapshots qui tombent un jour férié |

---

<a id="21-showtruncatefalse--afficher-le-texte-complet"></a>

# 21. `show(truncate=False)` — afficher le texte complet

> Notebook : `Spark_DIA3_Session_3.ipynb` — affichage des résultats SQL avec `df_q1.show(truncate=False)`

## Question

Que fait le paramètre `truncate=False` dans `df.show()` ?

```python
df_q1.show(truncate=False)
```

---

## Réponse

Dans Spark, `truncate=False` est utilisé avec `show()` pour dire :

**« N'abandonne pas l'affichage des chaînes de caractères, montre-les en entier. »**

Par défaut, Spark **tronque** les colonnes texte longues pour garder un affichage compact dans le notebook.

---

## Comportement par défaut

```python
df.show()
```

Si le DataFrame contient une longue description :

| nom | description |
|---|---|
| Alice | Ceci est une très longue description qui contient beaucoup de texte... |

Spark affiche par défaut :

```text
+-----+--------------------+
|nom  |description         |
+-----+--------------------+
|Alice|Ceci est une trè... |
+-----+--------------------+
```

Il **coupe** le texte.

---

## Avec `truncate=False`

```python
df.show(truncate=False)
```

Résultat :

```text
+-----+--------------------------------------------------------------+
|nom  |description                                                   |
+-----+--------------------------------------------------------------+
|Alice|Ceci est une très longue description qui contient beaucoup de texte...|
+-----+--------------------------------------------------------------+
```

Cette fois, tout le contenu est affiché.

---

## Pourquoi le paramètre s'appelle `truncate` ?

En anglais :

- **truncate** = tronquer, couper

Donc :

| Paramètre | Effet |
|---|---|
| `truncate=True` (défaut) | coupe les longues chaînes |
| `truncate=False` | n'en coupe aucune |

---

## Exemple concret

```python
data = [
    ("Alice", "Bonjour je suis développeuse Spark et je travaille sur un très gros projet Big Data.")
]

df = spark.createDataFrame(data, ["nom", "bio"])
```

**Sans `truncate=False` :**

```python
df.show()
```

```text
+-----+--------------------+
|nom  |bio                 |
+-----+--------------------+
|Alice|Bonjour je suis ... |
+-----+--------------------+
```

**Avec `truncate=False` :**

```python
df.show(truncate=False)
```

```text
+-----+----------------------------------------------------------------------------------------+
|nom  |bio                                                                                     |
+-----+----------------------------------------------------------------------------------------+
|Alice|Bonjour je suis développeuse Spark et je travaille sur un très gros projet Big Data.    |
+-----+----------------------------------------------------------------------------------------+
```

---

## Autre utilisation fréquente

On voit souvent :

```python
df.show(20, truncate=False)
```

Cela signifie :

- `20` → afficher **20 lignes**
- `truncate=False` → afficher le **contenu complet** des colonnes, sans le couper

Dans le notebook Session 3, `df_q1.show(truncate=False)` est utile pour lire en entier les noms de stations Vélib', qui peuvent être longs.

---

## Synthèse

| Écriture | Signification |
|---|---|
| `df.show()` | affichage compact, texte tronqué |
| `df.show(truncate=False)` | affichage complet des chaînes |
| `df.show(20, truncate=False)` | 20 lignes, texte non tronqué |

---

<a id="22-date_trunc--aligner-velib-et-météo-à-lheure"></a>

# 22. `DATE_TRUNC` — aligner Velib et météo à l'heure

> Notebook : `Spark_DIA3_Session_3.ipynb` — Question 2 (`df_q2`), jointure SQL Velib × météo

## Question

Que fait cette expression dans la requête `df_q2` ?

```sql
DATE_TRUNC('hour', TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm'Z'"))
```

Et pourquoi l'utilise-t-on **des deux côtés** de la jointure ?

---

## Réponse

`DATE_TRUNC` **tronque** une date/heure à la précision choisie : tout ce qui est plus fin (minutes, secondes…) est remis à zéro.

Dans ce projet, on l'utilise pour répondre à la question : **« à quelle heure ? »**, en ignorant les minutes et les secondes — indispensable pour joindre les snapshots Vélib' (fréquents) à la météo Montsouris (une mesure par heure).

---

## 1. Syntaxe Spark SQL

```sql
DATE_TRUNC(unité, timestamp)
```

| Paramètre | Rôle |
|---|---|
| `unité` | précision cible : `'year'`, `'month'`, `'day'`, `'hour'`, `'minute'`… |
| `timestamp` | valeur datetime à tronquer |

Exemple :

```sql
DATE_TRUNC('hour', timestamp '2020-06-15 14:37:22')
-- → 2020-06-15 14:00:00
```

Deux horodatages dans la **même heure** produisent la **même clé** après troncature.

---

## 2. Exemples concrets

| Valeur d'entrée | `DATE_TRUNC('hour', …)` |
|---|---|
| `2020-06-15 14:37:22` | `2020-06-15 14:00:00` |
| `2020-06-15 14:59:59` | `2020-06-15 14:00:00` |
| `2020-06-15 15:00:01` | `2020-06-15 15:00:00` |

Un snapshot Vélib' à **14:37** et une mesure météo à **14:00** tombent tous les deux sur la clé **`2020-06-15 14:00:00`**.

---

## 3. Pourquoi c'est nécessaire dans `df_q2`

Velib et météo n'ont **pas le même format** d'horodatage :

| Source | Exemple | Granularité |
|---|---|---|
| Vélib' (`disponibilite`) | `2020-01-01T14:37:00Z` | snapshot (minutes possibles) |
| Météo (`meteo` CSV) | `2020-01-01T14:00` | horaire |

La jointure se fait **heure par heure** :

```sql
WITH disponibilite_h AS (
    SELECT
        taux_occupation,
        DATE_TRUNC('hour', TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm'Z'")) AS heure_tronquee
    FROM disponibilite
),
meteo_h AS (
    SELECT
        DATE_TRUNC('hour', TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm")) AS heure_tronquee,
        ...
    FROM meteo
)
SELECT ...
FROM disponibilite_h v
LEFT JOIN meteo_h m
    ON v.heure_tronquee = m.heure_tronquee
```

Sans `DATE_TRUNC`, une égalité stricte sur l'horodatage complet échouerait presque toujours (`14:37:00` ≠ `14:00:00`).

---

## 4. Lien avec le `LEFT JOIN`

Après troncature :

- si une heure météo existe → `est_pluie` vaut `true` ou `false` ;
- si aucune correspondance → `est_pluie IS NULL` (groupe `jointure_meteo_absente`).

La requête `df_q2` compare alors les distributions de `taux_occupation` par condition météo :

| `condition_meteo` | Signification |
|---|---|
| `sec` | météo jointe, pas de pluie |
| `pluie` | météo jointe, précipitations > 0 |
| `jointure_meteo_absente` | aucune météo pour cette heure |

---

## 5. Autres précisions possibles

| Unité | Effet |
|---|---|
| `'year'` | 1er janvier 00:00:00 |
| `'month'` | 1er du mois 00:00:00 |
| `'day'` | minuit du jour |
| `'hour'` | début de l'heure (**cas du projet**) |
| `'minute'` | début de la minute |

---

## 6. Synthèse

| Expression | Signification |
|---|---|
| `DATE_TRUNC('hour', ts)` | ramène `ts` au début de l'heure |
| côté Vélib' | normalise les snapshots fréquents |
| côté météo | aligne les mesures horaires Montsouris |
| dans `df_q2` | clé de jointure pour comparer occupation sec / pluie |

> **QCM (Q16) :** `DATE_TRUNC('hour', horodatage)` sert à **aligner les deux sources sur la même granularité temporelle** (ici l'heure). Voir [§42](#42-notes-qcm--date_trunc-et-jointure-velib-meteo).

En une phrase : **`DATE_TRUNC('hour', …)` transforme des horodatages précis en clés horaires communes**, ce qui rend possible la jointure SQL entre disponibilité Vélib' et météo.

---

<a id="23-nb_snapshots--nombre-dobservations-pas-de-bornes-vides"></a>

# 23. `nb_snapshots` — nombre d'observations, pas de bornes vides

> Notebook : `Spark_DIA3_Session_3.ipynb` — Questions 2 (`df_q2`, `df_q2_arr`)

## Question

Dans les requêtes SQL, que signifie la colonne `nb_snapshots` ?

Est-ce le nombre de **bornes vides** ?

---

## Réponse

**Non.** `nb_snapshots` compte le **nombre de lignes** — c'est-à-dire le **nombre d'observations** (snapshots) dans le groupe SQL — et **pas** le nombre de bornettes libres.

En SQL, on l'obtient avec :

```sql
COUNT(*) AS nb_snapshots
```

Chaque ligne comptée correspond à **un snapshot** : une mesure de l'état d'une station Vélib' à un instant donné.

---

## 1. Qu'est-ce qu'un snapshot ?

Une ligne de la table `disponibilite` ressemble à :

```text
station X | 2020-03-15 14:00 | taux_occupation=0.06 | bornettes_libres=28 | statut=normal
```

C'est une **photo** de la station à un moment précis, pas une borne individuelle.

Donc :

- **1 snapshot** = 1 observation enregistrée ;
- **690 858 snapshots** = 690 858 mesures au total dans la table.

---

## 2. Exemple dans `df_q2`

```text
|condition_meteo|nb_snapshots|
|sec            |522240        |
|pluie          |168618        |
```

Interprétation :

- **522 240 snapshots** ont été pris par temps sec ;
- **168 618 snapshots** ont été pris sous la pluie.

Ce ne sont **pas** 522 240 bornes vides.

---

## 3. Ne pas confondre avec les bornes vides

| Colonne / notion | Signification |
|---|---|
| `nb_snapshots` | nombre de **mesures** dans le groupe |
| `bornettes_libres` | nombre de places libres sur **une station** à **un instant** |
| `statut = 'vide'` | station quasi vide (`taux_occupation < 10 %`) |
| `capacite` | nombre total de bornettes sur la station |

Pour compter les stations vides, on filtrerait plutôt :

```sql
WHERE statut = 'vide'
```

ou on utiliserait la colonne `bornettes_libres`.

---

## 4. Exemple dans `df_q2_arr`

```sql
COUNT(*) AS nb_snapshots
```

Ici, `nb_snapshots` signifie :

**combien de snapshots Velib' existent pour cet arrondissement (`code_arr`)**, avec une météo jointe.

Le `HAVING` de la requête impose par exemple :

```sql
HAVING COUNT(CASE WHEN est_pluie = false THEN 1 END) > 50
   AND COUNT(CASE WHEN est_pluie = true  THEN 1 END) > 50
```

Cela veut dire : **on ne garde que les arrondissements avec au moins 50 observations par temps sec et 50 par temps pluvieux** — pour que la comparaison soit statistiquement fiable.

---

## 5. Synthèse

| Terme | Signification |
|---|---|
| `snapshot` | une mesure horaire d'une station |
| `nb_snapshots` | nombre de ces mesures dans le groupe |
| `bornettes_libres` | places libres sur une station à un instant |

En une phrase : **`nb_snapshots` = combien de fois on a observé des stations**, pas combien de bornes étaient vides.

---

<a id="24-alias-d-et-m--jointure-velib--météo"></a>

# 24. Alias `d` et `m` — jointure Velib × météo

> Notebook : `Spark_DIA3_Session_3.ipynb` — exercice semaine sec / week-end pluvieux (`df_q2`, `df_q2_arr`, exercice ratio)
>
> Voir aussi : [§19 — Alias SQL : que signifie `d.station_id` ?](#19-alias-sql--que-signifie-dstation_id-)

## Question

Dans la requête SQL de l'exercice, que signifient les alias **`d`** et **`m`** dans :

```sql
SELECT
    d.station_id,
    d.nom_station,
    d.taux_occupation,
    d.est_weekend,
    m.est_pluie
FROM (...) d
LEFT JOIN (...) m
    ON d.heure_tronquee = m.heure_tronquee
```

---

## Réponse

`d` et `m` sont des **alias** (surnoms) de tables — une convention SQL pour raccourcir les noms et lever les ambiguïtés quand on joint plusieurs sources.

| Alias | Signification | Source |
|---|---|---|
| **`d`** | **d**isponibilité | snapshots Vélib' (`disponibilite`) |
| **`m`** | **m**étéo | données météo (`meteo`) |

Les lettres sont arbitraires : on pourrait écrire `velib` et `meteo`. `d` / `m` est simplement court et lisible.

---

## 1. Où sont définis les alias ?

```sql
FROM (
    SELECT
        station_id,
        nom_station,
        taux_occupation,
        est_weekend,
        DATE_TRUNC('hour', TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm'Z'")) AS heure_tronquee
    FROM disponibilite
    WHERE station_id IS NOT NULL
) d                          -- alias "d" = sous-requête Velib'
LEFT JOIN (
    SELECT
        DATE_TRUNC('hour', TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm")) AS heure_tronquee,
        CASE WHEN precipitations > 0 THEN true ELSE false END AS est_pluie
    FROM meteo
) m                          -- alias "m" = sous-requête météo
    ON d.heure_tronquee = m.heure_tronquee
```

Chaque alias porte sur une **sous-requête** entre parenthèses, pas directement sur la vue `disponibilite` ou la table `meteo`.

---

## 2. À quoi servent-ils ?

### Préfixer les colonnes

Quand deux tables (ou sous-requêtes) peuvent avoir des colonnes homonymes, le préfixe indique la provenance :

```sql
d.station_id    -- vient de Velib'
m.est_pluie     -- vient de la météo
```

Sans préfixe, SQL ne sait pas de quelle table vient la colonne.

### Écrire la jointure plus clairement

```sql
ON d.heure_tronquee = m.heure_tronquee
```

Les deux côtés sont alignés à l'heure grâce à `DATE_TRUNC` (voir [§22](#22-date_trunc--aligner-velib-et-météo-à-lheure)).

---

## 3. Schéma mental de la jointure

```text
disponibilite (d)              meteo (m)
─────────────────              ─────────
station_id                     heure_tronquee
nom_station                    est_pluie
taux_occupation        JOIN    (pluie ou sec)
est_weekend            ON       même heure
heure_tronquee    ═══════════  heure_tronquee
```

Le `SELECT` final mélange les deux sources :

- `d.station_id`, `d.taux_occupation`, `d.est_weekend` → côté Vélib'
- `m.est_pluie` → côté météo

---

## 4. Même convention ailleurs dans le notebook

D'autres requêtes utilisent la même logique avec des lettres différentes :

| Requête | Alias Velib | Alias météo |
|---|---|---|
| `df_q2` | `v` | `m` |
| Exercice ratio | `d` | `m` |
| `df_q2_arr` | `d` | `m` |

Peu importe la lettre choisie : **`v` ou `d` = Velib'**, **`m` = météo**.

---

## 5. Synthèse

| Écriture | Signification |
|---|---|
| `d.station_id` | colonne `station_id` de la sous-requête Velib' |
| `d.est_weekend` | booléen week-end, calculé côté Velib' |
| `m.est_pluie` | booléen pluie, calculé côté météo |
| `d.heure_tronquee = m.heure_tronquee` | clé de jointure horaire |

En une phrase : **`d` et `m` sont des noms courts pour distinguer les colonnes Velib' et météo dans une jointure SQL** — ils n'altèrent ni les données ni le résultat.

---

<a id="25-inspecter-le-format-de-horodatage-avant-to_timestamp"></a>

# 25. Inspecter le format de `horodatage` avant `TO_TIMESTAMP`

> Notebook : `Spark_DIA3_Session_3.ipynb` — cellules de diagnostic avant les jointures Velib × météo (`df_q2`, exercice ratio)
>
> Voir aussi : [§22 — `DATE_TRUNC`](#22-date_trunc--aligner-velib-et-météo-à-lheure), [§21 — `show(truncate=False)`](#21-showtruncatefalse--afficher-le-texte-complet)

## Question

Comment savoir quel pattern passer à `TO_TIMESTAMP(horodatage, "…")` ?

Par exemple, pourquoi écrire `"yyyy-MM-dd'T'HH:mm'Z'"` côté Velib' et `"yyyy-MM-dd'T'HH:mm"` côté météo ?

---

## Réponse

Il faut d'abord **regarder la chaîne brute** telle qu'elle est stockée — pas deviner. Deux requêtes simples suffisent, une par source.

**Prérequis :** la vue `disponibilite` est chargée ; la vue `meteo` est créée (cellule `df_q2`).

---

## 1. Velib' — `disponibilite`

```python
# Inspecter le format brut des horodatages
spark.sql("""
    SELECT horodatage
    FROM disponibilite
    LIMIT 5
""").show(truncate=False)
```

**Résultat typique :**

```text
+-----------------+
|horodatage       |
+-----------------+
|2020-12-04T03:07Z|
|2020-12-04T03:07Z|
...
```

**Pattern déduit :**

```sql
TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm'Z'")
```

| Morceau de la chaîne | Code dans le pattern |
|---|---|
| `2020-12-04` | `yyyy-MM-dd` |
| `T` | `'T'` (lettre fixe, entre quotes) |
| `03:07` | `HH:mm` |
| `Z` | `'Z'` (lettre fixe, entre quotes) |

---

## 2. Météo — `meteo`

```python
spark.sql("""
    SELECT horodatage
    FROM meteo
    LIMIT 5
""").show(truncate=False)
```

**Résultat typique :**

```text
+----------------+
|horodatage      |
+----------------+
|2020-01-01T00:00|
|2020-01-01T01:00|
...
```

**Pattern déduit :**

```sql
TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm")
```

Pas de `Z` final → pas de `'Z'` dans le pattern.

---

## 3. Pourquoi `truncate=False` ?

Sans `truncate=False`, Spark peut tronquer les chaînes longues dans l'affichage :

```text
|2020-12-04T03:0...|   ← on ne voit plus le Z final
```

On risque alors de choisir un mauvais pattern. `truncate=False` affiche la valeur **complète**.

---

## 4. Vérifier que le parsing fonctionne

Si le format est incorrect, `TO_TIMESTAMP` renvoie `NULL` :

```python
spark.sql("""
    SELECT
        horodatage,
        TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm'Z'") AS ts_parse
    FROM disponibilite
    LIMIT 5
""").show(truncate=False)
```

| `ts_parse` | Interprétation |
|---|---|
| valeur datetime | format OK |
| `null` | mauvais pattern |

Comptage global :

```sql
SELECT
    COUNT(*) AS total,
    COUNT(TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm'Z'")) AS parses_ok
FROM disponibilite
```

Si `parses_ok < total`, le pattern ne correspond pas à toutes les lignes.

---

## 5. Lire `T'HH:mm'Z'` dans le pattern Velib'

Pour la chaîne `2020-12-04T03:07Z` :

```text
2020-12-04  T  03:07  Z
yyyy-MM-dd 'T' HH:mm 'Z'
```

- `'T'` et `'Z'` = caractères **fixes** (quotes = littéraux)
- `HH` = heures, `mm` = minutes = parties **variables**

---

## 6. Synthèse

| Source | Exemple | Pattern `TO_TIMESTAMP` |
|---|---|---|
| Velib' (`disponibilite`) | `2020-12-04T03:07Z` | `"yyyy-MM-dd'T'HH:mm'Z'"` |
| Météo (`meteo`) | `2020-01-01T00:00` | `"yyyy-MM-dd'T'HH:mm"` |

En une phrase : **on inspecte d'abord `horodatage` avec `show(truncate=False)`, puis on construit le pattern en recopiant la structure de la chaîne** — les deux sources du projet n'ont pas le même suffixe (`Z` ou non).

---

<a id="26-spark-sql-over--window-w--lag-lead-moyenne-mobile"></a>

# 26. Spark SQL `OVER` / `WINDOW w` — LAG, LEAD, moyenne mobile

> Notebook : `Spark_DIA3_Session_3.ipynb` — §1.3 Fonctions de fenêtrage (`df_avec_lag`)
>
> Voir aussi : [`MEM-02SPARK_Window-Functions.md`](MEM-02SPARK_Window-Functions.md), [§29 — équivalent PySpark](#29-windowcurrentrow-api-pyspark--équivalent-de-rows-between)

## Question

Comment calculer en Spark SQL le taux du snapshot précédent/suivant et une moyenne mobile sur 3 observations par station ?

---

## Réponse

On définit une **fenêtre analytique** avec `WINDOW w AS (...)` puis on applique les fonctions avec `OVER w` :

```sql
SELECT
    station_id,
    nom_station,
    horodatage,
    taux_occupation,
    LAG(taux_occupation, 1) OVER w AS taux_precedent,
    LEAD(taux_occupation, 1) OVER w AS taux_suivant,
    ROUND(
        AVG(taux_occupation) OVER (
            PARTITION BY station_id
            ORDER BY horodatage
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        4
    ) AS moy_mobile_3
FROM disponibilite
WINDOW w AS (PARTITION BY station_id ORDER BY horodatage)
```

| Clause | Rôle |
|---|---|
| `PARTITION BY station_id` | une série temporelle **par station** |
| `ORDER BY horodatage` | ordre chronologique des snapshots |
| `LAG(..., 1)` | valeur du snapshot **précédent** |
| `LEAD(..., 1)` | valeur du snapshot **suivant** |
| `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` | fenêtre glissante de **3 lignes** |

---

## Synthèse

| Fonction | Usage |
|---|---|
| `LAG` / `LEAD` | navigation ligne par ligne |
| `AVG(...) OVER (... ROWS BETWEEN ...)` | agrégat sur un nombre fixe de lignes |
| `WINDOW w AS (...)` | factorise la définition de fenêtre réutilisée par plusieurs colonnes |

> **QCM (Q17) :** `LAG(col, 1) OVER (PARTITION BY station ORDER BY horodatage)` récupère la valeur de la ligne **précédente** dans la fenêtre. Voir [§43](#43-notes-qcm--lag-over-et-fenetre-analytique).

En une phrase : **`OVER` indique *sur quelles lignes* calculer la fonction ; `ROWS BETWEEN` précise *combien* de lignes inclure dans une moyenne mobile.**

---

<a id="27-row_number--classement-par-heure"></a>

# 27. `ROW_NUMBER()` — classement par heure

> Notebook : `Spark_DIA3_Session_3.ipynb` — classement horaire (`df_rank`)

## Question

Comment classer les stations par taux d'occupation moyen **à chaque heure de la journée** ?

---

## Réponse

1. Agréger le taux moyen par `(station_id, heure)` dans une CTE.
2. Numéroter les stations **dans chaque heure** avec `ROW_NUMBER()`.

```sql
WITH taux_horaire AS (
    SELECT
        station_id,
        nom_station,
        heure,
        ROUND(AVG(taux_occupation), 4) AS taux_moyen
    FROM disponibilite
    GROUP BY station_id, nom_station, heure
)
SELECT
    station_id,
    nom_station,
    heure,
    taux_moyen,
    ROW_NUMBER() OVER w AS rang
FROM taux_horaire
WINDOW w AS (PARTITION BY heure ORDER BY taux_moyen DESC)
ORDER BY heure, rang
```

| Élément | Signification |
|---|---|
| `PARTITION BY heure` | un classement **indépendant** par heure (0h, 1h, …) |
| `ORDER BY taux_moyen DESC` | la station la plus occupée obtient `rang = 1` |
| `ROW_NUMBER()` | numérotation **sans ex æquo** (contrairement à `RANK`) |

---

## Synthèse

`ROW_NUMBER() OVER (PARTITION BY heure ORDER BY taux_moyen DESC)` répond à : **« quelle station est la plus occupée à 8h, à 9h, etc. ? »**

> **QCM (Q18) :** `ROW_NUMBER() OVER (PARTITION BY heure ORDER BY taux DESC)` sert surtout à **attribuer un rang (1, 2, 3…) à chaque ligne dans un groupe**. Voir [§44](#44-notes-qcm--row_number-over-et-classement).

---

<a id="28-moyenne-cumulée-et-delta-entre-snapshots-rows-unbounded-preceding"></a>

# 28. Moyenne cumulée et delta entre snapshots (`ROWS UNBOUNDED PRECEDING`)

> Notebook : `Spark_DIA3_Session_3.ipynb` — variation et cumul (`df_delta`)

## Question

Comment calculer la **variation instantanée** du taux d'occupation et sa **moyenne cumulée** depuis le début de l'historique d'une station ?

---

## Réponse

```sql
SELECT
    station_id,
    nom_station,
    horodatage,
    taux_occupation,
    LAG(taux_occupation, 1) OVER w AS taux_precedent,
    ROUND(
        taux_occupation - LAG(taux_occupation, 1) OVER w,
        4
    ) AS delta_instantane,
    ROUND(
        AVG(taux_occupation) OVER (
            PARTITION BY station_id
            ORDER BY horodatage
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        4
    ) AS taux_moyen_cumul
FROM disponibilite
WHERE station_id = 66505513
WINDOW w AS (PARTITION BY station_id ORDER BY horodatage)
ORDER BY horodatage
```

| Colonne | Calcul |
|---|---|
| `delta_instantane` | différence avec le snapshot précédent |
| `taux_moyen_cumul` | moyenne depuis le **premier** snapshot de la station jusqu'à la ligne courante |

`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` = toutes les lignes déjà vues dans la partition, y compris la ligne actuelle.

> **Note :** la station `1042` est absente des données ; le notebook utilise `66505513` (François 1er - Lincoln).

---

## Synthèse

| Fenêtre | Effet |
|---|---|
| `2 PRECEDING AND CURRENT ROW` | moyenne mobile sur 3 snapshots ([§26](#26-spark-sql-over--window-w--lag-lead-moyenne-mobile)) |
| `UNBOUNDED PRECEDING AND CURRENT ROW` | moyenne **cumulée** depuis le début |

---

<a id="29-windowcurrentrow-api-pyspark--équivalent-de-rows-between"></a>

# 29. `Window.currentRow` (API PySpark) — équivalent de `ROWS BETWEEN`

> Notebook : `Spark_DIA3_Session_3.ipynb` — §1.3 (version historique DataFrame API)
>
> Le notebook utilise désormais **Spark SQL** ([§26](#26-spark-sql-over--window-w--lag-lead-moyenne-mobile)). Cette section documente l'équivalent PySpark.

## Question

Que signifie `Window.currentRow` dans :

```python
fenetre_moy_mobile_3 = (
    Window
    .partitionBy("station_id")
    .orderBy("horodatage")
    .rowsBetween(-2, Window.currentRow)
)
```

---

## Réponse

`Window.currentRow` désigne **la ligne en cours de traitement**. `rowsBetween(-2, Window.currentRow)` couvre **3 lignes** : les 2 précédentes + la courante.

Équivalence SQL :

```sql
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
```

| API PySpark | Spark SQL |
|---|---|
| `rowsBetween(-2, Window.currentRow)` | `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` |
| `rowsBetween(Window.unboundedPreceding, Window.currentRow)` | `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |

---

## Synthèse

`Window.currentRow` fixe la **borne droite** de la fenêtre sur la ligne actuelle — concept identique à `CURRENT ROW` en SQL.

---

<a id="30-simulation-batch-merge--décaler-un-horodatage-string"></a>

# 30. Simulation batch MERGE — décaler un `horodatage` string

> Notebook : `Spark_DIA3_Session_3.ipynb` — §1.4 Delta Lake, simulation `df_batch` avant `MERGE INTO`
>
> Voir aussi : [§25 — Inspecter le format de `horodatage`](#25-inspecter-le-format-de-horodatage-avant-to_timestamp), [§32 — MERGE INTO SQL](#32-merge-into-en-spark-sql--chemin-absolu-delta)

## Question

Pour simuler de **nouvelles lignes** dans un batch Delta, on veut décaler l'horodatage de 2 ans :

```python
.withColumn("horodatage", F.col("horodatage") + F.expr("INTERVAL 2 YEARS"))
```

Pourquoi cette écriture échoue-t-elle avec :

```text
DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES
Cannot resolve "(horodatage + INTERVAL '2' YEAR)"
the left and right operands have incompatible types ("DOUBLE" and "INTERVAL YEAR")
```

---

## Réponse

`horodatage` est stocké comme **chaîne de caractères** (`2020-01-15T10:30Z`), pas comme un type datetime.

On ne peut pas additionner directement une **string** et un **INTERVAL**. Il faut d'abord **parser** la chaîne en timestamp, ajouter l'intervalle, puis **reformater** en string si la table cible attend ce format.

---

## 1. Code corrigé du notebook

```python
# Corrections : mêmes clés (station_id, horodatage) → UPDATE au MERGE
df_corrections = (
    df_current
    .filter(F.col("mois") == 1)
    .limit(500)
    .withColumn("taux_occupation", F.round(F.col("taux_occupation") * 0.98, 4))
    .withColumn("source", lit("correction_batch"))
)

# Nouvelles lignes : horodatages décalés → INSERT au MERGE
df_nouveaux = (
    df_current
    .filter(F.col("mois") == 1)
    .orderBy("station_id", "horodatage")
    .offset(500)
    .limit(50)
    .withColumn(
        "horodatage",
        F.expr(
            "date_format("
            "to_timestamp(horodatage, \"yyyy-MM-dd'T'HH:mm'Z'\") + INTERVAL 2 YEARS, "
            "\"yyyy-MM-dd'T'HH:mm'Z'\""
            ")"
        ),
    )
    .withColumn("annee", lit(2022))
    .withColumn("mois", lit(1))
    .withColumn("source", lit("nouveau_batch"))
)

df_batch = df_corrections.unionByName(df_nouveaux).drop("source")
```

---

## 2. Étapes du décalage d'horodatage

```text
"2020-01-15T10:30Z"                         ← string
        ↓ to_timestamp(..., "yyyy-MM-dd'T'HH:mm'Z'")
2020-01-15 10:30:00                         ← timestamp
        ↓ + INTERVAL 2 YEARS
2022-01-15 10:30:00                         ← timestamp
        ↓ date_format(..., "yyyy-MM-dd'T'HH:mm'Z'")
"2022-01-15T10:30Z"                         ← string (format Velib')
```

---

## 3. Pourquoi `.offset(500).limit(50)` ?

Sans `offset(500)`, les 50 « nouvelles » lignes seraient un **sous-ensemble** des 500 corrections : mêmes `(station_id, horodatage)` → doublons dans le batch.

| Sous-ensemble | Rôle dans le MERGE |
|---|---|
| 500 premières lignes de janvier | **UPDATE** — taux corrigé (× 0.98) |
| 50 lignes suivantes, horodatage +2 ans | **INSERT** |

---

## 4. Synthèse

En une phrase : **pour décaler un `horodatage` string, on passe par `to_timestamp` → `+ INTERVAL` → `date_format`** — on n'additionne jamais l'intervalle directement sur la chaîne.

---

<a id="31-fcolmois--1--filtrer-sur-janvier"></a>

# 31. `F.col("mois") == 1` — filtrer sur janvier

> Notebook : `Spark_DIA3_Session_3.ipynb` — simulation batch MERGE (`df_corrections`, `df_nouveaux`)
>
> Voir aussi : [§30 — Simulation batch MERGE](#30-simulation-batch-merge--décaler-un-horodatage-string)

## Question

Que signifie ce filtre dans la cellule de simulation ?

```python
df_current.filter(F.col("mois") == 1)
```

---

## Réponse

`F.col("mois") == 1` ne conserve que les lignes dont le mois calendaire vaut **1** (janvier). Équivalent SQL : `WHERE mois = 1`.

La colonne `mois` est créée en Session 2 via `.withColumn("mois", F.month("ts"))`.

Dans le batch MERGE, ce filtre **réduit le volume** (~550 lignes) et garantit un exemple homogène avant de décaler les insertions en 2022.

---

## Synthèse

| Écriture | Signification |
|---|---|
| `F.col("mois") == 1` | janvier uniquement |
| `.filter(...)` | sous-ensemble pour la démo MERGE |

---

<a id="32-merge-into-en-spark-sql--chemin-absolu-delta"></a>

# 32. `MERGE INTO` en Spark SQL — chemin absolu Delta

> Notebook : `Spark_DIA3_Session_3.ipynb` — §1.4 Delta Lake (`MERGE INTO`)

## Question

Pourquoi utiliser un **chemin absolu** dans :

```sql
MERGE INTO delta.`/Users/.../data/output/delta/disponibilite` AS cible
```

---

## Réponse

En Spark SQL, `MERGE INTO delta.\`...\`` attend un chemin que la JVM peut résoudre sans ambiguïté. Un chemin **relatif** peut échouer selon le répertoire de travail du driver.

```python
delta_path = str(DELTA_DISPONIBLE.resolve())

spark.sql(f"""
    MERGE INTO delta.`{delta_path}` AS cible
    USING batch_entrant AS source
    ON  cible.station_id = source.station_id
    AND cible.horodatage = source.horodatage
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")
```

| Clause | Effet |
|---|---|
| `WHEN MATCHED THEN UPDATE SET *` | met à jour les lignes existantes (clé `(station_id, horodatage)`) |
| `WHEN NOT MATCHED THEN INSERT *` | insère les nouvelles lignes |

Chaque `MERGE` crée une **nouvelle version** Delta visible dans l'historique ([§33](#33-describe-history-et-time-travel-versionasof)).

---

## Synthèse

`MERGE INTO` = upsert SQL natif Delta : **UPDATE** si la clé existe, **INSERT** sinon. Toujours passer le chemin absolu via `.resolve()` en local.

> **QCM (Q21) :** `MERGE INTO` sur une table Delta sert surtout à faire des **upserts** (mettre à jour les lignes existantes, insérer les nouvelles). Voir [§47](#47-notes-qcm--merge-into-et-upserts-delta).

---

<a id="33-describe-history-et-time-travel-versionasof"></a>

# 33. `DESCRIBE HISTORY` et time travel (`versionAsOf`)

> Notebook : `Spark_DIA3_Session_3.ipynb` — exercice time-travel après MERGE

## Question

Comment lire l'historique Delta et comparer janvier 2022 **avant** et **après** un `MERGE` ?

---

## Réponse

### 1. Lire l'historique avec `DESCRIBE HISTORY`

`DESCRIBE HISTORY` est une **commande SQL autonome** — elle ne peut pas être placée dans un `FROM (...)` :

```python
delta_path = str(DELTA_DISPONIBLE.resolve())

derniere_op = (
    spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
    .orderBy(F.col("version").desc())
    .select("version", "operation")
    .first()
)
```

Versions typiques du pipeline Session 3 :

| Version | Opération | Contenu |
|---|---|---|
| v0 | `WRITE` (overwrite) | données 2020 |
| v1 | `WRITE` (append) | + données 2021 |
| v2+ | `MERGE` | corrections + insertions |

### 2. Choisir la version « avant MERGE »

```python
if derniere_op.operation == "MERGE":
    version_avant_merge = derniere_op.version - 1
```

### 3. Time travel avec `versionAsOf`

```python
df_avant = (
    spark.read.format("delta")
    .option("versionAsOf", version_avant_merge)
    .load(str(DELTA_DISPONIBLE))
)

df_apres = spark.read.format("delta").load(str(DELTA_DISPONIBLE))
```

On agrège ensuite sur janvier 2022 (`annee = 2022 AND mois = 1`) pour comparer le taux moyen avant/après merge.

---

## Synthèse

| Outil | Rôle |
|---|---|
| `spark.sql("DESCRIBE HISTORY delta.\`...\`")` | liste des versions et opérations |
| `.option("versionAsOf", n)` | relire la table **telle qu'elle était** à la version `n` |

> **QCM (Q19) :** pour relire une table Delta **telle qu'elle était** à la version 3, on utilise `.option("versionAsOf", 3)`. Voir [§45](#45-notes-qcm--versionasof-et-time-travel-delta).

> **QCM (Q20) :** `DESCRIBE HISTORY` permet de **lister les versions et opérations** (`WRITE`, `MERGE`, …). Voir [§46](#46-notes-qcm--describe-history-et-versions-delta).

En une phrase : **`DESCRIBE HISTORY` identifie la version juste avant le MERGE ; `versionAsOf` permet de voyager dans le temps pour comparer les agrégats.**

---

<a id="34-simulateur-de-flux--cellule-de-vérification-session-4"></a>

# 34. Simulateur de flux + cellule de vérification Session 4

> Notebook : [`Spark_DIA3_Session_4.ipynb`](Spark_DIA3_Session_4.ipynb) — §2.2 Le simulateur de flux (première cellule code)

## Note pratique — deux actions en parallèle

Pour que la cellule **« Vérification : le simulateur tourne-t-il ? »** affiche des fichiers JSON, il faut **lancer en même temps** :

1. **Dans un terminal séparé** (à la racine du projet, venv activé) — le simulateur qui **écrit** les JSON :

```bash
source .venv-spark/bin/activate
python scripts/simulateur_flux.py --output data/output/stream_input --vitesse 3
```

2. **Dans Jupyter** — la **première cellule code** de Session 4 (§2.2), qui **compte** les fichiers dans `data/output/stream_input`.

```
Terminal                          Notebook Session 4
────────                          ──────────────────
simulateur_flux.py  ──écrit──→   data/output/stream_input/*.json
       │                                    ↑
       │                                    │
       └──────── en continu ──────→  cellule de vérification (lit le dossier)
```

| Sans simulateur | Avec simulateur actif |
|---|---|
| `0 fichier(s) JSON` + message `[ATTENTION]` | `[OK] N fichier(s) JSON détecté(s)` + aperçu du dernier `snapshot_*.json` |

---

## Ordre recommandé

1. **Prérequis** : Parquet consolidé produit en Session 2 §2.8 (`data/output/disponibilite_consolidee.parquet`).
2. **Terminal** : lancer la commande ci-dessus et **la laisser tourner** pendant Session 4.
3. **Notebook** : exécuter la cellule de vérification (attente 3 s, puis listing des `.json`).
4. **Fin de séance** : `Ctrl+C` dans le terminal pour arrêter le simulateur.

> **Pourquoi en parallèle ?** Structured Streaming lit un **flux continu** de fichiers. Si le simulateur est arrêté, le dossier ne reçoit plus de nouveaux JSON et les requêtes `readStream` n’ont rien à consommer.

---

## Synthèse

| Composant | Rôle |
|---|---|
| `scripts/simulateur_flux.py` | rejoue le Parquet consolidé en fichiers JSON |
| `--output data/output/stream_input` | même répertoire que `STREAM_SOURCE_DIR` |
| `--vitesse 3` | 3 minutes d’historique simulées par seconde réelle |
| Cellule §2.2 Session 4 | confirme que des JSON arrivent avant `readStream` |

En une phrase : **lance le simulateur dans le terminal, puis exécute la cellule de vérification dans le notebook — les deux doivent tourner en même temps pour voir des JSON.**

---

<a id="35-sink-console-pyspark-vs-simulation-python-pure-24"></a>

# 35. Sink console PySpark vs simulation Python pure (§2.4)

> Notebook : [`Spark_DIA3_Session_4.ipynb`](Spark_DIA3_Session_4.ipynb) — §2.4 Première requête : sink console  
> Voir aussi : [§34 — Simulateur + vérification](#34-simulateur-de-flux--cellule-de-vérification-session-4)

## Question

Session 4 propose **deux cellules** qui affichent des micro-batchs de snapshots Vélib' enrichis. Quelle est la différence entre la cellule **PySpark** (`writeStream` + sink `console`) et la cellule **Python pure** juste en dessous ?

---

## Réponse

Les deux cellules répondent à la **même question métier** — « quelles stations arrivent dans le flux, avec quel taux d'occupation, et sont-elles quasi vides ? » — mais avec des **moteurs d'exécution différents**.

| | Cellule PySpark (Structured Streaming) | Cellule Python pure |
|---|---|---|
| **Moteur** | Spark (`readStream` → JVM) | Boucle `while` + `json` (stdlib) |
| **Source** | `stream_df` = table streaming infinie | Fichiers `*.json` dans `STREAM_SOURCE_DIR` |
| **Enrichissement** | `withColumn`, `filter`, `select` | Fonction `enrichir_ligne()` |
| **Déclenchement** | `.trigger(processingTime="5 seconds")` | `time.sleep(5)` dans une boucle |
| **Fichiers / batch** | `maxFilesPerTrigger=2` (option Spark) | `nouveaux[:2]` (max 2 fichiers non lus) |
| **Affichage** | `.format("console")`, `numRows=10` | `afficher_batch(..., max_lignes=10)` |
| **Durée** | `time.sleep(20)` puis `q_console.stop()` | boucle 20 s puis arrêt |
| **État / reprise** | `checkpointLocation` (obligatoire en prod) | ensemble `deja_vus` en mémoire Python |
| **Prérequis** | `spark`, Java, simulateur actif | `STREAM_SOURCE_DIR`, simulateur actif |

---

## 1. Ce qui est identique (logique métier)

Les deux pipelines appliquent la **même transformation** :

1. ignorer les lignes avec `capacite <= 0` ;
2. borner `velos_meca`, `velos_elec`, `bornettes_libres` à ≥ 0 ;
3. calculer `velos_total = velos_meca + velos_elec` ;
4. calculer `taux_occupation = round((capacite - bornettes_libres) / capacite, 4)` borné entre 0 et 1 ;
5. définir `est_vide = taux_occupation < 0.10` ;
6. afficher les colonnes : `station_id`, `nom_station`, `code_arr`, `horodatage`, `velos_total`, `taux_occupation`, `est_vide`.

La cellule Python sert surtout à **comprendre** ce que Spark fait sous le capot avant de passer aux fenêtres glissantes et aux sinks Delta.

---

## 2. Cellule PySpark — le vrai Structured Streaming

```python
q_console = (
    stream_console
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", ...)
    .start()
)
```

**Caractéristiques :**

- Spark traite le répertoire comme une **source de flux** : les nouveaux fichiers sont découverts automatiquement.
- Le plan de calcul (`filter`, `withColumn`, `select`) est **distribué** et réutilisable tel quel pour un sink Delta ou Kafka.
- Le **checkpoint** mémorise quels fichiers ont déjà été lus — en cas de redémarrage, Spark reprend sans relire tout depuis zéro.
- C'est l'approche **production** (même modèle mental que Kafka, Kinesis, etc.).

**Limite pédagogique :** le parsing de `horodatage` via schéma `TimestampType` peut afficher `NULL` si le format JSON (`2020-11-26T14:25Z`) n'est pas reconnu — la cellule Python affiche alors la **chaîne brute**, ce qui aide au diagnostic.

---

## 3. Cellule Python pure — simulation pédagogique

```python
while time.time() < fin:
    time.sleep(5)
    nouveaux = [f for f in fichiers if f not in deja_vus][:2]
    for chemin in nouveaux:
        for ligne in lire_fichier_json(chemin):
            enrichir_ligne(ligne)
    afficher_batch(...)
```

**Caractéristiques :**

- Pas de JVM, pas de SparkSession : uniquement `json`, `pathlib`, `time`.
- La « source » est simulée à la main : lister le dossier, prendre les fichiers non vus, les marquer dans un `set`.
- **Aucune tolérance aux pannes** : si le kernel redémarre, `deja_vus` est perdu.
- **Non scalable** : tout tourne sur le driver Python, fichier par fichier.

Utile pour **déboguer** le simulateur et vérifier l'enrichissement **sans** lancer une requête Spark.

---

## 4. Schéma comparatif

```
Simulateur (terminal)
        │
        ▼
 data/output/stream_input/*.json
        │
        ├──────────────────────────────┬──────────────────────────────
        ▼                              ▼
 PySpark readStream              Boucle Python
 (table infinie)                 (glob + json.loads)
        │                              │
        ▼                              ▼
 withColumn / filter              enrichir_ligne()
        │                              │
        ▼                              ▼
 writeStream.console              print Batch: N
 (checkpoint disque)              (deja_vus en RAM)
```

---

## 5. Quand utiliser laquelle ?

| Objectif | Cellule à privilégier |
|---|---|
| Comprendre l'enrichissement ligne à ligne | **Python pure** |
| Apprendre Structured Streaming (checkpoint, trigger, sink) | **PySpark** |
| Préparer fenêtres glissantes / Delta en aval | **PySpark** (même `stream_df`) |
| Vérifier que le simulateur écrit bien des JSON | **Python pure** ou cellule §2.2 |

---

## Synthèse

| En une phrase | |
|---|---|
| **PySpark** | moteur de streaming distribué, checkpoint, modèle production |
| **Python pur** | même logique métier, exécution manuelle pour comprendre le flux |

Les deux cellules sont **complémentaires** : Python explique *quoi* est calculé ; PySpark montre *comment* Spark industrialise ce calcul en flux continu.

---

<a id="36-driver-vs-workers--rôles-dans-spark"></a>

# 36. Driver vs workers — rôles dans Spark

> Notebook : [`Spark_DIA3_Session_4.ipynb`](Spark_DIA3_Session_4.ipynb) — Section 0 (initialisation)  
> Voir aussi : [§10 — Mac Apple Silicon, Java arm64 et `psutil`](#10-mac-apple-silicon--java-arm64-et-warning-psutil)

## Question

Dans Spark, quelle est la différence entre le **driver** et les **workers** ? Pourquoi la Section 0 de Session 4 configure-t-elle séparément `PYSPARK_DRIVER_PYTHON` et `PYSPARK_PYTHON` ?

---

## Réponse

Dans Spark, une application se découpe en **deux rôles** : le driver planifie et coordonne ; les workers exécutent le calcul en parallèle sur les partitions de données.

---

## 1. Driver — le chef d'orchestre

C'est le processus Python/JVM qui lance la session (`SparkSession`).

Il :

- lit votre code (notebook, script) ;
- construit le **plan d'exécution** (transformations, agrégations, jointures…) ;
- découpe le travail en tâches et les envoie aux workers ;
- récupère et assemble les résultats (`collect`, `show`, écriture finale) ;
- gère aussi le **streaming** : déclenche les micro-batches, checkpoints, etc.

En local (`local[*]`), le driver tourne sur votre machine.

---

## 2. Workers — les exécuteurs

Ce sont les processus qui font le travail concret.

Ils :

- reçoivent des morceaux de données (**partitions**) ;
- exécutent les tâches : lire des fichiers, filtrer, agréger, écrire ;
- tournent **en parallèle** pour aller plus vite.

En `local[*]`, les workers sont aussi sur votre machine (plusieurs processus JVM), mais le rôle reste le même :

| Rôle | Mission |
|---|---|
| **Driver** | planifie |
| **Workers** | calculent |

---

## 3. Analogie rapide

- **Driver** = le chef de chantier qui lit les plans et répartit le travail.
- **Workers** = les ouvriers qui font le travail sur chaque zone.

---

## 4. Pourquoi c'est important dans Session 4 (Mac ARM)

La Section 0 configure :

| Variable | Cible |
|---|---|
| `PYSPARK_DRIVER_PYTHON` | Python du **driver** |
| `PYSPARK_PYTHON` | Python des **workers** |

Si driver et workers n'utilisent pas la même architecture (**arm64** vs **x86**), vous pouvez obtenir des erreurs difficiles à diagnostiquer. D'où le wrapper `python-arm64` créé automatiquement sur Mac Apple Silicon.

> Pour le détail du conflit arm64/x86 et le warning `psutil`, voir [§10](#10-mac-apple-silicon--java-arm64-et-warning-psutil).

---

## Synthèse

| En une phrase | |
|---|---|
| **Driver** | planifie, coordonne, assemble les résultats |
| **Workers** | exécutent le calcul en parallèle sur les partitions |

En local, tout tourne sur la même machine — mais la séparation des rôles reste fondamentale pour comprendre Structured Streaming, les checkpoints et les sinks Delta.

---

<a id="37-delta-spark--à-quoi-ça-sert-en-session-4"></a>

# 37. Delta Spark — à quoi ça sert en Session 4 ?

> Notebook : [`Spark_DIA3_Session_4.ipynb`](Spark_DIA3_Session_4.ipynb) — Section 0, fenêtres glissantes, sink Delta  
> Voir aussi : [§13 — Delta Lake et le package `delta-spark`](#13-delta-lake-et-le-package-delta-spark)

## Question

À quoi sert **Delta Spark** (`delta-spark` / `configure_spark_with_delta_pip`) dans Session 4, alors que Spark sait déjà lire et écrire du Parquet ?

---

## Réponse

**Delta Lake** ajoute une **couche transactionnelle** au-dessus de fichiers Parquet. En Session 4, il est indispensable pour écrire en streaming vers des tables fiables (`.format("delta")`) et pour réutiliser les tables Delta créées en Session 3.

---

## 1. Sans Delta — Parquet seul

- vous écrivez des fichiers Parquet « à la main » ;
- en cas d'échec au milieu d'une écriture, vous pouvez avoir des **données partielles** ;
- pas de vraie gestion de versions, `MERGE`, time travel, etc.

---

## 2. Avec Delta — transactions ACID

- chaque écriture est une **transaction** (ACID) ;
- les lectures voient toujours un **état cohérent** ;
- vous pouvez faire :
  - **append** (ajouter des lignes),
  - **MERGE** (upsert),
  - **time travel** (lire une version passée),
  - **streaming + Delta** (écrire en continu proprement).

---

## 3. Usage concret dans Session 4

| Élément | Rôle |
|---|---|
| `DELTA_DISPONIBLE`, `DELTA_ALERTES` | lire / écrire des tables Delta (héritées de Session 3) |
| `.format("delta")` sur `writeStream` | sink streaming pour les fenêtres glissantes |
| `DeltaSparkSessionExtension`, `DeltaCatalog` | extensions activées dans `create_delta_spark_session()` |
| `_delta_is_configured()` | vérifie que Delta est bien actif avant les sinks |

La Section 0 appelle `configure_spark_with_delta_pip()` et lève une erreur explicite si Delta n'est pas actif — car les requêtes `readStream` / `writeStream` vers Delta échoueraient sinon.

---

## 4. Spark vs Delta Spark

| | Rôle |
|---|---|
| **Spark** | moteur de calcul distribué (batch ou streaming) |
| **Delta Spark** | format de stockage « intelligent » pour que ces calculs écrivent et lisent des données de façon **fiable et versionnée** |

---

## Synthèse

| Sans Delta | Avec Delta |
|---|---|
| fichiers Parquet bruts, risque d'incohérence | transactions ACID, historique, MERGE, time travel |
| streaming fragile en cas d'échec | sink `.format("delta")` + checkpoint = écriture continue fiable |

En une phrase : **Spark calcule ; Delta Spark garantit que ce qui est écrit (batch ou flux) reste cohérent, versionné et exploitable en production.**

---

<a id="38-sink-delta-des-fenêtres-glissantes-writestream"></a>

# 38. Sink Delta des fenêtres glissantes (`writeStream`)

> Notebook : [`Spark_DIA3_Session_4.ipynb`](Spark_DIA3_Session_4.ipynb) — §2.5 Fenêtres glissantes, écriture Delta  
> Voir aussi : [§37 — Delta Spark en Session 4](#37-delta-spark--à-quoi-ça-sert-en-session-4)

## Question

Que fait le bloc `writeStream` qui démarre `q_fenetres` ? À quoi servent `outputMode("append")`, le checkpoint, le trigger et `queryName` ?

---

## Réponse

Ce bloc **lance une requête Structured Streaming** qui écrit en continu les **agrégats fenêtrés par arrondissement** (`df_fenetre_arr`) dans une **table Delta** nommée `fenetres_arrondissement`.

---

## 1. Contexte — d'où vient `df_fenetre_arr` ?

Avant ce sink, le notebook construit un DataFrame streaming enrichi :

- source : fichiers JSON du simulateur (`readStream.json`) ;
- `watermark("horodatage", "5 minutes")` ;
- `groupBy(code_arr, window(horodatage, "10 minutes", "2 minutes"))` ;
- agrégats : `avg(velos_total)`, `count(*)`.

Colonnes produites : `code_arr`, `fenetre_debut`, `fenetre_fin`, `velos_moyens`, `nb_mesures`.

Le bloc `writeStream` ne recalcule pas ces fenêtres : il **branche un sink** sur ce plan déjà défini.

---

## 2. Code (cellule notebook)

```python
path_fenetres = str(DELTA_DISPONIBLE.parent / "fenetres_arrondissement")
_checkpoint_fenetres = Path(STREAM_CHECKPOINT) / "fenetres_arr"
_checkpoint_fenetres.mkdir(parents=True, exist_ok=True)

if stop_streaming_query("fenetres_arrondissement"):
    print("Requête précédente 'fenetres_arrondissement' arrêtée — redémarrage.")

q_fenetres = (
    df_fenetre_arr
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", str(_checkpoint_fenetres))
    .trigger(processingTime="10 seconds")
    .queryName("fenetres_arrondissement")
    .start(path_fenetres)
)
```

---

## 3. Ligne par ligne

| Élément | Rôle |
|---|---|
| `path_fenetres` | Dossier Delta de sortie (`data/output/delta/fenetres_arrondissement`) |
| `_checkpoint_fenetres` | Dossier checkpoint (`data/output/checkpoints/fenetres_arr`) |
| `stop_streaming_query(...)` | Évite l'erreur « query with that name is already active » en réexécution de cellule |
| `.writeStream` | Transforme le plan streaming en requête d'écriture continue |
| `.outputMode("append")` | N'écrit que les **nouvelles lignes** (fenêtres fermées grâce au watermark) |
| `.format("delta")` | Sink **Delta Lake** (transactions ACID, relecture batch possible) |
| `.option("checkpointLocation", ...)` | Mémorise offsets, watermark et état pour **reprise après panne** |
| `.trigger(processingTime="10 seconds")` | Micro-batch toutes les 10 secondes |
| `.queryName("fenetres_arrondissement")` | Nom affiché dans `spark.streams.active` et les logs |
| `.start(path_fenetres)` | Démarre la requête en arrière-plan ; retourne `q_fenetres` |

---

## 4. Schéma du flux

```
fichiers JSON (stream_input)
        ↓
   readStream + enrichissement
        ↓
   fenêtres glissantes (df_fenetre_arr)
        ↓
   writeStream → Delta (fenetres_arrondissement)
        +
   checkpoint (état / reprise)
```

---

## 5. Warning `spark.sql.adaptive.enabled`

Message fréquent au démarrage :

```
WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming ...
```

C'est **normal** : l'optimiseur adaptatif (batch) ne s'applique pas au streaming. Spark le désactive automatiquement — ce n'est pas une erreur bloquante.

---

## 6. Erreur fréquente — nom de requête déjà actif

```
IllegalArgumentException: Cannot start query with name fenetres_arrondissement
as a query with that name is already active in this SparkSession
```

**Cause :** la cellule a été relancée alors que `q_fenetres` tournait encore.

**Solutions :**

- réexécuter la Section 0 puis la cellule (helper `stop_streaming_query`) ;
- ou arrêter manuellement : `spark.streams.active` puis `q.stop()` sur la requête concernée ;
- ou exécuter la cellule §2.8 « Arrêt propre des requêtes ».

---

## Synthèse

| En une phrase | |
|---|---|
| **`writeStream` + Delta** | écriture continue et fiable des fenêtres par arrondissement |
| **Checkpoint** | reprise sans tout relire |
| **`append` + watermark** | seules les fenêtres **fermées** sont persistées |

En une phrase : **toutes les 10 s, Spark traite les nouveaux snapshots, calcule les fenêtres par arrondissement, et ajoute les résultats finalisés dans une table Delta, en mémorisant sa progression dans le checkpoint.**

---

<a id="39-transactions-acid--pourquoi-delta-lake-plutôt-que-parquet-seul"></a>

# 39. Transactions ACID — pourquoi Delta Lake plutôt que Parquet seul ?

> Notebook : `Spark_DIA3_Session_3.ipynb` — Delta Lake, écriture, `MERGE INTO`  
> Voir aussi : [§13 — Delta Lake et `delta-spark`](#13-delta-lake-et-le-package-delta-spark) · [§37 — Delta en Session 4](#37-delta-spark--à-quoi-ça-sert-en-session-4)

## Question

Pourquoi utiliser **Delta Lake** (`delta-spark`) plutôt que de simples fichiers Parquet ?

---

## Réponse

Les **transactions ACID** sont l'une des principales raisons d'utiliser Delta Lake plutôt que Parquet seul.

**ACID** signifie :

| Lettre | Propriété | En bref |
|---|---|---|
| **A** | Atomicity (Atomicité) | tout ou rien |
| **C** | Consistency (Cohérence) | état toujours valide |
| **I** | Isolation | écritures concurrentes sans corruption |
| **D** | Durability (Durabilité) | une fois validé, c'est persisté |

---

## 1. Atomicité (Atomicity)

Une transaction est exécutée **entièrement ou pas du tout**.

### Sans Delta (Parquet)

Tu écris 1 million de lignes. Au bout de 600 000 lignes, la machine tombe en panne.

Tu peux te retrouver avec :

```
part-0001.parquet
part-0002.parquet
part-0003.parquet
```

… mais il **manque** plusieurs fichiers. Le dataset est **incomplet**.

### Avec Delta

Delta écrit d'abord les nouveaux fichiers, puis met à jour le journal (`_delta_log`) **uniquement lorsque tout est terminé**.

Si le job échoue :

| Ancienne version | Nouvelle version |
|---|---|
| ✔ visible | ❌ jamais exposée |

Les utilisateurs continuent de voir l'ancienne version **complète**. Ils ne voient jamais une table à moitié écrite.

---

## 2. Cohérence (Consistency)

Les données restent toujours dans un **état valide**.

Exemple — table `clients` :

| id | nom |
|---|---|
| 1 | Alice |
| 2 | Bob |

Tu ajoutes un nouveau client. Avec Delta :

- le schéma reste cohérent ;
- les métadonnées sont mises à jour correctement ;
- les fichiers restent synchronisés.

Tu ne peux pas obtenir une table où les métadonnées indiquent 3 colonnes alors que les fichiers en contiennent 4.

---

## 3. Isolation (Isolation)

Deux utilisateurs peuvent écrire en même temps sans se gêner.

```
Utilisateur A ──┐
                ├── Table Delta
Utilisateur B ──┘
```

Les deux lancent un `UPDATE`. Delta utilise un mécanisme appelé **optimistic concurrency control** :

1. chaque transaction travaille sur une **version** de la table ;
2. au moment de valider, Delta vérifie qu'aucune transaction incompatible n'a modifié les mêmes données ;
3. en cas de conflit, une des transactions **échoue** et doit être relancée.

Cela évite les écritures incohérentes.

---

## 4. Durabilité (Durability)

Une fois la transaction **validée**, les données sont définitivement enregistrées.

Même si :

- Spark s'arrête ;
- le cluster redémarre ;
- une machine tombe en panne.

Les modifications validées restent disponibles.

---

## Pourquoi Parquet seul ne suffit pas ?

Imaginons que tu veuilles modifier une ligne dans `clients.parquet`.

En Parquet, tu ne peux **pas** modifier directement une ligne. Il faut généralement :

```
Lire tout le fichier
       ↓
Modifier une ligne
       ↓
Réécrire tout le fichier
```

Si le programme plante au milieu :

- ancien fichier supprimé ;
- nouveau fichier incomplet ;
- **risque de perte de données**.

Avec Delta :

```
Ancienne version
        │
        ▼
Création des nouveaux fichiers
        │
        ▼
Validation dans _delta_log
```

Si quelque chose échoue **avant** la validation, l'ancienne version reste utilisée.

---

## Le rôle du dossier `_delta_log`

Une table Delta ressemble à :

```
data/
│
├── part-0001.parquet
├── part-0002.parquet
├── part-0003.parquet
└── _delta_log/
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    └── 00000000000000000002.json
```

Le dossier `_delta_log` contient le **journal des transactions**. Chaque fichier JSON décrit :

- quels fichiers ont été **ajoutés** ;
- quels fichiers ont été **supprimés** ;
- quelle est la **nouvelle version** de la table.

C'est grâce à ce journal que Delta garantit les propriétés ACID.

---

## Exemple concret — `UPDATE`

Table initiale :

| id | solde |
|---|---|
| 1 | 1000 |

```python
from delta.tables import DeltaTable

table = DeltaTable.forPath(spark, "data/clients")

table.update(
    condition="id = 1",
    set={"solde": "900"},
)
```

Avec Delta :

1. les nouveaux fichiers sont préparés ;
2. le `_delta_log` est mis à jour ;
3. la nouvelle version devient visible.

Si le serveur s'éteint **juste avant** l'étape 2 :

- le solde reste à **1000** ;
- il ne passe jamais à une valeur intermédiaire ou incohérente.

---

## Synthèse

| Propriété | Signification |
|---|---|
| **Atomicité** | une modification est appliquée entièrement ou pas du tout |
| **Cohérence** | les données restent toujours dans un état valide |
| **Isolation** | plusieurs écritures simultanées ne corrompent pas la table |
| **Durabilité** | une transaction validée est conservée après redémarrage ou panne |

En une phrase : **Delta Lake se comporte comme une base de données transactionnelle tout en stockant des fichiers Parquet ; Parquet seul est un format de stockage optimisé, sans gestion native des transactions.**

---

<a id="40-notes-qcm--textfile-et-rddstr"></a>

# 40. Notes QCM — `textFile` et `RDD[str]`

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q8 — Session 1  
> Voir aussi : [§1 — Chargement CSV avec `textFile()`](#1-chargement-dun-csv-avec-textfile)

## Question (QCM Q8)

`sc.textFile("fichier.csv")` produit initialement un RDD de quel type ?

---

## Réponse

**`RDD[str]`** — chaque élément est **une ligne du fichier sous forme de chaîne de caractères**.

```python
raw_rdd = sc.textFile("fichier.csv")
# type logique : RDD[str]
# ex. "2020-11-26T14:25Z,82413301,Ménilmontant,40,..."
```

Ce n'est **pas** :

| Type | Pourquoi ce n'est pas le cas ici |
|---|---|
| `RDD[dict]` | le parsing CSV (`split`, `map(parse_ligne)`) vient **après** |
| `DataFrame` | il faudrait `spark.read.csv(...)` ou convertir le RDD |
| `RDD[int]` | Spark ne convertit pas les lignes en entiers automatiquement |

---

## À retenir pour le QCM

- `textFile` = source **texte brut**, ligne par ligne ;
- le typage métier (colonnes, booléens, nombres) se fait dans les étapes suivantes du pipeline Session 1 (`map`, `filter`, …) ;
- la lecture disque n'a lieu qu'à la **première action** (`count`, `take`, …).

En une phrase : **`textFile` donne un plan `RDD[str]` ; le CSV n'est pas encore « décodé » en tableau.**

---

<a id="41-notes-qcm--predicate-pushdown-et-parquet"></a>

# 41. Notes QCM — predicate pushdown et Parquet

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q11 — Session 2  
> Voir aussi : [§12 — Parquet vs CSV/JSON](#12-pourquoi-parquet-plutôt-que-csv-ou-json) (§6 predicate pushdown)

## Question (QCM Q11)

Le **predicate pushdown** avec Parquet permet à Spark de :

---

## Réponse

**Sauter des blocs de données selon les filtres, avant de tout charger.**

Parquet enregistre des **statistiques par bloc** (min, max, nulls…). Si un filtre exclut tout un bloc (ex. `annee = 2020` alors que le bloc ne contient que 2021), Spark **ne lit pas** ce bloc sur disque.

Ce n'est **pas** :

| Réponse piège | Pourquoi |
|---|---|
| Ignorer des colonnes entières | c'est le **column pruning** (`select`), pas le pushdown |
| Convertir CSV en JSON | hors sujet |
| Désactiver le shuffle | le shuffle reste possible après lecture |

---

## Exemple de code (Session 2)

```python
df_nov_2020 = (
    spark.read.parquet(str(VELIB_PARQ_DIR))
    .filter("annee = 2020 AND mois = 11")
)

# Vérifier que le filtre est poussé vers la source Parquet :
df_nov_2020.explain(mode="formatted")
```

Dans le plan, on cherche souvent une forme de **filtre appliqué à la lecture** (`PushedFilters`, `PartitionFilters`, `DataFilters` selon versions Spark) — signe que Spark élimine des blocs ou partitions **avant** de matérialiser toutes les lignes.

---

## Schéma mental

```
Sans pushdown (CSV)          Avec pushdown (Parquet)
───────────────────          ───────────────────────
Lire TOUT le fichier    →    Lire stats des blocs
        ↓                            ↓
Appliquer le filtre       →    Sauter blocs impossibles
        ↓                            ↓
Garder les lignes OK      →    Lire seulement blocs utiles
```

---

## À retenir pour le QCM

- **Column pruning** = ne lire que les colonnes demandées (`select`) ;
- **Predicate pushdown** = ne lire que les **blocs/lignes** compatibles avec le `filter` ;
- avec un CSV via `textFile`, Spark parcourt en général **toutes les lignes** pour filtrer.

En une phrase : **Parquet + `filter` = Spark peut éliminer des blocs entiers sans les charger en mémoire.**

---

<a id="42-notes-qcm--date_trunc-et-jointure-velib-meteo"></a>

# 42. Notes QCM — `DATE_TRUNC` et jointure Vélib' × météo

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q16 — Session 3  
> Voir aussi : [§22 — `DATE_TRUNC`](#22-date_trunc--aligner-velib-et-météo-à-lheure) · [§24 — jointure Velib × météo](#24-alias-d-et-m--jointure-velib--météo)

## Question (QCM Q16)

À quoi sert `DATE_TRUNC('hour', horodatage)` dans la jointure Vélib' × météo ?

---

## Réponse

**À aligner les deux sources sur la même granularité temporelle (ici l'heure).**

Les snapshots Vélib' peuvent avoir des minutes (`14:37:00`) alors que la météo Montsouris est **horaire** (`14:00`). `DATE_TRUNC('hour', …)` ramène chaque horodatage au **début de l'heure** (`14:00:00`) pour en faire une **clé de jointure commune**.

Ce n'est **pas** :

| Réponse piège | Pourquoi |
|---|---|
| Supprimer les lignes sans température | c'est un filtre `WHERE`, pas un `DATE_TRUNC` |
| Partitionner le disque par mois | c'est `partitionBy("mois")` à l'écriture |
| Activer le time travel Delta | c'est `versionAsOf` / `DESCRIBE HISTORY` ([§45](#45-notes-qcm--versionasof-et-time-travel-delta) · [§46](#46-notes-qcm--describe-history-et-versions-delta)) |

---

## Exemple de code (Session 3 — `df_q2`)

```sql
WITH disponibilite_h AS (
    SELECT
        taux_occupation,
        DATE_TRUNC(
            'hour',
            TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm'Z'")
        ) AS heure_tronquee
    FROM disponibilite
),
meteo_h AS (
    SELECT
        DATE_TRUNC(
            'hour',
            TO_TIMESTAMP(horodatage, "yyyy-MM-dd'T'HH:mm")
        ) AS heure_tronquee,
        precipitations_mm
    FROM meteo
)
SELECT ...
FROM disponibilite_h v
LEFT JOIN meteo_h m
    ON v.heure_tronquee = m.heure_tronquee
```

| Horodatage Vélib' | Horodatage météo | Clé après `DATE_TRUNC('hour', …)` |
|---|---|---|
| `2020-06-15 14:37:00` | `2020-06-15 14:00` | `2020-06-15 14:00:00` |
| `2020-06-15 14:59:59` | `2020-06-15 14:00` | `2020-06-15 14:00:00` |

Sans cette troncature, `14:37:00 = 14:00:00` échoue et la jointure ne matche presque jamais.

---

## Schéma mental

```
Vélib' (snapshots)          Météo (horaire)
14:37, 14:42, 14:55   →     14:00
        │                      │
        └──── DATE_TRUNC('hour') ────┘
                    │
              clé 14:00:00
                    │
              LEFT JOIN df_q2
```

---

## À retenir pour le QCM

- `DATE_TRUNC` = **tronquer** une date/heure à une unité (`hour`, `day`, `month`…) ;
- on l'applique **des deux côtés** de la jointure pour la même granularité ;
- objectif métier : comparer occupation Vélib' selon la pluie **à l'heure**.

En une phrase : **`DATE_TRUNC('hour', …)` fabrique une clé horaire commune pour joindre Vélib' et météo.**

---

<a id="43-notes-qcm--lag-over-et-fenetre-analytique"></a>

# 43. Notes QCM — `LAG(...) OVER` et fenêtre analytique

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q17 — Session 3  
> Voir aussi : [§26 — `OVER` / `WINDOW w`](#26-spark-sql-over--window-w--lag-lead-moyenne-mobile) · [MEM-02](MEM-02SPARK_Window-Functions.md)

## Question (QCM Q17)

En Spark SQL, `LAG(col, 1) OVER (PARTITION BY station ORDER BY horodatage)` permet de :

---

## Réponse

**Récupérer la valeur de la ligne précédente dans la fenêtre.**

`LAG` = *lag* (« retard ») : pour chaque ligne, on remonte d'**1** position dans l'ordre défini par `ORDER BY horodatage`, **à l'intérieur** de chaque `PARTITION BY station`.

Ce n'est **pas** :

| Réponse piège | Pourquoi |
|---|---|
| Compter le nombre total de stations | c'est `COUNT(DISTINCT station)` ou `COUNT(*) OVER (...)` |
| Supprimer les doublons | c'est `DISTINCT` ou `ROW_NUMBER` + filtre |
| Écrire en mode streaming | c'est `readStream` / `writeStream` (Session 4) |

---

## Exemple de code (Session 3)

```sql
SELECT
    station_id,
    horodatage,
    taux_occupation,
    LAG(taux_occupation, 1) OVER w AS taux_precedent,
    taux_occupation - LAG(taux_occupation, 1) OVER w AS delta_taux
FROM disponibilite
WINDOW w AS (PARTITION BY station_id ORDER BY horodatage)
```

| `station_id` | `horodatage` | `taux_occupation` | `taux_precedent` (`LAG`) |
|---|---|---|---|
| 101 | 08:00 | 0.40 | `NULL` (pas de ligne avant) |
| 101 | 08:10 | 0.55 | 0.40 |
| 101 | 08:20 | 0.48 | 0.55 |

- première ligne de chaque station → `LAG` vaut **`NULL`** ;
- on peut ensuite calculer un **écart** : `taux - LAG(taux, 1)`.

---

## Lire la clause `OVER`

```sql
LAG(col, 1) OVER (PARTITION BY station ORDER BY horodatage)
```

| Morceau | Rôle |
|---|---|
| `LAG(col, 1)` | valeur de `col` **1 ligne avant** |
| `PARTITION BY station` | fenêtre **par station** (séries indépendantes) |
| `ORDER BY horodatage` | ordre chronologique des snapshots |

Complément : `LEAD(col, 1)` fait l'inverse — valeur de la ligne **suivante**.

---

## Schéma mental

```
Station 101 :  0.40 → 0.55 → 0.48
                 ↑      ↑      ↑
LAG(...,1):   NULL   0.40   0.55
```

---

## À retenir pour le QCM

- **`LAG`** = ligne **précédente** ; **`LEAD`** = ligne **suivante** ;
- `PARTITION BY` découpe la fenêtre (ici par station) ;
- `ORDER BY` fixe l'ordre temporel dans chaque partition.

En une phrase : **`LAG(..., 1) OVER (...)` compare chaque snapshot au précédent dans l'historique d'une station.**

---

<a id="44-notes-qcm--row_number-over-et-classement"></a>

# 44. Notes QCM — `ROW_NUMBER() OVER` et classement

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q18 — Session 3  
> Voir aussi : [§27 — classement par heure](#27-row_number--classement-par-heure) · [MEM-02](MEM-02SPARK_Window-Functions.md)

## Question (QCM Q18)

`ROW_NUMBER() OVER (PARTITION BY heure ORDER BY taux DESC)` sert surtout à :

---

## Réponse

**Attribuer un rang (1, 2, 3…) à chaque ligne dans un groupe.**

`ROW_NUMBER()` numérote les lignes **à l'intérieur** de chaque partition définie par `PARTITION BY`. L'ordre du classement est fixé par `ORDER BY` : ici, le taux le plus élevé obtient le rang **1**.

Ce n'est **pas** :

| Réponse piège | Pourquoi |
|---|---|
| Calculer une moyenne mobile | c'est `AVG(...) OVER (... ROWS BETWEEN ...)` ([§26](#26-spark-sql-over--window-w--lag-lead-moyenne-mobile)) |
| Fusionner deux tables Delta | c'est `MERGE INTO` ([§32](#32-merge-into-en-spark-sql--chemin-absolu-delta) · [§47](#47-notes-qcm--merge-into-et-upserts-delta)) |
| Lire une version antérieure d'une table | c'est le time travel Delta `VERSION AS OF` ([§33](#33-describe-history-et-time-travel-versionasof)) |

---

## Exemple de code (Session 3)

```sql
WITH taux_horaire AS (
    SELECT
        station_id,
        nom_station,
        heure,
        ROUND(AVG(taux_occupation), 4) AS taux_moyen
    FROM disponibilite
    GROUP BY station_id, nom_station, heure
)
SELECT
    station_id,
    nom_station,
    heure,
    taux_moyen,
    ROW_NUMBER() OVER w AS rang
FROM taux_horaire
WINDOW w AS (PARTITION BY heure ORDER BY taux_moyen DESC)
ORDER BY heure, rang
```

| `heure` | `station_id` | `taux_moyen` | `rang` |
|---|---|---|---|
| 8 | 101 | 0.72 | **1** |
| 8 | 205 | 0.65 | **2** |
| 8 | 312 | 0.58 | **3** |
| 9 | 205 | 0.81 | **1** |
| 9 | 101 | 0.70 | **2** |

Le rang **repart à 1** à chaque nouvelle heure (`PARTITION BY heure`).

---

## Lire la clause `OVER`

```sql
ROW_NUMBER() OVER (PARTITION BY heure ORDER BY taux DESC)
```

| Morceau | Rôle |
|---|---|
| `ROW_NUMBER()` | attribue 1, 2, 3… **sans ex æquo** |
| `PARTITION BY heure` | un classement **indépendant** par heure |
| `ORDER BY taux DESC` | la ligne avec le plus grand `taux` est rang **1** |

Complément : `RANK()` et `DENSE_RANK()` gèrent les **ex æquo** (même valeur → même rang, puis saut ou pas selon la fonction).

---

## Schéma mental

```
Heure 8h :  station A (0.72) → rang 1
            station B (0.65) → rang 2
            station C (0.58) → rang 3

Heure 9h :  (repart à 1 pour chaque station de 9h)
```

---

## À retenir pour le QCM

- **`ROW_NUMBER()`** = rang unique 1, 2, 3… **dans chaque groupe** ;
- **`PARTITION BY`** = définit le groupe (ici l'heure) ;
- **`ORDER BY`** = définit qui est 1er, 2e, 3e…

En une phrase : **`ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)` classe les lignes à l'intérieur de chaque groupe.**

---

<a id="45-notes-qcm--versionasof-et-time-travel-delta"></a>

# 45. Notes QCM — `versionAsOf` et time travel Delta

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q19 — Session 3  
> Voir aussi : [§33 — `DESCRIBE HISTORY` et time travel](#33-describe-history-et-time-travel-versionasof)

## Question (QCM Q19)

Pour relire une table Delta **telle qu'elle était** à la version 3, on utilise :

---

## Réponse

**`.option("versionAsOf", 3)`**

Delta Lake conserve un **historique de versions** : chaque `WRITE`, `MERGE`, `DELETE`, etc. crée une nouvelle version. L'option `versionAsOf` indique à Spark de lire la table **à un instant passé** du journal (`_delta_log`), sans modifier les données actuelles.

Ce n'est **pas** :

| Réponse piège | Pourquoi |
|---|---|
| `.mode("overwrite")` | écrase la table à l'**écriture**, ne lit pas une version passée |
| `MERGE INTO … WHEN NOT MATCHED` | met à jour / insère des lignes, ne voyage pas dans le temps ([§47](#47-notes-qcm--merge-into-et-upserts-delta)) |
| `repartition(3)` | redistribue les données sur **3 partitions**, sans lien avec les versions Delta |

---

## Exemple de code (Session 3)

### Lecture DataFrame API

```python
delta_path = str(DELTA_DISPONIBLE.resolve())

# Table actuelle (dernière version)
df_actuelle = spark.read.format("delta").load(delta_path)

# Table telle qu'elle était à la version 3
df_v3 = (
    spark.read.format("delta")
    .option("versionAsOf", 3)
    .load(delta_path)
)
```

### Variante SQL

```sql
SELECT *
FROM delta.`/chemin/vers/disponibilite`
VERSION AS OF 3
```

Les deux syntaxes posent la même question : **« quelles étaient les lignes visibles à la version 3 ? »**

---

## Lire le code ligne par ligne

```python
spark.read.format("delta")      # lecteur Delta (pas Parquet brut)
    .option("versionAsOf", 3)   # version cible dans l'historique
    .load(delta_path)           # chemin de la table Delta
```

| Morceau | Rôle |
|---|---|
| `format("delta")` | active le moteur Delta (journal + fichiers Parquet) |
| `versionAsOf`, 3 | pointe sur la **3ᵉ version** enregistrée |
| `load(...)` | construit un DataFrame **figé** à cet instant |

---

## Contexte métier (après un `MERGE`)

Dans le notebook Session 3, on compare souvent **avant / après** une correction :

```python
derniere_op = (
    spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
    .orderBy(F.col("version").desc())
    .first()
)

version_avant_merge = derniere_op.version - 1

df_avant = (
    spark.read.format("delta")
    .option("versionAsOf", version_avant_merge)
    .load(delta_path)
)
```

`DESCRIBE HISTORY` liste les versions ; `versionAsOf` permet de **revenir** à l'une d'elles.

---

## Schéma mental

```
_delta_log/  →  v0  v1  v2  v3  v4  (dernière)
                      ↑
              versionAsOf = 3
              (on lit l'état à ce moment-là)
```

---

## À retenir pour le QCM

- **`versionAsOf`** = time travel **par numéro de version** ;
- équivalent SQL : `VERSION AS OF n` ;
- complément utile : `DESCRIBE HISTORY` pour connaître les versions disponibles.

En une phrase : **`.option("versionAsOf", 3)` relit la table Delta exactement comme elle était à la version 3.**

---

<a id="46-notes-qcm--describe-history-et-versions-delta"></a>

# 46. Notes QCM — `DESCRIBE HISTORY` et versions Delta

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q20 — Session 3  
> Voir aussi : [§33 — `DESCRIBE HISTORY` et time travel](#33-describe-history-et-time-travel-versionasof) · [§45 — `versionAsOf`](#45-notes-qcm--versionasof-et-time-travel-delta)

## Question (QCM Q20)

La commande `DESCRIBE HISTORY` sur une table Delta permet de :

---

## Réponse

**Lister les versions et opérations (`WRITE`, `MERGE`, `DELETE`, …).**

Chaque modification sur une table Delta crée une **nouvelle version** dans le journal `_delta_log`. `DESCRIBE HISTORY` affiche ce journal sous forme de tableau : numéro de version, type d'opération, horodatage, etc.

Ce n'est **pas** :

| Réponse piège | Pourquoi |
|---|---|
| Supprimer les anciennes partitions | c'est une opération de maintenance (`VACUUM`, `DELETE`, …) |
| Arrêter une requête streaming | c'est `query.stop()` ou `spark.streams.get(...).stop()` (Session 4) |
| Convertir Parquet en CSV | c'est `df.write.csv(...)` ou une conversion manuelle |

---

## Exemple de code (Session 3)

```python
delta_path = str(DELTA_DISPONIBLE.resolve())

historique = spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
historique.select("version", "timestamp", "operation").show(truncate=False)
```

**Résultat typique :**

```text
+-------+-------------------+---------+
|version|timestamp          |operation|
+-------+-------------------+---------+
|0      |2024-01-15 10:00:00|WRITE    |
|1      |2024-01-15 11:30:00|WRITE    |
|2      |2024-01-15 14:00:00|MERGE    |
|3      |2024-01-15 16:45:00|MERGE    |
+-------+-------------------+---------+
```

| Colonne | Signification |
|---|---|
| `version` | numéro de version (0, 1, 2, …) — utilisé par `versionAsOf` ([§45](#45-notes-qcm--versionasof-et-time-travel-delta)) |
| `operation` | type d'action : `WRITE`, `MERGE`, `DELETE`, `UPDATE`, … |
| `timestamp` | moment de l'opération |

---

## Lire la commande

```python
spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
```

| Morceau | Rôle |
|---|---|
| `DESCRIBE HISTORY` | commande SQL Delta dédiée à l'**audit** du journal |
| `delta.\`...\`` | chemin absolu vers la table Delta |
| résultat | DataFrame Spark → `.show()`, filtres, jointures possibles |

`DESCRIBE HISTORY` est une **commande autonome** : on ne peut pas l'imbriquer dans un `FROM (...)`.

---

## Enchaînement avec le time travel

```python
derniere_op = (
    spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
    .orderBy(F.col("version").desc())
    .select("version", "operation")
    .first()
)

if derniere_op.operation == "MERGE":
    version_avant = derniere_op.version - 1
    df_avant = (
        spark.read.format("delta")
        .option("versionAsOf", version_avant)
        .load(delta_path)
    )
```

1. **`DESCRIBE HISTORY`** → identifier la version et l'opération ;
2. **`versionAsOf`** → relire l'état à cette version.

---

## Schéma mental

```
_delta_log/
  ├── 00000000000000000000.json  → v0  WRITE
  ├── 00000000000000000001.json  → v1  WRITE
  ├── 00000000000000000002.json  → v2  MERGE
  └── ...

DESCRIBE HISTORY  →  tableau lisible de tout ça
```

---

## À retenir pour le QCM

- **`DESCRIBE HISTORY`** = journal des versions Delta ;
- on y voit les **opérations** (`WRITE`, `MERGE`, …) et les **numéros de version** ;
- complément : **`versionAsOf`** pour relire une version passée.

En une phrase : **`DESCRIBE HISTORY` liste l'historique des modifications d'une table Delta.**

---

<a id="47-notes-qcm--merge-into-et-upserts-delta"></a>

# 47. Notes QCM — `MERGE INTO` et upserts Delta

> QCM : [`qcm-etudiants.md`](qcm-etudiants.md) · Q21 — Session 3  
> Voir aussi : [§32 — `MERGE INTO` en Spark SQL](#32-merge-into-en-spark-sql--chemin-absolu-delta)

## Question (QCM Q21)

`MERGE INTO` sur une table Delta est particulièrement utile pour :

---

## Réponse

**Faire des upserts (mettre à jour les lignes existantes, insérer les nouvelles).**

Un **upsert** = **UP**date + in**SERT** : si la clé existe déjà dans la table cible → **mise à jour** ; sinon → **insertion**. C'est le cas typique des pipelines incrémentaux (corrections de données, nouveaux snapshots).

Ce n'est **pas** :

| Réponse piège | Pourquoi |
|---|---|
| Lire un fichier JSON ligne par ligne | c'est `spark.read.json(...)` ou l'API RDD |
| Remplacer le driver Python | le driver coordonne Spark, il n'est pas remplacé par `MERGE` |
| Désactiver le watermark | le watermark concerne le **streaming** (Session 4), pas `MERGE INTO` |

---

## Exemple de code (Session 3)

```python
delta_path = str(DELTA_DISPONIBLE.resolve())
batch_entrant.createOrReplaceTempView("batch_entrant")

spark.sql(f"""
    MERGE INTO delta.`{delta_path}` AS cible
    USING batch_entrant AS source
    ON  cible.station_id = source.station_id
    AND cible.horodatage = source.horodatage
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")
```

| Clause | Effet |
|---|---|
| `ON cible.station_id = source.station_id AND …` | clé de correspondance (ici station + horodatage) |
| `WHEN MATCHED THEN UPDATE SET *` | **met à jour** la ligne existante |
| `WHEN NOT MATCHED THEN INSERT *` | **insère** une nouvelle ligne |

---

## Scénario métier (Vélib')

```
Table Delta (cible)          Batch entrant (source)
─────────────────────        ──────────────────────
station 101, 08:00, 0.40     station 101, 08:00, 0.55  → MATCHED  → UPDATE
station 101, 08:10, 0.48     station 101, 08:20, 0.62  → NOT MATCHED → INSERT
```

- correction d'un taux déjà présent → **UPDATE** ;
- nouveau snapshot jamais vu → **INSERT**.

Chaque `MERGE` crée une **nouvelle version** Delta, visible via `DESCRIBE HISTORY` ([§46](#46-notes-qcm--describe-history-et-versions-delta)).

---

## Lire la commande

```sql
MERGE INTO delta.`...` AS cible
USING source AS source
ON  <clé de jointure>
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

| Mot-clé | Rôle |
|---|---|
| `MERGE INTO` | opération d'**upsert** atomique sur Delta |
| `USING` | jeu de données entrant (batch, vue temporaire, autre table) |
| `WHEN MATCHED` | branche **mise à jour** |
| `WHEN NOT MATCHED` | branche **insertion** |

---

## À retenir pour le QCM

- **`MERGE INTO`** = upsert natif Delta (UPDATE + INSERT en une transaction) ;
- indispensable pour les **mises à jour incrémentales** sans réécrire toute la table ;
- distinct du time travel (`versionAsOf`, [§45](#45-notes-qcm--versionasof-et-time-travel-delta)).

En une phrase : **`MERGE INTO` met à jour les lignes existantes et insère les nouvelles en une seule opération ACID.**

---

<a id="48-générateurs-python--yield-vs-return"></a>

# 48. Générateurs Python — `yield` vs `return`

> Rappel Python utile pour Session 1 (paresse des RDD)  
> Voir aussi : [§1 `textFile` lazy](#1-chargement-dun-csv-avec-textfile) · [§3 `filter` lazy](#3-filtrage-dun-rdd-avec-filter-en-tête)

## Question

Qu'est-ce qu'un **générateur** en Python, et pourquoi utiliser `yield` plutôt que `return` ?

---

## Réponse

Un **générateur** est une fonction qui produit des valeurs **une par une**, à la demande, au lieu de construire toute la collection en mémoire d'un coup.

- **`return`** : calcule **tout**, renvoie **un** résultat, puis la fonction **se termine**.
- **`yield`** : produit **une** valeur, **met la fonction en pause**, et reprend plus tard au même endroit.

---

## 1. `return` — tout d'un coup

```python
def nombres_return(n):
    resultat = []
    for i in range(n):
        resultat.append(i)
    return resultat   # liste complète en mémoire

liste = nombres_return(1_000_000)  # 1 million d'entiers déjà créés
```

Avantage : simple.  
Inconvénient : **occupe la mémoire** pour toute la liste, même si on n'utilise que les 10 premiers éléments.

---

## 2. `yield` — une valeur à la fois

```python
def nombres_yield(n):
    for i in range(n):
        yield i   # produit i, puis attend le prochain appel

gen = nombres_yield(1_000_000)  # presque rien en mémoire
print(next(gen))  # 0
print(next(gen))  # 1
```

| Appel | Effet |
|---|---|
| `nombres_yield(n)` | crée un **objet générateur** (pas encore de boucle complète) |
| `next(gen)` ou `for x in gen` | exécute jusqu'au prochain `yield`, puis **pause** |
| fin de la fonction | le générateur est **épuisé** (`StopIteration`) |

On peut aussi itérer naturellement :

```python
for x in nombres_yield(5):
    print(x)   # 0 1 2 3 4
```

---

## 3. Pourquoi `yield` plutôt que `return` ?

| Critère | `return` (liste) | `yield` (générateur) |
|---|---|---|
| Mémoire | charge **tout** | une valeur à la fois |
| Moment du calcul | **immédiat** | **à la demande** (lazy) |
| Réutilisable | oui (liste figée) | non (un générateur se consomme une fois) |
| Cas d'usage | petits volumes, accès aléatoire | gros flux, pipelines, fichiers ligne à ligne |

En résumé : on utilise `yield` quand on traite un **flux potentiellement grand** et qu'on ne veut pas tout stocker.

---

## 4. Lien avec Spark (Session 1)

L'esprit est le même que les **transformations RDD** :

| Python | Spark |
|---|---|
| `yield` / générateur | `textFile`, `filter`, `map` (transformations) |
| `for` / `next` / `list(...)` | `count()`, `take()`, `collect()` (actions) |

```text
générateur :  défini → pause → produit une valeur quand on tire
RDD        :  plan    → lazy  → calcule quand une action arrive
```

Ce n'est **pas** le même mécanisme interne, mais la même idée pédagogique : **décrire le calcul maintenant, exécuter seulement au besoin**.

---

## 5. Piège fréquent

```python
gen = nombres_yield(3)
print(list(gen))  # [0, 1, 2]
print(list(gen))  # []  ← déjà consommé !
```

Un générateur n'est en général **parcouru qu'une fois**. Pour recommencer, il faut **rappeler** la fonction (`nombres_yield(3)` à nouveau).

---

## À retenir

- **`return`** = un résultat final, calcul terminé ;
- **`yield`** = une série de résultats, calcul **paresseux** ;
- utile pour les **gros volumes** et les **pipelines** ;
- analogue mental aux RDD Spark : plan d'abord, exécution à l'action.

En une phrase : **`yield` fabrique un générateur qui produit les valeurs à la demande, sans tout charger en mémoire comme le ferait `return` d'une liste.**
