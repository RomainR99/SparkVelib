# Rapport — Spark Session 1 (API RDD)

Notes et explications des notebooks Spark du projet ClimaCity Paris :
- `Spark_DIA3_Session_1.ipynb` — API RDD
- `Spark_DIA3_Session_2.ipynb` — API DataFrame (§11–§12)
- `Spark_DIA3_Session_3.ipynb` — Spark SQL, Delta Lake (à partir de la §13)

## Sommaire

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
11. [Plan d'exécution : `df.explain(mode="formatted")`](#11-plan-dexécution--dfexplainmodeformatted)
12. [Pourquoi Parquet plutôt que CSV ou JSON ?](#12-pourquoi-parquet-plutôt-que-csv-ou-json)
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
26. [`Window.currentRow` et moyenne mobile sur 3 snapshots](#26-windowcurrentrow-et-moyenne-mobile-sur-3-snapshots)
27. [Simulation batch MERGE — décaler un `horodatage` string](#27-simulation-batch-merge--décaler-un-horodatage-string)
28. [`F.col("mois") == 1` — filtrer sur janvier](#28-fcolmois--1--filtrer-sur-janvier)

## Parcours du pipeline (liens entre sections)

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
| `sc.textFile()` | [§1 Chargement](#1-chargement-dun-csv-avec-textfile) |
| `getNumPartitions()` / `repartition()` | [§2 Partitions](#2-partitions-rdd-vs-sparksqlshufflepartitions) |
| `filter` en-tête | [§3 Filtrage en-tête](#3-filtrage-dun-rdd-avec-filter-en-tête) |
| `print(rdd)` lazy | [§4 PythonRDD](#4-affichage-dun-rdd-pythonrdd26) |
| `parse_ligne()` | [§5 Déballage](#5-déballage-de-liste-horodatage-capacite--champs) · [§6 Booléen](#6-conversion-booléenne-operativelower--true) |
| `filter(None)` | [§7 Malformées](#7-suppression-des-lignes-malformées-x-is-not-none) |
| `filter(taux)` | [§8 Taux d'occupation](#8-filtrage-par-taux-doccupation--010) |
| affichage top 10 | [§9 Format tableau](#9-formatage-dun-en-tête-de-tableau-40--12) |
| Section 0 (config Mac) | [§10 Java arm64 / psutil](#10-mac-apple-silicon--java-arm64-et-warning-psutil) |

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

## Question

Que fait la ligne suivante ?

```python
data_rdd = raw_rdd.filter(lambda line: line != entete)
```

---

## Réponse

Cette ligne crée un **nouveau RDD** (`data_rdd`) à partir de `raw_rdd`, en **conservant uniquement les lignes différentes de `entete`**.

---

## 1. Décomposition

**`raw_rdd`** — RDD contenant toutes les lignes brutes du CSV, une chaîne de caractères par ligne.

**`entete`** — Valeur obtenue à l'exercice 5 avec `first()` : la **première ligne** du fichier.

**`lambda line: line != entete`** — Fonction testée sur **chaque ligne** :
- si la ligne **n'est pas** égale à `entete` → `True` → la ligne est **conservée** ;
- si la ligne **est** égale à `entete` → `False` → la ligne est **rejetée**.

**`filter(...)`** — Transformation **paresseuse** : Spark enregistre la règle (« garder les lignes où `line != entete` ») sans lire ni filtrer le fichier immédiatement.

**`data_rdd = ...`** — Le **plan de calcul** du RDD filtré est stocké dans une nouvelle variable. `raw_rdd` n'est **pas modifié** (immutabilité des RDD).

---

## 2. Ce que ça ne fait pas

- **Ne lit pas** le fichier sur le disque tout de suite ;
- **N'affiche pas** les lignes filtrées ;
- **Ne compte pas** les résultats.

Tant qu'il n'y a pas d'**action** (`count()`, `take()`, `collect()`…), rien n'est exécuté.

---

## 3. Intention pédagogique

Dans un CSV **avec en-tête**, la première ligne ressemble à :

```
horodatage,capacite,velos_meca,...
```

Le filtre permet de la retirer pour ne garder que les **lignes de données**.

Avec **`historique_stations.csv`**, il n'y a **pas de ligne d'en-tête** : `entete` est déjà une ligne de données (ex. un snapshot de la station Benjamin Godard). Le filtre ne retire donc que les lignes **strictement identiques** à cette première — en pratique très peu, voire une seule.

---

## 4. Que voit-on après `filter` ?

**`print(data_rdd)`** affiche une **référence RDD** (ex. `MapPartitionsRDD[...] at filter`), pas les données. C'est normal : `filter` est une transformation, pas une action.

Pour obtenir un résultat concret, il faut une **action** :

- **`data_rdd.count()`** → nombre de lignes restantes ;
- **`data_rdd.take(5)`** → aperçu de quelques lignes.

---

## 5. Schéma du flux

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

## 6. Synthèse

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

<a id="26-windowcurrentrow-et-moyenne-mobile-sur-3-snapshots"></a>

# 26. `Window.currentRow` et moyenne mobile sur 3 snapshots

> Notebook : `Spark_DIA3_Session_3.ipynb` — §1.3 Fonctions de fenêtrage (`df_avec_lag`)

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

Et en quoi cela diffère-t-il de `fenetre_station` utilisée pour `LAG` / `LEAD` ?

---

## Réponse

`Window.currentRow` désigne **la ligne en cours de traitement** dans une fenêtre analytique Spark — l'équivalent de « la ligne sur laquelle on calcule la valeur maintenant ».

Dans `rowsBetween(-2, Window.currentRow)`, Spark définit une **fenêtre glissante en nombre de lignes** (pas en temps) :

| Paramètre | Signification |
|---|---|
| `-2` | 2 lignes **avant** la ligne courante |
| `Window.currentRow` | la ligne **courante** |

La fenêtre couvre donc **3 lignes** : les 2 précédentes + la ligne actuelle.

---

## 1. Code du notebook

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

fenetre_station = (
    Window
    .partitionBy("station_id")
    .orderBy("horodatage")
)

fenetre_moy_mobile_3 = (
    Window
    .partitionBy("station_id")
    .orderBy("horodatage")
    .rowsBetween(-2, Window.currentRow)  # courant + 2 précédents = 3 snapshots
)

df_avec_lag = (
    df
    .select("station_id", "nom_station", "horodatage", "taux_occupation")
    .withColumn("taux_precedent", F.lag("taux_occupation", 1).over(fenetre_station))
    .withColumn("taux_suivant", F.lead("taux_occupation", 1).over(fenetre_station))
    .withColumn("moy_mobile_3", F.round(F.avg("taux_occupation").over(fenetre_moy_mobile_3), 4))
)
```

---

## 2. Exemple pas à pas

Pour une même station, triée par `horodatage` :

| Ligne | `taux_occupation` | Fenêtre (-2 → current) | `moy_mobile_3` |
|---|---|---|---|
| 1 | 0.10 | [0.10] | 0.10 |
| 2 | 0.08 | [0.10, 0.08] | 0.09 |
| 3 | 0.06 | [0.10, 0.08, 0.06] | 0.08 |
| 4 | 0.12 | [0.08, 0.06, 0.12] | 0.087 |

À la ligne 4, Spark « oublie » la ligne 1 : la fenêtre ne garde que **3 snapshots**.

---

## 3. `Window.currentRow` vs borne numérique

```python
.rowsBetween(-2, Window.currentRow)                          # 2 lignes avant → ligne courante
.rowsBetween(-2, 0)                                          # équivalent (0 = ligne courante)
.rowsBetween(Window.unboundedPreceding, Window.currentRow)   # du début → ligne courante (cumul)
```

| Constante | Rôle |
|---|---|
| `Window.currentRow` | borne dynamique « ici et maintenant » |
| `Window.unboundedPreceding` | depuis le début de la partition |
| `Window.unboundedFollowing` | jusqu'à la fin de la partition |

---

## 4. Pourquoi deux fenêtres ?

| Fenêtre | Usage | Suffisant pour… |
|---|---|---|
| `fenetre_station` (`partitionBy` + `orderBy`) | `LAG`, `LEAD` | 1 ligne avant / 1 ligne après |
| `fenetre_moy_mobile_3` (+ `rowsBetween`) | `AVG` sur 3 lignes | moyenne mobile |

`partitionBy("station_id").orderBy("horodatage")` seul ne dit pas **combien** de lignes inclure dans une agrégat — d'où `rowsBetween(-2, Window.currentRow)` pour une moyenne sur **3 snapshots**.

---

## 5. Rappel : `LAG` et `LEAD`

| Fonction | Signification |
|---|---|
| `F.lag("taux_occupation", 1)` | valeur du snapshot **précédent** (NULL sur la 1ʳᵉ ligne de la station) |
| `F.lead("taux_occupation", 1)` | valeur du snapshot **suivant** (NULL sur la dernière ligne) |

Ces fonctions de **navigation** n'ont pas besoin de `rowsBetween` : le décalage est donné par le 2ᵉ argument (`1` = une ligne).

---

## 6. Synthèse

| Terme | Signification |
|---|---|
| `Window.currentRow` | la ligne en cours de calcul |
| `rowsBetween(-2, Window.currentRow)` | fenêtre de 3 lignes (2 avant + courante) |
| `moy_mobile_3` | moyenne de `taux_occupation` sur ces 3 snapshots |
| `partitionBy("station_id")` | chaque station a sa propre série temporelle |

En une phrase : **`Window.currentRow` fixe la borne droite de la fenêtre sur la ligne actuelle**, ce qui permet de calculer une moyenne glissante sur les 3 derniers snapshots de chaque station.

---

<a id="27-simulation-batch-merge--décaler-un-horodatage-string"></a>

# 27. Simulation batch MERGE — décaler un `horodatage` string

> Notebook : `Spark_DIA3_Session_3.ipynb` — §1.4 Delta Lake, simulation `df_batch` avant `MERGE INTO`
>
> Voir aussi : [§25 — Inspecter le format de `horodatage`](#25-inspecter-le-format-de-horodatage-avant-to_timestamp), [§13 — Delta Lake](#13-delta-lake-et-le-package-delta-spark)

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

| Étape | Fonction | Rôle |
|---|---|---|
| 1 | `to_timestamp` | string → datetime |
| 2 | `+ INTERVAL 2 YEARS` | décalage temporel |
| 3 | `date_format` | datetime → string au format d'origine |

---

## 3. Pourquoi `.offset(500).limit(50)` ?

Les **500 corrections** et les **50 nouvelles lignes** partent toutes deux de `mois == 1`.

Sans `offset(500)`, les 50 « nouvelles » lignes seraient un **sous-ensemble** des 500 corrections : mêmes `(station_id, horodatage)` dans le batch → risque de doublons avant le `MERGE`.

| Sous-ensemble | Rôle dans le MERGE |
|---|---|
| 500 premières lignes de janvier | `whenMatchedUpdateAll()` — taux corrigé (× 0.98) |
| 50 lignes suivantes, horodatage +2 ans | `whenNotMatchedInsertAll()` — insertions |

---

## 4. `union` vs `unionByName`

```python
df_corrections.unionByName(df_nouveaux).drop("source")
```

`unionByName` aligne les colonnes **par nom**, pas par position — plus sûr quand les deux DataFrames ont la même structure mais un ordre de colonnes potentiellement différent.

---

## 5. Lien avec le `MERGE INTO`

Le batch est ensuite fusionné dans Delta :

```python
delta_table.alias("cible").merge(
    df_batch.alias("source"),
    "cible.station_id = source.station_id AND cible.horodatage = source.horodatage"
)
.whenMatchedUpdateAll()
.whenNotMatchedInsertAll()
.execute()
```

| Ligne du batch | Clé `(station_id, horodatage)` | Effet |
|---|---|---|
| correction | existe déjà dans Delta | **UPDATE** du `taux_occupation` |
| nouvelle (horodatage décalé) | absente de Delta | **INSERT** |

---

## 6. Synthèse

| Problème | Solution |
|---|---|
| `horodatage` est une string | `to_timestamp` avant l'arithmétique temporelle |
| conserver le format Velib' | `date_format(..., "yyyy-MM-dd'T'HH:mm'Z'")` |
| éviter les doublons dans le batch | `offset(500)` sur les nouvelles lignes |
| union de deux DataFrames | `unionByName` |

En une phrase : **pour décaler un `horodatage` string, on passe par `to_timestamp` → `+ INTERVAL` → `date_format`** — on n'additionne jamais l'intervalle directement sur la chaîne.

---

<a id="28-fcolmois--1--filtrer-sur-janvier"></a>

# 28. `F.col("mois") == 1` — filtrer sur janvier

> Notebook : `Spark_DIA3_Session_3.ipynb` — simulation batch MERGE (`df_corrections`, `df_nouveaux`)
>
> Voir aussi : [§27 — Simulation batch MERGE](#27-simulation-batch-merge--décaler-un-horodatage-string)

## Question

Que signifie ce filtre dans la cellule de simulation ?

```python
df_current.filter(F.col("mois") == 1)
```

---

## Réponse

`F.col("mois") == 1` est un **filtre Spark** qui ne conserve que les lignes dont le mois calendaire vaut **1**, c'est-à-dire **janvier**.

Équivalent SQL :

```sql
WHERE mois = 1
```

---

## 1. Décomposition de l'expression

| Morceau | Signification |
|---|---|
| `F` | alias de `pyspark.sql.functions` (importé en haut du notebook) |
| `F.col("mois")` | référence à la colonne **`mois`** du DataFrame |
| `== 1` | condition d'égalité avec la valeur **1** |

On construit une **expression booléenne** (une colonne de type `boolean`) que `.filter()` utilise pour garder les lignes où le résultat est `true`.

---

## 2. D'où vient la colonne `mois` ?

Elle est créée en **Session 2** à partir de `horodatage` lors de l'enrichissement temporel du pipeline :

```python
.withColumn("mois", F.month("ts"))
```

| Valeur de `mois` | Mois calendaire |
|---|---|
| 1 | janvier |
| 2 | février |
| … | … |
| 12 | décembre |

Dans le projet, les données couvrent **2020 et 2021** : `mois == 1` sélectionne donc les snapshots de **janvier 2020 et janvier 2021**.

---

## 3. Pourquoi ce filtre dans le batch MERGE ?

```python
df_corrections = (
    df_current
    .filter(F.col("mois") == 1)
    .limit(500)
    ...
)

df_nouveaux = (
    df_current
    .filter(F.col("mois") == 1)
    .orderBy("station_id", "horodatage")
    .offset(500)
    .limit(50)
    ...
)
```

Objectifs :

- **réduire le volume** du batch simulé (550 lignes au lieu de ~690 000) ;
- garder un exemple **cohérent** pour le `MERGE INTO` ;
- travailler sur un sous-ensemble homogène (même mois) avant de décaler les « nouvelles » lignes de 2 ans (`annee = 2022`, `mois = 1`).

---

## 4. Variantes utiles

```python
# Janvier 2020 seulement
df_current.filter((F.col("mois") == 1) & (F.col("annee") == 2020))

# Syntaxe SQL (chaîne) — équivalent
df_current.filter("mois = 1")

# Plusieurs mois
df_current.filter(F.col("mois").isin(1, 7))
```

**Attention :** pour combiner plusieurs conditions en API DataFrame, utiliser `&` (et) ou `|` (ou) — pas `and` / `or` Python — et mettre chaque condition entre parenthèses :

```python
(F.col("mois") == 1) & (F.col("annee") == 2020)
```

---

## 5. Synthèse

| Écriture | Signification |
|---|---|
| `F.col("mois")` | colonne mois (1–12) |
| `== 1` | janvier uniquement |
| `.filter(...)` | ne garde que les lignes qui vérifient la condition |

En une phrase : **`F.col("mois") == 1` sélectionne les observations Vélib' enregistrées en janvier** — ici pour limiter le batch de démonstration du `MERGE`.
