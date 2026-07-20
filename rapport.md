# Rapport — Chargement d'un CSV avec Spark (`textFile`)

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

# Partitions RDD vs `spark.sql.shuffle.partitions`

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

# Filtrage d'un RDD avec `filter()`

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

# Affichage d'un RDD : `PythonRDD[26] at RDD at PythonRDD.scala:53`

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

# Déballage de liste : `horodatage, capacite, ... = champs`

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

# Conversion booléenne : `operative.lower() == "true"`

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

# Suppression des lignes malformées : `filter(lambda x: x is not None)`

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

# Filtrage par condition : `lambda r: r["taux_occupation"] < 0.10`

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
