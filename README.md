# SparkVelib — ClimaCity Paris (PySpark)

Projet de traitement de données Vélib' avec Apache Spark et PySpark.  
Notebook principal : `Spark_DIA3_Session_1.ipynb` · Notes détaillées : [`rapport.md`](rapport.md)

---

## Installation rapide

```bash
# 1. Environnement Python
python3 -m venv .venv-spark
source .venv-spark/bin/activate
pip install -r requirements.txt

# 2. Données
# Placer historique_stations.csv à la racine du projet (ou dans data/velib/raw/)

# 3. Lancer Jupyter
jupyter notebook Spark_DIA3_Session_1.ipynb
```

Choisir le kernel **venv-spark** dans Jupyter.

---

## Mac Apple Silicon — Java arm64 (OpenJDK 17)

Sur Mac M1/M2/M3, installer Java **natif arm64** avant de lancer Spark :

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

> **Note :** `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` ne fonctionne pas tant qu'OpenJDK n'est pas enregistré dans `/Library/Java/JavaVirtualMachines/` (Homebrew installe en *keg-only*). Utilisez le chemin Homebrew ci-dessus, ou la Section 0 du notebook le configure automatiquement.

Pour rendre `JAVA_HOME` permanent, ajouter la ligne `export` dans `~/.zshrc`.

### Pourquoi ?

Par défaut, macOS utilise souvent un Java **x86_64** (Rosetta), par exemple le plugin Oracle 1.8 :

| Composant | Sans OpenJDK arm64 | Avec OpenJDK 17 arm64 |
|-----------|-------------------|------------------------|
| Driver Python (Jupyter) | arm64 | arm64 |
| JVM Spark | **x86_64** (Rosetta) | **arm64** |
| Workers PySpark | **x86_64** | **arm64** |
| `psutil` (extension native) | arm64 | arm64 |

PySpark lance ses **workers Python** dans le même processus que la JVM. Quand Java tourne en x86_64, les workers héritent de cette architecture, alors que `psutil` (installé via pip dans le venv arm64) ne fournit qu'une bibliothèque native **arm64**.

Résultat : le driver importe `psutil` correctement, mais les workers échouent silencieusement et Spark affiche en boucle :

```
UserWarning: Please install psutil to have better support with spilling
```

Ce warning apparaît surtout lors des opérations de **shuffle** (`reduceByKey`, `join`, `sortBy`), même après un `pip install psutil` réussi.

**OpenJDK 17 via Homebrew** est compilé en arm64 natif. En pointant `JAVA_HOME` dessus, Java, les workers PySpark et `psutil` tournent tous en arm64 — le warning disparaît et Spark gère mieux le spilling mémoire.

### Vérifier que tout est aligné

```bash
java -version          # doit mentionner une JVM arm64 / OpenJDK 17
echo $JAVA_HOME        # doit pointer vers le JDK Homebrew, pas le plugin Oracle
python -c "import psutil; print('psutil OK')"
```

### Contournement dans le notebook

Si vous ne pouvez pas changer de Java tout de suite, la **Section 0** de `Spark_DIA3_Session_1.ipynb` crée automatiquement un wrapper `python-arm64` qui force les workers en arm64. Ce contournement fonctionne, mais installer OpenJDK 17 reste la solution propre.

---

## Fichiers utiles

| Fichier | Description |
|---------|-------------|
| `Spark_DIA3_Session_1.ipynb` | Session 1 — API RDD |
| `rapport.md` | Explications pas à pas du notebook |
| `historique_stations.csv` | Données Vélib' (~376 Mo, non versionné) |
| `TUT-01-SPARK_Ligne-de-Commande.md` | Tutoriel Spark en ligne de commande |
| `requirements.txt` | Dépendances Python (pyspark, jupyter, psutil, …) |
