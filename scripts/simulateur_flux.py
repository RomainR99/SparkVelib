#!/usr/bin/env python3
"""Simulateur de flux Velib' pour Spark Structured Streaming (file source).

Rejoue le Parquet consolidé en écrivant des fichiers JSON dans un répertoire
surveillé par spark.readStream.json().

Usage:
    python scripts/simulateur_flux.py --output data/output/stream_input --vitesse 3
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any


COLONNES = [
    "station_id",
    "nom_station",
    "code_arr",
    "capacite",
    "velos_meca",
    "velos_elec",
    "bornettes_libres",
    "horodatage",
]


def _racine_projet() -> Path:
    return Path(__file__).resolve().parents[1]


def _source_par_defaut() -> Path:
    for base in (_racine_projet() / "data", _racine_projet().parent / "data"):
        candidate = base / "output" / "disponibilite_consolidee.parquet"
        if candidate.exists():
            return candidate
    return _racine_projet() / "data" / "output" / "disponibilite_consolidee.parquet"


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep="T", timespec="seconds")
    return value


def _normaliser_ligne(row: dict[str, Any]) -> dict[str, Any]:
    return {col: _json_default(row[col]) for col in COLONNES if col in row}


def charger_snapshots(source: Path) -> list[dict[str, Any]]:
    """Lit le Parquet consolidé via PySpark (déjà installé dans le venv cours)."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("simulateur-flux")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(str(source)).select(*COLONNES).orderBy("horodatage")
    lignes = [_normaliser_ligne(row.asDict(recursive=True)) for row in df.collect()]
    spark.stop()
    return lignes


def grouper_par_horodatage(lignes: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for ligne in lignes:
        ts = str(ligne["horodatage"])
        buckets.setdefault(ts, []).append(ligne)
    return sorted(buckets.items(), key=lambda item: item[0])


def ecrire_fichier(chemin: Path, enregistrements: list[dict[str, Any]]) -> None:
    with chemin.open("w", encoding="utf-8") as f:
        json.dump(enregistrements, f, ensure_ascii=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rejoue disponibilite_consolidee.parquet en fichiers JSON (file source)."
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Répertoire de sortie (ex. data/output/stream_input)",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=None,
        help="Parquet source (défaut: data/output/disponibilite_consolidee.parquet)",
    )
    parser.add_argument(
        "--vitesse",
        type=float,
        default=3.0,
        help="Minutes d'historique simulées par seconde réelle (défaut: 3)",
    )
    parser.add_argument(
        "--stations-par-fichier",
        type=int,
        default=80,
        help="Nombre max de stations par fichier JSON",
    )
    parser.add_argument(
        "--boucle",
        action="store_true",
        help="Rejouer indéfiniment les données (Ctrl+C pour arrêter)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source = args.source or _source_par_defaut()

    if not source.exists():
        print(
            f"[ERREUR] Source introuvable : {source.resolve()}\n"
            "Exécutez Spark_DIA3_Session_2.ipynb §2.8 (Parquet consolidé) avant le simulateur.",
            file=sys.stderr,
        )
        return 1

    args.output.mkdir(parents=True, exist_ok=True)
    pause = 1.0 / max(args.vitesse, 0.1)

    print(f"[INFO] Chargement : {source.resolve()}")
    buckets = grouper_par_horodatage(charger_snapshots(source))
    print(f"[INFO] {len(buckets)} horodatages — sortie : {args.output.resolve()}")
    print(f"[INFO] Vitesse : {args.vitesse} min historiques / sec réelle")
    print("[INFO] Ctrl+C pour arrêter.\n")

    file_idx = 0
    try:
        while True:
            for horodatage, enregistrements in buckets:
                for offset in range(0, len(enregistrements), args.stations_par_fichier):
                    chunk = enregistrements[offset : offset + args.stations_par_fichier]
                    file_idx += 1
                    path = args.output / f"snapshot_{file_idx:06d}.json"
                    ecrire_fichier(path, chunk)
                    print(
                        f"[{file_idx:06d}] {horodatage} — "
                        f"{len(chunk)} station(s) → {path.name}"
                    )
                    time.sleep(pause)
            if not args.boucle:
                print("\n[INFO] Fin du jeu de données.")
                break
            print("\n[INFO] Nouvelle boucle…")
    except KeyboardInterrupt:
        print("\n[INFO] Simulateur arrêté.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
