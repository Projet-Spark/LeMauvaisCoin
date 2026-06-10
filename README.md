# LeMauvaisCoin — Streaming de graphes avec PySpark

## Description

**LeMauvaisCoin** est un projet de traitement de données en temps réel simulant une plateforme de petites annonces de type LeBonCoin. Un générateur produit en continu des événements d'interaction utilisateur (AIME, VOUT, ACHAT) sur des produits mis en vente par des vendeurs. Ces événements sont ingérés et traités par **PySpark Structured Streaming**, modélisés sous forme de **graphe** à l'aide de **GraphFrames**, puis visualisés dynamiquement dans un **dashboard interactif Dash + Cytoscape**.

L'objectif pédagogique est de mettre en pratique les concepts fondamentaux du streaming distribué (watermark, fenêtres glissantes, modes de sortie) et de la théorie des graphes appliquée à des données temps réel (degrés, PageRank).

---

## Architecture du pipeline

```
┌──────────────────────┐
│   generateur.py      │  Serveur TCP — émet 2 événements JSON/sec
│   (simulateur TCP)   │  sur localhost:9999
└──────────┬───────────┘
           │  socket TCP (JSON)
           ▼
┌──────────────────────┐
│  data_streaming.py   │  SparkSession + lecture socket TCP
│  (Structured         │  Schema enforcement strict
│   Streaming)         │  withWatermark("timestamp", "5 minutes")
└──────────┬───────────┘
           │  DataFrame streamé
           ▼
┌──────────────────────┐
│ data_transformation  │  Sliding window (1 min / 30s slide)
│       .py            │  Construction incrémentale du GraphFrame
│  (GraphFrames +      │  Calcul degrés (in/out) + PageRank
│   agrégations)       │  Stockage de l'état dans state.py
└──────────┬───────────┘
           │  état partagé (state.py)
           ▼
┌──────────────────────┐
│   dashboard.py       │  Dash + Cytoscape, refresh 5s
│   (visualisation)    │  Noeuds colorés par type, taille proportionnelle au degré
└──────────────────────┘
```

L'orchestration est assurée par `main.py` via du **threading Python** : le générateur TCP et le dashboard Dash tournent en daemon threads, Spark occupe le thread principal.

---

## Structure des fichiers

```
LeMauvaisCoin/
├── generateur.py          # Serveur TCP — génération des événements JSON
├── data_streaming.py      # SparkSession, lecture socket, watermark
├── data_transformation.py # Fenêtres, GraphFrames, PageRank
├── state.py               # État partagé Spark <-> Dashboard
├── dashboard.py           # Application Dash + Cytoscape
└── main.py                # Point d'entrée — orchestration des threads
```

### Détail des fichiers

| Fichier | Rôle |
|---|---|
| `generateur.py` | Serveur TCP qui émet des événements JSON à 2 events/sec. Champs : `timestamp`, `user_id`, `user_city`, `product_id`, `product_cat`, `seller_id`, `action_type` (AIME/VOUT/ACHAT), `price` |
| `data_streaming.py` | SparkSession configurée (4 shuffle partitions, 2 Go driver memory, jar GraphFrames), lecture socket TCP sur `localhost:9999`, application du schéma strict et du watermark |
| `data_transformation.py` | Fenêtre glissante (1 min / 30s) par `action_type`, construction du GraphFrame avec 3 types de noeuds et 3 types d'arêtes, calcul des degrés entrant/sortant et PageRank (maxIter=5), mise à jour de `state.py` |
| `state.py` | Variables partagées entre les threads Spark et Dash : `all_vertices`, `all_edges`, `graph_metrics` |
| `dashboard.py` | Application Dash avec composant Cytoscape, noeuds colorés par type (gris=USER, vert=SELLER, bleu=PRODUCT), arêtes colorées par relation, rafraîchissement toutes les 5 secondes, taille des noeuds proportionnelle au degré |
| `main.py` | Orchestration : 2 queries Spark (agrégation temporelle en mode `update` + construction de graphe via `foreachBatch`), threads daemon pour le générateur et le dashboard |

---

## Modèle de graphe

### Types de noeuds (vertices)

| Type | Couleur | Description |
|---|---|---|
| USER | Gris | Utilisateur ayant effectué une action |
| SELLER | Vert | Vendeur proposant des produits |
| PRODUCT | Bleu | Produit mis en vente |

Schéma d'un vertex : `(id, type, label)`

### Types d'arêtes (edges)

| Relation | Source -> Destination | Déclencheur |
|---|---|---|
| AIME / VOUT / ACHAT | USER -> PRODUCT | Toute action utilisateur sur un produit |
| ACHAT | USER -> SELLER | Transaction finalisée uniquement (pas VOUT) |
| PROPOSE | SELLER -> PRODUCT | Toujours (relation structurelle) |

Schéma d'une arête : `(src, dst, relation)`

---

## Concepts PySpark utilisés

### SparkSession et configuration

```python
SparkSession.builder
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.memory", "2g")
    .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13")
```

Le nombre de partitions de shuffle est réduit à 4 (contre 200 par défaut) car le volume de données est faible en mode local. Cela évite la surcharge due à la création de trop nombreuses tâches vides.

### Structured Streaming et schema enforcement

Le schéma est défini statiquement (`StructType`) avant la lecture. Spark Structured Streaming **exige** un schéma explicite sur les sources socket afin de garantir la cohérence du pipeline, contrairement à un `inferSchema` qui n'est pas supporté en streaming.

### withWatermark — gestion des retards

```python
.withWatermark("timestamp", "5 minutes")
```

Le watermark indique à Spark qu'il peut ignorer les événements arrivant avec plus de 5 minutes de retard par rapport à l'événement le plus récent vu. Cela permet à Spark de **libérer la mémoire d'état** en toute sécurité sans risquer de perdre des données encore attendues.

### Sliding Window — agrégation temporelle

```python
window("timestamp", "1 minute", "30 seconds")
```

Chaque batch produit des comptages par type d'action sur une fenêtre de 1 minute, recalculée toutes les 30 secondes. Les fenêtres se chevauchent, ce qui donne une vue lissée de l'activité.

### Output mode "update"

```python
.outputMode("update")
```

Seules les lignes dont la valeur a changé dans le batch courant sont émises. Ce mode est utilisé pour les agrégations avec fenêtres : il est plus efficace que `complete` (qui réémettrait tout l'état) et plus précis que `append` (qui n'émet que les lignes finalisées, inadapté aux fenêtres glissantes).

### foreachBatch — construction incrémentale du graphe

```python
.foreachBatch(process_batch)
```

`foreachBatch` expose chaque micro-batch sous forme de DataFrame statique, ce qui permet d'utiliser des opérations non supportées nativement en streaming (comme GraphFrames). C'est le seul moyen d'intégrer GraphFrames dans un pipeline Structured Streaming.

### GraphFrames — degrés et PageRank

```python
g = GraphFrame(vertices, edges)
g.inDegrees
g.outDegrees
g.pageRank(resetProbability=0.15, maxIter=5)
```

Le **PageRank** est calculé avec un nombre d'itérations limité à 5 pour rester compatible avec les contraintes temps réel de chaque micro-batch. Les degrés entrant et sortant permettent de dimensionner visuellement les noeuds dans le dashboard.

---

## Justification des choix techniques

### Sliding window vs Tumbling window

Une **fenêtre glissante** (sliding window, ici 1 min / 30s) a été choisie plutôt qu'une fenêtre tumbling (disjointe) pour plusieurs raisons :

- **Lissage des agrégations** : une fenêtre tumbling peut produire des sauts brutaux de comptage à chaque nouvelle fenêtre. La fenêtre glissante donne une vue continue et plus représentative de l'activité récente.
- **Détection de tendances** : avec un slide de 30 secondes, on peut observer si une catégorie de produit monte en popularité de façon progressive, ce qui serait masqué par une fenêtre tumbling.
- **Cohérence avec le cas d'usage** : sur une plateforme d'annonces, l'intérêt est de suivre l'évolution de l'activité en continu, pas sur des tranches discrètes et indépendantes.

### Output mode "update" plutôt que "complete" ou "append"

- `complete` réémettrait l'intégralité des résultats agrégés à chaque batch, ce qui est inutilement coûteux pour un dashboard qui n'affiche que les données récentes.
- `append` n'émet que les lignes définitivement finalisées. Or, avec une fenêtre glissante et un watermark, une fenêtre n'est finalisée qu'après le délai du watermark (5 min). Les données seraient trop tardives pour un dashboard temps réel.
- `update` est le compromis optimal : seules les fenêtres modifiées dans le batch courant sont émises, ce qui réduit le volume de données transmises tout en maintenant la fraîcheur de l'affichage.

### withWatermark de 5 minutes

La valeur de 5 minutes correspond à un équilibre entre trois contraintes :

- **Tolérance aux retards réseau** : dans un environnement local ou distribué réel, des événements peuvent arriver légèrement hors ordre. 5 minutes couvre les scénarios de retard raisonnables sans être excessif.
- **Consommation mémoire** : Spark doit conserver en mémoire l'état de toutes les fenêtres potentiellement concernées par des événements tardifs. Un watermark trop long (ex. 1 heure) ferait exploser la mémoire d'état.
- **Cohérence avec la fenêtre d'agrégation** : la fenêtre d'agrégation est de 1 minute. Un watermark de 5 minutes garantit que chaque fenêtre a le temps d'être complètement remplie avant d'être finalisée, avec une marge confortable de 4 fenêtres supplémentaires.

### foreachBatch pour la construction du graphe

GraphFrames est une bibliothèque conçue pour des **DataFrames statiques**. Elle n'est pas compatible avec l'API de streaming native de Spark (pas de support de `writeStream` direct avec GraphFrames). `foreachBatch` contourne cette limitation en exposant chaque micro-batch comme un DataFrame Spark classique, permettant ainsi :

- La construction d'un `GraphFrame` statique par batch
- Le calcul du PageRank et des degrés sur chaque batch
- La fusion incrémentale avec l'état global stocké dans `state.py`
- L'extensibilité : le graphe s'enrichit au fil des batches sans repartir de zéro à chaque micro-batch

---

## Prérequis

| Composant | Version minimale |
|---|---|
| Python | 3.12 |
| PySpark | 3.5 |
| Java | 11+ |
| Apache Spark | 3.5.x |

### Packages Python

```
pyspark>=3.5
graphframes
dash
dash-cytoscape
```

### JAR GraphFrames

Le JAR est téléchargé automatiquement au premier lancement via la configuration Spark :

```
graphframes:graphframes:0.8.4-spark3.5-s_2.13
```

Aucune installation manuelle du JAR n'est nécessaire.

---

## Installation

### 1. Cloner le dépôt

```bash
git clone <url-du-depot>
cd LeMauvaisCoin
```

### 2. Créer un environnement virtuel

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

### 3. Installer les dépendances Python

```bash
pip install "pyspark>=3.5" graphframes dash dash-cytoscape
```

### 4. Vérifier Java

```bash
java -version
# Doit afficher OpenJDK 11 ou supérieur
```

Si Java n'est pas installé :

```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# macOS (Homebrew)
brew install openjdk@11
```

### 5. Configurer JAVA_HOME si nécessaire

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

---

## Lancement

```bash
python main.py
```

Le script `main.py` démarre automatiquement dans l'ordre suivant :

1. Le **serveur TCP** (`generateur.py`) en thread daemon — écoute sur `localhost:9999`
2. Le **dashboard Dash** (`dashboard.py`) en thread daemon — accessible sur `http://localhost:8050`
3. La **session Spark** avec les deux queries de streaming en thread principal :
   - Query 1 : agrégation temporelle par fenêtre glissante (output mode: update)
   - Query 2 : construction et analyse du graphe (foreachBatch)

Le dashboard se rafraîchit toutes les 5 secondes et reflète l'état courant du graphe.

Pour arrêter le programme : `Ctrl+C`

---

## Visualisation du graphe

Le dashboard est accessible à l'adresse `http://localhost:8050` après le lancement.

### Graphe (zone centrale)

- **Noeuds gris** : utilisateurs (USER)
- **Noeuds verts** : vendeurs (SELLER)
- **Noeuds bleus** : produits (PRODUCT)
- **Taille des noeuds** : proportionnelle au degré total cumulé (in + out), mise à l'échelle entre 22 et 100 px
- **Arêtes dirigées** : flèches `bezier` indiquant le sens de la relation
- **Couleur des arêtes** : jaune=AIME, orange=VOUT, rouge=ACHAT, marron=PROPOSE
- **Rafraîchissement** : automatique toutes les 5 secondes (paramétrable via `dcc.Interval`)

### Panneau latéral gauche

| Section | Contenu |
|---|---|
| **Nœud sélectionné** | Cliquer sur un nœud affiche son type, son identifiant, ses degrés entrant/sortant et son score PageRank |
| **Top 5 PageRank** | Classement mis à jour toutes les 5s des 5 nœuds les plus influents du graphe courant |
| **Légende** | Correspondance couleurs / types de nœuds et d'arêtes |

### Déduplication des arêtes

Les arêtes identiques `(src, dst, relation)` sont dédupliquées à chaque micro-batch pour éviter l'accumulation de doublons dans l'état global. Seule la première occurrence d'une connexion est conservée.

---

## Métriques de performance du traitement de flux

Les métriques suivantes ont été relevées via le Spark UI (`http://localhost:4040`) après environ 2 minutes d'exécution en mode `local[*]` sur une machine de développement standard.

### Conditions de mesure

- Générateur : 2 événements/sec (intervalle de 0,5s entre chaque event)
- Trigger Spark : `processingTime = "5 seconds"`
- Fenêtre d'agrégation : 1 minute glissante / slide 30s
- Watermark : 5 minutes

### Résultats observés

| Métrique | Valeur observée | Interprétation |
|---|---|---|
| **Débit d'entrée** (régime stable) | ~50 records/sec | Accumulation initiale résorbée |
| **Débit de traitement** | ~80–90 records/sec | Supérieur au débit d'entrée |
| **Durée du 1er batch** | ~6 000 ms | Warmup JVM + init GraphFrames + téléchargement JAR |
| **Durée batch (régime stable)** | ~2 000–2 500 ms | Bien en dessous du trigger de 5 000 ms |
| **Marge avant backpressure** | ~2 500 ms/batch | Pipeline non saturé |
| **Backpressure** | Aucun | Process Rate > Input Rate en permanence |
| **Input Rows (pic initial)** | ~400 rows | Accumulation TCP pendant l'initialisation Spark |
| **Input Rows (régime stable)** | ~10–15 rows/batch | Cohérent avec 2 events/sec × 5s de trigger |

### Analyse

**Absence de backpressure** : le débit de traitement (~85 records/sec) reste constamment supérieur au débit d'entrée (~50 records/sec). Le pipeline n'accumule pas de retard, ce qui confirme que les paramètres choisis (4 partitions shuffle, trigger 5s) sont adaptés au volume généré.

**Pic initial** : le premier batch traite ~400 rows au lieu de ~10. Ce pic s'explique par le buffering TCP durant les ~30 secondes d'initialisation de Spark (démarrage JVM, résolution des dépendances GraphFrames, allocation mémoire). La durée de ce premier batch (~6s) dépasse le trigger de 5s, mais c'est le seul batch concerné — le régime stable s'installe dès le deuxième batch.

**Durée stable** (~2,5s) vs trigger (5s) : la marge de ~2,5s par batch représente 50% de capacité inutilisée. Cela laisse de la place pour absorber un pic de charge ou pour augmenter `maxIter` du PageRank sans risquer de dépasser le trigger.

**Operation Duration** : la décomposition par opération montre que GraphFrames (construction + PageRank) représente la majorité de la durée de traitement. Le coût du PageRank avec `maxIter=5` est acceptable en régime stable (~1,5s sur les ~2,5s totales).

---

## Limitations connues

- Le calcul de PageRank avec `maxIter=5` est une approximation volontaire pour rester dans les contraintes temps réel de chaque micro-batch. Pour des résultats plus précis, augmenter `maxIter` au détriment de la latence.
- L'état partagé via `state.py` (variables globales Python) est adapté à une exécution mono-machine. Pour un déploiement distribué, remplacer par un store externe (Redis, Kafka, base de données).
- La source socket TCP (`localhost:9999`) est destinée à la démonstration. En production, utiliser une source Kafka ou Kinesis.
- Le mode `local[*]` de Spark utilise tous les coeurs disponibles sur la machine locale. Ne pas déployer en mode cluster sans adapter la configuration réseau et les paramètres de partitionnement.
