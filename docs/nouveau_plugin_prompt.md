# Prompt réutilisable : créer un nouveau plugin de collecte de données

Ce fichier est un **prompt prêt à copier-coller** pour n'importe quelle IA de code
(Claude Code ou autre) qui n'a **aucun accès au reste du projet**. Il contient tout
ce qu'il faut savoir pour écrire un fichier plugin valide, en autonomie complète.

**Mode d'emploi** : complète les lignes `[COMPLÉTER: ...]` tout en haut (laisse vide
celles qui précisent "laisser vide si..." quand ça ne s'applique pas), puis colle
l'ensemble du prompt (à partir de "SYSTÈME" ci-dessous) à l'IA de ton choix. Elle te
rendra un seul fichier `.py`. Dépose ce fichier dans le dossier `plugins/` du projet
— rien d'autre à faire, il apparaîtra automatiquement dans la page "Collecte de
données", classé sous la catégorie que tu auras choisie.

---

## À compléter avant d'envoyer le prompt

```
Nom du fichier (sans .py, minuscules, underscores) : [COMPLÉTER: ex. news_coindesk]
Catégorie / section (existante ou nouvelle, libre) : [COMPLÉTER: ex. "News", "Crypto", "Actions", "Forex", "On-chain"...]
Nom affiché dans l'interface (label)               : [COMPLÉTER: ex. "Actualités CoinDesk"]
Description courte (affichée dans l'interface)      : [COMPLÉTER: dis ce qui est collecté, à quelle fréquence, dans quel fichier]
Description longue (affichée dans la bulle d'info "i", optionnelle) :
  [COMPLÉTER: explique en quelques phrases ce que c'est, d'où ça vient, ce que ça
  permet de faire avec -- laisser vide si la description courte suffit déjà]

Cette donnée est-elle déjà accessible, ou faut-il la collecter en direct ?
  [COMPLÉTER: "déjà accessible" ou "à collecter en direct"]

Si "à collecter en direct" -- source de la donnée (API, flux RSS, site, calcul...) :
  [COMPLÉTER: URL, endpoint, ou méthode de collecte -- laisser vide si "déjà accessible"]

Si "déjà accessible" -- comment y accéder (chemin du fichier/dossier/base, format,
requête à faire...) :
  [COMPLÉTER: ex. "base SQLite dans /home/utilisateur/data/trades.db, table trades,
  colonnes ts/price/qty" ou "dossier de fichiers CSV, un par jour, dans ~/exports/"
  -- laisser vide si "à collecter en direct"]

Détails spécifiques (champs à extraire, fréquence, format de sortie souhaité) : [COMPLÉTER]
```

---

## SYSTÈME — prompt à copier-coller intégralement à partir d'ici

Tu vas écrire un unique fichier Python autonome (`plugin`) pour un système de
collecte de données déjà existant. **Tu n'as pas accès au code de ce projet** — tout
ce que tu dois savoir est décrit ci-dessous, intégralement et sans ambiguïté. Ne
suppose rien d'autre.

### Ce qu'est ce système

Un processus Python (asyncio) tourne en arrière-plan et collecte des données de
marché. Il peut charger des "plugins" : des scripts indépendants qui tournent en
parallèle du reste, chacun s'occupant de SA propre donnée et l'écrivant dans SON
propre fichier. Un plugin peut porter sur absolument n'importe quoi : des news, des
prix d'un autre actif (ETH, actions, forex...), de l'order flow d'un autre exchange,
des métriques on-chain, des indicateurs macro, du sentiment, de la météo — tout ce
qui peut être récupéré par requête réseau ou calculé, à intervalle régulier ou en
flux continu.

Il y a deux façons pour un plugin d'obtenir sa donnée, et ça change tout dans la
façon dont `run()` est écrit :

- **La donnée n'existe pas encore quelque part accessible** : il faut la collecter
  en direct dans le temps (interroger une API, tenir une connexion, calculer
  périodiquement). C'est le mode **"collect"**.
- **La donnée existe déjà** : une base de données que tu as déjà, des fichiers déjà
  sur le disque, un service déjà en cours d'exécution ailleurs sur la même machine.
  Il n'y a rien à collecter "au fil du temps" — il suffit d'aller la lire et de la
  convertir dans un format que l'analyseur peut consommer. C'est le mode
  **"access"**, et il évite de re-collecter en direct une donnée qu'on a déjà.

Les deux modes se déclarent dans `PLUGIN_INFO` (voir plus bas) et déterminent la
forme que doit prendre `run()`.

### Le contrat exact (obligatoire, à respecter au caractère près)

Le fichier doit définir exactement deux choses au niveau module :

```python
PLUGIN_INFO = {
    "label": "...",        # obligatoire, str -- nom court affiché dans l'interface
    "description": "...",  # obligatoire, str -- ce qui est collecté/accédé, comment, à quelle fréquence, dans quel fichier
    "category": "...",     # optionnel, str -- section sous laquelle le plugin apparaît.
                            # Texte libre. Si cette catégorie n'existe pas encore parmi
                            # les autres plugins, elle est créée automatiquement dans
                            # l'interface -- aucune inscription ailleurs n'est nécessaire.
                            # Absent -> catégorie par défaut "Général".
    "mode": "collect",     # optionnel, str -- "collect" (par défaut) ou "access".
                            # "collect" : la donnée doit être récupérée dans le temps
                            # (API, flux, calcul périodique) -- run() boucle jusqu'à l'arrêt.
                            # "access" : la donnée existe déjà quelque part (base de
                            # données, fichiers déjà sur le disque, service déjà en
                            # cours d'exécution) -- run() la lit/convertit une fois et
                            # se termine. Voir la section suivante pour le détail des
                            # deux formes.
    "detail": "...",       # optionnel, str -- explication plus longue, affichée dans
                            # une bulle quand l'utilisateur clique sur un petit "i" à
                            # côté du plugin dans l'interface. Différent de
                            # "description" (toujours visible, doit rester courte) :
                            # "detail" ne s'affiche qu'à la demande, donc peut être
                            # plus long -- dis d'où vient la donnée, ce qu'elle
                            # permet de faire, une limite importante à connaître...
                            # Absent -> pas de bulle affichée pour ce plugin.
}


async def run(context) -> None:
    ...
```

`context` (objet déjà construit et passé par l'hôte, tu ne le crées jamais toi-même)
expose ces attributs :

- `context.data_dir` — un `pathlib.Path` : le dossier dans lequel écrire. **Écris
  uniquement dans ce dossier**, jamais ailleurs sur le disque, jamais de chemin
  absolu arbitraire.
- `context.stop_event` — un `asyncio.Event` : signalé quand la collecte doit
  s'arrêter. Ta boucle doit le vérifier régulièrement et s'arrêter proprement
  quand il est signalé (pas de sortie brutale, pas de fichier à moitié écrit).
  Sans objet en mode "access" (pas de boucle à interrompre).
- `context.log(message: str)` — une fonction à appeler avec de courtes lignes de
  statut lisibles par un humain (ex: `"12 nouvelles lignes récupérées"`,
  `"erreur réseau, nouvelle tentative dans 30s"`). Ces lignes s'affichent en
  direct dans l'interface pendant que la collecte tourne.
- `context.start_ts_ns` / `context.end_ts_ns` — deux `int | None`, uniquement
  pertinents en mode "access". Ce sont les bornes temporelles (epoch en
  nanosecondes) choisies par l'utilisateur dans l'interface "déjà collecté", à
  la place d'une durée. Les deux peuvent être `None` (borne non précisée). En
  mode "collect" elles valent toujours `None` -- ignore-les. En mode "access",
  utilise-les si ta source a une notion de plage temporelle (ex: ne convertir
  que les lignes de la base entre ces deux dates) ; si ta source n'en a pas
  (ex: un seul fichier figé sans horodatage), ignore-les aussi, ce n'est pas
  obligatoire.

### Règles strictes

1. **Un seul fichier** `.py`, autonome, pas de sous-package, pas d'import relatif
   vers d'autres fichiers du projet (tu n'y as pas accès de toute façon).
2. **N'importe que la bibliothèque standard Python** (3.11+) sauf mention contraire
   explicite ci-dessus dans la section à compléter. Préfère `urllib.request`,
   `json`, `csv`, `asyncio`, `time`, `re` etc. Si un paquet tiers est nécessaire
   (ex: `websockets`, déjà utilisé par le projet hôte), dis-le clairement en
   commentaire en tête de fichier pour que l'utilisateur sache quoi vérifier.
3. **Ne bloque jamais la boucle asyncio.** Un appel réseau synchrone
   (`urllib.request.urlopen`, `requests`, etc.) doit être enveloppé dans
   `await asyncio.to_thread(...)`. Un flux réellement asynchrone (`websockets`,
   `aiohttp`) peut être `await`é directement. Ça vaut aussi en mode "access" pour
   une lecture disque/base de données un peu longue (`sqlite3`, `pandas.read_csv`,
   parcours d'un gros dossier) : enveloppe-la dans `asyncio.to_thread(...)` plutôt
   que de la faire directement dans `run()`.
4. **La fonction doit s'appeler exactement `run`**, être `async def`, prendre
   exactement un paramètre (`context`).
5. **`PLUGIN_INFO` doit être un dict littéral au niveau module**, pas construit
   dynamiquement.
6. **Isolation des erreurs** : l'hôte capture déjà toute exception non gérée par
   `run()` et arrête proprement ce plugin sans affecter le reste de la collecte
   — donc pas besoin de `try/except` autour de TOUT, mais gère quand même les
   erreurs réseau attendues (timeout, 4xx/5xx) pour continuer à tourner plutôt
   que de s'arrêter à la première erreur transitoire.
7. **Écris un format de sortie simple et auto-descriptif** — JSONL (une ligne
   JSON par enregistrement, avec un timestamp) est recommandé sauf si l'usage
   précisé plus haut demande autre chose. En mode "collect", n'écrase jamais le
   fichier : ajoute (`"a"` en mode ouverture), pour ne jamais perdre ce qui a déjà
   été collecté pendant cette même exécution. En mode "access", `context.data_dir`
   est de toute façon un dossier neuf à chaque exécution, donc écrire le résultat
   complet en une fois (`"w"`) est correct -- il n'y a rien de précédent à préserver.
8. Le fichier ira dans le dossier `plugins/` du projet et ne doit **pas**
   commencer son nom par `_`.

### Les deux modes, en détail

Choisis le mode d'après la réponse donnée dans "À compléter" ci-dessus. C'est le
choix le plus important du prompt : il détermine toute la forme de `run()`.

#### Mode "collect" — deux formes valides pour la boucle `run()`

**Polling périodique** (le cas le plus courant — API REST, flux RSS, calcul
périodique) :

```python
import asyncio

async def run(context) -> None:
    while not context.stop_event.is_set():
        try:
            ... récupérer une donnée, l'écrire en append dans context.data_dir / "sortie.jsonl" ...
            context.log("...")
        except Exception as exc:
            context.log(f"erreur: {exc!r}")
        try:
            await asyncio.wait_for(context.stop_event.wait(), timeout=INTERVALLE_SECONDES)
        except TimeoutError:
            pass  # normal : c'est l'heure de repasser à la boucle
```

**Flux continu** (websocket propre au plugin, connexion persistante) :

```python
async def run(context) -> None:
    while not context.stop_event.is_set():
        try:
            async with connect(URL) as ws:
                while not context.stop_event.is_set():
                    message = await asyncio.wait_for(ws.recv(), timeout=30)
                    ... écrire la donnée ...
        except Exception as exc:
            context.log(f"reconnexion après erreur: {exc!r}")
            await asyncio.sleep(2)
```

#### Mode "access" — une seule forme valide pour `run()`

Rien à attendre dans le temps : la donnée est déjà là. `run()` lit la source
existante (décrite dans "À compléter" -- chemin de fichier, base de données,
service local...), la convertit dans un format que l'analyseur peut lire (JSONL
recommandé, comme pour le mode "collect"), l'écrit dans `context.data_dir`, logue
un résumé, et **retourne**. Pas de `while`, pas d'attente sur `stop_event` -- l'hôte
sait déjà (via `PLUGIN_INFO["mode"] == "access"`) qu'il ne doit pas s'attendre à ce
que ce plugin continue de tourner :

```python
import asyncio

async def run(context) -> None:
    # context.start_ts_ns / context.end_ts_ns : bornes optionnelles choisies
    # par l'utilisateur (epoch en nanosecondes, chacune peut être None). À
    # utiliser seulement si ta source a une notion de plage temporelle.
    records = await asyncio.to_thread(
        _read_and_convert_existing_source, context.start_ts_ns, context.end_ts_ns,
    )
    output_path = context.data_dir / "sortie.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    context.log(f"terminé : {len(records)} enregistrements convertis depuis la source existante")
    # se termine ici -- l'hôte ne le relance pas et n'attend pas plus longtemps
```

Si la source déjà accessible est volumineuse ou lente à lire en entier (grosse
base de données, gros dossier), rien n'empêche de la lire par blocs et d'écrire au
fur et à mesure -- l'essentiel est qu'il n'y ait pas de boucle infinie ni d'attente
sur `context.stop_event` : c'est une conversion qui se termine, pas une collecte
qui continue.

### Exemple complet fonctionnel — mode "collect" (à adapter, ne pas copier tel quel)

```python
"""Exemple : interroge une API publique toutes les 60s et journalise le résultat."""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request

PLUGIN_INFO = {
    "label": "Exemple générique",
    "category": "Général",
    "mode": "collect",
    "description": "Interroge une API toutes les 60s et écrit chaque réponse en JSONL dans exemple.jsonl.",
    "detail": "Explication plus longue affichée dans la bulle 'i' -- d'où vient la donnée, comment elle est mise à jour, ce qu'on peut en faire.",
}

_URL = "https://example.com/api/endpoint"
_POLL_SECONDS = 60.0


def _fetch() -> dict:
    request = urllib.request.Request(_URL, headers={"User-Agent": "plugin-collecteur"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


async def run(context) -> None:
    output_path = context.data_dir / "exemple.jsonl"
    while not context.stop_event.is_set():
        try:
            data = await asyncio.to_thread(_fetch)
            with output_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"collected_at_ns": time.time_ns(), **data}) + "\n")
            context.log("1 enregistrement collecté")
        except Exception as exc:
            context.log(f"erreur de collecte: {exc!r}")
        try:
            await asyncio.wait_for(context.stop_event.wait(), timeout=_POLL_SECONDS)
        except TimeoutError:
            pass
```

### Exemple complet fonctionnel — mode "access" (à adapter, ne pas copier tel quel)

```python
"""Exemple : convertit une base SQLite déjà existante en JSONL, une seule fois."""

from __future__ import annotations

import asyncio
import json
import sqlite3

PLUGIN_INFO = {
    "label": "Exemple accès base existante",
    "category": "Général",
    "mode": "access",
    "description": "Lit la table trades d'une base SQLite déjà présente sur le disque et la convertit en JSONL.",
    "detail": "Base SQLite alimentée par [préciser la source d'origine]. Contient les trades depuis [préciser la période couverte].",
}

_SOURCE_DB_PATH = "/home/utilisateur/data/trades.db"  # remplace par le chemin réel décrit dans "À compléter"


def _read_and_convert_existing_source(start_ts_ns: int | None, end_ts_ns: int | None) -> list[dict]:
    connection = sqlite3.connect(_SOURCE_DB_PATH)
    try:
        connection.row_factory = sqlite3.Row
        query = "SELECT ts, price, qty FROM trades"
        clauses, params = [], []
        if start_ts_ns is not None:
            clauses.append("ts >= ?")
            params.append(start_ts_ns // 1_000_000)  # adapte l'unité à ta table (ici: ms)
        if end_ts_ns is not None:
            clauses.append("ts <= ?")
            params.append(end_ts_ns // 1_000_000)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        rows = connection.execute(query + " ORDER BY ts", params).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


async def run(context) -> None:
    records = await asyncio.to_thread(
        _read_and_convert_existing_source, context.start_ts_ns, context.end_ts_ns,
    )
    output_path = context.data_dir / "sortie.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    context.log(f"terminé : {len(records)} enregistrements convertis depuis {_SOURCE_DB_PATH}")
```

### Ce que tu dois produire

Un seul bloc de code Python complet, prêt à être enregistré tel quel comme fichier
`.py`, implémentant `PLUGIN_INFO` et `run(context)` pour la donnée décrite dans la
section "À compléter" ci-dessus -- avec `PLUGIN_INFO["mode"]` et la forme de
`run()` qui correspondent à la réponse donnée à "cette donnée est-elle déjà
accessible, ou faut-il la collecter en direct ?". Pas d'explication superflue autour du code sauf
si un paquet tiers est requis (auquel cas, dis-le clairement en une phrase avant
le code).

---

*Fin du prompt à copier-coller. Une fois le fichier `.py` obtenu, dépose-le dans
`plugins/` à la racine du projet.*
