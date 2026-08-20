# Prompt réutilisable : créer un nouveau concept de stratégie

Ce fichier est un **prompt prêt à copier-coller** pour n'importe quelle IA de code
(Claude Code ou autre) qui n'a **aucun accès au reste du projet**. Il contient tout
ce qu'il faut savoir pour écrire un fichier concept valide, en autonomie complète.

**Mode d'emploi** : complète les lignes `[COMPLÉTER: ...]` tout en haut, puis colle
l'ensemble du prompt (à partir de "SYSTÈME" ci-dessous) à l'IA de ton choix. Elle te
rendra un seul fichier `.py`. Dépose ce fichier dans le dossier `concepts/` du
projet -- rien d'autre à faire, il apparaîtra automatiquement comme concept
sélectionnable dans le constructeur de stratégie.

Si tu es passé par l'interface (bouton « Copier » de l'assistant de stratégie), une
section **« Contexte : données sélectionnées »** a été ajoutée automatiquement à la
fin de ce prompt, listant les données que tu as choisies (et le code source de
chaque plugin concerné) -- garde-la, elle donne à l'IA le contexte exact dont elle a
besoin pour écrire un concept correct.

---

## À compléter avant d'envoyer le prompt

```
Nom du fichier (sans .py, minuscules, underscores)  : [COMPLÉTER: ex. funding_zscore]
Catégorie / section (existante ou nouvelle, libre)  : [COMPLÉTER: ex. "Funding", "Momentum", "Order flow"...]
Nom affiché dans l'interface (label)                : [COMPLÉTER: ex. "Z-score du funding"]
Description courte (affichée dans l'interface)      : [COMPLÉTER: dis en une phrase ce que ce concept mesure]

Quelle information ce concept doit-il produire ?
  [COMPLÉTER: décris en langage libre ce que tu veux obtenir -- ex. "un score qui
  indique à quel point le funding actuel est anormal par rapport à son historique
  récent"]

Paramètres ajustables souhaités (pour la configuration en étape 2) :
  [COMPLÉTER: liste libre -- ex. "une fenêtre de calcul en minutes, avec 30 par
  défaut" -- l'IA les formalise en CONFIG_SCHEMA, voir plus bas]
```

---

## SYSTÈME — prompt à copier-coller intégralement à partir d'ici

Tu vas écrire un unique fichier Python autonome (`concept`) pour un système de
stratégies déjà existant. **Tu n'as pas accès au code de ce projet** — tout ce que
tu dois savoir est décrit ci-dessous, intégralement et sans ambiguïté. Ne suppose
rien d'autre.

### Ce qu'est ce système

Une stratégie de trading se construit en cinq étages, toujours dans le même
ordre :

1. **Concepts** — transforment des données déjà collectées en « information ».
   C'est ce que tu écris ici.
2. **Configuration des concepts** — l'utilisateur ajuste les paramètres que ton
   concept a déclarés (voir CONFIG_SCHEMA plus bas), via un formulaire généré
   automatiquement depuis ta déclaration -- tu n'écris aucune interface.
3. **Microsystèmes** — combinent plusieurs concepts (et/ou des données brutes)
   pour construire la logique de raisonnement de l'analyse.
4. **Variables d'exécution** — les paramètres nécessaires pour exécuter la
   stratégie (quand prendre le trade).
5. **Profil de gestion** — les paramètres nécessaires pour gérer le trade une
   fois pris (stop-loss, take-profit, fixe ou adaptatif...).

**Rien ne s'exécute au moment où tu écris ce fichier.** Ton concept sera importé et
validé tout de suite (pour vérifier qu'il respecte le contrat ci-dessous), mais le
calcul réel (l'appel à `compute()` avec de vraies données) n'a lieu que plus tard,
quand la stratégie tourne dans le module de backtest.

### Le contrat exact (obligatoire, à respecter au caractère près)

Le fichier doit définir exactement deux choses au niveau module :

```python
CONCEPT_INFO = {
    "label": "...",         # obligatoire, str -- nom court affiché dans l'interface
    "description": "...",   # obligatoire, str -- ce que ce concept mesure/produit, en une phrase
    "category": "...",      # optionnel, str -- section sous laquelle le concept apparaît.
                             # Texte libre. Si cette catégorie n'existe pas encore parmi
                             # les autres concepts, elle est créée automatiquement dans
                             # l'interface -- aucune inscription ailleurs n'est nécessaire.
                             # Absent -> catégorie par défaut "Général".
    "detail": "...",        # optionnel, str -- explication plus longue, affichée dans une
                             # bulle d'info au clic sur un petit "i" à côté du concept.
                             # Absent -> pas de bulle affichée pour ce concept.
    "data_sources": [...],  # OBLIGATOIRE, list[str] non vide -- les clés des données que
                             # ce concept consomme. Une clé de source intégrée (ex.
                             # "binance_futures_kline") ou un id de plugin personnalisé
                             # (ex. "example_funding_history") -- exactement les mêmes
                             # clés que celles listées dans la section "Contexte : données
                             # sélectionnées" ci-dessous si elle est présente.
    "config_schema": [...], # optionnel, list -- les paramètres ajustables par
                             # l'utilisateur en étape 2. Défaut [] (rien d'ajustable).
                             # Chaque entrée :
                             # {
                             #     "name": "...",        # obligatoire, identifiant unique dans ce schema
                             #     "type": "number" | "text" | "select",  # obligatoire
                             #     "label": "...",        # obligatoire, affiché dans le formulaire
                             #     "default": ...,        # obligatoire, doit correspondre au type
                             #                            # (number -> int/float, text -> str,
                             #                            # select -> str présent dans "options")
                             #     "description": "...",  # optionnel
                             #     "options": [...],      # obligatoire (non vide) si type == "select",
                             #                            # sinon absent/ignoré
                             # }
}


def compute(context) -> ...:
    ...
```

`context` (objet déjà construit et passé par l'hôte, tu ne le crées jamais toi-même)
expose ces attributs :

- `context.data` — un dict (ou mapping) : `data_sources[i]` -> les données
  correspondantes. La forme exacte de chaque valeur est définie par le futur module
  de backtest, pas par ce contrat -- ne suppose pas une structure précise, limite-toi
  à ce que decrit le "Contexte" ci-dessous s'il est présent.
- `context.config` — un dict : `config_schema[i]["name"]` -> la valeur résolue
  (celle choisie par l'utilisateur, ou la valeur par défaut si non modifiée).
- `context.log(message: str)` — une fonction à appeler avec de courtes lignes de
  statut lisibles par un humain.

### Optionnel — accélérer un backtest long : `required_lookback_seconds`

Si ton concept ne regarde jamais plus qu'une fenêtre glissante de données (par
exemple : il reconstruit des bougies sur les N dernières minutes, ou ne
regarde que les X dernières valeurs), déclare-le en ajoutant au fichier :

```python
def required_lookback_seconds(config: dict) -> float | None:
    ...  # retourne la fenêtre (en secondes) dont ce concept a réellement besoin,
         # en te basant sur `config` (mêmes clés que config_schema) -- ou None
         # si le concept a vraiment besoin de tout l'historique depuis le début.
```

Par défaut (fonction absente), l'hôte donne à `context.data[...]` **tout**
l'historique accumulé depuis le début de la période testée, à chaque étape
d'évaluation -- toujours correct, mais lent si ton `compute()` retraite tout
cet historique à chaque appel (reconstruction de bougies depuis les trades
bruts, par exemple) plutôt que de faire confiance à une fenêtre plus
courte. Si tu déclares `required_lookback_seconds`, l'hôte ne te donne que
cette fenêtre glissante -- même résultat final si la fenêtre est réellement
assez large (prévois une marge confortable, ne vise jamais au plus juste),
mais beaucoup plus rapide sur une longue période. Dans le doute, ne définis
pas cette fonction plutôt que de risquer une fenêtre trop courte -- une
donnée pertinente mais trop ancienne disparaîtrait silencieusement.

### Règles strictes

1. **Un seul fichier** `.py`, autonome, pas de sous-package, pas d'import relatif
   vers d'autres fichiers du projet (tu n'y as pas accès de toute façon).
2. **N'importe que la bibliothèque standard Python** (3.11+), sauf mention
   contraire explicite dans la section "À compléter" ci-dessus.
3. **`compute` doit être une fonction normale (`def`), jamais `async def`.** Un
   concept ne fait jamais d'appel réseau ni n'attend rien -- toutes les données lui
   arrivent déjà prêtes via `context.data`, collectées en amont par une source
   intégrée ou un plugin. Un concept qui contiendrait un appel réseau, une
   connexion, ou tout code `await` serait rejeté par l'hôte au chargement.
4. **`CONCEPT_INFO` doit être un dict littéral au niveau module**, pas construit
   dynamiquement.
5. **`data_sources` ne doit jamais être vide.** Un concept sans donnée n'a rien à
   transformer.
6. **Isolation des erreurs** : si `compute()` lève une exception au moment de
   l'exécution réelle (plus tard, dans le backtest), cela n'affecte que ce concept,
   jamais le reste de la stratégie -- mais gère quand même les cas prévisibles
   (donnée manquante, valeur inattendue) plutôt que de laisser planter sans
   explication.
7. Le fichier ira dans le dossier `concepts/` du projet et ne doit **pas**
   commencer son nom par `_`.

### Exemple complet fonctionnel (à adapter, ne pas copier tel quel)

```python
"""Exemple : un score qui mesure à quel point le funding rate actuel s'écarte de
sa moyenne récente (un simple z-score), à partir de bougies de mark price/funding
déjà collectées."""

from __future__ import annotations

CONCEPT_INFO = {
    "label": "Z-score du funding",
    "category": "Funding",
    "description": "Écart-type du funding rate actuel par rapport à sa moyenne récente.",
    "detail": (
        "Calcule un z-score du funding rate sur une fenêtre glissante -- une "
        "valeur proche de 0 signifie un funding dans la norme récente, une "
        "valeur élevée (positive ou négative) signale une anomalie."
    ),
    "data_sources": ["binance_futures_mark_price"],
    "config_schema": [
        {
            "name": "lookback_minutes", "type": "number", "label": "Fenêtre (minutes)",
            "default": 30, "description": "Sur combien de temps calculer la moyenne/écart-type.",
        },
        {
            "name": "output_key", "type": "text", "label": "Nom du résultat",
            "default": "funding_zscore",
        },
    ],
}


def compute(context) -> dict:
    rows = context.data.get("binance_futures_mark_price") or []
    lookback = context.config["lookback_minutes"]
    output_key = context.config["output_key"]
    context.log(f"calcul du z-score sur {lookback} minutes, {len(rows)} points reçus")
    if len(rows) < 2:
        return {output_key: None}
    rates = [row["funding_rate"] for row in rows[-lookback:]]
    mean = sum(rates) / len(rates)
    variance = sum((r - mean) ** 2 for r in rates) / len(rates)
    stdev = variance ** 0.5
    zscore = 0.0 if stdev == 0 else (rates[-1] - mean) / stdev
    return {output_key: zscore}
```

### Ce que tu dois produire

Un seul bloc de code Python complet, prêt à être enregistré tel quel comme fichier
`.py`, implémentant `CONCEPT_INFO` et `compute(context)` pour l'information décrite
dans la section "À compléter" ci-dessus, en utilisant les données listées dans la
section "Contexte : données sélectionnées" si elle est présente. Pas d'explication
superflue autour du code.

---

*Fin du prompt à copier-coller. Une fois le fichier `.py` obtenu, dépose-le dans
`concepts/` à la racine du projet.*
