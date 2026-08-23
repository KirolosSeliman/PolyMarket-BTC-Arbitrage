# Prompt réutilisable : créer un nouveau microsystème de stratégie

Ce fichier est un **prompt prêt à copier-coller** pour n'importe quelle IA de code
(Claude Code ou autre) qui n'a **aucun accès au reste du projet**. Il contient tout
ce qu'il faut savoir pour écrire un fichier microsystème valide, en autonomie
complète.

**Mode d'emploi** : complète les lignes `[COMPLÉTER: ...]` tout en haut, puis colle
l'ensemble du prompt (à partir de "SYSTÈME" ci-dessous) à l'IA de ton choix. Elle te
rendra un seul fichier `.py`. Dépose ce fichier dans le dossier `microsystems/` du
projet -- rien d'autre à faire, il apparaîtra automatiquement comme microsystème
sélectionnable dans le constructeur de stratégie.

Si tu es passé par l'interface (bouton « Copier » de l'assistant de stratégie), une
section **« Contexte : données et concepts sélectionnés »** a été ajoutée
automatiquement à la fin de ce prompt, listant les données et les concepts que tu as
choisis (avec le code source de chaque plugin/concept concerné) -- garde-la, elle
donne à l'IA le contexte exact dont elle a besoin pour écrire un microsystème
correct.

---

## À compléter avant d'envoyer le prompt

```
Nom du fichier (sans .py, minuscules, underscores)  : [COMPLÉTER: ex. trend_reasoning]
Catégorie / section (existante ou nouvelle, libre)  : [COMPLÉTER: ex. "Tendance", "Régime de marché"...]
Nom affiché dans l'interface (label)                : [COMPLÉTER: ex. "Raisonnement de tendance"]
Description courte (affichée dans l'interface)      : [COMPLÉTER: dis en une phrase ce que ce microsystème conclut]

Comment ce microsystème doit-il raisonner à partir des concepts (et/ou des
données) qu'il reçoit ?
  [COMPLÉTER: décris en langage libre la logique -- ex. "si le z-score du funding
  dépasse un seuil ET que le momentum est positif, considérer la tendance comme
  haussière"]

Paramètres ajustables souhaités :
  [COMPLÉTER: liste libre -- ex. "un seuil de déclenchement, avec 1.5 par défaut"]
```

---

## SYSTÈME — prompt à copier-coller intégralement à partir d'ici

Tu vas écrire un unique fichier Python autonome (`microsystème`) pour un système de
stratégies déjà existant. **Tu n'as pas accès au code de ce projet** — tout ce que
tu dois savoir est décrit ci-dessous, intégralement et sans ambiguïté. Ne suppose
rien d'autre.

### Ce qu'est ce système

Une stratégie de trading se construit en cinq étages, toujours dans le même
ordre : concepts (transforment des données en information) → configuration des
concepts → **microsystèmes** (c'est ce que tu écris ici) → variables
d'exécution → profil de gestion.

Un microsystème est à un concept ce qu'un concept est à une donnée : la même
relation, un cran au-dessus. Il combine un ou plusieurs concepts déjà définis
(et/ou des données brutes directement) pour construire la logique de raisonnement
de l'analyse -- c'est là que la réflexion sur "qu'est-ce que ces signaux veulent
dire ensemble" prend forme. Une stratégie est constituée d'un ou plusieurs
microsystèmes.

**Rien ne s'exécute au moment où tu écris ce fichier** -- comme pour un concept, le
calcul réel n'a lieu que plus tard, dans le module de backtest.

### Le contrat exact (obligatoire, à respecter au caractère près)

```python
MICROSYSTEM_INFO = {
    "label": "...",           # obligatoire, str
    "description": "...",     # obligatoire, str -- ce que ce microsystème conclut, en une phrase
    "category": "...",        # optionnel, str -- défaut "Général", même règle de
                               # regroupement libre que pour les concepts.
    "detail": "...",          # optionnel, str -- bulle d'info. Absent -> pas de bulle.
    "concept_inputs": [...],  # optionnel, list[str] -- les ids des concepts que ce
                               # microsystème reçoit en entrée. Défaut [].
    "data_inputs": [...],     # optionnel, list[str] -- des clés de données brutes lues
                               # directement (même espace de clés que data_sources d'un
                               # concept). Défaut [].
    # concept_inputs et data_inputs ne peuvent pas être vides tous les deux -- un
    # microsystème sans rien en entrée n'a rien sur quoi raisonner.
    "config_schema": [...],   # optionnel, même forme que pour un concept (voir
                               # docs/nouveau_concept_prompt.md). Défaut [].
}


def compute(context) -> ...:
    ...
```

`context` expose :

- `context.concepts` — un dict : `concept_inputs[i]` -> le résultat déjà calculé de
  ce concept (ce que sa propre fonction `compute()` a retourné).
- `context.data` — un dict : `data_inputs[i]` -> les données correspondantes, même
  principe que `context.data` pour un concept.
- `context.config` — un dict : `config_schema[i]["name"]` -> la valeur résolue.
- `context.log(message: str)` — statut lisible par un humain.

### Optionnel — accélérer un backtest long : `required_lookback_seconds`

Même mécanisme que pour un concept (voir `docs/nouveau_concept_prompt.md`) :
si ce microsystème lit lui-même des `data_inputs` directement (pas seulement
via des concepts) et ne regarde jamais plus qu'une fenêtre glissante,
déclare :

```python
def required_lookback_seconds(config: dict) -> float | None:
    ...  # fenêtre en secondes, ou None si tout l'historique est nécessaire
```

Absent par défaut (tout l'historique, toujours correct mais potentiellement
plus lent sur une longue période). Ne concerne que les `data_inputs` propres
au microsystème -- chaque concept auquel il est branché gère sa propre
fenêtre indépendamment via sa propre déclaration.

### Important -- si tu veux qu'un profil d'exécution puisse agir sur le prix

Un profil d'exécution (l'étage suivant) ne reçoit **jamais** `context.concepts`
directement, seulement `context.microsystems` -- la sortie de *ton* `compute()`,
rien d'autre. Si un concept que tu utilises expose déjà un prix courant (beaucoup
le font, sous le nom `last_price`), ça ne "remonte" pas tout seul jusqu'au profil
d'exécution : ce n'est visible que si **toi** tu le recopies explicitement dans le
dict que tu retournes.

Beaucoup de profils d'exécution ont besoin de confirmer une condition contre le
prix actuel (un rebond, une cassure, ...) et cherchent un champ nommé
`last_price` (et parfois `last_high`/`last_low`) n'importe où dans la sortie du
microsystème. Si ton microsystème ne l'expose jamais, un tel profil d'exécution
restera **silencieusement neutre pour toujours** -- aucune erreur, juste aucun
trade, jamais, quel que soit le scénario. Si l'un des concepts que tu reçois
expose déjà `last_price` (ou une donnée équivalente), pense à le recopier dans
ton propre retour, par exemple :

```python
def compute(context) -> dict:
    ...
    return {
        "...": ...,
        "last_price": mon_concept_result.get("last_price"),  # propage-le, ne le recalcule pas
    }
```

Ce n'est pas obligatoire (un microsystème purement informatif n'a pas besoin
d'exposer de prix), mais si tu comptes brancher un profil d'exécution qui réagit
au prix, c'est le point le plus facile à oublier.

### Règles strictes

1. **Un seul fichier** `.py`, autonome, aucun import relatif vers d'autres fichiers
   du projet.
2. **N'importe que la bibliothèque standard Python** (3.11+), sauf mention
   contraire explicite ci-dessus.
3. **`compute` doit être une fonction normale (`def`), jamais `async def`** — même
   règle et même raison qu'un concept : tout arrive déjà calculé, rien à attendre.
4. **`MICROSYSTEM_INFO` doit être un dict littéral au niveau module.**
5. **`concept_inputs` et `data_inputs` ne peuvent pas être vides tous les deux.**
6. **Isolation des erreurs** : une exception dans `compute()` (plus tard, à
   l'exécution réelle) n'affecte que ce microsystème, jamais le reste de la
   stratégie.
7. Le fichier ira dans le dossier `microsystems/` du projet et ne doit **pas**
   commencer son nom par `_`.

### Exemple complet fonctionnel (à adapter, ne pas copier tel quel)

```python
"""Exemple : combine deux concepts (un z-score de funding et un momentum de prix)
en une conclusion de tendance simple par seuil."""

from __future__ import annotations

MICROSYSTEM_INFO = {
    "label": "Raisonnement de tendance",
    "category": "Tendance",
    "description": "Combine funding z-score et momentum en un signal de tendance.",
    "detail": (
        "Considère la tendance comme haussière si le z-score du funding et le "
        "momentum sont tous les deux au-dessus de leurs seuils respectifs, "
        "baissière dans le cas symétrique, neutre sinon."
    ),
    "concept_inputs": ["funding_zscore", "price_momentum"],
    "config_schema": [
        {
            "name": "zscore_threshold", "type": "number", "label": "Seuil du z-score",
            "default": 1.5,
        },
    ],
}


def compute(context) -> str:
    zscore = context.concepts.get("funding_zscore", {}).get("funding_zscore")
    momentum = context.concepts.get("price_momentum", {}).get("momentum")
    threshold = context.config["zscore_threshold"]
    context.log(f"zscore={zscore} momentum={momentum} threshold={threshold}")
    if zscore is None or momentum is None:
        return "neutre"
    if zscore > threshold and momentum > 0:
        return "haussier"
    if zscore < -threshold and momentum < 0:
        return "baissier"
    return "neutre"
```

### Ce que tu dois produire

Un seul bloc de code Python complet, prêt à être enregistré tel quel comme fichier
`.py`, implémentant `MICROSYSTEM_INFO` et `compute(context)` pour la logique décrite
dans la section "À compléter" ci-dessus, en te basant sur les concepts/données
listés dans la section "Contexte" si elle est présente. Pas d'explication superflue
autour du code.

---

*Fin du prompt à copier-coller. Une fois le fichier `.py` obtenu, dépose-le dans
`microsystems/` à la racine du projet.*
