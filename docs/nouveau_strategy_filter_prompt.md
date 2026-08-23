# Prompt réutilisable : créer un nouveau filtre de stratégie

Ce fichier est un **prompt prêt à copier-coller** pour n'importe quelle IA de code
(Claude Code ou autre) qui n'a **aucun accès au reste du projet**. Il contient tout
ce qu'il faut savoir pour écrire un fichier de filtre de stratégie valide, en
autonomie complète.

**Mode d'emploi** : complète les lignes `[COMPLÉTER: ...]` tout en haut, puis colle
l'ensemble du prompt (à partir de "SYSTÈME" ci-dessous) à l'IA de ton choix. Elle te
rendra un seul fichier `.py`. Dépose ce fichier dans le dossier `filter_profiles/`
du projet -- rien d'autre à faire, il apparaîtra automatiquement comme filtre
sélectionnable à la dernière étape du constructeur de stratégie.

Contrairement au prompt de concept ou de microsystème, ce prompt-ci ne dépend
d'aucune sélection préalable de données/concepts -- un filtre ne déclare que des
paramètres, jamais de dépendance directe à une donnée ou un concept.

---

## À compléter avant d'envoyer le prompt

```
Nom du fichier (sans .py, minuscules, underscores)  : [COMPLÉTER: ex. reward_risk_minimum]
Catégorie / section (existante ou nouvelle, libre)  : [COMPLÉTER: ex. "Gestion du risque"]
Nom affiché dans l'interface (label)                : [COMPLÉTER: ex. "Ratio risque/récompense minimum"]
Description courte (affichée dans l'interface)      : [COMPLÉTER: dis en une phrase ce que ce filtre écarte]

Dans quel(s) cas ce filtre doit-il refuser un trade ?
  [COMPLÉTER: décris la ou les conditions de refus en langage naturel -- il n'y a
  pas de liste fixe imposée. Des exemples possibles (à titre indicatif seulement,
  choisis ce qui te concerne vraiment) : ratio risque/récompense insuffisant,
  distance au stop trop grande, un microsystème particulier absent du signal,
  une heure de la journée à éviter -- ou toute autre condition pertinente pour
  ta stratégie]

Quels paramètres veux-tu pouvoir ajuster ?
  [COMPLÉTER: liste libre, mêmes seuils/paramètres que la condition ci-dessus,
  rendus réglables plutôt que codés en dur]
```

---

## SYSTÈME — prompt à copier-coller intégralement à partir d'ici

Tu vas écrire un unique fichier Python autonome (`filtre de stratégie`) pour un
système de stratégies déjà existant. **Tu n'as pas accès au code de ce projet** —
tout ce que tu dois savoir est décrit ci-dessous, intégralement et sans ambiguïté.
Ne suppose rien d'autre.

### Ce qu'est ce système

Une stratégie de trading se construit en six étages, toujours dans le même
ordre : concepts → configuration des concepts → microsystèmes → variables
d'exécution → profil de gestion → **filtre** (c'est ce que tu écris ici, le
dernier étage, optionnel).

Un filtre s'exécute une fois que le profil de gestion a déjà calculé un trade
complet et prêt à être ouvert (direction, prix d'entrée, stop-loss,
take-profit). Son seul pouvoir est de **refuser** ce trade -- il ne peut
**jamais** modifier la direction, le stop-loss ou le take-profit déjà décidés,
et il ne touche à aucun autre script de la stratégie (concepts, microsystèmes,
exécution, gestion restent inchangés, quoi que fasse le filtre). C'est ce qui
donne au filtre "sa propre nuance" : il ajoute une couche de jugement
indépendante par-dessus une stratégie qui fonctionne déjà telle quelle, sans
jamais avoir à la modifier. Une stratégie a au plus un filtre ; c'est optionnel
-- une stratégie sans filtre laisse passer tous les trades que la gestion
propose, exactement comme avant que ce mécanisme n'existe.

**Rien ne s'exécute au moment où tu écris ce fichier** -- comme pour un concept,
un microsystème, un profil d'exécution ou de gestion, l'appel réel n'a lieu que
plus tard, dans le module de backtest.

**Isolation des erreurs, importante à comprendre** : si ta fonction `filter()`
lève une exception au moment de l'exécution réelle, le moteur de backtest
traite ça comme "laisser passer le trade", jamais comme "le refuser". Un filtre
buggé qui plante silencieusement à chaque appel devient donc invisible plutôt
que de bloquer toute la stratégie sans qu'on comprenne pourquoi -- pense à
gérer toi-même les cas limites (valeur manquante, division par zéro, etc.)
plutôt que de compter sur une exception pour refuser un trade.

### Le contrat exact (obligatoire, à respecter au caractère près)

```python
FILTER_INFO = {
    "label": "...",          # obligatoire, str
    "description": "...",    # obligatoire, str -- ce que ce filtre écarte, en une phrase
    "category": "...",       # optionnel, str -- défaut "Général"
    "detail": "...",         # optionnel, str -- bulle d'info. Absent -> pas de bulle.
    "config_schema": [...],  # optionnel, même forme que pour un concept (voir
                              # docs/nouveau_concept_prompt.md) -- c'est le SEUL
                              # mécanisme pour déclarer un paramètre ici. Défaut [].
}


def filter(context) -> object:
    ...
```

`context` expose :

- `context.direction` — `"long"` ou `"short"` : la direction du trade déjà
  décidée.
- `context.entry_price`, `context.stop_loss`, `context.take_profit` — des
  nombres (stop_loss/take_profit peuvent être `None` si la gestion n'en a fixé
  aucun) : le trade complet, déjà résolu, que tu peux refuser mais jamais
  modifier.
- `context.execution` — ce que la fonction `execute()` du profil d'exécution de
  la stratégie a retourné.
- `context.management` — ce que la fonction `manage()` du profil de gestion de
  la stratégie a retourné (peut être `None` si la stratégie n'a pas de profil
  de gestion).
- `context.microsystems` — un dict : l'id de chaque instance de microsystème de
  la stratégie -> son résultat déjà calculé. *Tous* les microsystèmes de la
  stratégie sont présents, pas une sélection. Tu ne reçois **jamais**
  `context.concepts` -- même règle et même contournement qu'un profil
  d'exécution/de gestion (voir `docs/nouveau_execution_prompt.md`) si tu as
  besoin d'un champ qu'un concept expose.
- `context.config` — un dict : `config_schema[i]["name"]` -> la valeur résolue.
- `context.log(message: str)` — statut lisible par un humain, repris dans le
  journal de replay du backtest.

### Règles strictes

1. **Un seul fichier** `.py`, autonome, aucun import relatif vers d'autres fichiers
   du projet.
2. **N'importe que la bibliothèque standard Python** (3.11+), sauf mention
   contraire explicite ci-dessus.
3. **La fonction doit s'appeler exactement `filter`**, être une fonction normale
   (`def`), jamais `async def` -- même règle qu'un concept, un microsystème, un
   profil d'exécution ou de gestion. (Cela masque la fonction native Python
   `filter()` à l'intérieur de ce fichier -- sans conséquence, ce fichier n'a
   besoin d'aucune autre utilisation de `filter`.)
4. **`FILTER_INFO` doit être un dict littéral au niveau module.**
5. **Ne modifie jamais `context.direction`/`entry_price`/`stop_loss`/
   `take_profit`** -- ce ne sont que des valeurs à lire, ton seul pouvoir est de
   refuser le trade dans son ensemble (voir "Forme reconnue" ci-dessous), jamais
   de le corriger.
6. Le fichier ira dans le dossier `filter_profiles/` du projet et ne doit **pas**
   commencer son nom par `_`.

### Forme reconnue par le moteur de backtest (obligatoire pour refuser un trade)

Pour refuser (véto) le trade proposé, retourne un dict contenant `"veto": True` :

```python
return {"veto": True, "reason": "..."}  # "reason" est optionnel, repris dans le journal
```

Pour laisser passer le trade, retourne `None` (ou n'importe quelle valeur sans
`"veto": True`, mais `None` est la convention la plus claire). Il n'existe
aucune troisième option -- un filtre ne peut jamais changer les valeurs du
trade, seulement l'accepter ou le refuser entièrement.

### Exemple complet fonctionnel (à adapter, ne pas copier tel quel)

```python
"""Exemple : refuse un trade dont le ratio récompense/risque est trop faible --
la distance jusqu'au take-profit doit être au moins `min_ratio` fois la distance
jusqu'au stop-loss, sinon le trade n'est pas jugé assez intéressant pour être pris."""

from __future__ import annotations

FILTER_INFO = {
    "label": "Ratio risque/récompense minimum",
    "category": "Gestion du risque",
    "description": "Refuse un trade dont la récompense potentielle est trop faible face au risque pris.",
    "detail": (
        "Calcule reward = |take_profit - entry_price| et risk = |entry_price - stop_loss|. "
        "Refuse le trade si reward < min_ratio * risk, ou si stop_loss/take_profit est absent "
        "(impossible de juger le ratio sans les deux)."
    ),
    "config_schema": [
        {
            "name": "min_ratio", "type": "number", "label": "Ratio minimum (récompense / risque)",
            "default": 1.5,
        },
    ],
}


def filter(context) -> object:
    min_ratio = context.config["min_ratio"]
    if context.stop_loss is None or context.take_profit is None:
        context.log("stop-loss ou take-profit absent -- ratio impossible à juger, trade refusé")
        return {"veto": True, "reason": "stop-loss ou take-profit absent"}
    risk = abs(context.entry_price - context.stop_loss)
    reward = abs(context.take_profit - context.entry_price)
    if risk == 0:
        return {"veto": True, "reason": "stop-loss confondu avec le prix d'entrée"}
    ratio = reward / risk
    if ratio < min_ratio:
        context.log(f"ratio {ratio:.2f} < minimum {min_ratio} -- trade refusé")
        return {"veto": True, "reason": f"ratio récompense/risque {ratio:.2f} sous le minimum {min_ratio}"}
    context.log(f"ratio {ratio:.2f} >= minimum {min_ratio} -- trade accepté")
    return None
```

### Ce que tu dois produire

Un seul bloc de code Python complet, prêt à être enregistré tel quel comme fichier
`.py`, implémentant `FILTER_INFO` et `filter(context)` pour les paramètres décrits
dans la section "À compléter" ci-dessus. Pas d'explication superflue autour du
code.

---

*Fin du prompt à copier-coller. Une fois le fichier `.py` obtenu, dépose-le dans
`filter_profiles/` à la racine du projet.*
