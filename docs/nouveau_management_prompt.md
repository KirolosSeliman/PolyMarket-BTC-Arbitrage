# Prompt réutilisable : créer un nouveau profil de gestion de stratégie

Ce fichier est un **prompt prêt à copier-coller** pour n'importe quelle IA de code
(Claude Code ou autre) qui n'a **aucun accès au reste du projet**. Il contient tout
ce qu'il faut savoir pour écrire un fichier de profil de gestion valide, en
autonomie complète.

**Mode d'emploi** : complète les lignes `[COMPLÉTER: ...]` tout en haut, puis colle
l'ensemble du prompt (à partir de "SYSTÈME" ci-dessous) à l'IA de ton choix. Elle te
rendra un seul fichier `.py`. Dépose ce fichier dans le dossier
`management_profiles/` du projet -- rien d'autre à faire, il apparaîtra
automatiquement comme profil de gestion sélectionnable à la dernière étape du
constructeur de stratégie.

Comme le prompt de profil d'exécution, ce prompt-ci ne dépend d'aucune sélection
préalable de données/concepts -- un profil de gestion ne déclare que des
paramètres, jamais de dépendance directe à une donnée ou un concept.

---

## À compléter avant d'envoyer le prompt

```
Nom du fichier (sans .py, minuscules, underscores)  : [COMPLÉTER: ex. fixed_sl_tp]
Catégorie / section (existante ou nouvelle, libre)  : [COMPLÉTER: ex. "Gestion du risque"]
Nom affiché dans l'interface (label)                : [COMPLÉTER: ex. "SL/TP fixes"]
Description courte (affichée dans l'interface)      : [COMPLÉTER: dis en une phrase ce que ce profil détermine]

Quels paramètres de gestion veux-tu pouvoir ajuster ?
  [COMPLÉTER: liste libre -- il n'y a pas de liste fixe imposée. Des exemples
  possibles (à titre indicatif seulement, choisis ce qui te concerne vraiment) :
  niveau de stop-loss, niveau de take-profit, si ces niveaux restent fixes une
  fois le trade pris ou s'ils se recalculent quand de nouvelles informations
  arrivent (trailing stop, réévaluation périodique...), délai maximum avant de
  sortir une position sans signal -- ou tout autre paramètre pertinent pour ta
  stratégie]
```

---

## SYSTÈME — prompt à copier-coller intégralement à partir d'ici

Tu vas écrire un unique fichier Python autonome (`profil de gestion`) pour un
système de stratégies déjà existant. **Tu n'as pas accès au code de ce projet** —
tout ce que tu dois savoir est décrit ci-dessous, intégralement et sans ambiguïté.
Ne suppose rien d'autre.

### Ce qu'est ce système

Une stratégie de trading se construit en cinq étages, toujours dans le même
ordre : concepts → configuration des concepts → microsystèmes → variables
d'exécution → **profil de gestion** (c'est ce que tu écris ici).

Un profil de gestion regroupe les paramètres nécessaires pour gérer un trade
une fois que le profil d'exécution a décidé de le prendre -- typiquement :
stop-loss, take-profit, et si ces niveaux restent fixes ou s'adaptent à de
nouvelles informations. Il n'y a **aucune liste fixe** de ce que ces
paramètres doivent être — chaque stratégie peut avoir des besoins différents.
Une stratégie a exactement un profil de gestion, qui reçoit la sortie du
profil d'exécution ainsi que la sortie de *tous* les microsystèmes.

**Rien ne s'exécute au moment où tu écris ce fichier** -- comme pour un concept, un
microsystème ou un profil d'exécution, l'appel réel n'a lieu que plus tard, dans le
module de backtest.

### Le contrat exact (obligatoire, à respecter au caractère près)

```python
MANAGEMENT_INFO = {
    "label": "...",          # obligatoire, str
    "description": "...",    # obligatoire, str -- ce que ce profil détermine, en une phrase
    "category": "...",       # optionnel, str -- défaut "Général"
    "detail": "...",         # optionnel, str -- bulle d'info. Absent -> pas de bulle.
    "config_schema": [...],  # optionnel, même forme que pour un concept (voir
                              # docs/nouveau_concept_prompt.md) -- c'est le SEUL
                              # mécanisme pour déclarer un paramètre ici. Défaut [].
}


def manage(context) -> ...:
    ...
```

`context` expose :

- `context.execution` — ce que la fonction `execute()` du profil d'exécution de la
  stratégie a retourné (sa décision : quand/comment prendre le trade).
- `context.microsystems` — un dict : l'id de chaque instance de microsystème de la
  stratégie -> son résultat déjà calculé (ce que sa propre fonction `compute()` a
  retourné). *Tous* les microsystèmes de la stratégie sont présents, pas une
  sélection -- utile si ta gestion a besoin d'une donnée que la décision
  d'exécution elle-même n'a pas transmise (ex. une mesure de volatilité produite
  par un microsystème, pour dimensionner un stop-loss).
- `context.config` — un dict : `config_schema[i]["name"]` -> la valeur résolue.
- `context.log(message: str)` — statut lisible par un humain.

### Règles strictes

1. **Un seul fichier** `.py`, autonome, aucun import relatif vers d'autres fichiers
   du projet.
2. **N'importe que la bibliothèque standard Python** (3.11+), sauf mention
   contraire explicite ci-dessus.
3. **La fonction doit s'appeler exactement `manage`**, être une fonction normale
   (`def`), jamais `async def` -- même règle qu'un concept, un microsystème ou un
   profil d'exécution.
4. **`MANAGEMENT_INFO` doit être un dict littéral au niveau module.**
5. **Isolation des erreurs** : une exception dans `manage()` (plus tard, à
   l'exécution réelle) n'affecte que ce profil, jamais le reste de la stratégie.
6. Le fichier ira dans le dossier `management_profiles/` du projet et ne doit
   **pas** commencer son nom par `_`.

### Forme reconnue par le moteur de backtest (optionnel, additif)

Comme pour un profil d'exécution, le contrat ci-dessus n'impose aucune forme
à ce que `manage()` retourne -- mais le moteur de backtest reconnaît
spécifiquement deux clés dans le dict retourné, si tu veux que ton profil
détermine réellement où se ferme un trade en backtest :

- `"stop_loss_pct"` -- distance en pourcentage du prix d'entrée jusqu'au
  stop-loss (`None` ou absente = pas de stop-loss, le trade ne se ferme
  jamais sur ce critère).
- `"take_profit_pct"` -- même chose pour le take-profit.

Si aucune des deux ne résout à un nombre (pas de profil de gestion configuré,
ou un profil qui ne retourne rien de reconnu), le backtest ferme le trade
quand le signal du profil d'exécution change de sens ou redevient neutre --
un trade n'est donc jamais laissé ouvert indéfiniment, même sans niveaux
explicites.

### Exemple complet fonctionnel (à adapter, ne pas copier tel quel)

```python
"""Exemple : un stop-loss et un take-profit fixes, définis en pourcentage du prix
d'entrée au moment où le trade est pris -- ne se recalculent jamais ensuite."""

from __future__ import annotations

MANAGEMENT_INFO = {
    "label": "SL/TP fixes",
    "category": "Gestion du risque",
    "description": "Stop-loss et take-profit fixés en pourcentage à l'entrée, jamais recalculés.",
    "detail": (
        "Calcule un niveau de stop-loss et un niveau de take-profit une seule "
        "fois, au moment où le profil d'exécution prend le trade. Ces niveaux "
        "restent fixes pour toute la durée de la position -- pas de trailing "
        "stop, pas de réévaluation."
    ),
    "config_schema": [
        {
            "name": "stop_loss_pct", "type": "number", "label": "Stop-loss (%)",
            "default": 1.5,
        },
        {
            "name": "take_profit_pct", "type": "number", "label": "Take-profit (%)",
            "default": 3.0,
        },
    ],
}


def manage(context) -> dict:
    stop_loss_pct = context.config["stop_loss_pct"]
    take_profit_pct = context.config["take_profit_pct"]
    direction = context.execution.get("direction", "neutre") if isinstance(context.execution, dict) else "neutre"
    context.log(f"gestion fixe appliquée pour une position {direction}")
    if direction == "neutre":
        return {"stop_loss_pct": None, "take_profit_pct": None, "trailing": False}
    return {"stop_loss_pct": stop_loss_pct, "take_profit_pct": take_profit_pct, "trailing": False}
```

### Ce que tu dois produire

Un seul bloc de code Python complet, prêt à être enregistré tel quel comme fichier
`.py`, implémentant `MANAGEMENT_INFO` et `manage(context)` pour les paramètres
décrits dans la section "À compléter" ci-dessus. Pas d'explication superflue autour
du code.

---

*Fin du prompt à copier-coller. Une fois le fichier `.py` obtenu, dépose-le dans
`management_profiles/` à la racine du projet.*
