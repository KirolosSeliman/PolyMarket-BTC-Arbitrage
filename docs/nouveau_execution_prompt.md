# Prompt réutilisable : créer un nouveau profil d'exécution de stratégie

Ce fichier est un **prompt prêt à copier-coller** pour n'importe quelle IA de code
(Claude Code ou autre) qui n'a **aucun accès au reste du projet**. Il contient tout
ce qu'il faut savoir pour écrire un fichier de profil d'exécution valide, en
autonomie complète.

**Mode d'emploi** : complète les lignes `[COMPLÉTER: ...]` tout en haut, puis colle
l'ensemble du prompt (à partir de "SYSTÈME" ci-dessous) à l'IA de ton choix. Elle te
rendra un seul fichier `.py`. Dépose ce fichier dans le dossier `execution_profiles/`
du projet -- rien d'autre à faire, il apparaîtra automatiquement comme profil
d'exécution sélectionnable à la dernière étape du constructeur de stratégie.

Contrairement au prompt de concept ou de microsystème, ce prompt-ci ne dépend
d'aucune sélection préalable de données/concepts -- un profil d'exécution ne
déclare que des paramètres, jamais de dépendance directe à une donnée ou un
concept.

---

## À compléter avant d'envoyer le prompt

```
Nom du fichier (sans .py, minuscules, underscores)  : [COMPLÉTER: ex. conservative_sizing]
Catégorie / section (existante ou nouvelle, libre)  : [COMPLÉTER: ex. "Gestion du risque"]
Nom affiché dans l'interface (label)                : [COMPLÉTER: ex. "Dimensionnement prudent"]
Description courte (affichée dans l'interface)      : [COMPLÉTER: dis en une phrase ce que ce profil détermine]

Quels paramètres d'exécution veux-tu pouvoir ajuster ?
  [COMPLÉTER: liste libre -- il n'y a pas de liste fixe imposée. Des exemples
  possibles (à titre indicatif seulement, choisis ce qui te concerne vraiment) :
  taille de position, capital maximum engagé, limite de perte, délai avant
  d'agir sur un signal -- ou tout autre paramètre pertinent pour ta stratégie]
```

---

## SYSTÈME — prompt à copier-coller intégralement à partir d'ici

Tu vas écrire un unique fichier Python autonome (`profil d'exécution`) pour un
système de stratégies déjà existant. **Tu n'as pas accès au code de ce projet** —
tout ce que tu dois savoir est décrit ci-dessous, intégralement et sans ambiguïté.
Ne suppose rien d'autre.

### Ce qu'est ce système

Une stratégie de trading se construit en cinq étages, toujours dans le même
ordre : concepts → configuration des concepts → microsystèmes → **variables
d'exécution** (c'est ce que tu écris ici) → profil de gestion.

Un profil d'exécution regroupe les paramètres nécessaires pour exécuter
concrètement une stratégie une fois que ses microsystèmes ont produit leurs
conclusions -- typiquement, quand prendre le trade. Il n'y a **aucune liste
fixe** de ce que ces paramètres doivent être — chaque stratégie peut avoir des
besoins différents. Une stratégie a exactement un profil d'exécution, qui
reçoit la sortie de *tous* ses microsystèmes. Le stage suivant (profil de
gestion) prend ensuite le relais pour gérer le trade une fois pris -- ce
n'est pas à toi de t'en occuper ici.

**Rien ne s'exécute au moment où tu écris ce fichier** -- comme pour un concept ou
un microsystème, l'appel réel n'a lieu que plus tard, dans le module de backtest.

### Le contrat exact (obligatoire, à respecter au caractère près)

```python
EXECUTION_INFO = {
    "label": "...",          # obligatoire, str
    "description": "...",    # obligatoire, str -- ce que ce profil détermine, en une phrase
    "category": "...",       # optionnel, str -- défaut "Général"
    "detail": "...",         # optionnel, str -- bulle d'info. Absent -> pas de bulle.
    "config_schema": [...],  # optionnel, même forme que pour un concept (voir
                              # docs/nouveau_concept_prompt.md) -- c'est le SEUL
                              # mécanisme pour déclarer un paramètre ici. Défaut [].
}


def execute(context) -> ...:
    ...
```

`context` expose :

- `context.microsystems` — un dict : l'id de chaque instance de microsystème de la
  stratégie -> son résultat déjà calculé (ce que sa propre fonction `compute()` a
  retourné). *Tous* les microsystèmes de la stratégie sont présents, pas une
  sélection.
- `context.config` — un dict : `config_schema[i]["name"]` -> la valeur résolue.
- `context.log(message: str)` — statut lisible par un humain.

### Règles strictes

1. **Un seul fichier** `.py`, autonome, aucun import relatif vers d'autres fichiers
   du projet.
2. **N'importe que la bibliothèque standard Python** (3.11+), sauf mention
   contraire explicite ci-dessus.
3. **La fonction doit s'appeler exactement `execute`**, être une fonction normale
   (`def`), jamais `async def` -- même règle qu'un concept ou un microsystème.
4. **`EXECUTION_INFO` doit être un dict littéral au niveau module.**
5. **Isolation des erreurs** : une exception dans `execute()` (plus tard, à
   l'exécution réelle) n'affecte que ce profil, jamais le reste de la stratégie.
6. Le fichier ira dans le dossier `execution_profiles/` du projet et ne doit **pas**
   commencer son nom par `_`.

### Forme reconnue par le moteur de backtest (optionnel, additif)

Le contrat ci-dessus n'impose aucune forme à ce que `execute()` retourne --
mais le moteur de backtest, pour savoir si un trade doit être ouvert,
reconnaît spécifiquement une clé `"direction"` dans le dict retourné. Si tu
veux que ton profil produise des trades testables en backtest, retourne
`{"direction": ..., ...}` où `direction` vaut, insensible à la casse :

- `"long"`, `"buy"`, `"haussier"` ou `"bullish"` → ouvre (ou garde) une
  position longue.
- `"short"`, `"sell"`, `"baissier"` ou `"bearish"` → ouvre (ou garde) une
  position courte.
- toute autre valeur (ou l'absence de la clé) → aucun trade à cet instant.

Rien ne t'oblige à utiliser cette convention -- un profil qui ne la suit pas
reste un profil d'exécution valide, il ne produira simplement aucun trade
reconnu par le backtest.

### Exemple complet fonctionnel (à adapter, ne pas copier tel quel)

```python
"""Exemple : un dimensionnement de position prudent, borné par un capital maximum
et désactivé si aucun microsystème ne conclut à un signal exploitable."""

from __future__ import annotations

EXECUTION_INFO = {
    "label": "Dimensionnement prudent",
    "category": "Gestion du risque",
    "description": "Taille de position plafonnée, désactivée hors signal clair.",
    "detail": (
        "Alloue une fraction fixe du capital maximum autorisé quand au moins un "
        "microsystème conclut à un signal directionnel ('haussier'/'baissier'), "
        "ne prend aucune position sinon."
    ),
    "config_schema": [
        {
            "name": "max_position_usd", "type": "number", "label": "Capital maximum (USD)",
            "default": 500,
        },
        {
            "name": "allocation_fraction", "type": "number", "label": "Fraction allouée par signal",
            "default": 0.5,
        },
    ],
}


def execute(context) -> dict:
    max_position = context.config["max_position_usd"]
    fraction = context.config["allocation_fraction"]
    signals = [value for value in context.microsystems.values() if value in ("haussier", "baissier")]
    context.log(f"{len(signals)} microsystème(s) avec un signal directionnel")
    if not signals:
        return {"position_usd": 0, "direction": "neutre"}
    direction = signals[0]
    return {"position_usd": max_position * fraction, "direction": direction}
```

### Ce que tu dois produire

Un seul bloc de code Python complet, prêt à être enregistré tel quel comme fichier
`.py`, implémentant `EXECUTION_INFO` et `execute(context)` pour les paramètres
décrits dans la section "À compléter" ci-dessus. Pas d'explication superflue autour
du code.

---

*Fin du prompt à copier-coller. Une fois le fichier `.py` obtenu, dépose-le dans
`execution_profiles/` à la racine du projet.*
