# @data-fair/processing-majic

Plugin data-fair-processings : importe la base MAJIC (parcelles des personnes
morales, DGFiP via data.gouv.fr) dans un jeu de données data-fair.

## Modes de sortie

Deux schémas de sortie coexistent, choisis par la case **Mode legacy** de la
configuration :

- **Par défaut** (`legacy: false`) — un jeu de données par code droit
  (`lib/schema.json`, 17 colonnes). Seules les parcelles portant le code droit
  choisi sont extraites, et les colonnes personne sont multi-valuées et alignées
  quand une parcelle a plusieurs titulaires.
- **Legacy** (`legacy: true`) — le schéma d'avant la v0.4
  (`lib/schema.legacy.json`, 18 colonnes dont `code_droit`) : toutes les lignes,
  aucune déduplication, un seul titulaire par parcelle. Réservé à la mise à jour
  des jeux de données créés avant la v0.4.

Le schéma legacy et son agrégation (`aggregateDepartmentLegacy`) sont **gelés** :
toute nouvelle colonne ou correction va dans le chemin par défaut uniquement.

## Développement

- Node 24 (`nvm use`), TypeScript natif (aucune étape de build).
- `npm install`
- `npm run build-types` — génère les types depuis les schémas JSON.
- `npm test` — tests `node:test`.
- `npm run lint` / `npm run lint-fix`.

## Publication

⚠️ La publication au registre est faite par l'équipe (workflows GitHub Actions :
push sur `main` → staging, tag `v*` → production). Ne jamais publier manuellement.
