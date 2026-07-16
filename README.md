# @data-fair/processing-majic

Plugin data-fair-processings : importe la base MAJIC (parcelles des personnes
morales, DGFiP via data.gouv.fr) dans un jeu de données data-fair.

## Développement

- Node 24 (`nvm use`), TypeScript natif (aucune étape de build).
- `npm install`
- `npm run build-types` — génère les types depuis les schémas JSON.
- `npm test` — tests `node:test`.
- `npm run lint` / `npm run lint-fix`.

## Publication

⚠️ La publication au registre est faite par l'équipe (workflows GitHub Actions :
push sur `main` → staging, tag `v*` → production). Ne jamais publier manuellement.
