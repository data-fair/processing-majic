import { describe, it } from 'node:test'
import assert from 'assert'
import schema from '../lib/schema.json' with { type: 'json' }

describe('output schema (frozen — the map depends on it)', () => {
  it('has exactly these keys, in this order', () => {
    const keys = schema.map((f: { key: string }) => f.key)
    assert.deepEqual(keys, [
      'departement', 'code_commune', 'nom_commune', 'adresse', 'code_parcelle',
      'code_voie_majic', 'code_voie_rivoli', 'contenance_parcelle', 'SUF',
      'nature_culture', 'contenance_suf', 'code_droit', 'numero_majic',
      'numero_siren', 'groupe_personne', 'code_forme_juridique',
      'forme_juridique_abregee', 'denomination'
    ])
  })

  it('declares "," as the multi-value separator on the aggregated fields', () => {
    const withSep = schema.filter((f: { separator?: string }) => f.separator === ',').map((f: { key: string }) => f.key)
    assert.deepEqual(withSep, ['SUF', 'nature_culture', 'contenance_suf', 'code_droit'])
  })
})
