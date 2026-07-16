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

  // These types must match the LIVE production dataset, otherwise enabling
  // forceUpdate would push a schema that corrupts the data. In particular
  // `departement` must stay a string ("01" would become 1 as an integer,
  // breaking the map's department filter) and `contenance_suf` is multi-valued
  // ("30,30"), so it can never be an integer.
  it('declares types matching the production dataset', () => {
    const types = Object.fromEntries(schema.map((f: { key: string, type: string }) => [f.key, f.type]))
    assert.deepEqual(types, {
      departement: 'string',
      code_commune: 'string',
      nom_commune: 'string',
      adresse: 'string',
      code_parcelle: 'string',
      code_voie_majic: 'integer',
      code_voie_rivoli: 'string',
      contenance_parcelle: 'integer',
      SUF: 'string',
      nature_culture: 'string',
      contenance_suf: 'string',
      code_droit: 'string',
      numero_majic: 'string',
      numero_siren: 'string',
      groupe_personne: 'string',
      code_forme_juridique: 'string',
      forme_juridique_abregee: 'string',
      denomination: 'string'
    })
  })

  // Every value of groupe_personne is a single digit "0".."9". data-fair's
  // CSV sniffer samples raw values top-down and, seeing only digits, infers
  // type "integer" — overriding the declared "string" (confirmed against a
  // full 17.6M-row run). x-labels can only be applied to a string field, so
  // without this pin the map's legend silently stops working. x-transform.type
  // is the one property data-fair re-applies after the sniff-driven override.
  it('pins groupe_personne as string via x-transform (it looks all-numeric to the CSV sniffer)', () => {
    const field = schema.find((f: { key: string }) => f.key === 'groupe_personne') as { 'x-transform'?: { type?: string } }
    assert.equal(field['x-transform']?.type, 'string')
  })
})
