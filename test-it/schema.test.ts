import { describe, it } from 'node:test'
import assert from 'assert'
import schema from '../lib/schema.json' with { type: 'json' }
import legacySchema from '../lib/schema.legacy.json' with { type: 'json' }

describe('output schema (frozen — the map depends on it)', () => {
  // code_droit is gone: one dataset per code droit (selected in the processing
  // config), so the column would be constant. The 6 person columns are now
  // multi-valued (a parcelle can have several owners) — see ticket #1371.
  it('has exactly these keys, in this order', () => {
    const keys = schema.map((f: { key: string }) => f.key)
    assert.deepEqual(keys, [
      'departement', 'code_commune', 'nom_commune', 'adresse', 'code_parcelle',
      'code_voie_majic', 'code_voie_rivoli', 'contenance_parcelle', 'SUF',
      'nature_culture', 'contenance_suf', 'numero_majic',
      'numero_siren', 'groupe_personne', 'code_forme_juridique',
      'forme_juridique_abregee', 'denomination'
    ])
  })

  it('declares "," as the multi-value separator on the aggregated fields', () => {
    const withSep = schema.filter((f: { separator?: string }) => f.separator === ',').map((f: { key: string }) => f.key)
    assert.deepEqual(withSep, [
      'SUF', 'nature_culture', 'contenance_suf',
      'numero_majic', 'numero_siren', 'groupe_personne',
      'code_forme_juridique', 'forme_juridique_abregee', 'denomination'
    ])
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

// GELÉ : ce schéma est celui des jeux de données créés avant la v0.4, servi par
// le mode legacy (processingConfig.legacy). Il ne doit JAMAIS évoluer — toute
// nouvelle colonne va dans lib/schema.json seul. Un jeu de données legacy est
// mis à jour en place : changer une clé ici casse checkHeader en production.
describe('legacy output schema (frozen — pre-0.4 datasets)', () => {
  it('has exactly these 18 keys, in this order, with code_droit at index 11', () => {
    const keys = legacySchema.map((f: { key: string }) => f.key)
    assert.deepEqual(keys, [
      'departement', 'code_commune', 'nom_commune', 'adresse', 'code_parcelle',
      'code_voie_majic', 'code_voie_rivoli', 'contenance_parcelle', 'SUF',
      'nature_culture', 'contenance_suf', 'code_droit', 'numero_majic',
      'numero_siren', 'groupe_personne', 'code_forme_juridique',
      'forme_juridique_abregee', 'denomination'
    ])
  })

  // The person columns carry a single owner in legacy mode (first source line
  // wins), so declaring a separator there would make data-fair split a
  // denomination containing a comma into several values.
  it('declares "," only on the four columns the legacy aggregation concatenates', () => {
    const withSep = legacySchema.filter((f: { separator?: string }) => f.separator === ',').map((f: { key: string }) => f.key)
    assert.deepEqual(withSep, ['SUF', 'nature_culture', 'contenance_suf', 'code_droit'])
  })

  it('declares types matching the pre-0.4 production dataset', () => {
    const types = Object.fromEntries(legacySchema.map((f: { key: string, type: string }) => [f.key, f.type]))
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

  // Same CSV-sniffer trap as the modern schema: all-digit values get inferred
  // as integer, which drops the x-labels the map's legend renders.
  it('pins groupe_personne as string via x-transform', () => {
    const field = legacySchema.find((f: { key: string }) => f.key === 'groupe_personne') as { 'x-transform'?: { type?: string } }
    assert.equal(field['x-transform']?.type, 'string')
  })

  // The legend of the legacy map reads these labels; the column is gone from
  // the modern schema, so this file is their only home.
  it('keeps the 25 code droit labels', () => {
    const field = legacySchema.find((f: { key: string }) => f.key === 'code_droit') as { 'x-labels'?: Record<string, string> }
    assert.equal(Object.keys(field['x-labels'] ?? {}).length, 25)
    assert.equal(field['x-labels']?.P, 'Propriétaire')
    assert.equal(field['x-labels']?.G, 'Gérant, mandataire, gestionnaire')
  })
})
