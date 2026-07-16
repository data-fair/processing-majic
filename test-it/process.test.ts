import { describe, it } from 'node:test'
import assert from 'assert'
import fs from 'fs'
import { fileURLToPath } from 'url'
import { parseGroupe, processDepartmentCsv } from '../lib/process.ts'

const sample = fs.readFileSync(fileURLToPath(new URL('./resources/PM_sample.csv', import.meta.url)), 'utf8')

describe('parseGroupe', () => {
  it('extracts the bare code from a "code - libellé" value', () => {
    assert.equal(parseGroupe('0 - Personnes morales non remarquables '), '0')
  })
  it('keeps a bare code as-is', () => {
    assert.equal(parseGroupe('0'), '0')
  })
  it('does not mangle "Associés" (bug: replace("A","") on the label)', () => {
    assert.equal(parseGroupe('8 - Associés'), '8')
  })
  it('handles empty input', () => {
    assert.equal(parseGroupe(''), '')
  })
})

describe('processDepartmentCsv', () => {
  const rows = processDepartmentCsv(sample)
  // r[4] = code_parcelle

  it('produces one row per parcelle (aggregates SUF lines)', () => {
    // dept01 parcelle appears on 2 SUF lines -> 1 aggregated row
    assert.equal(rows.length, 3)
  })
  it('reduces groupe_personne to a bare code (fixes the map)', () => {
    for (const r of rows) assert.match(r[14], /^[0-9]$/) // r[14] = groupe_personne
  })
  it('decodes UTF-8: denomination keeps its accents', () => {
    const dept01 = rows.find(r => r[0] === '01')!
    assert.equal(dept01[17], 'SOCIÉTÉ IMMOBILIÈRE DU HÉRISSON') // r[17] = denomination
  })
  it('joins multi-valued fields with ","', () => {
    const dept01 = rows.find(r => r[0] === '01')!
    assert.equal(dept01[8], 'A,B')      // SUF
    assert.equal(dept01[9], 'BS,T')     // nature_culture (codes only)
    assert.equal(dept01[10], '30,30')   // contenance_suf
    assert.equal(dept01[11], 'P,P')     // code_droit — no dedup, no codeDroit filter (that is ticket #1371, out of scope)
  })
  it('builds a 14-char code_parcelle', () => {
    for (const r of rows) assert.equal(String(r[4]).length, 14)
  })
})
