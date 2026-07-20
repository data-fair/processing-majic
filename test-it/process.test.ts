import { describe, it } from 'node:test'
import assert from 'assert'
import fs from 'fs'
import { Readable } from 'stream'
import { fileURLToPath } from 'url'
import os from 'os'
import path from 'path'
import processDepartments, { parseGroupe, aggregateDepartment, OUTPUT_FILE } from '../lib/process.ts'

const sample = fs.readFileSync(fileURLToPath(new URL('./resources/PM_sample.csv', import.meta.url)), 'utf8')

/** Collect the streamed aggregation into an array, since the tests assert on rows. */
const collect = async (csvText: string): Promise<string[][]> => {
  const rows: string[][] = []
  for await (const row of aggregateDepartment(Readable.from([csvText]))) rows.push(row)
  return rows
}

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

describe('aggregateDepartment', () => {
  // r[4] = code_parcelle

  it('produces one row per parcelle (aggregates SUF lines)', async () => {
    // dept01 parcelle appears on 2 SUF lines -> 1 aggregated row
    assert.equal((await collect(sample)).length, 3)
  })
  it('reduces groupe_personne to a bare code (fixes the map)', async () => {
    for (const r of await collect(sample)) assert.match(r[14], /^[0-9]$/) // r[14] = groupe_personne
  })
  it('decodes UTF-8: denomination keeps its accents', async () => {
    const dept01 = (await collect(sample)).find(r => r[0] === '01')!
    assert.equal(dept01[17], 'SOCIÉTÉ IMMOBILIÈRE DU HÉRISSON') // r[17] = denomination
  })
  it('joins multi-valued fields with ","', async () => {
    const dept01 = (await collect(sample)).find(r => r[0] === '01')!
    assert.equal(dept01[8], 'A,B')      // SUF
    assert.equal(dept01[9], 'BS,T')     // nature_culture (codes only)
    assert.equal(dept01[10], '30,30')   // contenance_suf
    assert.equal(dept01[11], 'P,P')     // code_droit — no dedup, no codeDroit filter (that is ticket #1371, out of scope)
  })
  it('builds a 14-char code_parcelle', async () => {
    for (const r of await collect(sample)) assert.equal(String(r[4]).length, 14)
  })
  it('emits rows in first-appearance order', async () => {
    assert.deepEqual((await collect(sample)).map(r => r[0]), ['01', '90', '75'])
  })

  it('merges parcelle lines that are NOT contiguous in the source', async () => {
    // Real MAJIC files interleave parcelles: on dept 90, 3760 of 29717 parcelles
    // reappear after unrelated lines (max span 647 lines). A flush-on-key-change
    // aggregation would emit duplicate rows here, so the Map must span the file.
    const lines = sample.trimEnd().split('\n')
    const interleaved = [lines[0], lines[1], lines[3], lines[4], lines[2]].join('\n') + '\n'
    const rows = await collect(interleaved)
    assert.equal(rows.length, 3)
    const dept01 = rows.find(r => r[0] === '01')!
    assert.equal(dept01[8], 'A,B') // both SUF lines merged despite the gap
    assert.equal(dept01[9], 'BS,T')
  })

  it('leaves multi-valued fields empty when every source value is empty', async () => {
    const lines = sample.trimEnd().split('\n')
    const cols = lines[3].split(';')
    cols[14] = ''; cols[15] = ''; cols[16] = ''; cols[17] = '' // SUF / nature / contenance_suf / code_droit
    const rows = await collect([lines[0], cols.join(';')].join('\n') + '\n')
    assert.equal(rows.length, 1)
    assert.deepEqual(rows[0].slice(8, 12), ['', '', '', ''])
  })

  it('drops empty and zero values from multi-valued fields', async () => {
    const lines = sample.trimEnd().split('\n')
    const cols = lines[2].split(';') // second SUF line of the dept01 parcelle
    cols[14] = ''; cols[15] = ''; cols[16] = '0'; cols[17] = ''
    const rows = await collect([lines[0], lines[1], cols.join(';')].join('\n') + '\n')
    assert.equal(rows.length, 1)
    assert.deepEqual(rows[0].slice(8, 12), ['A', 'BS', '30', 'P'])
  })
})

describe('process (full department run)', () => {
  const noop = async () => {}
  const log = { step: noop, info: noop, task: noop, progress: noop, warning: noop, error: noop } as never

  it('writes exactly one header line and no blank record', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'majic-'))
    fs.writeFileSync(path.join(tmpDir, 'PM_25_NB_010.csv'), sample)
    await processDepartments({} as never, tmpDir, log)
    const out = fs.readFileSync(path.join(tmpDir, OUTPUT_FILE), 'utf8').split('\n')
    assert.match(out[0], /^departement,code_commune,/)
    assert.notEqual(out[1], '') // a blank second line means an empty record was streamed
    assert.equal(out.filter(l => l !== '').length, 4) // header + 3 parcelles
    fs.rmSync(tmpDir, { recursive: true })
  })

  it('does not re-ingest its own output when tmpDir was not cleared', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'majic-'))
    fs.writeFileSync(path.join(tmpDir, 'PM_25_NB_010.csv'), sample)
    await processDepartments({} as never, tmpDir, log)
    const first = fs.readFileSync(path.join(tmpDir, OUTPUT_FILE), 'utf8')
    await processDepartments({} as never, tmpDir, log) // output file is still there
    assert.equal(fs.readFileSync(path.join(tmpDir, OUTPUT_FILE), 'utf8'), first)
    fs.rmSync(tmpDir, { recursive: true })
  })
})
