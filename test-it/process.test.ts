import { describe, it } from 'node:test'
import assert from 'assert'
import fs from 'fs'
import { Readable } from 'stream'
import { fileURLToPath } from 'url'
import os from 'os'
import path from 'path'
import processDepartments, { parseGroupe, aggregateDepartment, aggregateDepartmentLegacy, OUTPUT_FILE } from '../lib/process.ts'

const sample = fs.readFileSync(fileURLToPath(new URL('./resources/PM_sample.csv', import.meta.url)), 'utf8')

/** Collect the streamed aggregation into an array, since the tests assert on rows. */
const collect = async (csvText: string, codeDroit = 'P'): Promise<string[][]> => {
  const rows: string[][] = []
  for await (const row of aggregateDepartment(Readable.from([csvText]), codeDroit)) rows.push(row)
  return rows
}

/** Same, for the legacy aggregation (no code droit filter, so no codeDroit arg). */
const collectLegacy = async (csvText: string): Promise<string[][]> => {
  const rows: string[][] = []
  for await (const row of aggregateDepartmentLegacy(Readable.from([csvText]))) rows.push(row)
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
  // Output columns (code_droit removed): r[4]=code_parcelle, r[8..10]=SUF/nature/
  // contenance_suf, r[11..16]=numero_majic/numero_siren/groupe_personne/
  // code_forme_juridique/forme_juridique_abregee/denomination.

  it('keeps only parcelles that carry the requested code droit (P)', async () => {
    // sample: dept01 (2 P lines) + dept90 (1 P line) -> 2 rows; dept75 is a G
    // line (Aéroport de Paris) so it is dropped from the propriétaires dataset.
    assert.deepEqual((await collect(sample, 'P')).map(r => r[0]), ['01', '90'])
  })
  it('produces a different dataset for another code droit (G)', async () => {
    const rows = await collect(sample, 'G')
    assert.equal(rows.length, 1)
    assert.equal(rows[0][0], '75')
    assert.equal(rows[0][16], 'AÉROPORT DE PARIS') // denomination
  })
  it('reduces groupe_personne to a bare code (fixes the map)', async () => {
    for (const r of await collect(sample)) assert.match(r[13], /^[0-9]$/) // r[13] = groupe_personne
  })
  it('decodes UTF-8: denomination keeps its accents', async () => {
    const dept01 = (await collect(sample)).find(r => r[0] === '01')!
    assert.equal(dept01[16], 'SOCIÉTÉ IMMOBILIÈRE DU HÉRISSON') // r[16] = denomination
  })
  it('joins the subdivision fields with ","', async () => {
    const dept01 = (await collect(sample)).find(r => r[0] === '01')!
    assert.equal(dept01[8], 'A,B')      // SUF
    assert.equal(dept01[9], 'BS,T')     // nature_culture (codes only)
    assert.equal(dept01[10], '30,30')   // contenance_suf
  })
  it('deduplicates a person appearing on several subdivision lines', async () => {
    // dept01 has the same owner (PBBQWS) on both SUF lines -> one person value
    const dept01 = (await collect(sample)).find(r => r[0] === '01')!
    assert.equal(dept01[11], 'PBBQWS')                            // numero_majic
    assert.equal(dept01[16], 'SOCIÉTÉ IMMOBILIÈRE DU HÉRISSON')   // denomination
  })
  it('multi-values and aligns the 6 person columns when a parcelle has several owners', async () => {
    // Give the dept01 parcelle a second, different owner on its 2nd SUF line.
    const lines = sample.trimEnd().split('\n')
    const colsB = lines[2].split(';')
    colsB[18] = 'NEWMAJIC'; colsB[19] = '999888777'; colsB[23] = 'VILLE DE TEST'
    const [row] = await collect([lines[0], lines[1], colsB.join(';')].join('\n') + '\n')
    assert.equal(row[11], 'PBBQWS,NEWMAJIC')                                     // numero_majic
    assert.equal(row[12], 'U21651730,999888777')                                // numero_siren
    assert.equal(row[16], 'SOCIÉTÉ IMMOBILIÈRE DU HÉRISSON,VILLE DE TEST')       // denomination aligned
    assert.equal(row[8], 'A,B')                                                  // subdivisions still merged
  })
  it('builds a 14-char code_parcelle', async () => {
    for (const r of await collect(sample)) assert.equal(String(r[4]).length, 14)
  })
  it('builds a 14-char code_parcelle in the DOM (3-char department code)', async () => {
    // Réunion/Mayotte/Antilles/Guyane: département is on 3 chars and the source
    // commune code already carries the 3rd one ("974" + "401" = INSEE 97401), so
    // concatenating both gave a 6-char commune and a 15-char code_parcelle, which
    // matched nothing in the cadastre enrichment.
    const lines = sample.trimEnd().split('\n')
    const cols = lines[1].split(';')
    cols[0] = '974'; cols[2] = '401'; cols[3] = 'LES AVIRONS'
    const rows = await collect([lines[0], cols.join(';')].join('\n') + '\n')
    assert.equal(rows[0][1], '97401') // code_commune = code INSEE
    assert.equal(rows[0][4], '974010000A0018')
    assert.equal(rows[0][4].length, 14)
  })
  it('emits rows in first-appearance order', async () => {
    assert.deepEqual((await collect(sample)).map(r => r[0]), ['01', '90'])
  })

  it('merges parcelle lines that are NOT contiguous in the source', async () => {
    // Real MAJIC files interleave parcelles: on dept 90, 3760 of 29717 parcelles
    // reappear after unrelated lines (max span 647 lines). A flush-on-key-change
    // aggregation would emit duplicate rows here, so the Map must span the file.
    const lines = sample.trimEnd().split('\n')
    const interleaved = [lines[0], lines[1], lines[3], lines[4], lines[2]].join('\n') + '\n'
    const rows = await collect(interleaved)
    assert.equal(rows.length, 2) // dept01 (merged) + dept90; dept75 is a G line
    const dept01 = rows.find(r => r[0] === '01')!
    assert.equal(dept01[8], 'A,B') // both SUF lines merged despite the gap
    assert.equal(dept01[9], 'BS,T')
  })

  it('leaves subdivision fields empty when every source value is empty', async () => {
    const lines = sample.trimEnd().split('\n')
    const cols = lines[3].split(';')
    cols[14] = ''; cols[15] = ''; cols[16] = '' // SUF / nature / contenance_suf (droit kept = P)
    const rows = await collect([lines[0], cols.join(';')].join('\n') + '\n')
    assert.equal(rows.length, 1)
    assert.deepEqual(rows[0].slice(8, 11), ['', '', ''])
  })

  it('drops empty and zero values from the subdivision fields', async () => {
    const lines = sample.trimEnd().split('\n')
    const cols = lines[2].split(';') // second SUF line of the dept01 parcelle
    cols[14] = ''; cols[15] = ''; cols[16] = '0' // droit kept = P
    const rows = await collect([lines[0], lines[1], cols.join(';')].join('\n') + '\n')
    assert.equal(rows.length, 1)
    assert.deepEqual(rows[0].slice(8, 11), ['A', 'BS', '30'])
  })
})

describe('aggregateDepartmentLegacy (frozen — pre-0.4 output shape)', () => {
  // Output columns: r[4]=code_parcelle, r[8..10]=SUF/nature/contenance_suf,
  // r[11]=code_droit, r[12..17]=the 6 person columns.
  // This path is frozen on purpose: it feeds datasets created before the
  // one-dataset-per-code-droit split, which are updated in place.

  it('keeps every parcelle whatever its code droit', async () => {
    // The modern aggregation drops dept75 (a G line); legacy keeps all three.
    assert.deepEqual((await collectLegacy(sample)).map(r => r[0]), ['01', '90', '75'])
  })

  it('concatenates code_droit, one value per source line', async () => {
    const rows = await collectLegacy(sample)
    assert.equal(rows.find(r => r[0] === '01')![11], 'P,P') // two P lines merged
    assert.equal(rows.find(r => r[0] === '75')![11], 'G')
  })

  it('keeps the person columns single-valued, from the first source line', async () => {
    // Give the dept01 parcelle a second owner on its 2nd SUF line: legacy
    // ignores it (the multi-owner fix belongs to the modern path only).
    const lines = sample.trimEnd().split('\n')
    const colsB = lines[2].split(';')
    colsB[18] = 'NEWMAJIC'; colsB[19] = '999888777'; colsB[23] = 'VILLE DE TEST'
    const [row] = await collectLegacy([lines[0], lines[1], colsB.join(';')].join('\n') + '\n')
    assert.deepEqual(row.slice(12, 18), ['PBBQWS', 'U21651730', '0', '6540', 'SCI', 'SOCIÉTÉ IMMOBILIÈRE DU HÉRISSON'])
  })

  it('does NOT deduplicate identical subdivision lines', async () => {
    // The modern aggregation collapses this to 'A'; legacy repeats it.
    const lines = sample.trimEnd().split('\n')
    const rows = await collectLegacy([lines[0], lines[1], lines[1]].join('\n') + '\n')
    assert.equal(rows.length, 1)
    assert.deepEqual(rows[0].slice(8, 12), ['A,A', 'BS,BS', '30,30', 'P,P'])
  })

  it('joins the subdivision fields with "," and drops empty and zero values', async () => {
    const lines = sample.trimEnd().split('\n')
    const cols = lines[2].split(';')
    cols[14] = ''; cols[15] = ''; cols[16] = '0'
    const rows = await collectLegacy([lines[0], lines[1], cols.join(';')].join('\n') + '\n')
    assert.deepEqual(rows[0].slice(8, 11), ['A', 'BS', '30'])
  })

  it('reduces groupe_personne to a bare code', async () => {
    for (const r of await collectLegacy(sample)) assert.match(r[14], /^[0-9]$/)
  })

  it('merges parcelle lines that are NOT contiguous in the source', async () => {
    const lines = sample.trimEnd().split('\n')
    const rows = await collectLegacy([lines[0], lines[1], lines[3], lines[4], lines[2]].join('\n') + '\n')
    assert.equal(rows.length, 3)
    assert.equal(rows.find(r => r[0] === '01')![8], 'A,B')
  })

  it('builds a 14-char code_parcelle, in the DOM too (the fix is shared, not reverted)', async () => {
    for (const r of await collectLegacy(sample)) assert.equal(String(r[4]).length, 14)
    const lines = sample.trimEnd().split('\n')
    const cols = lines[1].split(';')
    cols[0] = '974'; cols[2] = '401'; cols[3] = 'LES AVIRONS'
    const rows = await collectLegacy([lines[0], cols.join(';')].join('\n') + '\n')
    assert.equal(rows[0][1], '97401')
    assert.equal(rows[0][4], '974010000A0018')
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
    assert.equal(out.filter(l => l !== '').length, 3) // header + 2 propriétaire parcelles (dept75 is a G line)
    fs.rmSync(tmpDir, { recursive: true })
  })

  it('writes every parcelle and the code_droit column in legacy mode', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'majic-legacy-'))
    fs.writeFileSync(path.join(tmpDir, 'PM_25_NB_010.csv'), sample)
    await processDepartments({ legacy: true } as never, tmpDir, log)
    const out = fs.readFileSync(path.join(tmpDir, OUTPUT_FILE), 'utf8').split('\n').filter(l => l !== '')
    assert.match(out[0], /,code_droit,/)
    assert.equal(out.length, 4) // header + the 3 parcelles, G line included
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
