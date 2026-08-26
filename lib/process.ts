import fs from 'fs-extra'
import path from 'path'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import { parse } from 'csv-parse'
import { stringify as csvStringify } from 'csv-stringify'
import { stringify as csvStringifySync } from 'csv-stringify/sync'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import schema from './schema.json' with { type: 'json' }

const fields = schema.map((field: { key: string }) => field.key)

export const OUTPUT_FILE = 'Parcelles des personnes morales.csv'

let shouldBeStopped = false
export const setShouldBeStopped = (v: boolean) => { shouldBeStopped = v }
export const isStopped = (): boolean => shouldBeStopped

/**
 * Reduce a "Groupe personne" source value to its bare code.
 * Source is inconsistent per department: "0" (e.g. dept 90) or
 * "0 - Personnes morales non remarquables" (e.g. dept 01). The map colors on
 * the bare code (0-9), so we must strip the label. The legacy `replace('A','')`
 * is kept but applied to the *code* only, so labels like "Associés" are safe.
 */
export const parseGroupe = (raw: string): string => {
  const code = (raw ?? '').split(' - ')[0].trim()
  return code.includes('A') ? code.replace('A', '') : code
}

export const DEFAULT_CODE_DROIT = 'P'

/** One accumulated parcelle: fixed columns + deduped subdivisions + deduped persons. */
interface Parcelle {
  scalars: string[]                                                   // columns [0..7]
  subdivisions: Map<string, [string, string, string]>                // SUF / nature / contenance_suf
  persons: Map<string, [string, string, string, string, string, string]> // the 6 person columns
}

/** Join a deduped multi-value, dropping empties (SUF "A,A" collapses, "0"/"" fall out). */
const joinFiltered = (rows: string[][], col: number): string =>
  rows.map(r => r[col]).filter(Boolean).join(',')

/** Join person values WITHOUT filtering, so the 6 person columns stay aligned per owner. */
const joinAligned = (rows: string[][], col: number): string =>
  rows.map(r => r[col]).join(',')

/**
 * Aggregate one department's CSV stream into one row per parcelle that carries
 * `codeDroit`, in `schema.json` key order, yielded in first-appearance order.
 *
 * One dataset per code droit (ticket #1371): only lines whose code droit equals
 * `codeDroit` are kept, so a "P" run yields the propriétaires, a "G" run the
 * gestionnaires, etc. The `code_droit` column is therefore gone. A parcelle can
 * have several owners of the same droit (0.3% of them), so the 6 person columns
 * are multi-valued and built together, deduped per owner and aligned. SUF /
 * nature / contenance_suf are deduped per subdivision (duplicate source lines
 * differing only by code_voie_majic no longer inflate them).
 *
 * Memory: only the accumulator survives a line; the Maps dedup as they grow and
 * each row is dropped as it is yielded, keeping the peak near the department's
 * distinct-parcelle footprint (well under the 1024MB heap cap on dept 33).
 *
 * The Map has to span the whole file: parcelle lines are NOT contiguous in the
 * source (on dept 90, 3760 of 29717 parcelles reappear later, up to 647 lines
 * apart), so flushing on key change would emit duplicate rows.
 */
export async function * aggregateDepartment (input: Readable, codeDroit: string = DEFAULT_CODE_DROIT): AsyncGenerator<string[]> {
  const parcelles = new Map<string, Parcelle>()

  const lines = input.pipe(parse({ delimiter: ';', bom: true, from_line: 2 }))
  for await (const line of lines as AsyncIterable<string[]>) {
    if (line[17].split(' - ')[0] !== codeDroit) continue

    // INSEE commune codes are 5 chars: 2 for the département + 3 for the commune.
    // In the DOM the source département is on 3 chars and its commune code already
    // repeats the 3rd one ("974" + "401" = 97401), so the département is what must
    // be truncated: concatenating both gave a 6-char commune code and a 15-char
    // code_parcelle, which matched nothing in the cadastre enrichment.
    const codeCommune = line[0].trim().slice(0, 2) + line[2].trim().padStart(3, '0')
    const codeParcelle = codeCommune + line[4].trim().padStart(3, '0') + line[5].trim().padStart(2, '0') + line[6].padStart(4, '0')

    let parcelle = parcelles.get(codeParcelle)
    if (parcelle === undefined) {
      parcelle = {
        scalars: [
          line[0],
          codeCommune,
          line[3],
          ((line[7] ? Number(line[7]) + ', ' : '') + (line[11] ? line[11] + ' ' : '') + line[12].replace(/"/g, ' ')).trim(),
          codeParcelle,
          line[9],
          line[10],
          line[13]
        ],
        subdivisions: new Map(),
        persons: new Map()
      }
      parcelles.set(codeParcelle, parcelle)
    }

    const subdivision: [string, string, string] = [line[14], line[15].split(' - ')[0], Number(line[16]) > 0 ? line[16] : '']
    const subKey = subdivision.join('')
    if (!parcelle.subdivisions.has(subKey) && subdivision.some(Boolean)) parcelle.subdivisions.set(subKey, subdivision)

    const person: [string, string, string, string, string, string] = [line[18], line[19], parseGroupe(line[20]), line[21], line[22], line[23]]
    const personKey = person.join('')
    if (!parcelle.persons.has(personKey) && person.some(Boolean)) parcelle.persons.set(personKey, person)
  }

  // Drop each row as it is yielded so the Map shrinks while the output is written.
  for (const [codeParcelle, parcelle] of parcelles) {
    parcelles.delete(codeParcelle)
    const subs = [...parcelle.subdivisions.values()]
    const persons = [...parcelle.persons.values()]
    yield [
      ...parcelle.scalars,
      joinFiltered(subs, 0),   // SUF
      joinFiltered(subs, 1),   // nature_culture
      joinFiltered(subs, 2),   // contenance_suf
      joinAligned(persons, 0), // numero_majic
      joinAligned(persons, 1), // numero_siren
      joinAligned(persons, 2), // groupe_personne
      joinAligned(persons, 3), // code_forme_juridique
      joinAligned(persons, 4), // forme_juridique_abregee
      joinAligned(persons, 5)  // denomination
    ]
  }
}

/**
 * Read every `*.csv` in tmpDir (optionally filtered by department), aggregate,
 * and write the output CSV. Decodes as UTF-8 (2024+ source encoding).
 */
export default async (processingConfig: ProcessingConfig, tmpDir: string, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Traitement des données')

  const codeDroit = processingConfig.codeDroit || DEFAULT_CODE_DROIT
  await log.info(`Code droit sélectionné : ${codeDroit}`)

  // Exclude our own output: it also ends in `.csv`, so a re-run over a tmpDir that
  // was not cleared (clearFiles off, or a retry after a failure) would ingest the
  // previous multi-GB result as if it were a department file.
  const files = fs.readdirSync(tmpDir).filter(f => f.slice(-4) === '.csv' && f !== OUTPUT_FILE)
  const depFiles = files.filter(f =>
    !processingConfig.filter ||
    f.includes(processingConfig.filter + '0') ||
    (processingConfig.filter.length === 3 && f.includes(processingConfig.filter))
  )

  const writeStream = fs.createWriteStream(path.join(tmpDir, OUTPUT_FILE))
  try {
    // Output delimiter must be ',' to match the dataset schema's comma-joined
    // header (see execute.ts checkHeader) — production's csv-stringify config
    // leaves `separator` unset, which defaults the join AND the delimiter to ','.
    // Header only — `Readable.from([[]])` would stream one *empty record* on top of it.
    writeStream.write(csvStringifySync([], { header: true, delimiter: ',', columns: fields }))

    await log.task('Traitement des fichiers')
    await log.progress('Traitement des fichiers', 0, depFiles.length)
    let i = 0
    for (const depFile of depFiles) {
      if (shouldBeStopped) break
      await log.info('Traitement du fichier ' + depFile + '...')
      await pipeline(
        aggregateDepartment(fs.createReadStream(path.join(tmpDir, depFile), 'utf-8'), codeDroit),
        csvStringify({ header: false, delimiter: ',', columns: fields }),
        writeStream,
        { end: false }
      )
      i++
      await log.progress('Traitement des fichiers', i, depFiles.length)
    }
  } finally {
    await new Promise<void>((resolve, reject) => writeStream.end((err?: Error | null) => err ? reject(err) : resolve()))
  }
}
