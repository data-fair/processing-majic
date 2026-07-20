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

// Indexes of the multi-valued output columns, in `schema.json` key order.
const SUF = 8
const NATURE = 9
const CONTENANCE_SUF = 10
const CODE_DROIT = 11

/** Append a value to a multi-valued column, skipping empties (iso to 0.2.8's filter+join). */
const append = (row: string[], col: number, value: string): void => {
  if (!value) return
  row[col] = row[col] ? row[col] + ',' + value : value
}

/**
 * Aggregate one department's CSV stream into one row per parcelle, in
 * `schema.json` key order, yielded in first-appearance order.
 *
 * Memory: the whole file used to be held four times over (full text, all parsed
 * lines, a Parcelle object per row, then a flattened copy) which peaked at ~16x
 * the input size — over 1.7GB on dept 33 (105MB), blowing the 1024MB heap cap.
 * Here only the accumulator survives a line: rows are stored in their final
 * output shape and multi-values grow by string concat, for ~3x the input size.
 *
 * The Map has to span the whole file: parcelle lines are NOT contiguous in the
 * source (on dept 90, 3760 of 29717 parcelles reappear later, up to 647 lines
 * apart), so flushing on key change would emit duplicate rows.
 */
export async function * aggregateDepartment (input: Readable): AsyncGenerator<string[]> {
  const parcelles = new Map<string, string[]>()

  const lines = input.pipe(parse({ delimiter: ';', bom: true, from_line: 2 }))
  for await (const line of lines as AsyncIterable<string[]>) {
    const codeCommune = line[0] + line[2].padStart(5 - line[0].length, '0')
    const codeParcelle = codeCommune + line[4].trim().padStart(3, '0') + line[5].trim().padStart(2, '0') + line[6].padStart(4, '0')

    const suf = line[14]
    const nature = line[15].split(' - ')[0]
    const contenanceSuf = Number(line[16]) > 0 ? line[16] : ''
    const codeDroit = line[17].split(' - ')[0]

    const row = parcelles.get(codeParcelle)
    if (row === undefined) {
      parcelles.set(codeParcelle, [
        line[0],
        codeCommune,
        line[3],
        ((line[7] ? Number(line[7]) + ', ' : '') + (line[11] ? line[11] + ' ' : '') + line[12].replace(/"/g, ' ')).trim(),
        codeParcelle,
        line[9],
        line[10],
        line[13],
        suf,
        nature,
        contenanceSuf,
        codeDroit,
        line[18],
        line[19],
        parseGroupe(line[20]),
        line[21],
        line[22],
        line[23]
      ])
    } else {
      append(row, SUF, suf)
      append(row, NATURE, nature)
      append(row, CONTENANCE_SUF, contenanceSuf)
      append(row, CODE_DROIT, codeDroit)
    }
  }

  // Drop each row as it is yielded so the Map shrinks while the output is written.
  for (const [codeParcelle, row] of parcelles) {
    parcelles.delete(codeParcelle)
    yield row
  }
}

/**
 * Read every `*.csv` in tmpDir (optionally filtered by department), aggregate,
 * and write the output CSV. Decodes as UTF-8 (2024+ source encoding).
 */
export default async (processingConfig: ProcessingConfig, tmpDir: string, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Traitement des données')

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
        aggregateDepartment(fs.createReadStream(path.join(tmpDir, depFile), 'utf-8')),
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
