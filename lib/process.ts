import fs from 'fs-extra'
import path from 'path'
import { parse } from 'csv-parse/sync'
import { stringify as csvStringify } from 'csv-stringify/sync'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import schema from './schema.json' with { type: 'json' }

const fields = schema.map((field: { key: string }) => field.key)

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

type Parcelle = {
  departement: string
  codeCommune: string
  nomCommune: string
  adresse: string
  codeParcelle: string
  codeVoieMajic: string
  codeVoieRivoli: string
  contencanceParcelle: string
  suf: string[]
  nature: string[]
  contenanceSuf: number[]
  codeDroit: string[]
  numMajic: string
  numSiren: string
  groupe: string
  codeFormeJuridique: string
  formeJuridiqueAbregee: string
  denomination: string
}

/**
 * Parse one department's decoded CSV text and aggregate one row per parcelle.
 * Iso to 0.2.8: multi-line parcelles accumulate SUF / nature / contenance_suf /
 * code_droit, joined with ",". Returns rows as arrays in `schema.json` key order.
 */
export const processDepartmentCsv = (csvText: string): string[][] => {
  const lines: string[][] = parse(csvText, { delimiter: ';', bom: true })
  lines.shift() // header
  const parcelleMap = new Map<string, Parcelle>()
  for (const line of lines) {
    const codeCommune = line[0] + line[2].padStart(5 - line[0].length, '0')
    const codeParcelle = codeCommune + line[4].trim().padStart(3, '0') + line[5].trim().padStart(2, '0') + line[6].padStart(4, '0')
    if (!parcelleMap.has(codeParcelle)) {
      parcelleMap.set(codeParcelle, {
        departement: line[0],
        codeCommune,
        nomCommune: line[3],
        adresse: ((line[7] ? Number(line[7]) + ', ' : '') + (line[11] ? line[11] + ' ' : '') + line[12].replace(/"/g, ' ')).trim(),
        codeParcelle,
        codeVoieMajic: line[9],
        codeVoieRivoli: line[10],
        contencanceParcelle: line[13],
        suf: [line[14]],
        nature: [line[15].split(' - ')[0]],
        contenanceSuf: [Number(line[16])],
        codeDroit: [line[17].split(' - ')[0]],
        numMajic: line[18],
        numSiren: line[19],
        groupe: parseGroupe(line[20]),
        codeFormeJuridique: line[21],
        formeJuridiqueAbregee: line[22],
        denomination: line[23]
      })
    } else {
      const parcelle = parcelleMap.get(codeParcelle)!
      parcelle.suf.push(line[14])
      parcelle.nature.push(line[15].split(' - ')[0])
      parcelle.contenanceSuf.push(Number(line[16]))
      parcelle.codeDroit.push(line[17].split(' - ')[0])
    }
  }
  const rows: string[][] = []
  for (const parcelle of parcelleMap.values()) {
    const flat = {
      ...parcelle,
      suf: parcelle.suf.filter(s => s.length > 0).join(','),
      nature: parcelle.nature.filter(n => n.length > 0).join(','),
      contenanceSuf: parcelle.contenanceSuf.filter(c => c > 0).join(','),
      codeDroit: parcelle.codeDroit.filter(c => c.length > 0).join(',')
    }
    rows.push(Object.values(flat) as string[])
  }
  return rows
}

/**
 * Read every `*.csv` in tmpDir (optionally filtered by department), aggregate,
 * and write the output CSV. Decodes as UTF-8 (2024+ source encoding).
 */
export default async (processingConfig: ProcessingConfig, tmpDir: string, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Traitement des données')

  const files = fs.readdirSync(tmpDir).filter(f => f.slice(-4) === '.csv')
  const depFiles = files.filter(f =>
    !processingConfig.filter ||
    f.includes(processingConfig.filter + '0') ||
    (processingConfig.filter.length === 3 && f.includes(processingConfig.filter))
  )

  const writeStream = fs.openSync(path.join(tmpDir, 'Parcelles des personnes morales.csv'), 'w')
  try {
    // Output delimiter must be ',' to match the dataset schema's comma-joined
    // header (see execute.ts checkHeader) — production's csv-stringify config
    // leaves `separator` unset, which defaults the join AND the delimiter to ','.
    fs.writeSync(writeStream, csvStringify([], { header: true, delimiter: ',', columns: fields }))

    await log.task('Traitement des fichiers')
    await log.progress('Traitement des fichiers', 0, depFiles.length)
    let i = 0
    for (const depFile of depFiles) {
      if (shouldBeStopped) break
      await log.info('Traitement du fichier ' + depFile + '...')
      const csvText = await fs.readFile(path.join(tmpDir, depFile), 'utf-8')
      const rows = processDepartmentCsv(csvText)
      fs.writeSync(writeStream, csvStringify(rows, { header: false, delimiter: ',', columns: fields }))
      i++
      await log.progress('Traitement des fichiers', i, depFiles.length)
    }
  } finally {
    fs.closeSync(writeStream)
  }
}
