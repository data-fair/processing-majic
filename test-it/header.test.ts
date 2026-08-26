import { describe, it } from 'node:test'
import assert from 'assert'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { fileURLToPath } from 'url'
import schema from '../lib/schema.json' with { type: 'json' }
import legacySchema from '../lib/schema.legacy.json' with { type: 'json' }
import process from '../lib/process.ts'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'

const sample = fileURLToPath(new URL('./resources/PM_sample.csv', import.meta.url))

// process.ts only calls log.step/task/progress/info; the other members of
// LogFunctions are required by the type but unused here, so they're no-ops.
const log: ProcessingContext<ProcessingConfig>['log'] = {
  step: async () => {},
  task: async () => {},
  progress: async () => {},
  info: async () => {},
  error: async () => {},
  warning: async () => {},
  debug: async () => {}
}

// process.ts's default export only reads processingConfig.filter; leaving it
// unset processes every *.csv found in tmpDir (i.e. our copied sample).
const processingConfig = { dataset: {} } as ProcessingConfig

// Regression test for the production incident where process.ts wrote the
// output CSV with delimiter: ';' while execute.ts's checkHeader compares the
// generated header against the dataset schema's comma-joined keys. That
// mismatch made every production run (datasetMode: update, forceUpdate: false)
// throw "En-têtes différentes entre les fichiers" before upload ever ran.
//
// Unlike a hardcoded reproduction, this drives lib/process.ts's real default
// export end-to-end and inspects the file it actually writes, so reverting the
// delimiter to ';' fails this test.
describe('process.ts output header matches the dataset schema, or every production run aborts before upload (execute.ts checkHeader)', () => {
  it('writes a comma-delimited header that satisfies checkHeader\'s comparison', async () => {
    const tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'processing-majic-header-'))
    try {
      await fs.promises.copyFile(sample, path.join(tmpDir, 'PM_sample.csv'))

      await process(processingConfig, tmpDir, log)

      const outputFile = path.join(tmpDir, 'Parcelles des personnes morales.csv')
      const content = await fs.promises.readFile(outputFile, 'utf8')
      const head = content.split('\n')[0]

      const fields = schema.map((f: { key: string }) => f.key)
      // What execute.ts's checkHeader builds from the dataset schema
      // (GET .../schema -> map key -> quote -> join(',') -> strip quotes).
      const schemaActuel = fields.map((k: string) => `"${k}"`).join(',').replace(/['"]+/g, '')

      // This is exactly what execute.ts's checkHeader asserts. If process.ts's
      // output delimiter regresses to ';', this comparison fails here exactly
      // as it would in production.
      assert.ok(head.includes(schemaActuel.slice(0, head.length - 1)))

      // Make the intent unmissable: the header must be comma-delimited.
      assert.ok(head.startsWith('departement,code_commune,'))
      assert.ok(!head.includes(';'))
    } finally {
      await fs.promises.rm(tmpDir, { recursive: true, force: true })
    }
  })
})

// Same contract for the legacy mode: a pre-0.4 dataset is updated in place, so
// the generated header must match ITS schema, not the modern one. Getting this
// wrong aborts every legacy run in checkHeader instead of uploading garbage —
// but it aborts them all the same.
describe('process.ts honours processingConfig.legacy when writing the header', () => {
  it('writes the legacy 18-column header, code_droit included', async () => {
    const tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'processing-majic-legacy-header-'))
    try {
      await fs.promises.copyFile(sample, path.join(tmpDir, 'PM_sample.csv'))

      await process({ dataset: {}, legacy: true } as ProcessingConfig, tmpDir, log)

      const content = await fs.promises.readFile(path.join(tmpDir, 'Parcelles des personnes morales.csv'), 'utf8')
      const head = content.split('\n')[0]

      const fields = legacySchema.map((f: { key: string }) => f.key)
      const schemaActuel = fields.map((k: string) => `"${k}"`).join(',').replace(/['"]+/g, '')
      assert.ok(head.includes(schemaActuel.slice(0, head.length - 1)))
      assert.ok(head.includes(',code_droit,'))
    } finally {
      await fs.promises.rm(tmpDir, { recursive: true, force: true })
    }
  })
})
