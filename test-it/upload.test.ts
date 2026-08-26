import { describe, it } from 'node:test'
import assert from 'assert'
import fs from 'fs'
import os from 'os'
import path from 'path'
import upload, { displayBytes } from '../lib/upload.ts'
import { schemaFor } from '../lib/output-schema.ts'
import schema from '../lib/schema.json' with { type: 'json' }
import legacySchema from '../lib/schema.legacy.json' with { type: 'json' }
import type { ProcessingConfig } from '#types/processingConfig/index.ts'

describe('displayBytes', () => {
  it('formats zero', () => { assert.equal(displayBytes(0), '0 octets') })
  it('formats kilobytes', () => { assert.match(displayBytes(2000), /ko$/) })
  it('formats megabytes', () => { assert.match(displayBytes(5_000_000), /Mo$/) })
})

// The four aggregated columns are comma-joined by process.ts. Declaring the
// separator makes data-fair index them as multi-valued, so a facet on
// nature_culture sees "BS,T" as two values instead of one opaque string.
// 0.2.8 read separator from an unset processing config, so JSON.stringify
// dropped it and production ended up with none at all; we send it as-is now.
describe('schemaFor (schema sent on dataset creation/forced update)', () => {
  it('carries the "," separator on the aggregated fields', () => {
    const withSep = schemaFor({}).filter((f: { separator?: string }) => f.separator === ',').map((f: { key: string }) => f.key)
    assert.deepEqual(withSep, [
      'SUF', 'nature_culture', 'contenance_suf',
      'numero_majic', 'numero_siren', 'groupe_personne',
      'code_forme_juridique', 'forme_juridique_abregee', 'denomination'
    ])
  })
  it('keeps the same field count and keys as lib/schema.json', () => {
    assert.equal(schemaFor({}).length, schema.length)
    assert.deepEqual(schemaFor({}).map((f: { key: string }) => f.key), schema.map((f: { key: string }) => f.key))
  })
})

// Reading the schema back off the wire, rather than off an exported constant:
// the flag is only useful if it reaches the multipart body data-fair receives.
const sentSchema = async (processingConfig: ProcessingConfig): Promise<{ key: string }[]> => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'majic-upload-'))
  try {
    fs.writeFileSync(path.join(tmpDir, 'Parcelles des personnes morales.csv'), 'departement\n01\n')
    let body = ''
    const axios = (async (opts: { data: NodeJS.ReadableStream }) => {
      body = await new Promise((resolve, reject) => {
        const chunks: Buffer[] = []
        opts.data.on('data', (c: Buffer) => chunks.push(Buffer.from(c)))
        opts.data.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
        opts.data.on('error', reject)
        // form-data is a paused CombinedStream: attaching listeners is not enough,
        // it only starts emitting once released.
        opts.data.resume()
      })
      return { data: { id: 'ds', title: 'T' } }
    }) as never
    const noop = async () => {}
    const log = { step: noop, info: noop, task: noop, progress: noop, warning: noop, error: noop, debug: noop } as never
    await upload(processingConfig, tmpDir, axios, log, noop as never)
    const part = body.split('name="schema"')[1]
    return JSON.parse(part.slice(part.indexOf('['), part.lastIndexOf(']') + 1))
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true })
  }
}

describe('the schema actually sent to data-fair follows processingConfig.legacy', () => {
  it('sends the current schema by default', { timeout: 10000 }, async () => {
    const sent = await sentSchema({ datasetMode: 'create', dataset: {} } as ProcessingConfig)
    assert.deepEqual(sent.map(f => f.key), schema.map((f: { key: string }) => f.key))
  })

  it('sends the frozen legacy schema in legacy mode', { timeout: 10000 }, async () => {
    const sent = await sentSchema({ datasetMode: 'create', dataset: {}, legacy: true } as ProcessingConfig)
    assert.deepEqual(sent.map(f => f.key), legacySchema.map((f: { key: string }) => f.key))
    assert.ok(sent.some(f => f.key === 'code_droit'))
  })
})
