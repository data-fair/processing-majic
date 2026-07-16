import { describe, it } from 'node:test'
import assert from 'assert'
import { displayBytes, uploadSchema } from '../lib/upload.ts'
import schema from '../lib/schema.json' with { type: 'json' }

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
describe('uploadSchema (schema sent on dataset creation/forced update)', () => {
  it('carries the "," separator on the aggregated fields', () => {
    const withSep = uploadSchema.filter((f: { separator?: string }) => f.separator === ',').map((f: { key: string }) => f.key)
    assert.deepEqual(withSep, ['SUF', 'nature_culture', 'contenance_suf', 'code_droit'])
  })
  it('keeps the same field count and keys as lib/schema.json', () => {
    assert.equal(uploadSchema.length, schema.length)
    assert.deepEqual(uploadSchema.map((f: { key: string }) => f.key), schema.map((f: { key: string }) => f.key))
  })
})
