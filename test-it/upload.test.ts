import { describe, it } from 'node:test'
import assert from 'assert'
import { displayBytes, uploadSchema } from '../lib/upload.ts'
import schema from '../lib/schema.json' with { type: 'json' }

describe('displayBytes', () => {
  it('formats zero', () => { assert.equal(displayBytes(0), '0 octets') })
  it('formats kilobytes', () => { assert.match(displayBytes(2000), /ko$/) })
  it('formats megabytes', () => { assert.match(displayBytes(5_000_000), /Mo$/) })
})

// Regression test: 0.2.8 set schema[].separator from the (unset) processing
// config, so JSON.stringify happened to drop it and production's dataset
// schema carries no separator field. uploadSchema strips it explicitly now;
// this locks that behaviour in (lib/schema.json has some fields with a
// separator, used only to join multi-valued CSV columns while processing).
describe('uploadSchema (schema sent on dataset creation/forced update)', () => {
  it('carries no field-level separator, matching production', () => {
    for (const field of uploadSchema) assert.ok(!('separator' in field))
  })
  it('keeps the same field count and keys as lib/schema.json', () => {
    assert.equal(uploadSchema.length, schema.length)
    assert.deepEqual(uploadSchema.map((f: { key: string }) => f.key), schema.map((f: { key: string }) => f.key))
  })
})
