import { describe, it } from 'node:test'
import assert from 'assert'
import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }

describe('MAJIC processing', () => {
  it('exposes a processing config schema for users', () => {
    assert.equal(processingConfigSchema.type, 'object')
  })
})
