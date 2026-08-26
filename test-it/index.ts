import { describe, it } from 'node:test'
import assert from 'assert'
import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }

const params = processingConfigSchema.allOf.find(g => g.title === 'Paramètres')!.properties as Record<string, {
  type?: string
  default?: unknown
  layout?: { if?: string }
}>

describe('MAJIC processing', () => {
  it('exposes a processing config schema for users', () => {
    assert.equal(processingConfigSchema.type, 'object')
  })
})

describe('legacy mode is opt-in and hides the options it ignores', () => {
  // Default false: an existing processing keeps producing the current schema
  // after an upgrade, and a new one is never accidentally created legacy.
  it('declares legacy as a boolean defaulting to false', () => {
    assert.equal(params.legacy.type, 'boolean')
    assert.equal(params.legacy.default, false)
  })

  // The legacy aggregation keeps every code droit, so leaving the picker
  // visible would promise a filter that silently does nothing.
  it('hides the code droit picker when legacy is on', () => {
    assert.equal(params.codeDroit.layout?.if, 'parent.data.legacy !== true')
  })
})
