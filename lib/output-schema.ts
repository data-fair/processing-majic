import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import schema from './schema.json' with { type: 'json' }
import legacySchema from './schema.legacy.json' with { type: 'json' }

/** A data-fair schema field, as declared in the two schema JSON files. */
export interface OutputField {
  key: string
  type: string
  [prop: string]: unknown
}

/**
 * The output schema of a run: the frozen pre-0.4 one in legacy mode, the
 * current one otherwise. Both `process` (CSV header + column order) and
 * `upload` (schema sent to data-fair) must read the run's schema from here,
 * or a legacy run writes one shape and declares another.
 */
export const schemaFor = (processingConfig: Pick<ProcessingConfig, 'legacy'>): OutputField[] =>
  processingConfig.legacy ? legacySchema : schema
