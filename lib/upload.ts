import fs from 'fs-extra'
import path from 'path'
import { promisify } from 'util'
import FormData from 'form-data'
import type { AxiosInstance } from 'axios'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import schema from './schema.json' with { type: 'json' }

// 0.2.8 set schema[].separator from the (unset) config, so JSON.stringify dropped it:
// production's dataset schema carries no separator. Keep it that way.
export const uploadSchema = schema.map(({ separator, ...field }: Record<string, unknown>) => field)

export const displayBytes = (aSize: number): string => {
  aSize = Math.abs(parseInt(String(aSize), 10))
  if (aSize === 0) return '0 octets'
  const def: [number, string][] = [[1, 'octets'], [1000, 'ko'], [1000 * 1000, 'Mo'], [1000 * 1000 * 1000, 'Go'], [1000 * 1000 * 1000 * 1000, 'To'], [1000 * 1000 * 1000 * 1000 * 1000, 'Po']]
  for (let i = 0; i < def.length; i++) {
    if (aSize < def[i][0]) return (aSize / def[i - 1][0]).toLocaleString() + ' ' + def[i - 1][1]
  }
  return aSize + ' octets'
}

export default async (
  processingConfig: ProcessingConfig,
  tmpDir: string,
  axios: AxiosInstance,
  log: ProcessingContext<ProcessingConfig>['log'],
  patchConfig: ProcessingContext<ProcessingConfig>['patchConfig']
): Promise<void> => {
  const formData = new FormData()

  if (processingConfig.datasetMode === 'update') {
    await log.step('Mise à jour du jeu de données')
    if (processingConfig.forceUpdate) {
      await log.info('Mise à jour du schéma')
      formData.append('schema', JSON.stringify(uploadSchema))
    }
  } else {
    formData.append('schema', JSON.stringify(uploadSchema))
    formData.append('title', processingConfig.dataset?.title || 'Parcelles des personnes morales')
    await log.step('Création du jeu de données')
  }

  const filePath = path.join(tmpDir, 'Parcelles des personnes morales.csv')
  formData.append('file', fs.createReadStream(filePath), { filename: path.parse(filePath).base })

  const contentLength = await promisify(formData.getLength).call(formData)
  await log.info(`Chargement de (${displayBytes(contentLength)})`)

  const datasetId = processingConfig.dataset?.id
  const dataset = (await axios({
    method: 'post',
    url: datasetId ? `api/v1/datasets/${datasetId}` : 'api/v1/datasets',
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })).data

  if (processingConfig.datasetMode === 'update') {
    await log.info(`jeu de données mis à jour, id="${dataset.id}", title="${dataset.title}"`)
  } else {
    await log.info(`jeu de données créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  }
}
