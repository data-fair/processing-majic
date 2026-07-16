import fs from 'fs-extra'
import path from 'path'
import type { RunFunction, ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import download from './download.ts'
import process, { setShouldBeStopped, isStopped } from './process.ts'
import upload from './upload.ts'

export const stop = async (): Promise<void> => { setShouldBeStopped(true) }

/**
 * In update mode without forceUpdate, ensure the generated file header matches
 * the existing dataset schema; otherwise abort (the user must force the update).
 */
const checkHeader = async ({ processingConfig, tmpDir, axios, log }: ProcessingContext<ProcessingConfig>): Promise<void> => {
  try {
    await log.step('Vérification de l\'en-tête du jeu de données')
    const schemaActuel = (await axios.get(`api/v1/datasets/${processingConfig.dataset!.id}/schema`, { params: { calculated: false } }))
      .data.map((elem: { key: string }) => `"${elem.key}"`).join(',').replace(/['"]+/g, '')

    const file = path.join(tmpDir, 'Parcelles des personnes morales.csv')
    if (!await fs.pathExists(file)) throw new Error('Aucun fichier CSV généré')
    const headStream = fs.createReadStream(file, { encoding: 'utf8' })
    let head = ''
    await new Promise<void>((resolve) => {
      headStream.once('data', (chunk: string) => { head = chunk.slice(0, chunk.indexOf('\n')); resolve() })
    })
    headStream.destroy()

    if (!head.includes(schemaActuel.slice(0, head.length - 1))) {
      await log.info('Le jeu de données ne possède pas la même en-tête que le fichier généré. Activez la mise à jour forcée pour mettre à jour.')
      throw new Error('En-têtes différentes entre les fichiers')
    }
    await log.info('En-têtes identiques, mise à jour')
  } catch (err) {
    await log.info(err instanceof Error ? err.message : String(err))
    throw err
  }
}

export const run: RunFunction<ProcessingConfig> = async (context) => {
  const { processingConfig, tmpDir, axios, log, patchConfig } = context
  setShouldBeStopped(false)

  await log.step('Configuration')
  await log.info(`Jeu de données à traiter : ${processingConfig.datasetID}`)
  await log.info(`Mise à jour forcée : ${processingConfig.forceUpdate}`)

  await download(processingConfig, tmpDir, axios, log)
  await process(processingConfig, tmpDir, log)

  if (isStopped()) {
    await log.warning('Traitement interrompu, pas de publication')
    return
  }

  if (processingConfig.datasetMode === 'update' && !processingConfig.forceUpdate) {
    await checkHeader(context)
  }

  await upload(processingConfig, tmpDir, axios, log, patchConfig)

  if (processingConfig.clearFiles) await fs.emptyDir(tmpDir)
}
