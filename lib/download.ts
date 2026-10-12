import fs from 'fs-extra'
import path from 'path'
import { promisify } from 'util'
import { execFile as execFileCb } from 'child_process'
import pumpCb from 'pump'
import type { AxiosInstance } from 'axios'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'

const execFile = promisify(execFileCb)
const pump = promisify(pumpCb) as (...streams: unknown[]) => Promise<void>

const DATA_GOUV = 'https://www.data.gouv.fr/api/1/datasets/'

/**
 * Pick the most recent year present in the resource urls (<= currentYear) and
 * keep the "des_parcelles" zip urls of that year.
 */
export const selectLatestYearFiles = (resources: { url: string }[], currentYear: number): { year: number, urls: string[] } => {
  let year = currentYear
  let found = false
  while (!found && year > currentYear - 20) {
    if (resources.some(r => r.url.includes(`${year}`))) found = true
    else year--
  }
  const urls = resources
    .filter(r => r.url.includes(`${year}`) && r.url.includes('des_parcelles') && r.url.includes('zip'))
    .map(r => r.url)
  if (urls.length === 0) throw new Error('Aucun fichier trouvé')
  return { year, urls }
}

/**
 * unzip restores the symbolic links of an archive: reading or writing one of
 * them would reach a file of the worker outside of the tmp directory.
 */
export const removeSymlinks = async (dir: string): Promise<void> => {
  for (const f of await fs.readdir(dir)) {
    if ((await fs.lstat(path.join(dir, f))).isSymbolicLink()) await fs.remove(path.join(dir, f))
  }
}

export default async (processingConfig: ProcessingConfig, tmpDir: string, axios: AxiosInstance, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Téléchargement des données')
  await log.info('Filtrage des dernières données')
  const { data } = await axios.get(DATA_GOUV + processingConfig.datasetID + '/')
  let selection
  try {
    selection = selectLatestYearFiles(data.resources, new Date().getFullYear())
  } catch (err) {
    await log.error('Aucun fichier trouvé')
    throw err
  }
  const { year, urls } = selection
  await log.info(`Dernière année trouvée : ${year}`)
  await log.info(`Nombre de fichiers trouvés : ${urls.length}`)

  for (const url of urls) {
    const fileName = path.basename(url)
    const file = path.join(tmpDir, fileName)
    await log.info(`Téléchargement du fichier ${fileName}...`)
    let res
    try {
      res = await axios.get(url, { responseType: 'stream' })
    } catch (err) {
      await log.error(`Téléchargement du fichier ${fileName} a échoué`)
      throw err
    }
    await fs.ensureFile(file)
    await pump(res.data, fs.createWriteStream(file))
    await log.info('Téléchargement terminé')
    // Force syncing before reading (prevents an NFS race)
    const fd = await fs.open(file, 'r')
    await fs.fsync(fd)
    await fs.close(fd)
    await log.info(`Décompression du fichier ${fileName}...`)
    try {
      await execFile('unzip', ['-j', file, '-d', tmpDir])
    } catch {
      await log.warning('Impossible d\'extraire l\'archive, le fichier est peut-être déjà extrait')
    }
    await removeSymlinks(tmpDir)
    await log.info('Décompression terminée')
    await fs.remove(file)
  }
}
