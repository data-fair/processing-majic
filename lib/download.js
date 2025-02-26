const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const exec = util.promisify(require('child_process').exec)
const pump = util.promisify(require('pump'))

const url = 'https://www.data.gouv.fr/api/1/datasets/'

module.exports = async (processingConfig, tmpDir, axios, log) => {
  await log.step('Téléchargement des données')

  await log.info('Filtrage des dernières données')
  const json = await axios.get(url + processingConfig.datasetID + '/')
  const filtre = []
  const data = json.data.resources
  let year
  let found = false
  let i = 0
  while (!found) {
    year = new Date().getFullYear() - i
    data.forEach(element => {
      if (element.url.includes(`${year}`)) {
        found = true
      }
    })
    i++
  }
  await log.info(`Dernière année trouvée : ${year}`)
  for (let i = 0; i < data.length; i++) {
    if (data[i].url.includes(`${year}`)) {
      if (data[i].url.includes('des_parcelles')) {
        if (data[i].url.includes('zip')) {
          filtre.push(data[i].url)
        }
      }
    }
  }
  if (filtre.length === 0) {
    await log.error('Aucun fichier trouvé')
    throw new Error('Aucun fichier trouvé')
  } else {
    await log.info(`Nombres fichies trouvés : ${filtre.length}`)
    await log.info('Téléchargement des fichiers...')

    let res
    for (let i = 0; i < filtre.length; i++) {
      const fileName = path.basename(filtre[i])
      const file = path.join(tmpDir, fileName)
      await log.info(`Téléchargement du fichier ${fileName}...`)
      try {
        res = await axios.get(filtre[i], { responseType: 'stream' })
      } catch (err) {
        await log.error(`Téléchargement du fichier ${fileName} a échoué`)
        await log.error(err)
        throw new Error(JSON.stringify(err, null, 2))
      }
      await fs.ensureFile(file)
      await pump(res.data, fs.createWriteStream(file))
      await log.info('Téléchargement terminé')
      // Try to prevent weird bug with NFS by forcing syncing file before reading it
      const fd = await fs.open(file, 'r')
      await fs.fsync(fd)
      await fs.close(fd)
      await log.info(`Décompression du fichier ${fileName}...`)
      try {
        await exec(`unzip -j ${file} -d ${tmpDir}`)
      } catch (err) {
        log.warning('Impossible d\'extraire l\'archive, le fichier est peut-être déjà extrait')
      }
      await log.info('Décompression terminée')
      await fs.remove(file)
    }
  }
}
