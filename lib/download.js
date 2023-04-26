const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const exec = util.promisify(require('child_process').exec)
const pump = util.promisify(require('pump'))

module.exports = async (processingConfig, tmpDir, axios, log) => {
  await log.step('Téléchargement des données')

  await log.info('Filtrage des dernières données')
  const json = await axios.get(processingConfig.url)
  const filtre = []
  const attachments = json.data.attachments
  let year
  let found = false
  let i = 0
  while (!found) {
    year = new Date().getFullYear() - i
    attachments.forEach(element => {
      if (element.id.includes(`${year}`)) {
        found = true
      }
    })
    i++
  }
  log.info(`Dernière année trouvée : ${year}`)
  for (let i = 0; i < attachments.length; i++) {
    if (attachments[i].id.includes(`${year}`)) {
      if (attachments[i].id.includes('fichier_des_parcelles')) {
        if (attachments[i].id.includes('zip')) {
          filtre.push(attachments[i].id)
        }
      }
    }
  }
  if (filtre.length === 0) {
    await log.warning('Aucun fichier trouvé')
  } else {
    await log.info(`Nombres fichies trouvés : ${filtre.length}`)
    await log.info('Téléchargement des fichiers...')

    const fileName = path.basename(processingConfig.url)
    const file = path.join(tmpDir, fileName)
    let res
    for (let i = 0; i < filtre.length; i++) {
      await log.info(`Téléchargement du fichier ${filtre[i]}...`)
      try {
        res = await axios.get(processingConfig.url + '/attachments/' + filtre[i], { responseType: 'stream' })
      } catch (err) {
        await log.error(`Téléchargement du fichier ${filtre[i]} a échoué`)
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
      await log.info(`Décompression du fichier ${filtre[i]}...`)
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
