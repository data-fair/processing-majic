const fs = require('fs-extra')
const path = require('path')
const { parse } = require('csv-parse/sync')
const iconv = require('iconv-lite')
const csvStringify = require('csv-stringify/sync').stringify
const schema = require('./schema.json')

const parserOpts = { delimiter: ';' }
const fields = schema.map((field) => field.key)
const items = []

module.exports = async (proccesingConfig, tmpDir, log) => {
  await log.step('Traitement des données')
  for (const key in schema) {
    if (schema[key].separator) {
      schema[key].separator = proccesingConfig.separator
    }
  }
  let delimiter = ','
  if (proccesingConfig.separator === ',') {
    delimiter = ';'
  }

  const files = await fs.readdirSync(path.join(tmpDir)).filter(f => f.slice(-4) === '.csv')
  const depFiles = []
  for (const filetmp of files) {
    if (!proccesingConfig.filter || filetmp.includes(proccesingConfig.filter + '0') || (proccesingConfig.filter.length === 3 && filetmp.includes(proccesingConfig.filter))) {
      depFiles.push(filetmp)
    }
  }

  const writeStream = await fs.openSync(path.join(tmpDir, 'Parcelles des personnes morales.csv'), 'w')
  await fs.writeSync(writeStream, csvStringify(items, { header: true, delimiter, columns: fields }))

  await log.task('Traitement des fichiers')
  await log.progress('Traitement des fichiers', 0, depFiles.length)
  let i = 1
  for (const depFile of depFiles) {
    const items = []
    const parcelleMap = new Map()
    await log.info('Traitement du fichier ' + depFile + '...')
    const data = iconv.decode(await fs.readFileSync(path.join(tmpDir, depFile)), 'iso-8859-1')
    const lines = parse(data, parserOpts)
    lines.shift()
    for (const line of lines) {
      const codeCommune = line[0] + line[2].padStart(5 - line[0].length, '0')
      const codeParcelle = codeCommune + line[4].trim().padStart(3, '0') + line[5].trim().padStart(2, '0') + line[6].padStart(4, '0')
      if (!parcelleMap.has(codeParcelle)) {
        const parcelle = {
          departement: line[0],
          codeCommune,
          nomCommune: line[3],
          adresse: ((line[7] ? Number(line[7]) + ', ' : '') + (line[11] ? line[11] + ' ' : '') + line[12].replace(/"/g, ' ')).trim(),
          codeParcelle,
          codeVoieMajic: line[9],
          codeVoieRivoli: line[10],
          contencanceParcelle: line[13],
          suf: [line[14]],
          nature: [line[15].split(' - ')[0]],
          contenanceSuf: [Number(line[16])],
          codeDroit: [line[17].split(' - ')[0]],
          numMajic: line[18],
          numSiren: line[19],
          groupe: line[20].includes('A') ? line[20].replace('A', '') : line[20] || '',
          codeFormeJuridique: line[21],
          formeJuridiqueAbregee: line[22],
          denomination: line[23]
        }
        parcelleMap.set(codeParcelle, parcelle)
      } else {
        const parcelle = parcelleMap.get(codeParcelle)
        parcelle.suf.push(line[14])
        parcelle.nature.push(line[15].split(' - ')[0])
        parcelle.contenanceSuf.push(Number(line[16]))
        parcelle.codeDroit.push(line[17].split(' - ')[0])
      }
    }
    const parcelles = [...parcelleMap.values()]
    for (const parcelle of parcelles) {
      parcelle.suf = parcelle.suf.filter(s => s.length > 0).join(proccesingConfig.separator)
      parcelle.nature = parcelle.nature.filter(n => n.length > 0).join(proccesingConfig.separator)
      parcelle.contenanceSuf = parcelle.contenanceSuf.filter(c => c > 0).join(proccesingConfig.separator)
      parcelle.codeDroit = parcelle.codeDroit.filter(c => c.length > 0).join(proccesingConfig.separator)
      items.push(Object.values(parcelle))
    }
    await fs.writeSync(writeStream, csvStringify(items, { header: false, delimiter, columns: fields }))
    await log.progress('Traitement des fichiers', i, depFiles.length)
    i++
  }
}
