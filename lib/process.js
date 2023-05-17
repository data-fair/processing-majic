const fs = require('fs-extra')
const path = require('path')
const { parse } = require('csv-parse/sync')
const iconv = require('iconv-lite')
const csvStringify = require('csv-stringify/sync').stringify
const schema = require('./schema.json')

const parserOpts = { delimiter: ';' }
const fields = schema.map((field) => field.key)
const items = []
const parcelleMap = new Map()

module.exports = async (proccesingConfig, tmpDir, log) => {
  await log.step('Traitement des données')
  for (const key in schema) {
    if (schema[key].separator) {
      schema[key].separator = proccesingConfig.separator
    }
  }
  const writeStream = await fs.openSync(path.join(tmpDir, 'Parcelles des personnes morales.csv'), 'w')

  const files = await fs.readdirSync(path.join(tmpDir)).filter(f => f.slice(-4) === '.txt')
  const depFiles = []
  for (const filetmp of files) {
    if (proccesingConfig.filter) {
      if (proccesingConfig.filter.length === 2) {
        if (filetmp.includes(proccesingConfig.filter + '0')) {
          depFiles.push(filetmp)
        }
      } else {
        if (filetmp.includes(proccesingConfig.filter)) {
          depFiles.push(filetmp)
        }
      }
    } else {
      depFiles.push(filetmp)
    }
  }
  for (const depFile of depFiles) {
    log.info('Traitement du fichier ' + depFile + '...')
    const data = iconv.decode(await fs.readFileSync(path.join(tmpDir, depFile)), 'iso-8859-1')
    const lines = parse(data, parserOpts)
    lines.shift()
    const suf = []; const nature = []; const contenanceSuf = []; const codeDroit = []
    for (const line of lines) {
      const codeCommune = line[0] + line[2].padStart(5 - line[0].length, '0')
      const codeParcelle = codeCommune + line[4].trim().padStart(3, '0') + line[5].padStart(2, '0') + line[6].padStart(4, '0')
      if (!parcelleMap.has(codeParcelle)) {
        suf.length = 0; nature.length = 0; contenanceSuf.length = 0; codeDroit.length = 0
        const parcelle = {
          departement: line[0],
          codeCommune,
          nomCommune: line[3],
          adresse: ((line[7] ? Number(line[7]) + ', ' : '') + (line[11] ? line[11] + ' ' : '') + line[12].replace(/"/g, ' ')).trim(),
          codeParcelle,
          codeVoieMajic: line[9],
          codeVoieRivoli: line[10],
          contencanceParcelle: line[13],
          suf: line[14],
          nature: line[15],
          contenanceSuf: Number(line[16]),
          codeDroit: line[17],
          numMajic: line[18],
          numSiren: line[19],
          groupe: line[20] || '',
          codeFormeJuridique: line[21],
          formeJuridiqueAbregee: line[22],
          denomination: line[23]
        }
        suf.push(parcelle.suf)
        nature.push(parcelle.nature)
        contenanceSuf.push(parcelle.contenanceSuf)
        codeDroit.push(parcelle.codeDroit)
        parcelleMap.set(codeParcelle, parcelle)
      } else {
        const parcelle = parcelleMap.get(codeParcelle)
        suf.push(line[14])
        nature.push(line[15])
        contenanceSuf.push(Number(line[16]))
        codeDroit.push(line[17])
        parcelle.suf = suf.filter((suf) => suf.length > 0).join(proccesingConfig.separator)
        parcelle.nature = nature.filter((nature) => nature.length > 0).join(proccesingConfig.separator)
        parcelle.contenanceSuf = contenanceSuf.filter((contenanceSuf) => contenanceSuf > 0).join(proccesingConfig.separator)
        parcelle.codeDroit = codeDroit.filter((codeDroit) => codeDroit.length > 0).join(proccesingConfig.separator)
      }
    }
    const parcelles = [...parcelleMap.values()]
    for (const parcelle of parcelles) {
      items.push(Object.values(parcelle))
    }
  }
  await log.info(`Ecriture de ${items.length} lignes dans le fichier Parcelles des personnes morales.csv`)
  await fs.writeSync(writeStream, csvStringify(items, { header: true, delimiter: ',', columns: fields }))
}
