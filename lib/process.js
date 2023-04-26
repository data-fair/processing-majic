const fs = require('fs-extra')
const path = require('path')
const { parse } = require('csv-parse/sync')
const parserOpts = { delimiter: ';' }
const iconv = require('iconv-lite')

const fields = [
  'Département',
  'Code Commune',
  'Nom Commune',
  'Adresse',
  'Code parcelle',
  'Code voie MAJIC',
  'Code voie rivoli',
  'Contenance parcelle',
  'SUF',
  'Nature culture',
  'Contenance SUF',
  'Code droit',
  'N° MAJIC',
  'N° SIREN',
  'Groupe personne',
  'Code forme juridique',
  'Forme juridique abrégée',
  'Dénomination'
]

module.exports = async (proccesingConfig, tmpDir, log) => {
  await log.step('Traitement des données')
  const writeStream = await fs.openSync(path.join(tmpDir, 'Parcelles des personnes morales.csv'), 'w')
  fs.writeSync(writeStream, fields.join(',') + '\n')

  const files = await fs.readdirSync(path.join(tmpDir)).filter(f => f.slice(-4) === '.txt')
  const depFiles = []
  for (const filetmp of files) {
    if (proccesingConfig.filter.length === 2) {
      if (filetmp.includes(proccesingConfig.filter + '0')) {
        depFiles.push(filetmp)
      }
    } else {
      if (filetmp.includes(proccesingConfig.filter)) {
        depFiles.push(filetmp)
      }
    }
  }
  for (const depFile of depFiles) {
    log.info('Traitement du fichier ' + depFile + '...')
    const data = iconv.decode(await fs.readFileSync(path.join(tmpDir, depFile)), 'iso-8859-1')
    const lines = parse(data, parserOpts)
    lines.shift()
    for (const line of lines) {
      const codeCommune = line[0] + line[2].padStart(5 - line[0].length, '0')
      const codeParcelle = codeCommune + line[4].trim().padStart(3, '0') + line[5].padStart(2, '0') + line[6].padStart(4, '0')
      const item = {
        Département: line[0],
        'Code Commune': codeCommune,
        'Nom Commune': line[3],
        Adresse: ((line[7] ? line[7] + ', ' : '') + (line[11] ? line[11] + ' ' : '') + line[12].replace(/"/g, ' ')).trim(),
        'Code parcelle': codeParcelle,
        'Code voie MAJIC': line[9],
        'Code voie rivoli': line[10],
        'Contenance parcelle': line[13],
        SUF: line[14],
        'Nature culture': line[15],
        'Contenance SUF': line[16],
        'Code droit': line[17],
        'N° MAJIC': line[18],
        'N° SIREN': line[19],
        'Groupe personne': line[20] || '',
        'Code forme juridique': line[21],
        'Forme juridique abrégée': line[22],
        Dénomination: line[23]
      }
      fs.writeSync(writeStream, fields.map(f => item[f] ? `"${item[f]}"` : '').join(',') + '\n')
    }
  }
}
