const fs = require('fs-extra')
const path = require('path')
const { parse } = require('csv-parse/sync')
const parserOpts = { delimiter: ';' }
const iconv = require('iconv-lite')

const codesGroupes = {
  0: 'Personnes morales non remarquables',
  1: 'État',
  2: 'Région',
  3: 'Département',
  4: 'Commune',
  5: 'Office HLM',
  6: 'Personnes morales représentant des sociétés d’économie mixte',
  7: 'Copropriétaires',
  8: 'Associés',
  9: 'Établissements publics ou organismes associés'
}

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

module.exports = async (dir = 'data', log) => {
  const dirName = __dirname.split(path.sep).slice(0, -1).join(path.sep)
  await log.step('Traitement des données')
  const writeStream = await fs.openSync(path.join(dirName + '/' + dir, 'Parcelles des personnes morales.csv'), 'w')
  fs.writeSync(writeStream, fields.join(',') + '\n')

  const files = await fs.readdirSync(path.join(dirName, dir)).filter(f => f.slice(-4) === '.txt')
  // eslint-disable-next-line no-unreachable-loop
  for (const file of files) {
    // const tok = file.slice(9, 12)
    // const dep = ['971', '972', '973', '974', '976'].includes(tok) ? tok : tok.slice(0, 2)
    // const coordinates = JSON.parse(await fs.readFileSync(path.join(dirName, 'data/parcelsCoords/' + dep + '.json'), 'utf-8'))
    log.info('Traitement du fichier ' + file + '...')
    const data = iconv.decode(await fs.readFileSync(path.join(dirName, dir, file)), 'iso-8859-1')
    const lines = parse(data, parserOpts)
    lines.shift()
    for (const line of lines) {
      const codeCommune = line[0] + line[2].padStart(5 - line[0].length, '0')
      const codeParcelle = codeCommune + line[4].trim().padStart(3, '0') + line[5].padStart(2, '0') + line[6].padStart(4, '0')
      // const coords = coordinates[codeParcelle]
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
        'Groupe personne': codesGroupes[line[20]] || '',
        'Code forme juridique': line[21],
        'Forme juridique abrégée': line[22],
        Dénomination: line[23]
        // Longitude: coords ? coords[0] : undefined,
        // Latitude: coords ? coords[1] : undefined
      }
      fs.writeSync(writeStream, fields.map(f => item[f] ? `"${item[f]}"` : '').join(',') + '\n')
    }
    break
  }
}
