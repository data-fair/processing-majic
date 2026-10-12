import { describe, it } from 'node:test'
import assert from 'assert'
import os from 'node:os'
import path from 'node:path'
import fs from 'fs-extra'
import { selectLatestYearFiles, removeSymlinks } from '../lib/download.ts'

const resources = [
  { url: 'https://x/fichier_des_parcelles_situation_2024_dpts_01_a_60_zip' },
  { url: 'https://x/fichier_des_parcelles_situation_2025_dpts_01_a_56_zip' },
  { url: 'https://x/fichier_des_parcelles_situation_2025_dpts_57_a_976_zip' },
  { url: 'https://x/fichier_des_locaux_2025_dpts_01_a_56_zip' },
  { url: 'https://x/autre_2025.pdf' }
]

describe('selectLatestYearFiles', () => {
  it('picks the most recent year available at/under currentYear', () => {
    const { year } = selectLatestYearFiles(resources, 2026)
    assert.equal(year, 2025)
  })
  it('keeps only des_parcelles zip urls of that year', () => {
    const { urls } = selectLatestYearFiles(resources, 2026)
    assert.deepEqual(urls.sort(), [
      'https://x/fichier_des_parcelles_situation_2025_dpts_01_a_56_zip',
      'https://x/fichier_des_parcelles_situation_2025_dpts_57_a_976_zip'
    ])
  })
  it('throws when no parcelles zip is found', () => {
    assert.throws(() => selectLatestYearFiles([{ url: 'https://x/nothing_2020.txt' }], 2026))
  })
})

describe('removeSymlinks', () => {
  it('removes the symbolic links but keeps the files and their targets', async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'majic-'))
    try {
      await fs.writeFile(path.join(dir, 'target.txt'), 'secret')
      await fs.writeFile(path.join(dir, 'dep01.csv'), 'a,b')
      await fs.ensureDir(path.join(dir, 'sub'))
      await fs.symlink(path.join(dir, 'target.txt'), path.join(dir, 'link.csv'))
      await fs.symlink(path.join(dir, 'sub'), path.join(dir, 'link-dir'))
      await removeSymlinks(dir)
      assert.deepEqual((await fs.readdir(dir)).sort(), ['dep01.csv', 'sub', 'target.txt'])
      assert.equal(await fs.readFile(path.join(dir, 'target.txt'), 'utf8'), 'secret')
    } finally {
      await fs.remove(dir)
    }
  })
})
