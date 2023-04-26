process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const processData = require('../lib/process.js')
const download = require('../lib/download.js')
const upload = require('../lib/upload.js')
const majicProcessing = require('../')

describe('Download', function () {
  it('should download a zip', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        url: 'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales'
      },
      tmpDir: 'data/'
    }, config, false)
    await download(context.processingConfig, context.tmpDir, context.axios, context.log)
  })
})
describe('Process', function () {
  it('should create a csv', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        url: 'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales',
        filter: '56' // Département du Morbihan
      },
      tmpDir: 'data/'
    }, config, false)
    await processData(context.processingConfig, context.tmpDir, context.log)
  })
})

describe('Upload', function () {
  it('should upload a csv', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'Parcelles des personnes morales' },
        url: 'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales',
        filter: '56' // Département du Morbihan
      },
      tmpDir: 'data/'
    }, config, false)
    await upload(context.processingConfig, context.tmpDir, context.axios, context.log, context.patchConfig)
  })
})

describe('MAJIC', function () {
  it('should download, process files and upload a csv on the staging', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        clearFiles: true,
        datasetMode: 'create',
        dataset: { title: 'Parcelles des personnes morales - MAJIC', id: 'majic-test' },
        url: 'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales',
        filter: '56', // Département du Morbihan
        processFile: 'fichiers-des-parcelles-des-personnes-morales',
        forceUpdate: false
      },
      tmpDir: 'data/'
    }, config, false)
    await majicProcessing.run(context)
  })
})
