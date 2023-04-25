process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const processMAJIC = require('../lib/process.js')
// const download = require('../lib/download.js')
// const majicProcessing = require('../')
/*
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
}) */
describe('Process', function () {
  it('should create a csv', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        url: 'https://data.economie.gouv.fr/api/datasets/1.0/fichiers-des-locaux-et-des-parcelles-des-personnes-morales'
      },
      tmpDir: 'data/'
    }, config, false)
    await processMAJIC(context.tmpDir, context.log)
  })
})
