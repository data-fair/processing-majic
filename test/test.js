process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const majicProcessing = require('../')

describe('MAJIC', function () {
  it('should download, process files and upload a csv on the staging', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        clearFiles: false,
        datasetMode: 'create',
        dataset: { title: 'Parcelles des personnes morales - MAJIC', id: 'parcelles-44' },
        datasetID: 'fichiers-des-locaux-et-des-parcelles-des-personnes-morales',
        filter: '44', // Département de la Loire-Atlantique
        separator: ';', // Mutltivalued fields separator
        forceUpdate: false
      },
      tmpDir: 'data/'
    }, config, false)
    await majicProcessing.run(context)
  })
})

const wait = ms => new Promise(resolve => setTimeout(resolve, ms))

describe('MAJIC', function () {
  it('should download, process files and update a dataset on the staging', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        clearFiles: false,
        datasetMode: 'update',
        dataset: { id: 'parcelles-44', title: 'Parcelles des personnes morales - Update' },
        datasetID: 'fichiers-des-locaux-et-des-parcelles-des-personnes-morales',
        filter: '56', // Département du Morbihan
        separator: ';', // Mutltivalued fields separator
        forceUpdate: false
      },
      tmpDir: 'data/'
    }, config, false)
    await wait(420000) // Wait 7 minutes to wait for the dataset to be created
    await majicProcessing.run(context)
  })
})
