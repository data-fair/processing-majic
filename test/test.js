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
        clearFiles: true,
        datasetMode: 'create',
        dataset: { title: 'Parcelles des personnes morales - MAJIC' },
        datasetID: 'fichiers-des-locaux-et-des-parcelles-des-personnes-morales',
        separator: ';', // Mutltivalued fields separator
        forceUpdate: false
      },
      tmpDir: 'data/'
    }, config, false)
    await majicProcessing.run(context)
  })
})
