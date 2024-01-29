process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const processing = require('../')

describe('test', function () {
  it('try', async function () {
    this.timeout(1000000)

    const context = testUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'Json mapping test' },
        apiURL: 'https://www.georisques.gouv.fr/api/v1/gaspar/catnat?latlon=2.29253%2C48.92572&page=1&page_size=200&rayon=10000',
        resultPath: 'data',
        nextPagePath: 'next',
        columns: [
          {
            columnPath: 'code_insee',
            multivalued: false
          },
          {
            columnPath: 'code_national_catnat',
            columnName: 'Code national du risque',
            multivalued: false
          },
          {
            columnPath: 'libelle_risque_jo',
            columnName: 'Nom du risque',
            multivalued: false
          }
        ],
        clearFile: false
      },
      tmpDir: 'data'
    }, config, false)

    await processing.run(context)
  })
})
