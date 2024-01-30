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
        apiURL: 'https://www.data.gouv.fr/api/1/organizations/?page=1&page_size=100',
        resultPath: 'data',
        nextPagePath: 'next_page',
        columns: [
          {
            columnPath: 'acronym',
            columnName: 'Acronyme',
            columnType: 'string'
          },
          {
            columnPath: 'logo',
            columnName: 'Logo',
            columnType: 'string'
          },
          {
            columnPath: 'badges',
            columnName: 'Badges',
            columnType: 'object',
            multivalued: true
          },
          {
            columnPath: 'metrics.views',
            columnName: 'Nombre de vues',
            columnType: 'integer'
          }
        ]
      },
      tmpDir: 'data'
    }, config, false)

    // const context = testUtils.context({
    //   pluginConfig: {},
    //   processingConfig: {
    //     datasetMode: 'create',
    //     dataset: { title: 'Json mapping test' },
    //     apiURL: 'https://api.insee.fr/metadonnees/V1/concepts/definitions',
    //     resultPath: '',
    //     nextPagePath: '',
    //     columns: [
    //       {
    //         columnPath: 'id',
    //         columnName: 'Identifiant'
    //       },
    //       {
    //         columnPath: 'intitule',
    //         columnName: 'Intitulé'
    //       }
    //     ],
    //     authorizationHeader: config.inseeAuthorizationHeader,
    //     clearFile: false
    //   },
    //   tmpDir: 'data'
    // }, config, false)

    await processing.run(context)
  })
})
