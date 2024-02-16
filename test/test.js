process.env.NODE_ENV = 'test'
const config = require('config')
const assert = require('assert').strict
const processing = require('../')

describe('test', function () {
  it('should expose a plugin config schema for super admins', async () => {
    const schema = require('../plugin-config-schema.json')
    assert.ok(schema)
  })
  it('should expose a processing config schema for users', async () => {
    const schema = require('../processing-config-schema.json')
    assert.equal(schema.type, 'object')
  })

  it('try', async function () {
    this.timeout(1000000)

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'Organisation avec schéma auto' },
        apiURL: 'https://www.data.gouv.fr/api/1/organizations/?page=1&page_size=100',
        resultPath: 'data',
        nextPagePath: 'next_page',
        detectSchema: false,
        columns: [
          {
            columnPath: 'acronym',
            columnName: 'Acronyme',
            columnType: 'Texte'
          },
          {
            columnPath: 'logo',
            columnName: 'Logo',
            columnType: 'Texte'
          },
          {
            columnPath: 'badges',
            columnName: 'Badges',
            multivalued: true
          },
          {
            columnPath: 'metrics.views',
            columnName: 'Nombre de vues',
            columnType: 'Nombre'
          }
        ]
      },
      tmpDir: 'data'
    }, config, false)

    // const context = testsUtils.context({
    //   pluginConfig: {},
    //   processingConfig: {
    //     datasetMode: 'create',
    //     dataset: { title: 'Json mapping test' },
    //     apiURL: 'https://api.insee.fr/metadonnees/V1/concepts/definitions',
    //     resultPath: '',
    //     nextPagePath: '',
    //     detectSchema: true,
    //     // auth: {
    //     //   authMethod: 'bearerAuth',
    //     //   token: config.inseeToken
    //     // },
    //     auth: {
    //       authMethod: 'oauth2',
    //       grantType: 'client_credentials',
    //       tokenURL: 'https://api.insee.fr/token',
    //       clientId: config.inseeClientId,
    //       clientSecret: config.inseeClientSecret
    //     },
    //     clearFile: false
    //   },
    //   tmpDir: 'data'
    // }, config, false)

    await processing.run(context)
  })
})
