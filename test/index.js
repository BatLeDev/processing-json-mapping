process.env.NODE_ENV = 'test'

const nock = require('nock')
const config = require('config')
const assert = require('node:assert').strict
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

  it.skip('should use oauth connection to insee API', async function () {
    this.timeout(1000000)

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    // const context = testsUtils.context({
    //   pluginConfig: {},
    //   processingConfig: {
    //     datasetMode: 'create',
    //     dataset: { title: 'Organisation avec schéma auto' },
    //     apiURL: 'https://www.data.gouv.fr/api/1/organizations/?page=1&page_size=100',
    //     resultPath: 'data',
    //     nextPagePath: 'next_page',
    //     detectSchema: false,
    //     columns: [
    //       {
    //         columnPath: 'acronym',
    //         columnName: 'Acronyme',
    //         columnType: 'Texte'
    //       },
    //       {
    //         columnPath: 'logo',
    //         columnName: 'Logo',
    //         columnType: 'Texte'
    //       },
    //       {
    //         columnPath: 'badges',
    //         columnName: 'Badges',
    //         multivalued: true
    //       },
    //       {
    //         columnPath: 'metrics.views',
    //         columnName: 'Nombre de vues',
    //         columnType: 'Nombre'
    //       }
    //     ]
    //   },
    //   tmpDir: 'data'
    // }, config, false)

    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'Json mapping test' },
        apiURL: 'https://api.insee.fr/metadonnees/V1/concepts/definitions',
        resultPath: '',
        nextPagePath: '',
        detectSchema: true,
        // auth: {
        //   authMethod: 'bearerAuth',
        //   token: config.inseeToken
        // },
        auth: {
          authMethod: 'apiKey',
          apiKeyHeader: 'Authorization',
          apiKeyValue: `Bearer ${config.inseeToken}`
        },
        // auth: {
        //   authMethod: 'oauth2',
        //   grantType: 'client_credentials',
        //   tokenURL: 'https://api.insee.fr/token',
        //   clientId: config.inseeClientId,
        //   clientSecret: config.inseeClientSecret
        // },
        clearFile: false
      },
      tmpDir: 'data'
    }, config, false)

    await processing.run(context)
  })

  it('should create a dataset from a public API, detect the schema, perform a simple pagination', async function () {
    const scope = nock('https://test.com')
      .get('/api/items')
      .reply(200, {
        data: [
          { id: 1, name: 'item1', price: 10 },
          { id: 2, name: 'item2', price: 20 }
        ],
        next_page: 'https://test.com/api/items?page=2'
      })
      .get('/api/items?page=2')
      .reply(200, {
        data: [
          { id: 3, name: 'item3', price: 30 }
        ]
      })

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'processing-json-mapping test 1' },
        apiURL: 'https://test.com/api/items',
        resultPath: 'data',
        nextPagePath: 'next_page',
        detectSchema: true
      },
      tmpDir: 'data'
    }, config, false)
    await processing.run(context)
    assert.ok(scope.isDone())

    assert.equal(context.processingConfig.datasetMode, 'update')
    assert.equal(context.processingConfig.dataset.title, 'processing-json-mapping test 1')
    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')
      let dataset = (await context.axios.get(`api/v1/datasets/${datasetId}`)).data
      assert.equal(dataset.schema.filter(p => !p['x-calculated']).length, 3)
      assert.equal(dataset.count, 3)

      const scope2 = nock('https://test.com')
        .get('/api/items')
        .reply(200, {
          data: [
            { id: 4, name: 'item4', price: 40 }
          ]
        })
      await processing.run(context)
      assert.ok(scope2.isDone())

      await context.ws.waitForJournal(datasetId, 'finalize-end')

      dataset = (await context.axios.get(`api/v1/datasets/${datasetId}`)).data
      assert.equal(dataset.count, 4)
    } finally {
      await context.axios.delete(`api/v1/datasets/${datasetId}`)
    }
  })

  it('should create a dataset from a public API with explicit schema', async function () {
    const scope = nock('https://test.com')
      .get('/api/items')
      .reply(200, {
        data: [
          { id: 1, name: 'item1', price: 10 },
          { id: 2, name: 'item2', price: 20 }
        ],
        next_page: 'https://test.com/api/items?page=2'
      })
      .get('/api/items?page=2')
      .reply(200, {
        data: [
          { id: 3, name: 'item3', price: 30 }
        ]
      })

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'processing-json-mapping test 2' },
        apiURL: 'https://test.com/api/items',
        resultPath: 'data',
        nextPagePath: 'next_page',
        detectSchema: false,
        columns: [
          {
            columnPath: 'id',
            columnName: 'Id',
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'name',
            columnName: 'Name',
            columnType: 'Texte'
          },
          {
            columnPath: 'price',
            columnName: 'Price',
            columnType: 'Nombre'
          }
        ]
      },
      tmpDir: 'data'
    }, config, false)
    await processing.run(context)
    assert.ok(scope.isDone())

    assert.equal(context.processingConfig.datasetMode, 'update')
    assert.equal(context.processingConfig.dataset.title, 'processing-json-mapping test 2')
    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')
      let dataset = (await context.axios.get(`api/v1/datasets/${datasetId}`)).data
      assert.equal(dataset.schema.filter(p => !p['x-calculated']).length, 3)
      assert.equal(dataset.count, 3)

      const scope2 = nock('https://test.com')
        .get('/api/items')
        .reply(200, {
          data: [
            { id: 1, name: 'item1 changed', price: 41 }
          ]
        })
      await processing.run(context)
      assert.ok(scope2.isDone())

      await context.ws.waitForJournal(datasetId, 'finalize-end')

      dataset = (await context.axios.get(`api/v1/datasets/${datasetId}`)).data
      assert.equal(dataset.count, 3)
    } finally {
      await context.axios.delete(`api/v1/datasets/${datasetId}`)
    }
  })

  it('should fail to update a dataset with a type change', async function () {
    const scope = nock('https://test.com')
      .get('/api/items')
      .reply(200, { data: [{ id: 1, name: 'item1' }] })

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'processing-json-mapping test 3' },
        apiURL: 'https://test.com/api/items',
        resultPath: 'data',
        nextPagePath: 'next_page',
        detectSchema: false,
        columns: [
          {
            columnPath: 'id',
            columnName: 'Id',
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'name',
            columnName: 'Name',
            columnType: 'Texte'
          }
        ]
      },
      tmpDir: 'data'
    }, config, false)
    await processing.run(context)
    assert.ok(scope.isDone())

    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')

      context.processingConfig.columns[1].columnType = 'Nombre'
      await assert.rejects(processing.run(context), (err) => {
        assert.equal(err.message, 'La configuration a changé depuis la création du jeu de donnée. La colonne name a changé de type.')
        return true
      })
    } finally {
      await context.axios.delete(`api/v1/datasets/${datasetId}`)
    }
  })

  it('should fail to update a dataset with a new column', async function () {
    const scope = nock('https://test.com')
      .get('/api/items')
      .reply(200, { data: [{ id: 1, name: 'item1' }] })

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'processing-json-mapping test 3' },
        apiURL: 'https://test.com/api/items',
        resultPath: 'data',
        nextPagePath: 'next_page',
        detectSchema: false,
        columns: [
          {
            columnPath: 'id',
            columnName: 'Id',
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'name',
            columnName: 'Name',
            columnType: 'Texte'
          }
        ]
      },
      tmpDir: 'data'
    }, config, false)
    await processing.run(context)
    assert.ok(scope.isDone())

    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')

      context.processingConfig.columns.push({
        columnPath: 'price',
        columnName: 'Price',
        columnType: 'Nombre'
      })
      await assert.rejects(processing.run(context), (err) => {
        assert.equal(err.message, 'La configuration a changé depuis la création du jeu de donnée. La colonne price n\'existe pas. La configuration peut être mise à jour avec la mise a jour forcée.')
        return true
      })
    } finally {
      await context.axios.delete(`api/v1/datasets/${datasetId}`)
    }
  })

  it('should update a dataset with a new column if forceUpdate is true', async function () {
    const scope = nock('https://test.com')
      .get('/api/items')
      .reply(200, { data: [{ id: 1, name: 'item1' }] })

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {},
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'processing-json-mapping test 3' },
        apiURL: 'https://test.com/api/items',
        resultPath: 'data',
        nextPagePath: 'next_page',
        detectSchema: false,
        columns: [
          {
            columnPath: 'id',
            columnName: 'Id',
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'name',
            columnName: 'Name',
            columnType: 'Texte'
          }
        ]
      },
      tmpDir: 'data'
    }, config, false)
    await processing.run(context)
    assert.ok(scope.isDone())

    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')

      context.processingConfig.columns.push({
        columnPath: 'price',
        columnName: 'Price',
        columnType: 'Nombre'
      })
      context.processingConfig.forceUpdate = true

      const scope2 = nock('https://test.com')
        .get('/api/items')
        .reply(200, {
          data: [
            { id: 1, name: 'item1 changed', price: 41 }
          ]
        })
      await processing.run(context)
      assert.ok(scope2.isDone())

      await context.ws.waitForJournal(datasetId, 'finalize-end')

      const dataset = (await context.axios.get(`api/v1/datasets/${datasetId}`)).data
      assert.equal(dataset.schema.filter(p => !p['x-calculated']).length, 3)
      assert.equal(dataset.count, 1)
    } finally {
      await context.axios.delete(`api/v1/datasets/${datasetId}`)
    }
  })
})
