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
    //         columnType: 'Texte'
    //       },
    //       {
    //         columnPath: 'logo',
    //         columnType: 'Texte'
    //       },
    //       {
    //         columnPath: 'badges',
    //         multivalued: true
    //       },
    //       {
    //         columnPath: 'metrics.views',
    //         columnType: 'Nombre'
    //       }
    //     ]
    //   },
    //   tmpDir: 'data'
    // }, config, true)

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
    }, config, true)

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
        dataset: { title: 'processing-json-mapping test simple' },
        apiURL: 'https://test.com/api/items',
        resultPath: 'data',
        pagination: {
          method: 'nextPageData',
          nextPagePath: 'next_page'
        },
        detectSchema: true
      },
      tmpDir: 'data'
    }, config, true)
    await processing.run(context)
    assert.ok(scope.isDone())

    assert.equal(context.processingConfig.datasetMode, 'update')
    assert.equal(context.processingConfig.dataset.title, 'processing-json-mapping test simple')
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

  it('should perform a pagination based on offset parameter', async function () {
    const scope = nock('https://test.com')
      .get('/api/items?offset=0&limit=2')
      .reply(200, {
        data: [
          { id: 1, name: 'item1', price: 10 },
          { id: 2, name: 'item2', price: 20 }
        ],
        next_page: 'https://test.com/api/items?offset=2&limit=2'
      })
      .get('/api/items?offset=2&limit=2')
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
        dataset: { title: 'processing-json-mapping test offset page' },
        apiURL: 'https://test.com/api/items',
        resultPath: 'data',
        pagination: {
          method: 'queryParams',
          offsetKey: 'offset',
          offsetFrom0: true,
          limitKey: 'limit',
          limitValue: 2
        },
        detectSchema: true
      },
      tmpDir: 'data'
    }, config, true)
    await processing.run(context)
    assert.ok(scope.isDone())

    assert.equal(context.processingConfig.datasetMode, 'update')
    assert.equal(context.processingConfig.dataset.title, 'processing-json-mapping test offset page')
    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')
      let dataset = (await context.axios.get(`api/v1/datasets/${datasetId}`)).data
      assert.equal(dataset.schema.filter(p => !p['x-calculated']).length, 3)
      assert.equal(dataset.count, 3)

      const scope2 = nock('https://test.com')
        .get('/api/items?offset=0&limit=2')
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
          { id: 1, info: { name: 'item1', price: 10 } },
          { id: 2, info: { name: 'item2', price: 20 } }
        ],
        next_page: 'https://test.com/api/items?page=2'
      })
      .get('/api/items?page=2')
      .reply(200, {
        data: [
          { id: 3, info: { name: 'item3', price: 30 } }
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
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'info.name',
            columnType: 'Texte'
          },
          {
            columnPath: 'info.price',
            columnType: 'Nombre'
          }
        ]
      },
      tmpDir: 'data'
    }, config, true)
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
            { id: 1, info: { name: 'item1 changed', price: 41 } }
          ]
        })
      await processing.run(context)
      assert.ok(scope2.isDone())

      await context.ws.waitForJournal(datasetId, 'finalize-end')

      dataset = (await context.axios.get(`api/v1/datasets/${datasetId}`)).data
      assert.equal(dataset.count, 3)
      assert.equal(dataset.schema[0].key, 'id')
      assert.equal(dataset.schema[1].key, 'infoname')
      assert.equal(dataset.schema[1].title, 'info.name')
      assert.equal(dataset.schema[2].key, 'infoprice')
      assert.equal(dataset.schema[2].title, 'info.price')

      const lines = (await context.axios.get(`api/v1/datasets/${datasetId}/lines`)).data.results
      assert.equal(lines.length, 3)
      assert.equal(lines[0].id, 1)
      assert.equal(lines[0].infoname, 'item1 changed')
      assert.equal(lines[0].infoprice, 41)
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
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'name',
            columnType: 'Texte'
          }
        ]
      },
      tmpDir: 'data'
    }, config, true)
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
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'name',
            columnType: 'Texte'
          }
        ]
      },
      tmpDir: 'data'
    }, config, true)
    await processing.run(context)
    assert.ok(scope.isDone())

    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')

      context.processingConfig.columns.push({
        columnPath: 'price',
        columnType: 'Nombre'
      })
      await assert.rejects(processing.run(context), (err) => {
        assert.equal(err.message, 'La configuration a changé depuis la création du jeu de donnée. La colonne price n\'existe pas. La configuration peut être mise à jour avec la mise à jour forcée.')
        return true
      })
    } finally {
      await context.axios.delete(`api/v1/datasets/${datasetId}`)
    }
  })

  it('should update a dataset with a new column name', async function () {
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
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'nameko',
            columnType: 'Texte'
          }
        ]
      },
      tmpDir: 'data'
    }, config, true)
    await processing.run(context)
    assert.ok(scope.isDone())

    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')

      context.processingConfig.columns[1].columnPath = 'na.me'
      context.processingConfig.forceUpdate = true

      const scope2 = nock('https://test.com')
        .get('/api/items')
        .reply(200, {
          data: [
            { id: 1, na: { me: 'item1' }, price: 41 }
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
            columnType: 'Nombre',
            isPrimaryKey: true
          },
          {
            columnPath: 'name',
            columnType: 'Texte'
          }
        ]
      },
      tmpDir: 'data'
    }, config, true)
    await processing.run(context)
    assert.ok(scope.isDone())

    const datasetId = context.processingConfig.dataset.id

    try {
      await context.ws.waitForJournal(datasetId, 'finalize-end')

      context.processingConfig.columns.push({
        columnPath: 'price',
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
