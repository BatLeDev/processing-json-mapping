const util = require('util')
const FormData = require('form-data')
const getHeaders = require('./lib/authentications')

// Get the value of an object by a "path" string
// obj: a javascript object like { a: { b: { c: 1 } } }
// path: a string like "a.b.c"
function getValueByPath (obj, path) {
  const keys = path.split('.')
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, key) && obj[key] !== null && obj[key] !== undefined) {
      obj = obj[key]
    } else {
      return null
    }
  }
  return obj
}

// Get an array from a "path" string
// obj: a javascript object like { a: { b: [ { c: 007 }, { c: 42 }, { c: 418 }, ] } }
// path: a string like "a.b.c"
// level: where the array is. Start by 0. Ex here: array level is 1, it's the key 'b'
// return an array like [ 007, 42, 418 ]
function getArrayByPath (obj, path, level) {
  const keys = path.split('.')
  const arrayPath = keys.slice(0, level + 1).join('.')
  const arrayObject = getValueByPath(obj, arrayPath)
  if (!arrayObject || !Array.isArray(arrayObject)) return []
  const valuePath = keys.slice(level + 1).join('.')
  return arrayObject.map((element) => {
    if (valuePath === '') return element
    else return getValueByPath(element, valuePath)
  })
}

/**
 * @param {string} columnPath 
 * @returns 
 */
function normalizeColumnKey (columnPath) {
  let columnKey = columnPath.replace(/\./g, '')
  if (columnKey.startsWith('_')) columnKey = columnKey.replace('_', '')
  return columnKey
}

async function updateSchema (schema, dataset, axios, log, ws) {
  await log.info('Mise à jour du schéma')

  const formData = new FormData()
  formData.append('schema', JSON.stringify(schema))
  formData.getLength = util.promisify(formData.getLength)
  const contentLength = await formData.getLength()

  dataset = (await axios({
    method: 'post',
    url: `api/v1/datasets/${dataset.id}`,
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })).data
  if (dataset.status !== 'finalized') await ws.waitForJournal(dataset.id, 'finalize-end')
  await log.info('Schéma mis à jour')
}

/**
 * @param {import('./lib/types.mjs').JSONMappingProcessingContext} context
 * @param {number} linesOffset
 * @param {number} pagesOffset
 * @param {any} [data]
 * @param {any[]} [lines]
 */
async function getPageUrl (context, linesOffset, pagesOffset, data, lines) {
  const url = context.processingConfig.apiURL
  const paginationConfig = context.processingConfig.pagination

  // deprecated parameter
  if (!paginationConfig && context.processingConfig.nextPagePath) {
    await context.log.warning('nextPagePath is deprecated, please use pagination configuration')
    return data ? getValueByPath(data, context.processingConfig.nextPagePath) : url
  }

  if (!paginationConfig || paginationConfig.method === 'none') return data ? null : url
  if (paginationConfig.method === 'queryParams') {
    const urlObj = new URL(url)
    if (paginationConfig.limitKey) {
      if (lines && paginationConfig.limitValue && lines.length < paginationConfig.limitValue) {
        await context.log.info('Le nombre de lignes récupérées est inférieur au nombre de lignes demandé, fin de la pagination.')
        return null
      }
      await context.log.debug(`Limit parameter: ${paginationConfig.limitKey}=${paginationConfig.limitValue}`)
      urlObj.searchParams.set(paginationConfig.limitKey, paginationConfig.limitValue + '')
    }
    let offset = paginationConfig.offsetPages ? pagesOffset : linesOffset
    if (!paginationConfig.offsetFrom0) offset++
    await context.log.debug(`Offset parameter: ${paginationConfig.offsetKey}=${offset}`)
    urlObj.searchParams.set(paginationConfig.offsetKey, offset + '')
    return urlObj.href
  }
  if (paginationConfig.method === 'nextPageData') {
    if (!data) {
      const urlObj = new URL(url)
      if (paginationConfig.limitKey) {
        await context.log.debug(`Limit parameter: ${paginationConfig.limitKey}=${paginationConfig.limitValue}`)
        urlObj.searchParams.set(paginationConfig.limitKey, paginationConfig.limitValue + '')
      }
      return urlObj.href
    }
    return getValueByPath(data, paginationConfig.nextPagePath)
  }
}

/**
 *
 * @param {import('./lib/types.mjs').JSONMappingProcessingContext} context
 */
exports.run = async (context) => {
  const { processingConfig, processingId, axios, log, patchConfig, ws } = context

  await log.step('Initialisation')

  // ------------------ Création du dataset ------------------
  await log.info('Génération du schéma')
  const datasetBase = {
    isRest: true,
    title: processingConfig.dataset.title,
    /** @type {string[]} */
    primaryKey: [],
    /** @type {import('./lib/types.mjs').SchemaField[]} */
    schema: []
  }

  if (!processingConfig.detectSchema) {
    // Lecture du schéma passé dans la configuration
    for (const column of processingConfig.columns ?? []) {
      const typeConversion = {
        Texte: 'string',
        Nombre: 'number',
        'Nombre entier': 'integer',
        Date: 'string',
        'Date et heure': 'string',
        Booléen: 'boolean',
        Objet: 'string'
      }
      const columnKey = normalizeColumnKey(column.columnPath)
      if (column.isPrimaryKey) {
        datasetBase.primaryKey.push(columnKey)
      }

      /** @type {import('./lib/types.mjs').SchemaField} */
      const schemaColumn = {
        key: columnKey,
        type: column.multivalued ? 'string' : typeConversion[column.columnType],
        title: column.columnPath
      }
      if (!column.multivalued) {
        if (column.columnType === 'Date') {
          schemaColumn.format = 'date'
          if (column.dateFormat) schemaColumn.dateFormat = column.dateFormat
        } else if (column.columnType === 'Date et heure') {
          schemaColumn.format = 'date-time'
          if (column.dateTimeFormat) schemaColumn.dateTimeFormat = column.dateTimeFormat
        }
      }
      if (column.columnPath !== columnKey) schemaColumn['x-originalName'] = column.columnPath
      if (column.multivalued) schemaColumn.separator = ';'

      datasetBase.schema.push(schemaColumn)
    }

    if (datasetBase.primaryKey.length === 0) {
      await log.warning('Aucune clé primaire n\'a été définie')
      // throw new Error('Aucune clé primaire n\'a été définie')
    }
  }

  // used when detectSchema is true
  let datasetSchemaChanged = false
  let datasetSchema = []

  let dataset
  if (processingConfig.datasetMode === 'create') {
    await log.info('Création du jeu de données')
    dataset = (await axios.post('api/v1/datasets', {
      ...datasetBase,
      id: processingConfig.dataset.id,
      title: processingConfig.dataset.title,
      extras: { processingId }
    })).data

    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
    await ws.waitForJournal(dataset.id, 'finalize-end')
  } else if (processingConfig.datasetMode === 'update') {
    await log.info('Vérification du jeu de données')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`Le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`Le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
    if (!processingConfig.detectSchema) {
      let schemaChanged = false
      for (const newColumn of datasetBase.schema) {
        const existingColumn = dataset.schema.find((c) => c.key === newColumn.key)
        if (existingColumn) {
          if (
            existingColumn.type !== newColumn.type ||
            existingColumn.format !== newColumn.format
          ) {
            throw new Error(`La configuration a changé depuis la création du jeu de donnée. La colonne ${newColumn.key} a changé de type.`)
          }
          if (
            existingColumn.separator !== newColumn.separator ||
            existingColumn['x-originalName'] !== newColumn['x-originalName'] ||
            existingColumn.format !== newColumn.format ||
            existingColumn.dateFormat !== newColumn.dateFormat ||
            existingColumn.dateTimeFormat !== newColumn.dateTimeFormat
          ) {
            if (processingConfig.forceUpdate) {
              existingColumn.separator = newColumn.separator
              existingColumn['x-originalName'] = newColumn['x-originalName']
              existingColumn.format = newColumn.format
              existingColumn.dateFormat = newColumn.dateFormat
              existingColumn.dateTimeFormat = newColumn.dateTimeFormat
              schemaChanged = true
            } else {
              throw new Error(`La configuration a changé depuis la création du jeu de donnée. Les informations de la colonne ${newColumn.key} ont changé. La configuration peut être mise à jour avec la mise à jour forcée.`)
            }
          }
        } else {
          if (processingConfig.forceUpdate) {
            dataset.schema.push(newColumn)
            schemaChanged = true
          } else {
            throw new Error(`La configuration a changé depuis la création du jeu de donnée. La colonne ${newColumn.key} n'existe pas. La configuration peut être mise à jour avec la mise à jour forcée.`)
          }
        }
      }

      if (schemaChanged) await updateSchema(dataset.schema, dataset, axios, log, ws)
    } else {
      datasetSchema = dataset.schema
    }
  }

  // ------------------ Récupération, conversion et envoi des données ------------------
  await log.step('Récupération, conversion et envoi des données')
  let headers = { Accept: 'application/json' }
  if (processingConfig.auth && processingConfig.auth.authMethod !== 'noAuth') {
    const authHeader = await getHeaders(processingConfig.auth, axios, log)
    headers = { ...headers, ...authHeader }
  }

  let linesOffset = 0
  let pagesOffset = 0
  /** @type {string | null} */
  let nextPageURL = await getPageUrl(context, linesOffset, pagesOffset)
  while (nextPageURL) {
    await log.info(`Récupération de ${nextPageURL}`)
    const res = await axios({
      method: 'get',
      url: nextPageURL,
      headers,
      timeout: 10 * 60000 // very long timeout as we don't control the API and some export logic are very slow
    })
    let data
    if (!res.data) {
      await log.warning('Aucune donnée n\'a été récupérée')
      break
    }
    if (processingConfig.resultPath) {
      data = getValueByPath(res.data, processingConfig.resultPath)
      if (!data) throw new Error(`Le chemin ${processingConfig.resultPath} n'existe pas dans la réponse de l'API`)
    } else {
      data = res.data
    }
    if (!Array.isArray(data)) {
      data = [data]
      await log.warning('Le résultat de l\'API n\'est pas un tableau.')
    }

    if (!data || data.length === 0) {
      await log.warning('Aucune donnée n\'a été récupérée')
      break
    }
    if (data.length > 10000) {
      await log.warning('Le nombre de lignes est trop important, privilégier une pagination plus petite.')
    }

    pagesOffset++
    linesOffset += data.length
    nextPageURL = await getPageUrl(context, linesOffset, pagesOffset, res.data, data)

    await log.info(`Conversion de ${data.length} lignes`)

    const formattedLines = [] // Contains all transformed lines
    for (const row of data) {
      // For each object in result tab

      /** @type {any} */
      const formattedRow = {}

      if (processingConfig.detectSchema) {
        for (const [key, value] of Object.entries(row)) {
          if (!datasetSchema.find((c) => c.key === key) && key !== null) {
            const type = typeof value
            /** @type {import('./lib/types.mts').SchemaField} */
            const schemaColumn = { key, type }
            if (type === 'number') schemaColumn.type = Number.isInteger(value) ? 'integer' : 'number'
            else if (Array.isArray(value)) {
              schemaColumn.type = 'string'
              schemaColumn.separator = ';'
            } else if (type === 'object') {
              schemaColumn.type = 'string'
            }
            datasetSchema.push(schemaColumn)
            datasetSchemaChanged = true
          }
          if (value) {
            if (Array.isArray(value)) formattedRow[key] = value.map(element => JSON.stringify(element)).join(';')
            else if (typeof value === 'object') formattedRow[key] = JSON.stringify(value)
            else formattedRow[key] = value
            // TODO Check and parse integer
          }
        }
      } else {
        for (const column of processingConfig.columns ?? []) {
          const key = normalizeColumnKey(column.columnPath)
          if (column.multivalued) {
            const index = (processingConfig.columns ?? []).findIndex((c) => c.columnPath === column.columnPath)
            const level = processingConfig.columns?.[index].levelOfTheArray || 0
            const valueArray = getArrayByPath(row, column.columnPath, level)
            for (let i = 0; i < valueArray.length; i++) {
              if (column.columnType === 'Nombre') {
                valueArray[i] = parseFloat(valueArray[i])
              } else if (column.columnType === 'Nombre entier') {
                valueArray[i] = parseInt(valueArray[i])
              } else if (column.columnType === 'Objet') {
                valueArray[i] = JSON.stringify(valueArray[i])
              }
            }
            formattedRow[key] = valueArray.join(';')
          } else {
            const value = getValueByPath(row, column.columnPath)
            if (value) {
              if (column.columnType === 'Nombre') {
                formattedRow[key] = parseFloat(value)
              } else if (column.columnType === 'Nombre entier') {
                formattedRow[key] = parseInt(value)
              } else if (column.columnType === 'Objet') {
                formattedRow[key] = JSON.stringify(value)
              } else if (value !== null && value !== undefined && value !== '') {
                formattedRow[key] = value
              }
            }
          }
        }
      }
      formattedLines.push(formattedRow)
    }

    if (formattedLines.length > 0) {
      if ((processingConfig.detectSchema && datasetSchemaChanged)) {
        datasetSchemaChanged = false
        await updateSchema(datasetSchema, dataset, axios, log, ws)
      }

      await log.info(`Envoi de ${formattedLines.length} lignes.`)
      const bulkRes = (await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, formattedLines)).data
      await log.info(`Lignes envoyées : ${JSON.stringify(bulkRes)}`)
    }
  }
  await log.step('Toutes les données ont été envoyées')
}
