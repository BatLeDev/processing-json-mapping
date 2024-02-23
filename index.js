const util = require('util')
const FormData = require('form-data')
const { isValid, parseISO } = require('date-fns')

// Get the value of an object by a "path" string
// obj: a javascript object like { a: { b: { c: 1 } } }
// path: a string like "a.b.c"
function getValueByPath (obj, path) {
  const keys = path.split('.')
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      obj = obj[key]
    } else {
      return null
    }
  }
  return obj
}

exports.run = async ({ pluginConfig, processingConfig, processingId, tmpDir, axios, log, patchConfig, ws }) => {
  await log.step('Initialisation')

  await log.info('Génération du schéma')
  const datasetBase = {
    isRest: true,
    title: processingConfig.dataset.title,
    primaryKey: [],
    schema: []
  }

  if (!processingConfig.detectSchema) {
    for (const column of processingConfig.columns) {
      const typeConversion = {
        Texte: 'string',
        Nombre: 'number',
        'Nombre entier': 'integer',
        Date: 'string',
        'Date et heure': 'string',
        Booléen: 'boolean',
        Objet: 'string'
      }
      if (column.isPrimaryKey) {
        datasetBase.primaryKey.push(column.columnPath)
      }

      const schemaColumn = {
        key: column.columnPath.replace('.', ''),
        type: column.multivalued ? 'string' : typeConversion[column.columnType],
        title: column.columnName ? column.columnName : column.columnPath
      }
      if (column.columnType === 'Date') schemaColumn.format = 'date'
      if (column.columnType === 'Date et heure') schemaColumn.format = 'date-time'
      if (column.columnPath.includes('.')) schemaColumn['x-originalName'] = column.columnPath
      if (column.multivalued) schemaColumn.separator = ';'

      datasetBase.schema.push(schemaColumn)
    }
    if (datasetBase.primaryKey.length === 0) {
      await log.error('Aucune clé primaire n\'a été définie')
      // throw new Error('Aucune clé primaire n\'a été définie')
    }
  }

  // if processingConfig.detectSchema
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
    datasetSchema = dataset.schema
  }

  await log.step('Récupération, conversion et envoi des données')
  const headers = { Accept: 'application/json' }
  if (processingConfig.auth && processingConfig.auth.authMethod !== 'noAuth') {
    const auth = processingConfig.auth
    if (auth.authMethod === 'bearerAuth') headers.Authorization = `Bearer ${auth.token}`
    else if (auth.authMethod === 'basicAuth') headers.Authorization = `Basic ${Buffer.from(`${auth.username}:${auth.password}`).toString('base64')}`
    else if (auth.authMethod === 'apiKey') headers[auth.apiKeyHeader] = auth.apiKeyValue
    else if (auth.authMethod === 'oauth2') {
      const formData = new URLSearchParams()

      formData.append('grant_type', auth.grantType)
      formData.append('client_id', auth.clientId)
      formData.append('client_secret', auth.clientSecret)

      if (auth.grantType === 'password_Credentials') {
        formData.append('username', auth.username)
        formData.append('password', auth.password)
      }

      try {
        const res = await axios.post(auth.tokenURL, formData)
        headers.Authorization = `Bearer ${res.data.access_token}`
      } catch (e) {
        await log.error('Erreur lors de l\'obtention du token')
        await log.error(JSON.stringify(e))
        throw new Error('Erreur lors de l\'obtention du token')
      }
    } else if (auth.authMethod === 'session') {
      const headersSession = { 'Content-Type': 'application/json' }
      if (auth.username !== '' && auth.password !== '') {
        headersSession.Authorization = `Basic ${Buffer.from(`${auth.username}:${auth.password}`).toString('base64')}`
      } else if (auth.userToken !== '') {
        headersSession.Authorization = `user_token ${auth.userToken}`
      } else {
        throw new Error('Aucune méthode d\'authentification n\'a été renseignée')
      }

      if (auth.appToken !== '') {
        headers['App-Token'] = auth.appToken
        headersSession['App-Token'] = auth.appToken
      }

      const sessionRes = await axios.get(auth.loginURL, { headers: headersSession })
      if (sessionRes.data && sessionRes.data.session_token) {
        headers['Session-Token'] = sessionRes.data.session_token
      } else {
        throw new Error('Erreur lors de la récupération du token de session')
      }
    }
  }

  let nextPageURL = processingConfig.apiURL
  while (nextPageURL) {
    await log.info(`Récupération de ${nextPageURL}`)
    let res
    try {
      res = await axios({
        method: 'get',
        url: nextPageURL,
        headers
      })
    } catch (e) {
      if (e.status && e.status === 401) {
        await log.error('Erreur d\'authentification')
        await log.error(JSON.stringify(e))
      }
      throw new Error('Erreur lors de la récupération des données')
    }
    let data = null
    if (res && res.data) {
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
      if (processingConfig.nextPagePath) {
        nextPageURL = getValueByPath(res.data, processingConfig.nextPagePath)
      } else {
        nextPageURL = null
      }
    }

    if (data) {
      if (data.length > 10000) await log.warning('Le nombre de lignes est trop important, privilégier une pagination plus petite.')
      if (data.length === 0) await log.warning('Aucune donnée n\'a été récupérée')
      await log.info(`Conversion de ${data.length} lignes`)
      const formattedLines = []
      for (const row of data) {
        const formattedRow = {}

        if (processingConfig.detectSchema) {
          for (const [key, value] of Object.entries(row)) {
            if (!datasetSchema.find((c) => c.key === key) && key !== null) {
              const type = typeof value
              const schemaColumn = { key, type }
              if (type === 'number') schemaColumn.type = Number.isInteger(value) ? 'integer' : 'number'
              else if (Array.isArray(value)) {
                schemaColumn.type = 'string'
                schemaColumn.separator = ';'
              } else if (type === 'object') {
                schemaColumn.type = 'string'
              } else if (type === 'string') {
                if (isValid(parseISO(value))) {
                  const dateValue = new Date(value)
                  const isoString = dateValue.toISOString()
                  if (isoString.endsWith('T00:00:00.000Z')) schemaColumn.format = 'date'
                  else schemaColumn.format = 'date-time'
                }
              }
              datasetSchema.push(schemaColumn)
              datasetSchemaChanged = true
            }
            if (value) {
              if (Array.isArray(value)) formattedRow[key] = value.map(element => JSON.stringify(element)).join(';')
              else if (typeof value === 'object') formattedRow[key] = JSON.stringify(value)
              else if (datasetSchema.find((c) => c.key === key && c.type === 'string' && c.format === 'date')) formattedRow[key] = new Date(value).toISOString()
              else formattedRow[key] = value
            }
          }
        } else {
          for (const column of processingConfig.columns) {
            const value = getValueByPath(row, column.columnPath)
            const path = column.columnPath.replace('.', '')
            if (column.multivalued) {
              if (Array.isArray(value)) {
                const values = []
                for (const v of value) {
                  values.push(JSON.stringify(v))
                }
                formattedRow[path] = values.join(';')
              } else {
                formattedRow[path] = JSON.stringify(value)
              }
            } else if (value) {
              if (column.columnType === 'Nombre') {
                formattedRow[path] = parseInt(value)
              } else if (column.columnType === 'Objet') {
                formattedRow[path] = JSON.stringify(value)
              } else {
                formattedRow[path] = value
              }
            }
          }
        }
        formattedLines.push(formattedRow)
      }

      if (formattedLines.length > 0) {
        try {
          if (processingConfig.detectSchema && datasetSchemaChanged) {
            await log.info('Mise a jour du schéma')
            datasetSchemaChanged = false

            const formData = new FormData()
            formData.append('schema', JSON.stringify(datasetSchema))
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
            await ws.waitForJournal(dataset.id, 'finalize-end')
            await log.info('Schéma mis à jour')
          }

          await log.info(`Envoi de ${formattedLines.length} lignes`)
          await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, formattedLines)
          await log.info('Lignes envoyées')
        } catch (e) {
          await log.error('Erreur lors de l\'envoi des données')
          if (e.data && e.data.errors) {
            await log.error(JSON.stringify(e.data.errors))
          } else {
            await log.error(e.message)
          }
        }
      }
    }
  }
  await log.step('Toutes les données ont été envoyées')
}
