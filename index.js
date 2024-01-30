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

exports.run = async ({ pluginConfig, processingConfig, processingId, tmpDir, axios, log, patchConfig }) => {
  await log.step('Initialisation')

  await log.info('Génération du schéma')
  const datasetBase = {
    isRest: true,
    title: processingConfig.dataset.title,
    primaryKey: [],
    schema: []
  }
  for (const column of processingConfig.columns) {
    if (column.isPrimaryKey) {
      datasetBase.primaryKey.push(column.columnPath)
    }

    const schema = {
      key: column.columnPath.replace('.', ''),
      type: column.columnType,
      title: column.columnName ? column.columnName : column.columnPath
    }
    if (column.columnPath.includes('.')) {
      schema['x-originalName'] = column.columnPath
    }
    datasetBase.schema.push(schema)
  }

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
  } else if (processingConfig.datasetMode === 'update') {
    await log.info('Vérification du jeu de données')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`Le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`Le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
  }

  await log.step('Récupération, conversion et envoi des données')
  const headers = { Accept: 'application/json' }
  if (processingConfig.authorizationHeader) {
    headers.Authorization = processingConfig.authorizationHeader
  }

  let nextPageURL = processingConfig.apiURL
  while (nextPageURL) {
    await log.info(`Récupération de ${nextPageURL}`)
    const res = await axios({
      method: 'get',
      url: nextPageURL,
      headers
    })

    let data = null
    if (res && res.data) {
      if (processingConfig.resultPath) {
        data = getValueByPath(res.data, processingConfig.resultPath)
        if (!data) {
          throw new Error(`Le chemin ${processingConfig.resultPath} n'existe pas dans la réponse de l'API`)
        }
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
      await log.info('Conversion des données')
      const formattedLines = []
      for (const row of data) {
        const formattedRow = {}
        for (const column of processingConfig.columns) {
          const path = column.columnPath
          const value = getValueByPath(row, path)
          if (column.multivalued && Array.isArray(value)) {
            const values = []
            for (const v of value) {
              values.push(v)
            }
            formattedRow[path] = values.join(';')
          } else if (value) {
            formattedRow[path] = value
          } else {
            formattedRow[path] = ''
          }
        }
        formattedLines.push(formattedRow)
      }

      if (formattedLines.length > 0) {
        await log.info(`Envoi de ${formattedLines.length} lignes`)
        await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, formattedLines)
      }
    }
  }
}
