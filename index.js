const fs = require('fs-extra')
const path = require('path')
const upload = require('./lib/upload')

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
  const tmpFile = path.join(tmpDir, 'transformed.csv')
  await fs.ensureDir(tmpDir)
  const csvHeader = processingConfig.columns.map(c => c.columnPath).join(',')
  await fs.writeFile(tmpFile, `${csvHeader}\n`)

  await log.step('Récupération des données depuis l\'API')
  const headers = {}
  if (processingConfig.apiKey) {
    headers.Authorization = processingConfig.apiKey
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
      await log.info('Conversion en CSV')
      for (const row of data) {
        const csvRow = processingConfig.columns.reduce((acc, column) => {
          acc[column.columnPath] = ''
          return acc
        }, {})

        for (const column in row) {
          const columnConfig = processingConfig.columns.find(c => c.columnPath === column)
          if (columnConfig) {
            if (columnConfig.multivalued) {
              csvRow[column] = row[column].join(';')
            } else {
              csvRow[column] = row[column]
            }
          }
        }

        await fs.appendFile(tmpFile, `${Object.values(csvRow).join(',')}\n`)
      }
    }
  }

  await upload(processingConfig, tmpFile, axios, log, patchConfig)

  if (processingConfig.clearFiles) {
    await fs.emptyDir(tmpDir)
  }
}
