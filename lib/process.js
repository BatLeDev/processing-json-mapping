const fs = require('fs-extra')
const { promisify } = require('util')
const csv = require('csv')

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

const appendFilePromise = promisify(fs.appendFile)

module.exports = async (processingConfig, tmpFile, axios, log) => {
  const csvHeader = processingConfig.columns.map(c => c.columnPath).join(',')
  await fs.writeFile(tmpFile, `${csvHeader}\n`)

  await log.step('Récupération des données depuis l\'API')
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
      await log.info('Conversion en CSV')
      for (const row of data) {
        const csvRow = {}

        for (const column of processingConfig.columns) {
          const path = column.columnPath
          const value = getValueByPath(row, path)
          if (column.multivalued && Array.isArray(value)) {
            const values = []
            for (const v of value) {
              if (typeof v === 'object') {
                values.push(JSON.stringify(v))
              } else {
                values.push(v)
              }
            }
            csvRow[path] = values.join(';')
          } else if (value) {
            if (typeof value === 'object') {
              csvRow[path] = JSON.stringify(value)
            } else {
              csvRow[path] = value
            }
          } else {
            csvRow[path] = ' ' // Add a space to avoid empty cells in the CSV file
          }
        }

        const output = await promisify(csv.stringify)([csvRow], { header: false })
        await appendFilePromise(tmpFile, output)
      }
    }
  }
}
