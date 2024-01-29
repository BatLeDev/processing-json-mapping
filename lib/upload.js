const FormData = require('form-data')
const path = require('path')
const fs = require('fs-extra')
const util = require('util')

function displayBytes (aSize) {
  aSize = Math.abs(parseInt(aSize, 10))
  if (aSize === 0) return '0 octets'
  const def = [[1, 'octets'], [1000, 'ko'], [1000 * 1000, 'Mo'], [1000 * 1000 * 1000, 'Go'], [1000 * 1000 * 1000 * 1000, 'To'], [1000 * 1000 * 1000 * 1000 * 1000, 'Po']]
  for (let i = 0; i < def.length; i++) {
    if (aSize < def[i][0]) return (aSize / def[i - 1][0]).toLocaleString() + ' ' + def[i - 1][1]
  }
}

module.exports = async (processingConfig, tmpFile, axios, log) => {
  await log.step('Création du dataset')

  await log.info('Chargement du fichier csv')
  const formData = new FormData()
  const datasetSchema = processingConfig.columns.reduce((acc, column) => {
    acc.push({ key: column.columnPath, title: column.columnName || column.columnPath })
    return acc
  }, [])

  formData.append('schema', JSON.stringify(datasetSchema))
  formData.append('title', processingConfig.dataset.title)
  formData.append('file', await fs.createReadStream(tmpFile), { filename: path.parse(tmpFile).base })
  formData.getLength = util.promisify(formData.getLength)

  const contentLength = await formData.getLength()
  await log.info(`Chargement de (${displayBytes(contentLength)})`)

  await axios({
    method: 'post',
    url: (processingConfig.dataset && processingConfig.dataset.id) ? `api/v1/datasets/${processingConfig.dataset.id}` : 'api/v1/datasets',
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })
}
