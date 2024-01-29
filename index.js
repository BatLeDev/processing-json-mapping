const fs = require('fs-extra')
const path = require('path')
const process = require('./lib/process')
const upload = require('./lib/upload')

exports.run = async ({ pluginConfig, processingConfig, processingId, tmpDir, axios, log, patchConfig }) => {
  const tmpFile = path.join(tmpDir, 'transformed.csv')
  await fs.ensureDir(tmpDir)

  await process(processingConfig, tmpFile, axios, log)
  await upload(processingConfig, tmpFile, axios, log, patchConfig)

  if (processingConfig.clearFiles) {
    await fs.emptyDir(tmpDir)
  }
}
