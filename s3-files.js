var Stream = require('stream')
var AWS = require('aws-sdk')
var streamify = require('stream-array')
var concat = require('concat-stream')
var path = require('path')

var s3Files = {}
module.exports = s3Files

s3Files.connect = function (opts) {
  var self = this

  if ('s3' in opts) {
    self.s3 = opts.s3
  } else {
    AWS.config.update({
      region: opts.region
    })
    self.s3 = new AWS.S3()
  }

  self.bucket = opts.bucket
  return self
}

s3Files.createKeyStream = function (folder, keys) {
  if (!keys) return null
  var paths = []
  keys.forEach(function (key) {
    if (folder) {
      paths.push(path.posix.join(folder, key))
    } else {
      paths.push(key)
    }
  })
  return streamify(paths)
}

s3Files.createFileStream = function (keyStream, preserveFolderPath) {
  var self = this
  if (!self.bucket) return null

  var rs = new Stream()
  rs.readable = true

  var fileCounter = 0
  keyStream.on('data', function (file) {
    fileCounter += 1

    // console.log('->file', file);
    var params = { Bucket: self.bucket, Key: file }
    var s3File = self.s3.getObject(params).createReadStream()

    s3File.pipe(
      concat(function buffersEmit (buffer) {
        // console.log('buffers concatenated, emit data for ', file);
        var path = preserveFolderPath ? file : file.replace(/^.*[\\/]/, '')
        rs.emit('data', { data: buffer, path: path })
      })
    )
    s3File.on('end', function () {
      fileCounter -= 1
      if (fileCounter < 1) {
        // console.log('all files processed, emit end');
        rs.emit('end')
      }
    })

    s3File.on('error', function (err) {
      err.file = file;
      rs.emit('error', err)
    })
  })
  return rs
}
