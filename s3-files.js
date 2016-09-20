var Stream = require('stream');
var AWS = require('aws-sdk');
var streamify = require('stream-array');
var concat = require('concat-stream');


module.exports = s3Files = {};

s3Files.connect = function (opts) {
  var self = this;
  AWS.config.update({
    'region': opts.region
  });
  self.s3 = new AWS.S3();
  self.bucket = opts.bucket;
  return self;
};

s3Files.createKeyStream = function (folder, keys) {
  var self = this;
  self.folder = folder;
  if (!keys) return null;
  var paths = [];
  keys.forEach(function (key) {
    paths.push(folder + key);
  });
  return streamify(paths);
};

s3Files.createFileStream = function (keyStream) {
  var self = this;
  if (!self.bucket) return null;

  var rs = new Stream();
  rs.readable = true;

  var fileCounter = 0;
  keyStream
    .on('data', function (file) {
      fileCounter += 1;

      // console.log('->file', file);
      var params = { Bucket: self.bucket, Key: file };
      var s3File = self.s3.getObject(params).createReadStream();

      s3File.pipe(
        concat(function buffersEmit (buffer) {
          // console.log('buffers concatenated, emit data for ', file);
          rs.emit('data', { data: buffer, path: file.replace(self.folder, '') });
        })
      );
      s3File
        .on('end', function () {
          fileCounter -= 1;
          if (fileCounter < 1) {
            // console.log('all files processed, emit end');
            rs.emit('end');
          }
        });

      s3File
        .on('error', function (err) {
          rs.emit('error', err);
        });

    });
  return rs;
};
