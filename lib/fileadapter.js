/*
options:
  daysToKeep: Number
  path: String
*/

var path = require('path');
var fs = require('fs');
var async = require('async');
var zlib = require('zlib');
var tar = require('tar-fs');

var mkdir = function(dir){
  var rePathSep = /(\\|\/|\:)/;
  var parts = dir.split(rePathSep);
  var part;
  var path = '';

  while(parts.length){
    part = parts.shift();
    if(part && !part.match(rePathSep)){
      path += '/' + part;
      try{
        fs.mkdirSync(path);
      }catch(e){}
    }
  }
};

var noop = function(){};
var noopOrError = function(err){
  if(err){
    throw err;
  }
};

var FileAdapter = function(options){
  options = options || {};
  this.path = path.resolve('./', options.path || 'logs')+path.sep;
  mkdir(this.path);
  this.daysToKeep = options.daysToKeep||30;
  this.offsetKeep = this.daysToKeep * 24 * 60 * 60 * 1000;
  this.writers = {};
};

FileAdapter.prototype.trimOldLogs = function(callback){
  var today = new Date();
  var keepGt = new Date(today.getTime() - this.offsetKeep);
  var trimmed = [];
  var errors = {};
  var basePath = this.path;
  fs.readdir(this.path, function(err, files){
    async.each(files, function(file, next){
      var fileDate = file.split('.')[1];
      if(fileDate && Date.parse(fileDate)){
        fileDate = fileDate.replace(/-/g, '/');
        var fileTime = new Date(Date.parse(fileDate));
        if(fileTime <= keepGt){
          return fs.unlink(path.resolve(basePath, file), function(err){
            if(err){
              errors[file] = err;
              return next();
            }
            trimmed.push(file);
            return next();
          });
        }
        return next();
      }
      return next();
    }, function(){
      if(Object.keys(errors).length){
        return (callback||noopOrError)(errors, trimmed);
      }
      return (callback||noopOrError)(null, trimmed);
    });
  });
};

FileAdapter.prototype.compressFile = function(options, callback){
  if(typeof(options)==='string'){
    options = {
      inFile: options
    };
  }
  var inFile = options.inFile;
  var outFile = options.outFile || (options.inFile + '.tar.gz');
  var reExtractPath = /^.*(\\|\/|\:)/;
  var dir = (inFile.match(reExtractPath)||[])[0]||'';
  var file = inFile.substr(dir.length);
  fs.exists(outFile, function(exists){
    if(exists){
      fs.unlink(outFile);
    }
    try{
      var dest = fs.createWriteStream(outFile);
    }catch(e){
      return (callback||noopOrError)(e);
    }
    var packer = tar.pack(dir, {
      entries: [file]
    });
    var compressor = zlib.createGzip();
    var o = packer.emit;
    packer.on('error', function(err){
      return (callback||noopOrError)(err);
    });
    packer.on('close', function(){
      compressor.end();
    });
    compressor.on('error', function(err){
      return (callback||noopOrError)(err);
    });
    compressor.on('end', function(){
      dest.end();
    });
    dest.on('close', function(){
      return (callback||noopOrError)(null, outFile);
    });
    packer.pipe(compressor).pipe(dest);
  });
};

FileAdapter.prototype.compressDailyLogs = function(date, callback){
  var fn = date.toISOString().substr(0, 10)+'.log';
  var options = {
    inFile: this.path+fn,
    outFile: this.path+'archive.'+fn+'.tar.gz'
  };
  this.compressFile(options, function(err, outfile){
    if(err){
      return callback(err);
    }
    return callback(null, {
      logFile: options.inFile,
      archiveFile: options.outFile
    });
  });
};

FileAdapter.prototype.getWriter = function(fileName, callback){
  var writer = this.writers[fileName];
  if(typeof(writer) === 'boolean'){
    return setImmediate(function(){
      this.getWriter(fileName, callback);
    }.bind(this));
  }
  if(!writer){
    var fileLoc = this.path+fileName;
    this.writers[fileName] = true;
    return fs.exists(fileLoc, function(exists){
      if(exists){
        this.writers[fileName] = fs.createWriteStream(fileLoc, {flags: 'a'});
        writer = this.writers[fileName];
        return callback(null, writer);
      }
      this.writers[fileName] = fs.createWriteStream(fileLoc, {flags: 'w'});
      writer = this.writers[fileName];
      return callback(null, writer);
    }.bind(this));
  }
  return setImmediate(function(){
    return callback(null, writer);
  }.bind(this));
};

FileAdapter.prototype.checkCloseOldWriters = function(callback){
  var writers = Object.keys(this.writers);
  var now = (new Date()).getTime();
  var dayOld = 1000 * 60 * 60 * 24;
  var closed = [];
  async.each(writers, function(fn, next){
    var fd = fn.replace(/\.log$/, '');
    var dt = new Date(fd+'T00:00:00.000Z');
    var diff = now - dt.getTime();
    if(diff > dayOld){
      return this.getWriter(fn, function(err, writer){
        this.writers[fn].on('finish', function(){
          closed.push({
            fileName: fn,
            fileDate: dt
          });
          return next();
        });
        this.writers[fn].end();
      }.bind(this));
    }
    return next();
  }.bind(this), function(){
    callback(null, closed);
  });
};

FileAdapter.prototype.push = function(data, callback){
  var dt = new Date(Date.parse(data.time || date.date));
  // Logs should be based on GMT date time not local date time
  var gmtDt = new Date(dt.getTime() + dt.getTimezoneOffset() * 60000);
  // Now we know the real date time of the log we can append it
  var fn = gmtDt.toISOString().substr(0, 10)+'.log';
  this.getWriter(fn, function(err, writer){
    writer.lastLogTime = gmtDt.getTime();
    writer.write(JSON.stringify(data)+'\n', callback);
  });
};

module.exports = {
  FileAdapter: FileAdapter
};
