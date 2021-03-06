var request = require('browser-request');
var EventEmitter = require('events').EventEmitter;
var async = require('async');
var semver = require('semver');
var crypto = require('crypto');
var _ = require('underscore');
var url = require('url');
var mungeSemver = require('munge-semver');

var TIMEOUT = 100;

Downloader = function(){
  this.concurrency = 5;
  this.downloadCache = 'downloadCache';
  this.modulePath = ['installed'].join('/');
  this.bundlePath = 'bundled'
  this.maxFailures = 3;
  this.currentDls = {};
  return this;
};

Downloader.prototype = EventEmitter.prototype;

Downloader.prototype.init = function() {
  this.rootPaths = {
    cdv: {
      installed: ['cdvfile://localhost/library-nosync', this.modulePath].join('/'),
      bundled: ['cdvfile://localhost/bundle/www', this.bundlePath].join('/'),
      cache: ['cdvfile://localhost/cache/', this.downloadCache].join('/')
    },
    fs: {
      installed: ['NoCloud', this.modulePath].join('/'),
      bundled: [cordova.file.applicationDirectory, 'www', this.bundlePath].join('/'),
      cache: ['Caches', this.downloadCache].join('/')
    }
  };
};

Downloader.prototype.updateProgress = function(file, incomplete) {
  if (!this.progress) return new Error("No progress object defined");
  if (incomplete) return this.progress.bytes -= parseInt(file.size);
  return this.progress.bytes += parseInt(file.size);
};

Downloader.prototype.downloadFile = function(file, callback) {
  var _this = this;
  var transfer = new window.parent.FileTransfer();

  var id = Math.floor(Math.random() * (1 << 24)).toString(16);

  this.currentDls[id] = transfer;

  var fetchSuccess = function() {
    delete _this.currentDls[id];
    _this.updateProgress(file, false);
    callback();
  };

  var fetchFailure = function(err) {
    delete _this.currentDls[id];
    callback(err);
  };

  transfer.download(file.url, this.rootPaths.cdv.cache+'/'+file.sha, fetchSuccess, fetchFailure);
};

Downloader.prototype.downloadModuleToDevice = function(module, callback) {
  var that = this;

  var files = module.files;

  var totalSize = 0;

  files.forEach(function(file) {
    totalSize += file.size;
  });

  this.progress = {
    bytes: 0,
    totalSize: totalSize
  };

  var errs = [];

  var aborted = false;

  this.on('abort', function() {
    aborted = true;
    return callback(new Error('Download aborted'));
  });

  var queue = async.queue(function(file, callback) {
    if (aborted) return callback();

    that.checkFile(file, function(err, comp) {
      if (err) {
        errs.push(err);
        return callback();
      }

      if (comp) {
        that.updateProgress(file, false);
        that.emit('file', file);
        return callback();
      };

      that.downloadFile(file, function(err) {
        if (err) {
          console.log('error while downloading file', err, file);
          errs.push(err);
          return callback();
        };

        that.updateProgress(file, false);
        that.emit('file', file);

        return callback();
      });

    });

  }, this.concurrency);

  queue.drain = function() {
    if (aborted) return;
    that.removeAllListeners('abort');
    if (errs.length === 0) {
      return callback(null);
    } else {
      return callback(errs);
    }
  };

  _.each(files, function(file) {
    queue.push(file);
  }, that);
};

Downloader.prototype.cacheCheck = function(files, callback) {
  var that = this;

  var errs = [];
  var complete = true;

  var queue = async.queue(function(file, callback) {
    that.checkFile(file, function(err, comp) {

      if (err) errs.push(err);
      if (!comp) {
        complete = false;
        that.updateProgress(file, true);
      }
      callback();
    });

  }, 5);

  queue.drain = function() {
    if (errs.length === 0) errs = null;
    callback(errs, complete);
  };

  _.each(files, function(file) {
    queue.push(file);
  }, this);
};

Downloader.prototype.checkFile = function(file, callback) {
  var destPath = [this.rootPaths.fs.cache, file.sha].join('/');

  var success = function(fileHandle) {
    var success = function(metadata) {
      if (metadata.size === file.size) {
        return callback(null, true);
      } else {
        console.error('sizes did not match', metadata.size, file.size, file);
        return callback(null, false);
      }
    };
    var failure = function(err) {
      console.error('failed to get metadata', err);
      return callback(err, false);
    };
    fileHandle.getMetadata(success, failure);
  }

  var failure = function(err) {
    if (err.code === 1) return callback(null, false); // File not found
    console.error('failed to get file handler for', file, err);
    return callback(err, false);
  }

  this.fs.root.getFile(destPath, {exclusive: false}, success, failure);
}

Downloader.prototype.copyModuleIntoPlace = function(module, callback) {
  var _this = this;
  var queue = async.queue(function(file, callback) {
    var destParent = [_this.rootPaths.fs.installed, module._id, file.localPath].join('/');
    var source = [_this.rootPaths.fs.cache, file.sha].join('/');
    _this.mkdirp(destParent, function(err) {
      if (err) return callback(err);

      var fail = function(err) { callback(err) };

      var onGetFileSuccess = function(sourceFileHandle) {

        var success = function() { callback() };

        _this.fs.root.getDirectory(destParent, {create: false, exclusive: false}, function(destParentHandle) {
          _this.removeFileIfExists([destParent, file.localName].join('/'), function(err) {
            sourceFileHandle.copyTo(destParentHandle, file.localName, success, fail);
          });
        }, fail);
      };

      _this.fs.root.getFile(source, {create: false, exclusive: false}, onGetFileSuccess, fail);
    });
  });

  queue.drain = function(errs) {
    if (!errs || errs.length === 0) {
      return callback(null);
    } else {
      return callback(errs);
    }
  };

  queue.push(module.files);
};

Downloader.prototype.removeFileIfExists = function(name, callback) {
  var _this = this;
  var fail = function(err) {
    if (err.code === 1) return callback(); // If the file didn't exist, that's fine, pretend we removed it.
    callback(err);
  }

  _this.fs.root.getFile(name, {}, function(handle){
    handle.remove(function(){
      callback();
    }, fail);
  }, fail);
};

Downloader.prototype.writeVersionFile = function(module, callback) {
  var _this = this;
  var targetFile = [this.rootPaths.fs.installed, module._id, 'version.json'].join('/');
  var success = function(entry) {
    var success = function(writer) {
      writer.onwrite = function() {
        return callback(null);
      };
      writer.write(JSON.stringify({version: module.version}));
    };
    entry.createWriter(success, failure);
  };
  var failure = function(err) {
    return callback(err);
  };
  this.fs.root.getFile(targetFile, {create: true, exclusive: false}, success, failure);
};

Downloader.prototype.downloadAndVerify = function(module, onUpdate, callback) {
  if (arguments.length === 2) {
    callback = onUpdate;
    onUpdate = function(){};
  }
  var failures = 0;
  var that = this;
  this.on('file', function() {
    onUpdate && onUpdate(that.progress.bytes, that.progress.totalSize);
  });
  var iterate = function() {
    that.downloadModuleToDevice(module, function(err) {
      that.cacheCheck(module.files, function(err, complete) {
        if (err) {
          console.error('err from cacheCheck', err);
          failures++;
          if (failures >= that.maxFailures) return callback(err);
          return iterate();
        } else if (!complete) {
          console.error('cache is not complete!');
          failures++;
          if (failures >= that.maxFailures) return callback(new Error('max failures reached'));
          return iterate();
        }
        that.copyModuleIntoPlace(module, function(err) {
          if (err) return callback(err);
          return that.writeVersionFile(module, callback);
        });
      })
    })
  };
  iterate()
};

Downloader.prototype.removeModuleFromDevice = function(moduleId, callback) {
  var that = this;

  var targetPath = [that.rootPaths.fs.installed, moduleId].join('/');
  var success = function(dirHandle) {
    var success = function() {
      return callback(null);
    };
    var failure = function(err) {
      console.error('failed to delete', targetPath);
      return callback(err);
    };
    dirHandle.removeRecursively(success, failure);
  }
  var failure = function(err) {
    console.error('failed to get directoryHandle for', targetPath, err);
    return callback(null); // return null as this likely means the directory does not exist, thus does not need to be deleted
  }
  that.fs.root.getDirectory(targetPath, {exclusive: false}, success, failure);
}

Downloader.prototype.cancelDownload = function(callback) {
  if (!callback) callback = function(){};
  this.emit('abort');

  var closed = 0;

  _.each(this.currentDls, function(currentDl) {
    currentDl.abort();
  });

  this.emit('aborted');
  callback();
};

Downloader.prototype.moduleInfo = function(moduleId, callback) {
  var that = this;

  var fetchInfo = function(path, callback) {
    request.get({uri: path+'/version.json', timeout: TIMEOUT}, function(err, res, data) {
      if (err && err.code === 'ETIMEDOUT') return callback(null, null);
      if (res.statusCode === 404) return callback(null, null);
      if (err) return callback(err, null);
      if (!data) return callback(null, null);
      try {
        var info = JSON.parse(data.toString());
      } catch (e) {
        err = e;
      }
      callback(err, info);
    });
  };

  async.parallel({
    installed: function(callback) {
      var targetPath = [that.rootPaths.cdv.installed, moduleId].join('/');
      fetchInfo(targetPath, callback);
    },
    bundled: function(callback) {
      var targetPath = [that.rootPaths.cdv.bundled, moduleId].join('/');
      fetchInfo(targetPath, callback);
    }
  }, callback);
};

Downloader.prototype.bundledOrInstalled = function(moduleName, callback) {
  var that = this;

  this.moduleInfo(moduleName, function(err, info) {
    if (err) return callback(err);

    ['installed', 'bundled'].forEach(function(prop) {
      if (!info[prop]) info[prop] = {};
      if (!info[prop].version) info[prop].version = '0.0.0';
    });

    var installedVersion = mungeSemver(info.installed.version);
    var bundledVersion = mungeSemver(info.bundled.version);

    if(semver.gt(installedVersion, bundledVersion)) {
      return callback(null, {loc: 'installed', version: installedVersion});
    } else {
      return callback(null, {loc: 'bundled', version: bundledVersion});
    }
  });
};

Downloader.prototype.getNavigationUrl = function(navId, callback) {
  var that = this;
  this.bundledOrInstalled(navId, function(err, info) {
    if (err) return callback(err);
    return callback(null, [that.rootPaths.cdv[info.loc], navId, 'index.html'].join('/'));
  });
};

Downloader.prototype.mkdirp = function(name, callback) {
  var that = this;
  var createDirectory = function() {
    var success = function(dir){
      window.created_dir = dir;
      return callback(null);
    };
    var failure = function(err){
      return callback(err);
    };
    that.fs.root.getDirectory(name, {create: true, exclusive: false}, success, failure);
  };

  if (name.split('/').length > 1) {
    var parent = name.split('/').slice(0, -1).join('/');
    this.mkdirp(parent, function(err) {
      if (err) return callback(err);
      createDirectory();
    });
  } else {
    createDirectory();
  }
};

Downloader.prototype.listEntries = function(handle, callback) {
  _this = this;

  var fail = function(err) {
    callback(err);
  };

  var reader = handle.createReader();
  var entries = [];
  var readEntries = function() {
    reader.readEntries(function(results) {
      if (!results.length) {
        callback(null, entries);
      } else {
        entries = entries.concat(Array.prototype.slice.call(results || [], 0));
        readEntries();
      }
    }, fail);
  };
  readEntries();
};

Downloader.prototype.listAllModules = function(callback) {
  var _this = this;
  async.parallel({
    installed: function(callback) {
      _this.fs.root.getDirectory(_this.rootPaths.fs.installed, {}, function(handle) {
        _this.listEntries(handle, callback);
      }, callback);
    },
    bundled: function(callback) {
      window.resolveLocalFileSystemURL([cordova.file.applicationDirectory, 'www', _this.bundlePath].join('/'), function(handle) {
        _this.listEntries(handle, callback);
      }, callback);
    }
  }, function(err, lists) {
    var names = []
    _.each(lists, function(list) {
      list.forEach(function(item) {
        if (item.name === '.DS_Store') return;
        names.push(item.name);
      });
    });

    var infoFuns = {};
    names.forEach(function(name) {
      infoFuns[name] = function() {
        args = Array.prototype.slice.call(arguments);
        args.unshift(name);
        _this.moduleInfo.apply(_this, args);
      };
    });

    async.parallel(infoFuns, function(err, info) {
      if (err) return callback(err);
      var modules = {};
      _.each(info, function(locations, name) {
        var bundledVersion = '0.0.0';
        var installedVersion = '0.0.0';
        var winningLocation = 'bundled';
        if (locations.bundled) bundledVersion = mungeSemver(locations.bundled.version);
        if (locations.installed) installedVersion = mungeSemver(locations.installed.version);

        if (semver.gt(bundledVersion, installedVersion)) winningLocation = 'bundled';
        if (semver.lt(bundledVersion, installedVersion)) winningLocation = 'installed';

        modules[name] = {
          version: locations[winningLocation].version,
          url: [_this.rootPaths.cdv[winningLocation], name].join('/')
        }
      });
      callback(err, modules);
    });
  });
}

Downloader.prototype.cdvToFs = function(url) {
  var winner = {};
  var match = _.each(this.rootPaths.cdv, function(path, id) {
    if (url.indexOf(path) > -1) winner = {path: path, id: id};
  });
  if (!winner.path) return null;
  return url.replace(winner.path, this.rootPaths.fs[winner.id])
};

module.exports = new Downloader();
