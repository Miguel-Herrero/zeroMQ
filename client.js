var zmq = require('zmq');
var uuid = require('node-uuid');
var fs = require('fs');
var debug = require('debug')('client');

/**
 * Connect to socket
 */
var localFE = zmq.socket('dealer');
localFE.connect('ipc://localFE.ipc');
debug('Connected to localFE');

// Enviados al Broker pendientes de procesarse.
var receivedJobs = [];
var processingJobs = [];

/**
 * Send 2 jobs with different UUIDs.
 */
setInterval(function() {
  var jobUuid = uuid.v4();
  var data = 'Lorem ipsum dolor sit amet';
  localFE.send(['', jobUuid, '', data]);
  receivedJobs.push({
    uuid: uuid,
    ts: new Date().getTime(),
  });
  saveData('./tmp/received/' + jobUuid, data);

  debug('Trabajos recibidos:', receivedJobs);
}, 5000);

/**
 * Receive messages with UUID.
 */
localFE.on('message', function(message) {
  debug(arguments.length)

  // Status + UUID = Broker sent job to Worker.
  if (arguments.length === 4) {
    debug('Me llegaron 4 argumentos');
    var status = arguments[1].toString();
    var uuid = arguments[3].toString();

    if (parseInt(status.toString()) === 200) {
      // Movemos el archivo a otra carpeta.
      debug('Me llegó un OK')
      // Copiamos el archivo original a /processing.
      fs.createReadStream('./tmp/received/' + uuid).pipe(fs.createWriteStream('./tmp/processing/' + uuid));
      // Eliminamos el original de /received.
      fs.unlink('./tmp/received/' + uuid);

      // Eliminamos el trabajo de la cola de Recibidos.
      var receivedJob = objectFindByKey(receivedJobs, 'uuid', uuid);
      var index = receivedJobs.indexOf(receivedJob);
      if (index > -1) {
        // Eliminamos el trabajo de la cola de /received.
        receivedJobs.splice(index, 1);

        // Añadimos el trabajo a la cola de /processing.
        processingJobs.push({
          uuid: uuid,
          ts: new Date().getTime(),
        });
      }

    }
  }

});

/**
 * Comprobamos los trabajos aceptados pero no recibidos como Finalizados.
 */
setInterval(function() {
  var now = new Date().getTime();

  // Comprobamos si hay algún trabajo procesándose que se ha quedado atorado.
  processingJobs.forEach(function(element, index, array) {
    // Si lleva procesándose más de 5 s, lo ponemos como ATORADO.
    if ((element.ts + 5000) < now) {
      debug('Elemento probablemente ATORADO:', element.ts, now);
    }
  })
  debug('\n');

}, 3000);

/**
 * Find Object By key
 */
function objectFindByKey(array, key, value) {
  for (var i = 0; i < array.length; i++) {
    if (array[i][key] === value) {
      return array[i];
    }
  }
  return null;
}

/**
 * Escribe en disco.
 */
function saveData(filename, data) {
  fs.writeFile(filename, data, function(err){
    if (err) throw err;

    // Save file to S3
  });
}
