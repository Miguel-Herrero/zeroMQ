var zmq = require('zmq');
var uuid = require('node-uuid');
var fs = require('fs');
var debug = require('debug')('client');

var SIGDONE = 'SIGDONE'; // El trabajo ha sido procesado.

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
    uuid: jobUuid,
    ts: new Date().getTime(),
  });
  saveData('./tmp/received/' + jobUuid, data);
}, 2000);

/**
 * Receive messages with UUID.
 */
localFE.on('message', function(message) {

  switch (arguments.length) {

    // ['', 200, '', uuid]    === El trabajo ha sido aceptado por un worker.
    // ['', SIGDONE, '', uuid]    == El trabajo se ha finalizado.
    case 4: {

      var header = arguments[1].toString();
      var uuid = arguments[3].toString();

      switch (header) {
        // El worker ha aceptado el trabajo y lo empieza a procesar.
        case "200": {
          debug(uuid, 'Trabajo aceptado.');
          // Movemos el archivo original a /processing.
          fs.rename('./tmp/received/' + uuid, './tmp/processing/' + uuid, function(error) {});

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
          } else {
            debug('received no encontrado')
          }
          break;
        }
        // El trabajo ha terminado de procesarse. Lo pasamos a finalizado.
        case SIGDONE: {
          debug(uuid, 'Trabajo finalizado.');
          // Movemos el archivo procesado a /finished.
          fs.rename('./tmp/processing/' + uuid, './tmp/finished/' + uuid, function(error) {});

          // Eliminamos el trabajo de pendientes.
          var processedJob = objectFindByKey(processingJobs, 'uuid', uuid);
          var index = processingJobs.indexOf(processedJob);
          if (index > -1) {
            // Eliminamos el trabajo de la cola de /received.
            processingJobs.splice(index, 1);
          }
        }
        default:
      }
    }
    default:
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
      debug(element.uuid, 'Trabajo probablemente atorado (enviado hace ' + (now - element.ts) + ' ms)');
    }
  });

}, 1000);

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

/**
 * Handling Ctrl-C cleanly
 */
process.on('SIGINT', function() {
  localFE.close();
  process.exit();
});
