var zmq = require('zmq');
var uuid = require('node-uuid');
var fs = require('fs');
var debug = require('debug')('client');
var config = require('./config');

var SIGDONE = 'SIGDONE'; // El trabajo ha sido procesado.

/**
 * Connect to socket
 */
var localFE = zmq.socket('dealer');
localFE.connect('ipc://localFE.ipc');

debug('Connected to localFE');

// Enviados al Broker pendientes de procesarse.
var receivedJobs = [];
var acceptedJobs = [];

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
  saveData(config.receivedPath + jobUuid, data);
}, randomBetween(2000, 4000));

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
          debug(uuid, 'Petición aceptado.');
          // Movemos el archivo original a /accepted.
          fs.rename(config.receivedPath + uuid, config.acceptedPath + uuid, function(error) {});

          // Eliminamos el trabajo de la cola de Recibidos.
          var receivedJob = objectFindByKey(receivedJobs, 'uuid', uuid);
          var index = receivedJobs.indexOf(receivedJob);
          if (index > -1) {
            // Eliminamos el trabajo de la cola de /received.
            receivedJobs.splice(index, 1);

            // Añadimos el trabajo a la cola de /accepted.
            acceptedJobs.push({
              uuid: uuid,
              ts: new Date().getTime(),
            });
          } else {
            debug('UUID de petición no encontrada.')
          }
          break;
        }
        // El trabajo ha terminado de procesarse. Lo pasamos a finalizado.
        case SIGDONE: {
          debug(uuid, 'Petición confirmada finalizada.');
          // Movemos el archivo procesado a /doneConfirmed.
          fs.rename(config.acceptedPath + uuid, config.doneConfirmedPath + uuid, function(error) {});

          // Eliminamos el trabajo de pendientes.
          var processedJob = objectFindByKey(acceptedJobs, 'uuid', uuid);
          var index = acceptedJobs.indexOf(processedJob);
          if (index > -1) {
            // Eliminamos el trabajo de la cola de /received.
            acceptedJobs.splice(index, 1);
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
  acceptedJobs.forEach(function(element, index, array) {
    // Si lleva procesándose más de 5 s, lo ponemos como ATORADO.
    if ((element.ts + 5000) < now) {

      // Incrementamos el número de aviso para este elemento.
      if (element.hasOwnProperty('noWarning')) {
        element.noWarning++;
      } else {
        element.noWarning = 0;
      }

      if (element.noWarning === 3) {
        debug(element.uuid, 'Petición NO confirmada finalizada. Moviendo a directorio.');
        fs.rename(config.acceptedPath + element.uuid, config.doneNotConfirmedPath + element.uuid, function(error) {});
        acceptedJobs.splice(index, 1);
      } else {
        debug(element.uuid, 'Petición probablemente atorada (enviada hace ' + (now - element.ts) + ' ms)');
      }


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

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
}
