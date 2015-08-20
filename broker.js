var zmq = require('zmq');
var debug = require('debug')('broker');

var localFE = zmq.socket('router');
var localBE = zmq.socket('router');

var workers = []; // Array de Objetos con el WorkerId y el ts del Heartbeat.
var availableWorkers = []; // Array de workerIds para poder LRU.

var SIGREADY = 'SIGREADY';
var SIGINT = 'SIGINT';
var SIGWORK = 'SIGWORK';
var SIGDONE = 'SIGDONE'; // El trabajo ha sido procesado.

/**
 * Bind to Local FrontEnd
 */
localFE.bind('ipc://localFE.ipc', function(error) {
  debug('Bound to localFE.ipc');

  /**
   * Receive message
   */
  localFE.on('message', function(msg) {

    var clientId = arguments[0];
    var uuid = arguments[2].toString();
    var requestBody = arguments[4].toString();

    // Enviamos el mensaje a un worker disponible.
    if (availableWorkers.length > 0) {
      var workerId = availableWorkers.shift();
      localBE.send([workerId, '', clientId, '', SIGWORK, '', uuid, '', 'TRABAJOOOOO']);
    } else {
      // Reintentamos hasta que un worker esté disponible.
      // TODO: Unir ambas funciones de lenght>0 en una externa.
      var checkAvailableWorkerInterval = setInterval(function() {
        if (availableWorkers.length > 0) {
          var workerId = availableWorkers.shift();
          localBE.send([workerId, '', clientId, '', SIGWORK, '', uuid, '', 'TRABAJOOOOO']);
          clearInterval(checkAvailableWorkerInterval);
        }
      }, 500);
    }
  });
});

/**
 * Bind to Local BackEnd
 */
localBE.bind('ipc://localBE.ipc', function(error) {
  debug('Bound to localBE.ipc');
  /**
   * Receive from Local BE
   */
  localBE.on('message', function(msg) {

    switch (arguments.length) {
      // [workerId,'',SIGREADY]     === Worker empieza el Heartbeat.
      // [workerId,'',SIGINT]       === El worker se desconecta.
      case 3: {

        var workerId = arguments[0];
        var header = arguments[2].toString();

        switch (header) {
          // El worker anuncia que está activo.
          case SIGREADY: {
            // O es un Worker nuevo, o es uno diciendo que está listo para trabajar.
            // Si no podemos actualizar el worker porque es deconocido, lo añadimos.
            if (!updateKnownWorker(workerId)) {
              // Lo añadimos a la lista de workers conocidos.
              addWorker(workerId);
            }

            // En cualquier caso, respondemos que OK al worker.
            localBE.send([workerId, '', 200, '', 'Hello worker!']);
            break;
          }
          // El worker se desconecta voluntariamente.
          case SIGINT: {
            // Lo quitamos rápidamente de las listas.
            deleteWorker(workerId);
          }
          default:
        }
        break;
      }

      //[workerId,'', clientId, '', 200, '', uuid] => Trabajo aceptado.
      //[workerId,'', clientId, '', SIGDONE, '', uuid] => Trabajo procesado.
      case 7: {

        var workerId = arguments[0];
        var clientId = arguments[2];
        var header = arguments[4].toString();
        var uuid = arguments[6].toString();

        switch (header) {
          // El worker ha terminado de procesar el trabajo. Avisamos al cliente.
          case SIGDONE: {
            debug(workerId, 'Trabajo procesado por el worker: ' + uuid);
            // Avisamos al cliente de que lo marque como finalizado.
            localFE.send([clientId, '', SIGDONE, '', uuid]);

            // Tenemos que actualizar el timestamp del worker.
            updateKnownWorker(workerId);
            // Tenemos que reañadirlo a la lista de disponibles.
            availableWorkers.push(workerId);
            break;
          }
          // El worker ha aceptado el trabajo. Avisamos al cliente.
          case "200": {
            debug(workerId, 'Trabajo aceptado por el worker: ' + uuid);
            // Avisamos al cliente que lo pase a /processing.
            localFE.send([clientId, '', 200, '', uuid]);
            break;
          }
          default:
        }
      }
      default:
    }
  });
});


/**
 * Comprobamos workers que no han hecho Heartbeat desde hace más de 1 s.
 */
setInterval(function() {
  var now = new Date().getTime();

  // Comprobamos si hay algún worker muerto o desconectado.
  workers.forEach(function(element, index, array) {
    // Si su último ping fue hace más de 1 s lo marcamos como MUERTO.
    if ((element.ts + 2000) < now) {

      debug(element.workerId, 'Worker probablemente muerto (reportó hace ' + (now - element.ts) + ' ms)');
      deleteWorker(element.workerId);
    }
  });

}, 2000);

/**
 * Si el worker no lo tenemos reconocido, lo añadimos.
 * También guardamos los timestamps para los Heartbeats futuros que haga.
 *
 * @param {Buffer} workerId
 */
function updateKnownWorker(workerId) {
  var workerToCheck = objectFindByBufferKey(workers, 'workerId', workerId);
  var index = workers.indexOf(workerToCheck);
  if (index > -1) {
    // Este worker ya lo tenemos listado. Actualizamos su timestamp.
    workers[index].ts = new Date().getTime();
    return true;
  } else {
    // Todavía no había comunicado este worker.
    return false;
  }
}

/**
 * Devuelve un objeto si existe en el array.
 *
 * @param {Array} array - El array de objetos donde buscar.
 * @param {String} key - La clave del objeto con que comparar.
 * @param {Buffer} value - El valor que buscar, en formato Buffer (workerId).
 */
function objectFindByBufferKey(array, key, value) {
  for (var i = 0; i < array.length; i++) {
    // Si los Buffers son iguales devuelve cero, y devolvemos ese objeto.
    if (array[i][key].compare(value) === 0) {
      return array[i];
    }
  }
  return null;
}

/**
 * Añade un worker tanto a disponibles como a conocidos (con timestamp)
 *
 * @param {Buffer} workerId
 */
function addWorker(workerId) {
  // Lo añadimos a disponibles.
  availableWorkers.push(workerId);
  debug(workerId, 'Worker disponible.', availableWorkers.length + ' workers disponibles.');

  // Lo añadimos a conocidos.
  workers.push({
    workerId: workerId,
    ts: new Date().getTime(),
  });
  debug(workerId, 'Worker conocido.', workers.length + ' workers conocidos.');
}

/**
 * Borra tanto de workers como de availableWorkers.
 *
 * @param {Buffer} workerId
 */
function deleteWorker(workerId) {

  // Lo borramos de los workers activos.
  // TODO: externalizar este bucle.
  for (var i = 0; i < availableWorkers.length; i++) {
    if (availableWorkers[i].compare(workerId) === 0) {
      availableWorkers.splice(i, 1);
      debug(workerId, 'Worker NO disponible.', availableWorkers.length + ' workers disponibles.');
    }
  }


  // Lo borramos de los workers conocidos.
  var workerToDelete = objectFindByBufferKey(workers, 'workerId', workerId);
  var index = workers.indexOf(workerToDelete);
  if (index > -1) {
    // Este worker ya lo tenemos listado.
    workers.splice(index, 1);
    debug(workerId, 'Worker repudiado.', workers.length + ' workers conocidos.');
  }
}
