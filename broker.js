var zmq = require('zmq');
var debug = require('debug')('broker');

var SIGREADY = 'SIGREADY';
var SIGINT = 'SIGINT';

// Array para almacenar los workers activos.
var workers = [];

/**
 * Bind to socket
 */
var localFE = zmq.socket('router');
localFE.bind('ipc://localFE.ipc')
debug('Bound to localFE.ipc');

var localBE = zmq.socket('router');
localBE.bind('ipc://localBE.ipc')
debug('Bound to localBE.ipc');

var clientId = 0;
var uuid = '';

var jobs = [];

/**
 * Receive message
 */
localFE.on('message', function(msg) {

  var clientId = arguments[0];
  var uuid = arguments[2].toString();
  var requestBody = arguments[4].toString();

  // Reply with OK (job received)
  localFE.send([clientId, '', 200, '', uuid]);

  // Procesamos el mensaje…

  // Enviamos la respuesta del Worker.

});

/**
 * Receive from Local BE
 */
localBE.on('message', function(msg) {

  //debug(Array.apply(null, arguments).toString());

  var workerId = arguments[0];

  // WorkerId,'',SIGREADY === Worker empieza el Heartbeat
  if (arguments.length === 3) {

    var header = arguments[2].toString();

    switch (header) {
      case SIGREADY:
        // O es un Worker nuevo, o es uno diciendo que está listo para trabajar.
        //debug(typeof workerId);
        var receivedWorker = findWorkerById(workerId);
        var index = workers.indexOf(receivedWorker);
        if (index > -1) {
          // Es uno diciendo que está listo. Actualizamos su fecha de Heartbeat.
          workers[index].ts = new Date().getTime();
        } else {
          // Añadimos el Worker a la lista de workers
          workers.push({
            workerId: workerId,
            ts: new Date().getTime(),
          });
          checkActiveWorkers(workers);
        }

        // En cualquier caso, respondemos que OK al worker.
        localBE.send([workerId, '', 200, '', 'Hello worker!']);
        break;
      case SIGINT:
        // El worker se desconecta. Lo quitamos rápidamente de la lista de activos.
        var receivedWorker = findWorkerById(workerId);
        var index = workers.indexOf(receivedWorker);
        if (index > -1) {
          workers.splice(index, 1);
          checkActiveWorkers(workers);
        }
      default:

    }
  }
});

/**
 * Comprobamos workers que no han hecho Heartbeat desde hace más de 1 s.
 */
setInterval(function() {
  var now = new Date().getTime();

  // Comprobamos si hay algún worker muerto o desconectado.
  workers.forEach(function(element, index, array) {
    // Si su último ping fue hace más de 1 s lo marcamos como MUERTO.
    if ((element.ts + 1000) < now) {

      workers.splice(index, 1);
      debug(element.workerId, 'Worker probablemente muerto (reportó hace ' + (now - element.ts) + ' ms)');
      checkActiveWorkers(workers);
    }
  });

}, 2000);

/**
 * Find Object By key
 */
function objectFindByKey(array, key, value) {
  for (var i = 0; i < array.length; i++) {
    if (array[i][key].toString() === value.toString()) {
      return array[i];
    }
  }
  return null;
}

/**
 * Devuelve un Worker de la lista en base a su WorkerId
 *
 * @param {Buffer} workerId - El workerId según viene en el envelope.
 */
function findWorkerById(workerId) {
  for (var i = 0; i < workers.length; i++) {
    // Si los Buffers de WorkerId son iguales, da cero, y devolvemos ese objeto.
    if (workers[i].workerId.compare(workerId) === 0) {
      return workers[i];
    }
  }
  return null;
}

function checkActiveWorkers(array) {
  debug('Workers activos: ' + array.length);
}
