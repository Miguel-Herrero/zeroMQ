var zmq = require('zmq');
var debug = require('debug')('worker');

var SIGREADY = 'SIGREADY';
var SIGINT = 'SIGINT';
var busy = false;

/**
 * Connect to socket
 */
var localBE = zmq.socket('dealer');
localBE.connect('ipc://localBE.ipc');
debug('Connected to localBE');

localBE.on('message', function() {
  debug(Array.apply(null, arguments).toString());
});

/**
 * Envío la señal de listo (SIGNREADY).
 */
setInterval(function() {
  // Envío la señal de listo solo cuando no estoy procesando datos.
  if (busy === false) {
    localBE.send(['', SIGREADY]);
  }
}, 1000);

/**
 * Handling Ctrl-C cleanly
 */
process.on('SIGINT', function() {
  localBE.send(['', SIGINT]);
  localBE.close();
  process.exit();
});

// Si se le envía un Kill -9 podemos ejemplificar que se ha quedado colgado.
