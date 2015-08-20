var zmq = require('zmq');
var fs = require('fs');
var debug = require('debug')('worker');

var SIGREADY = 'SIGREADY';
var SIGINT = 'SIGINT';
var SIGWORK = 'SIGWORK';
var SIGDONE = 'SIGDONE'; // El trabajo ha sido procesado.
var busy = false; // SI el worker está trabajando, no enviamos Heartbeats.

/**
 * Connect to socket
 */
var localBE = zmq.socket('dealer');
localBE.connect('ipc://localBE.ipc');
debug('Connected to localBE');

localBE.on('message', function() {

  switch (arguments.length) {
    // [WorkerId,'',ClientID,'',SIGWORK,'',uuid,'',work]
    // Nos llega un trabajo para procesar.
    case 8: {
      var clientId = arguments[1];
      var signal = arguments[3].toString();
      var uuid = arguments[5].toString();
      var requestBody = arguments[7].toString();
      debug(uuid, 'Procesando…');

      // Enviamos señal de que aceptamos el trabajo.
      localBE.send(['', clientId, '', 200, '', uuid]);

      // Dejamos de enviar Heartbeats cuando trabajamos.
      busy = true;

      // Comprobamos que piden al worker que trabaje.
      if (signal === SIGWORK) {
        // Procesamos el trabajo y lo reenviamos
        setTimeout(function() {
          debug(uuid, 'OK');
          busy = false; // Para volver a enviar Heartbeats.
          localBE.send(['', clientId, '', SIGDONE, '', uuid]);
        }, randomBetween(300, 2000));
      }
      break;
    }
    default:

  }

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


function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
}
