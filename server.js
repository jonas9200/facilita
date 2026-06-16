// =============================================================
// FacilitAgro — Proxy WebSocket → MQTT TLS
// Browser (WebSocket) ↔ Este proxy ↔ mqtt.facilitagro.com.br:8883
// Deploy: Render (Web Service, Node.js)
// =============================================================

const http  = require('http');
const { WebSocketServer } = require('ws');
const mqtt  = require('mqtt');

const PORT       = process.env.PORT || 3000;
const MQTT_HOST  = process.env.MQTT_HOST  || 'mqtt.facilitagro.com.br';
const MQTT_PORT  = parseInt(process.env.MQTT_PORT  || '8883');
const MQTT_USER  = process.env.MQTT_USER  || '';
const MQTT_PASS  = process.env.MQTT_PASS  || '';

// =============================================================
// Servidor HTTP mínimo (health-check do Render)
// =============================================================
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok',
    service: 'FacilitAgro MQTT Proxy',
    broker: `${MQTT_HOST}:${MQTT_PORT}`,
    clients: wss ? wss.clients.size : 0,
  }));
});

// =============================================================
// Servidor WebSocket (recebe conexões do browser)
// =============================================================
const wss = new WebSocketServer({ server: httpServer });

console.log(`[Proxy] Iniciando — broker: ${MQTT_HOST}:${MQTT_PORT}`);

wss.on('connection', (ws, req) => {
  const clientIP = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const clientId = 'proxy_' + Math.random().toString(16).substr(2, 8);

  console.log(`[+] Browser conectado | IP: ${clientIP} | clientId: ${clientId}`);

  // -----------------------------------------------------------
  // Conecta ao broker MQTT via TCP+TLS (porta 8883)
  // -----------------------------------------------------------
  const mqttOpts = {
    host:      MQTT_HOST,
    port:      MQTT_PORT,
    protocol:  'mqtts',          // MQTT over TLS (TCP 8883)
    clientId:  clientId,
    clean:     true,
    rejectUnauthorized: false,   // permite certificado auto-assinado
    reconnectPeriod: 0,          // sem reconexão automática — o browser gerencia
    connectTimeout: 10000,
  };
  if (MQTT_USER) {
    mqttOpts.username = MQTT_USER;
    mqttOpts.password = MQTT_PASS;
  }

  const mqttClient = mqtt.connect(mqttOpts);

  // -----------------------------------------------------------
  // MQTT → Browser: repassa mensagens recebidas do broker
  // -----------------------------------------------------------
  mqttClient.on('connect', () => {
    console.log(`[MQTT] Conectado ao broker para cliente ${clientId}`);
    sendToBrowser(ws, { type: 'connack', sessionPresent: false });
  });

  mqttClient.on('message', (topic, payload, packet) => {
    console.log(`[MQTT→WS] tópico: ${topic} | ${payload.length} bytes`);
    sendToBrowser(ws, {
      type:    'publish',
      topic:   topic,
      payload: payload.toString(),
      qos:     packet.qos,
      retain:  packet.retain,
    });
  });

  mqttClient.on('error', (err) => {
    console.error(`[MQTT] Erro (${clientId}):`, err.message);
    sendToBrowser(ws, { type: 'error', message: err.message });
    ws.close();
  });

  mqttClient.on('close', () => {
    console.log(`[MQTT] Conexão com broker encerrada para ${clientId}`);
  });

  // -----------------------------------------------------------
  // Browser → MQTT: processa mensagens vindas do browser
  // -----------------------------------------------------------
  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      console.warn(`[WS] Mensagem inválida de ${clientId}:`, raw.toString().substring(0, 100));
      return;
    }

    switch (msg.type) {

      case 'publish': {
        const { topic, payload, qos = 1, retain = false } = msg;
        if (!topic) return;
        console.log(`[WS→MQTT] publish | tópico: ${topic} | ${String(payload).length} bytes`);
        mqttClient.publish(topic, String(payload), { qos, retain }, (err) => {
          if (err) {
            sendToBrowser(ws, { type: 'puback', success: false, error: err.message, topic });
          } else {
            sendToBrowser(ws, { type: 'puback', success: true, topic });
          }
        });
        break;
      }

      case 'subscribe': {
        const { topic, qos = 1 } = msg;
        if (!topic) return;
        console.log(`[WS→MQTT] subscribe | tópico: ${topic}`);
        mqttClient.subscribe(topic, { qos }, (err) => {
          sendToBrowser(ws, {
            type:    'suback',
            topic:   topic,
            success: !err,
            error:   err ? err.message : null,
          });
        });
        break;
      }

      case 'unsubscribe': {
        const { topic } = msg;
        if (!topic) return;
        mqttClient.unsubscribe(topic);
        break;
      }

      case 'ping':
        sendToBrowser(ws, { type: 'pong' });
        break;

      default:
        console.warn(`[WS] Tipo desconhecido: ${msg.type}`);
    }
  });

  // -----------------------------------------------------------
  // Browser desconectou → fecha conexão MQTT
  // -----------------------------------------------------------
  ws.on('close', () => {
    console.log(`[-] Browser desconectado: ${clientId}`);
    mqttClient.end(true);
  });

  ws.on('error', (err) => {
    console.error(`[WS] Erro (${clientId}):`, err.message);
    mqttClient.end(true);
  });
});

// -----------------------------------------------------------
// Envia mensagem JSON ao browser (seguro — verifica estado)
// -----------------------------------------------------------
function sendToBrowser(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

// =============================================================
// Inicia servidor
// =============================================================
httpServer.listen(PORT, () => {
  console.log(`[Proxy] Ouvindo na porta ${PORT}`);
  console.log(`[Proxy] Broker MQTT: ${MQTT_HOST}:${MQTT_PORT}`);
});
