const express = require('express');
const amqp = require('amqplib/callback_api');

const app = express();
app.use(express.json());

const RABBITMQ_URL = 'amqp://receive:receive@localhost';
const QUEUE_NAME = 'lista-documentos';

let documents = {}; 

amqp.connect(RABBITMQ_URL, (error0, connection) => {
  if (error0) {
    console.error('Error al conectar a RabbitMQ:', error0);
    process.exit(1);
  }
  connection.createChannel((error1, channel) => {
    if (error1) {
      console.error('Error al crear el canal:', error1);
      process.exit(1);
    }
    channel.assertQueue(QUEUE_NAME, {
      durable: true
    });

    channel.consume(QUEUE_NAME, (msg) => {
      if (msg) {
        const doc = JSON.parse(msg.content.toString());
        const docId = doc.id;
        documents[docId] = 'En Proceso';
        
        setTimeout(() => {
          const newState = Math.random() > 0.5 ? 'Aceptado' : 'Rechazado';
          documents[docId] = newState;
          console.log(`Documento ${docId} actualizado a ${newState}`);
          
          channel.ack(msg);
        }, 30000);
      }
    }, {
      noAck: false
    });

    app.post('/receive_documents', (req, res) => {
      const docs = req.body.documents;
      docs.forEach(doc => {
        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(doc)), { persistent: true });
      });
      res.json({ message: 'En Proceso' });
    });

    app.get('/check_status/:id', (req, res) => {
      const docId = req.params.id;
      const status = documents[docId];
      if (status) {
        res.json({ id: docId, status: status });
      } else {
        res.status(404).json({ message: 'Documento no encontrado' });
      }
    });

  });
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Receive corriendo en el puerto ${PORT}`);
});