const express = require('express');
const axios = require('axios');
const amqp = require('amqplib');

const app = express();
app.use(express.json());

const ADMIN_DOCS_URL = 'http://localhost:3000';
const RABBITMQ_URL = 'amqp://send2:send2@localhost';
const QUEUE_NAME = 'lista-documentos';
let documents = []; 

// Conexion
async function conectarRabbitMQ() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  return channel;
}

const sendDocuments = async (docs) => {
  try {
    const channel = await conectarRabbitMQ();
    docs.forEach(doc => {
      channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(doc)), { persistent: true });
    });
    console.log('Documentos enviados a la cola');
  } catch (error) {
    console.error('Error enviando documentos:', error);
  }
};

const checkStatus = async () => {
  try {
    for (const doc of documents) {
      const response = await axios.get(`${ADMIN_DOCS_URL}/check_status/${doc.id}`);
      console.log(`Documento ${doc.id}: ${response.data.status}`);
    }
  } catch (error) {
    console.error('Error consultando estado:', error);
  }
};

setInterval(() => {
  const newDocs = Array.from({ length: 100 }, (_, i) => ({ id: `doc-${new Date().toISOString()}-${i}` }));
  documents.push(...newDocs);
  sendDocuments(newDocs);
}, 50000);

setInterval(checkStatus, 30000);

const PORT = 3002;
app.listen(PORT, () => {
  console.log(`send2 corriendo en el puerto ${PORT}`);
});