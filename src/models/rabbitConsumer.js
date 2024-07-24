import amqplib from 'amqplib';
import { pool } from "./dbConnection.js";

const queue = 'fila1';

const startConsumer = async () => {
  try {
    // Conectar a RabbitMQ
    const conn = await amqplib.connect('amqps://zsfpkhaq:0iroPd-VsW9zkrfYt4-kg4BK-wftfsh4@cow.rmq2.cloudamqp.com/zsfpkhaq');
    console.log("Conexion a RabbitMQ exitosa");
    const channel = await conn.createChannel();
    await channel.assertQueue(queue, { durable: true });

    // Configurar el consumidor
    channel.consume(queue, async (msg) => {
      if (msg !== null) {
        try {
          const messageContent = msg.content.toString();
          console.log('Mensaje recibido:', messageContent);
          
          const data = JSON.parse(messageContent);
          if (data.action === 'INSERT_USER') {
            await pool.query("INSERT INTO users SET ?", data.user);
          }

          // Confirmar que el mensaje fue recibido
          channel.ack(msg);
        } catch (error) {
          console.error('Error al procesar el mensaje:', error);
          // Si ocurre un error, podrías no hacer ack y dejar que el mensaje se reenvíe más tarde
          // channel.nack(msg, false, true); // Re-enqueue message
        }
      }
    });

    console.log(`Escuchando en la cola: ${queue}`);
  } catch (error) {
    console.error('Error al conectar o consumir de RabbitMQ:', error);
  }
};

startConsumer();
