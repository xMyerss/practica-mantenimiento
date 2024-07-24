import { pool } from "./dbConnection.js";
import amqplib from 'amqplib';

// Función para enviar mensajes a RabbitMQ
const sendToRabbitMQ = async (queue, message) => {
  try {
    const conn = await amqplib.connect('amqps://zsfpkhaq:0iroPd-VsW9zkrfYt4-kg4BK-wftfsh4@cow.rmq2.cloudamqp.com/zsfpkhaq');
    const channel = await conn.createChannel();
    await channel.assertQueue(queue);
    await channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)));
    await channel.close();
    await conn.close();
  } catch (error) {
    console.error('Error sending message to RabbitMQ', error);
  }
};

export const getUsers = async () => {
  try {
    const [rows] = await pool.query("SELECT * FROM users ORDER BY id");

    // Enviar datos a RabbitMQ usando JSON Envelope
    const message = {
      status: 'success',
      code: 200,
      message: 'Users retrieved successfully',
      data: rows
    };
    await sendToRabbitMQ('fila1', message);

    return rows;
  } catch (error) {
    console.error('Error fetching users:', error);
    throw new Error('Database query failed');
  }
};

export const insertUser = async (userData) => {
  try {
    const [result] = await pool.query("INSERT INTO users SET ?", userData);
    const newUser = {
      ...userData,
      id: result.insertId,
    };

    // Enviar datos a RabbitMQ usando JSON Envelope
    const message = {
      status: 'success',
      code: 201,
      message: 'User created successfully',
      data: newUser
    };
    await sendToRabbitMQ('fila1', message);

    return newUser;
  } catch (error) {
    console.error('Error inserting user:', error);
    throw new Error('Database query failed');
  }
};


export const updateUser = async (userData) => {
  const sql = `
      UPDATE users SET
      username = ${pool.escape(userData.username)},
      password = ${pool.escape(userData.password)},
      email = ${pool.escape(userData.email)}
      WHERE id = ${userData.id}`;

  const [result] = await pool.query(sql);
  return result;
};

export const deleteUser = async (id) => {
  const sql = `DELETE FROM users WHERE id=` + pool.escape(id);
  const [result] = await pool.query(sql);
  return result;
};
