const { Worker } = require('worker_threads');
const path = require('path');
const readline = require('readline');

const THREAD_COUNT = 4;
const MAX_REGISTROS = 100;
const NAME_SIZE = 50;

const mutexBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
const countBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
const recordBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * MAX_REGISTROS * 2);
const nameBuffer = new SharedArrayBuffer(Uint8Array.BYTES_PER_ELEMENT * MAX_REGISTROS * NAME_SIZE);

const mutex = new Int32Array(mutexBuffer);
const count = new Int32Array(countBuffer);
const registros = new Int32Array(recordBuffer);
const nomes = new Uint8Array(nameBuffer);

const decoder = new TextDecoder();
const queue = [];
const workers = [];
let nextTaskId = 1;
let shutdownRequested = false;

function baseIndex(slot) {
  return slot * 2;
}

function nameOffset(slot) {
  return slot * NAME_SIZE;
}

function isActive(slot) {
  return registros[baseIndex(slot) + 1] === 1;
}

function getId(slot) {
  return registros[baseIndex(slot)];
}

function readName(slot) {
  const offset = nameOffset(slot);
  let end = offset;

  while (end < offset + NAME_SIZE && nomes[end] !== 0) {
    end++;
  }

  return decoder.decode(nomes.slice(offset, end));
}

function listDatabase() {
  const rows = [];

  for (let slot = 0; slot < MAX_REGISTROS; slot++) {
    if (isActive(slot)) {
      rows.push({
        id: getId(slot),
        nome: readName(slot)
      });
    }
  }

  return rows.sort((a, b) => a.id - b.id);
}

function printDatabase() {
  const rows = listDatabase();

  console.log('\n===== ESTADO ATUAL DO BANCO =====');
  if (rows.length === 0) {
    console.log('(vazio)');
  } else {
    console.table(rows);
  }
  console.log(`Total de registros: ${count[0]}`);
  console.log('=================================\n');
}

function createWorker(index) {
  const worker = new Worker(path.resolve(__dirname, 'worker.js'), {
    workerData: {
      maxRegistros: MAX_REGISTROS,
      nameSize: NAME_SIZE,
      mutexBuffer,
      countBuffer,
      recordBuffer,
      nameBuffer
    }
  });

  const entry = {
    name: `Worker-${index + 1}`,
    worker,
    busy: false
  };

  worker.on('message', (message) => {
    entry.busy = false;

    const status = message.ok ? 'OK' : 'ERRO';
    console.log(
      `[${entry.name}] [Thread ${message.threadId}] [Tarefa ${message.taskId}] ${status} - ${message.message}`
    );

    if (['INSERT', 'DELETE', 'UPDATE'].includes(findTaskOp(message.taskId))) {
      printDatabase();
    }

    dispatch();
  });

  worker.on('error', (error) => {
    entry.busy = false;
    console.error(`[${entry.name}] erro:`, error);
    dispatch();
  });

  worker.on('exit', (code) => {
    if (shutdownRequested) {
      return;
    }

    if (code !== 0) {
      console.error(`[${entry.name}] encerrada com código ${code}`);
    }
  });

  workers.push(entry);
}

const taskHistory = new Map();

function findTaskOp(taskId) {
  return taskHistory.get(taskId)?.op;
}

function dispatch() {
  for (const entry of workers) {
    if (entry.busy || queue.length === 0) {
      continue;
    }

    const task = queue.shift();
    entry.busy = true;
    entry.worker.postMessage(task);
  }

  attemptShutdown();
}

function allWorkersIdle() {
  return workers.every((entry) => !entry.busy);
}

async function attemptShutdown() {
  if (!shutdownRequested) {
    return;
  }

  if (queue.length > 0 || !allWorkersIdle()) {
    return;
  }

  console.log('Encerrando servidor...');
  await Promise.all(workers.map(({ worker }) => worker.terminate()));
  process.exit(0);
}

function enqueueTask(task) {
  task.taskId = nextTaskId++;
  taskHistory.set(task.taskId, task);
  queue.push(task);

  console.log(`[SERVIDOR] Tarefa ${task.taskId} enfileirada:`, task);
  dispatch();
}

function parseCommand(line) {
  const text = line.trim();
  if (!text) {
    return null;
  }

  const parts = text.split(/\s+/);
  const op = parts[0].toUpperCase();

  if (op === 'INSERT') {
    if (parts.length < 3) {
      throw new Error('Uso: INSERT <id> <nome>');
    }

    return {
      op,
      id: Number(parts[1]),
      nome: parts.slice(2).join(' ')
    };
  }

  if (op === 'UPDATE') {
    if (parts.length < 3) {
      throw new Error('Uso: UPDATE <id> <novo_nome>');
    }

    return {
      op,
      id: Number(parts[1]),
      nome: parts.slice(2).join(' ')
    };
  }

  if (op === 'DELETE' || op === 'SELECT') {
    if (parts.length !== 2) {
      throw new Error(`Uso: ${op} <id>`);
    }

    return {
      op,
      id: Number(parts[1])
    };
  }

  if (op === 'LIST' || op === 'EXIT' || op === 'DEMO' || op === 'HELP') {
    return { op };
  }

  throw new Error(`Comando inválido: ${op}`);
}

function validateTask(task) {
  if (!['INSERT', 'DELETE', 'SELECT', 'UPDATE'].includes(task.op)) {
    return;
  }

  if (!Number.isInteger(task.id) || task.id <= 0) {
    throw new Error('O id deve ser um número inteiro positivo.');
  }

  if (['INSERT', 'UPDATE'].includes(task.op)) {
    if (!task.nome || task.nome.trim() === '') {
      throw new Error('O nome não pode ser vazio.');
    }
  }
}

function runDemo() {
  const demoTasks = [
    { op: 'INSERT', id: 1, nome: 'Ana' },
    { op: 'INSERT', id: 2, nome: 'Bruno' },
    { op: 'INSERT', id: 3, nome: 'Carla' },
    { op: 'SELECT', id: 2 },
    { op: 'UPDATE', id: 2, nome: 'Bruno Lima' },
    { op: 'DELETE', id: 1 },
    { op: 'SELECT', id: 1 },
    { op: 'INSERT', id: 4, nome: 'Daniel' }
  ];

  for (const task of demoTasks) {
    enqueueTask(task);
  }
}

function printHelp() {
  console.log(`
Comandos disponíveis:
  INSERT <id> <nome>
  SELECT <id>
  UPDATE <id> <novo_nome>
  DELETE <id>
  LIST
  DEMO
  HELP
  EXIT
`);
}

for (let i = 0; i < THREAD_COUNT; i++) {
  createWorker(i);
}

console.log('Servidor iniciado com pool de threads.');
console.log(`Threads ativas: ${THREAD_COUNT}`);
printHelp();

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'servidor> '
});

rl.prompt();

rl.on('line', (line) => {
  try {
    const task = parseCommand(line);
    if (!task) {
      rl.prompt();
      return;
    }

    switch (task.op) {
      case 'LIST':
        printDatabase();
        break;
      case 'HELP':
        printHelp();
        break;
      case 'DEMO':
        runDemo();
        break;
      case 'EXIT':
        rl.close();
        return;
      default:
        validateTask(task);
        enqueueTask(task);
        break;
    }
  } catch (error) {
    console.error(`[ERRO] ${error.message}`);
  }

  rl.prompt();
});

rl.on('close', () => {
  shutdownRequested = true;

  if (queue.length > 0 || !allWorkersIdle()) {
    console.log('Aguardando conclusão das tarefas em andamento...');
  }

  attemptShutdown();
});
