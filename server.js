const { Worker } = require('worker_threads');
const path = require('path');
const readline = require('readline');

const THREAD_COUNT = 4;
const MAX_REGISTROS = 100;
const NAME_SIZE = 50;

// buffers compartilhados entre as threads
const mutexBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
const countBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
const recordBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * MAX_REGISTROS * 2);
const nameBuffer = new SharedArrayBuffer(Uint8Array.BYTES_PER_ELEMENT * MAX_REGISTROS * NAME_SIZE);

// acesso dos buffers compartilhados
const mutex = new Int32Array(mutexBuffer);
const count = new Int32Array(countBuffer);
const registros = new Int32Array(recordBuffer);
const nomes = new Uint8Array(nameBuffer);

// estruturas auxiliares do servidor
const decoder = new TextDecoder();
const queue = [];
const workers = [];
let nextTaskId = 1;
let shutdownRequested = false;

// retorna a posição base de um slot no array de registros
function baseIndex(slot) {
  return slot * 2;
}

// retorna o deslocamento do nome no buffer de nomes
function nameOffset(slot) {
  return slot * NAME_SIZE;
}

// verifica se um slot esta ativo
function isActive(slot) {
  return registros[baseIndex(slot) + 1] === 1;
}

// retorna o id armazenado no slot
function getId(slot) {
  return registros[baseIndex(slot)];
}

// Lê o nome armazenado em um slot
function readName(slot) {
  const offset = nameOffset(slot);
  let end = offset;

  while (end < offset + NAME_SIZE && nomes[end] !== 0) {
    end++;
  }

  return decoder.decode(nomes.slice(offset, end));
}

// monta a lista atual de registros ativos
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

// mostra estado atual do banco
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

// cria uma worker thread e configura seus eventos
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

  // quando a thread termina uma tarefa
  worker.on('message', (message) => {
    entry.busy = false;

    const status = message.ok ? 'OK' : 'ERRO';
    console.log(
      `[${entry.name}] [Thread ${message.threadId}] [Tarefa ${message.taskId}] ${status} - ${message.message}`
    );

    // após operações mostra o estado atual
    if (['INSERT', 'DELETE', 'UPDATE'].includes(findTaskOp(message.taskId))) {
      printDatabase();
    }

    dispatch();
  });

  // trata erro da thread
  worker.on('error', (error) => {
    entry.busy = false;
    console.error(`[${entry.name}] erro:`, error);
    dispatch();
  });

  // trata encerramento da thread
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

// salva histórico das tarefas enfileiradas
const taskHistory = new Map();

// busca a operação da tarefa pelo id
function findTaskOp(taskId) {
  return taskHistory.get(taskId)?.op;
}

// distribui tarefas da fila para workers livres
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

// verifica se todas as threads estão livres
function allWorkersIdle() {
  return workers.every((entry) => !entry.busy);
}

// encerra o servidor somente quando tudo terminar
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

// adiciona uma tarefa na fila
function enqueueTask(task) {
  task.taskId = nextTaskId++;
  taskHistory.set(task.taskId, task);
  queue.push(task);

  console.log(`[SERVIDOR] Tarefa ${task.taskId} enfileirada:`, task);
  dispatch();
}

// converte o comando digitado em uma tarefa
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

// valida os dados antes de enfileirar a tarefa
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

// monta um conjunto pronto de tarefas para teste
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

// mostra os comandos disponíveis
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

// cria todas as threads do pool
for (let i = 0; i < THREAD_COUNT; i++) {
  createWorker(i);
}

console.log('Servidor iniciado com pool de threads.');
console.log(`Threads ativas: ${THREAD_COUNT}`);
printHelp();

// interface para ler comandos digitados
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'servidor> '
});

rl.prompt();

// trata cada linha digitada
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

// espera as tarefas terminarem antes de encerrar
rl.on('close', () => {
  shutdownRequested = true;

  if (queue.length > 0 || !allWorkersIdle()) {
    console.log('Aguardando conclusão das tarefas em andamento...');
  }

  attemptShutdown();
});