const { parentPort, workerData, threadId } = require('worker_threads');

const MAX_REGISTROS = workerData.maxRegistros;
const NAME_SIZE = workerData.nameSize;

const mutex = new Int32Array(workerData.mutexBuffer);
const count = new Int32Array(workerData.countBuffer);
const registros = new Int32Array(workerData.recordBuffer); // [id, ativo, id, ativo, ...]
const nomes = new Uint8Array(workerData.nameBuffer);

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function lock() {
  while (true) {
    if (Atomics.compareExchange(mutex, 0, 0, 1) === 0) {
      return;
    }
    Atomics.wait(mutex, 0, 1, 50);
  }
}

function unlock() {
  Atomics.store(mutex, 0, 0);
  Atomics.notify(mutex, 0, 1);
}

function baseIndex(slot) {
  return slot * 2;
}

function nameOffset(slot) {
  return slot * NAME_SIZE;
}

function getId(slot) {
  return registros[baseIndex(slot)];
}

function setId(slot, id) {
  registros[baseIndex(slot)] = id;
}

function isActive(slot) {
  return registros[baseIndex(slot) + 1] === 1;
}

function setActive(slot, value) {
  registros[baseIndex(slot) + 1] = value ? 1 : 0;
}

function writeName(slot, nome) {
  const offset = nameOffset(slot);
  nomes.fill(0, offset, offset + NAME_SIZE);

  const bytes = encoder.encode(nome);
  const limit = Math.min(bytes.length, NAME_SIZE - 1);

  for (let i = 0; i < limit; i++) {
    nomes[offset + i] = bytes[i];
  }
}

function readName(slot) {
  const offset = nameOffset(slot);
  let end = offset;

  while (end < offset + NAME_SIZE && nomes[end] !== 0) {
    end++;
  }

  return decoder.decode(nomes.slice(offset, end));
}

function clearSlot(slot) {
  setId(slot, 0);
  setActive(slot, 0);
  writeName(slot, '');
}

function findSlotById(id) {
  for (let slot = 0; slot < MAX_REGISTROS; slot++) {
    if (isActive(slot) && getId(slot) === id) {
      return slot;
    }
  }
  return -1;
}

function findFreeSlot() {
  for (let slot = 0; slot < MAX_REGISTROS; slot++) {
    if (!isActive(slot)) {
      return slot;
    }
  }
  return -1;
}

function insertRegistro(id, nome) {
  if (findSlotById(id) !== -1) {
    return { ok: false, message: `INSERT falhou: id ${id} já existe.` };
  }

  const slot = findFreeSlot();
  if (slot === -1) {
    return { ok: false, message: 'INSERT falhou: banco cheio.' };
  }

  setId(slot, id);
  writeName(slot, nome);
  setActive(slot, 1);
  Atomics.add(count, 0, 1);

  return { ok: true, message: `INSERT realizado: { id: ${id}, nome: "${nome}" }` };
}

function deleteRegistro(id) {
  const slot = findSlotById(id);
  if (slot === -1) {
    return { ok: false, message: `DELETE falhou: id ${id} não encontrado.` };
  }

  clearSlot(slot);
  Atomics.sub(count, 0, 1);

  return { ok: true, message: `DELETE realizado: id ${id} removido.` };
}

function selectRegistro(id) {
  const slot = findSlotById(id);
  if (slot === -1) {
    return { ok: false, message: `SELECT: id ${id} não encontrado.` };
  }

  return {
    ok: true,
    message: `SELECT: { id: ${getId(slot)}, nome: "${readName(slot)}" }`
  };
}

function updateRegistro(id, novoNome) {
  const slot = findSlotById(id);
  if (slot === -1) {
    return { ok: false, message: `UPDATE falhou: id ${id} não encontrado.` };
  }

  writeName(slot, novoNome);
  return {
    ok: true,
    message: `UPDATE realizado: id ${id} agora possui nome "${novoNome}".`
  };
}

async function processRequest(task) {
  const startedAt = new Date().toISOString();

  // Simulação de processamento para deixar o paralelismo visível.
  await sleep(300 + Math.floor(Math.random() * 900));

  lock();
  let result;

  try {
    switch (task.op) {
      case 'INSERT':
        result = insertRegistro(task.id, task.nome);
        break;
      case 'DELETE':
        result = deleteRegistro(task.id);
        break;
      case 'SELECT':
        result = selectRegistro(task.id);
        break;
      case 'UPDATE':
        result = updateRegistro(task.id, task.nome);
        break;
      default:
        result = { ok: false, message: `Operação inválida: ${task.op}` };
        break;
    }
  } finally {
    unlock();
  }

  return {
    taskId: task.taskId,
    threadId,
    startedAt,
    finishedAt: new Date().toISOString(),
    ...result
  };
}

parentPort.on('message', async (task) => {
  const response = await processRequest(task);
  parentPort.postMessage(response);
});
