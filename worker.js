const { parentPort, workerData, threadId } = require('worker_threads');

const MAX_REGISTROS = workerData.maxRegistros; // qtd max de registros que o "banco" suporta

const NAME_SIZE = workerData.nameSize; // tamanho max para cada nome

// buffer compartilhado usado como mutex
// mutex[0] = 0 -> livre
// mutex[0] = 1 -> ocupado
const mutex = new Int32Array(workerData.mutexBuffer);

const count = new Int32Array(workerData.countBuffer); // buffer queguarda a qtd atual de registros ativos

const registros = new Int32Array(workerData.recordBuffer); // buffer com os dados principais dos registros

const nomes = new Uint8Array(workerData.nameBuffer); // buffer para armazenar os nomes em bytes

// Encoder/Decoder para converter string para bytes
const encoder = new TextEncoder();
const decoder = new TextDecoder();

// simula um tempo de processamento
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// tenta adquirir o lock do mutex e se o mutex estiver ocupado, a thread espera
function lock() {
  while (true) {
    // se o valor atual for 0, troca para 1
    if (Atomics.compareExchange(mutex, 0, 0, 1) === 0) {
      return;
    }
    
    Atomics.wait(mutex, 0, 1, 50); // se outra thread estiver usando, espera um pouco antes de tentar novamente
  }
}

// libera o mutex e notifica outra thread que estiver esperando
function unlock() {
  Atomics.store(mutex, 0, 0);
  Atomics.notify(mutex, 0, 1);
}

// retorna um slot no array de registros [id, ativo]
function baseIndex(slot) {
  return slot * 2;
}

// retorna o deslocamento inicial do nome de um slot no buffer de nomes
function nameOffset(slot) {
  return slot * NAME_SIZE;
}

// le o id armazenado em um slot
function getId(slot) {
  return registros[baseIndex(slot)];
}

// define o id de um slot
function setId(slot, id) {
  registros[baseIndex(slot)] = id;
}

// verifica se o slot está ativo
// ativo = 1 -> em uso
// ativo = 0 -> livre
function isActive(slot) {
  return registros[baseIndex(slot) + 1] === 1;
}

// marca um slot como ativo ou inativo
function setActive(slot, value) {
  registros[baseIndex(slot) + 1] = value ? 1 : 0;
}

// limpa o texto antigo e escreve o novo nome no buffer de nomes 
function writeName(slot, nome) {
  const offset = nameOffset(slot);

  nomes.fill(0, offset, offset + NAME_SIZE); // limpa tudo

  const bytes = encoder.encode(nome); // converte string para bytes

  const limit = Math.min(bytes.length, NAME_SIZE - 1); // reserva 1 byte para o terminador 0

  for (let i = 0; i < limit; i++) {
    nomes[offset + i] = bytes[i];
  }
}


// le o nome armazenado em um slot
function readName(slot) {
  const offset = nameOffset(slot);
  let end = offset;

  while (end < offset + NAME_SIZE && nomes[end] !== 0) {
    end++;
  }

  return decoder.decode(nomes.slice(offset, end));
}

// limpa completamente um slot
// id = 0
// ativo = 0
// nome = vazio 
function clearSlot(slot) {
  setId(slot, 0);
  setActive(slot, 0);
  writeName(slot, '');
}

// procura um registro ativo pelo id
function findSlotById(id) {
  for (let slot = 0; slot < MAX_REGISTROS; slot++) {
    if (isActive(slot) && getId(slot) === id) {
      return slot;
    }
  }
  return -1;
}

// procura slot livre
function findFreeSlot() {
  for (let slot = 0; slot < MAX_REGISTROS; slot++) {
    if (!isActive(slot)) {
      return slot;
    }
  }
  return -1;
}

// insere um novo registro no banco simulado
function insertRegistro(id, nome) {
  // não deixa inserir ids duplicados
  if (findSlotById(id) !== -1) {
    return { ok: false, message: `INSERT falhou: id ${id} já existe.` };
  }

// procura slot livre
  const slot = findFreeSlot();
  if (slot === -1) {
    return { ok: false, message: 'INSERT falhou: banco cheio.' };
  }

  // salva os dados no slot encontrado
  setId(slot, id);
  writeName(slot, nome);
  setActive(slot, 1);

  Atomics.add(count, 0, 1);

  return { ok: true, message: `INSERT realizado: { id: ${id}, nome: "${nome}" }` };
}

// remove um registro do banco simulado
function deleteRegistro(id) {
  const slot = findSlotById(id);
  if (slot === -1) {
    return { ok: false, message: `DELETE falhou: id ${id} não encontrado.` };
  }

  // limpa os dados do slot removido
  clearSlot(slot);

  Atomics.sub(count, 0, 1);

  return { ok: true, message: `DELETE realizado: id ${id} removido.` };
}

// busca um registro pelo id e retorna os dados
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

// atualiza o nome de um registro existente
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

// processa uma requisição recebida pela thread
async function processRequest(task) {
  const startedAt = new Date().toISOString();

  // simula um atraso
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

  // mostra resultados e qual thread processou
  return {
    taskId: task.taskId,
    threadId,
    startedAt,
    finishedAt: new Date().toISOString(),
    ...result
  };
}

// recebe msg e processa a requisição
parentPort.on('message', async (task) => {
  const response = await processRequest(task);
  parentPort.postMessage(response);
});