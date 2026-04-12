# Servidor com threads - Trabalho de Sistemas Operacionais

## O que este projeto entrega agora
Esta versão implementa **somente o servidor com threads funcionando**, que é exatamente a entrega parcial pedida no enunciado.

## Tecnologias
- Node.js
- `worker_threads`
- `SharedArrayBuffer`
- `Atomics` (mutex simples)

## Como executar
```bash
node server.js
```

## Comandos
```bash
INSERT 1 Ana
INSERT 2 Bruno
SELECT 1
UPDATE 2 Bruno Lima
DELETE 1
LIST
DEMO
EXIT
```

## O que está sendo demonstrado
- pool de threads no servidor;
- processamento paralelo de requisições;
- proteção de região crítica com mutex;
- banco simulado em memória compartilhada;
- operações INSERT, SELECT, UPDATE e DELETE.

## O que pode ser feito depois
Na próxima etapa, vocês podem adicionar:
- processo cliente separado;
- comunicação IPC real entre cliente e servidor (pipe ou memória compartilhada);
- arquivo de log ou canal de resposta.
