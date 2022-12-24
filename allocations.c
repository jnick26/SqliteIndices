#include <assert.h>
#include <jemalloc/jemalloc.h>

#define CHUNKS 1024 * 512

#define CHUNKSIZE 16

struct Chunk64 {
  char __buf[CHUNKSIZE];
};

struct Node {
  struct Node *next;
};

struct Pool64 {
  union {
    struct Chunk64 chunk;
    struct Node node;
  } chunks[CHUNKS];
  struct Node *head;
};

struct Pool64 *getPool64() {
  static struct Pool64 memory;
  static struct Pool64 *pool = NULL;
  if (!pool) {
    for (int i = 0; i < CHUNKS - 1; ++i) {
      memory.chunks[i].node.next = &memory.chunks[i + 1].node;
    }
    memory.chunks[CHUNKS - 1].node.next = NULL;
    memory.head = &memory.chunks[0].node;
    pool = &memory;
  }
  return pool;
}

void *allocInPool64(struct Pool64 *pool) {
  struct Node *node = pool->head;
  if (node == NULL) {
    // fprintf(stderr, "no space\n");
    return NULL;
  }

  pool->head = pool->head->next;
  return node;
}

void *allocInPool(size_t __size) {
  if (__size <= CHUNKSIZE) {
    return allocInPool64(getPool64());
  }

  return NULL;
}

bool freeFromPool64(void *ptr) {
  struct Pool64 *pool = getPool64();
  void *poolBegin = &pool->chunks;
  void *poolEnd = ((void *)&pool->chunks) + sizeof(struct Chunk64[CHUNKS]);
  if (ptr < poolBegin || poolEnd < ptr) {
    return false;
  }

  int index = (ptr - poolBegin) / sizeof(struct Chunk64);
  assert(index < CHUNKS);

  struct Node *head = ptr;
  head->next = pool->head;
  pool->head = head;
  return true;
}

bool freeFromPool(void *ptr) {
  if (freeFromPool64(ptr)) {
    return true;
  }

  return false;
}

void *malloc(size_t __size) {
  void *mem;
  if ((mem = allocInPool(__size)) != NULL) {
    return mem;
  }
  return je_malloc(__size);
}

void *calloc(size_t __count, size_t __size) {
  return je_calloc(__count, __size);
}

void *realloc(void *__ptr, size_t __size) { return je_realloc(__ptr, __size); }

void free(void *ptr) {
  if (freeFromPool(ptr)) {
    return;
  }
  return je_free(ptr);
}
