#ifndef MURMUR_GO_H
#define MURMUR_GO_H
#include <stdint.h>
#include <stddef.h>
typedef struct { uint8_t *ptr; size_t len; size_t cap; } murmur_buffer;
typedef int32_t (*murmur_callback)(uint64_t, uint32_t, uint64_t, const uint8_t *, size_t, murmur_buffer *);
uint32_t murmur_abi_version(void);
void murmur_buffer_free(murmur_buffer);
murmur_buffer murmur_buffer_copy(const uint8_t *, size_t);
int32_t murmur_call(uint64_t, const uint8_t *, size_t, murmur_callback, murmur_buffer *);
/* SQL parameters/results use a little-endian tagged binary format, documented in ABI.md. */
int32_t murmur_sql(uint64_t, uint32_t, const uint8_t *, size_t, murmur_buffer *);
#endif
