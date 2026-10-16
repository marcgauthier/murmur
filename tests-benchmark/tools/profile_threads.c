/* Linux benchmark-only pthread interposer for gperftools per-thread CPU timers.
 * Build with -shared -fPIC, linking libprofiler and libdl. No allocator override.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>

extern void ProfilerRegisterThread(void);
struct entry { void *(*function)(void *); void *argument; };
static void *registered_entry(void *argument) {
    struct entry entry = *(struct entry *)argument;
    free(argument);
    ProfilerRegisterThread();
    return entry.function(entry.argument);
}
int pthread_create(pthread_t *thread, const pthread_attr_t *attributes,
                   void *(*function)(void *), void *argument) {
    int (*create)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *) =
        dlsym(RTLD_NEXT, "pthread_create");
    if (!create) return ENOSYS;
    struct entry *entry = malloc(sizeof(*entry));
    if (!entry) return ENOMEM;
    *entry = (struct entry){function, argument};
    int result = create(thread, attributes, registered_entry, entry);
    if (result) free(entry);
    return result;
}
