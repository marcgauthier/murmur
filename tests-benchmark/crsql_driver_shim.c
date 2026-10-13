/* The cr-sqlite fork expects the repldb persistence callback at load time.
 * This benchmark has no external persistence work to perform. */
int crsql_driver_commit(void *db) {
  (void)db;
  return 0;
}
