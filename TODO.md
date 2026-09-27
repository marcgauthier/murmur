ask muse to review all the tests-live in ../GALVANIZE and when possible convert those tests to golang for SPeD-SQL

ask muse to create tests for the code to increase tests coverage of the code.

ask codex to review architecture/README.md and its linked documents and confirm if parts of the architecture are not implemented yet.


- [x] add schema storage and migration in pebble.

- [x] are the cache settings for pebble available for the user to set when opening the database? (Available via Config.Pebble / DefaultPebbleConfig)

- [x] add Backup and restore function (Implemented in backup/ with online hard-link checkpoints and pure ciphertext streaming)
