//go:build murmur_testhooks

package main

import db "github.com/marcgauthier/murmur"

func installSchemaCrashForTest(database *db.DB, phase string) error {
	return db.InstallSchemaStoreCrashForTest(database, phase)
}
