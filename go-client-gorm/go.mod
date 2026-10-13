module github.com/marcgauthier/murmur/go-client-gorm

go 1.24

require (
	github.com/marcgauthier/murmur/go-client v0.0.0
	gorm.io/gorm v1.31.2
)

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	golang.org/x/text v0.20.0 // indirect
)

replace github.com/marcgauthier/murmur/go-client => ../go-client
