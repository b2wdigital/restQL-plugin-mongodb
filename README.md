# MongoDB Plugin for RestQL Go

This plugin is an interface that allow RestQL to use MongoDB as data store for queries and mappings.

## Usage

The plugin depends on the following environment variables:

- `RESTQL_DATABASE_CONNECTION_STRING`: sets the MongoDB connection string. 
- `RESTQL_DATABASE_NAME`: sets the MongoDB database name
- `RESTQL_DATABASE_CONNECTION_TIMEOUT`: sets database connection timeout, accepts a [Golang Duration string](https://golang.org/pkg/time/#ParseDuration).
- `RESTQL_DATABASE_MAPPINGS_READ_TIMEOUT`: sets the timeout for read mappings from the database, accepts a [Golang Duration string](https://golang.org/pkg/time/#ParseDuration).
- `RESTQL_DATABASE_QUERY_READ_TIMEOUT`: sets the timeout for read a query from the database, accepts a [Golang Duration string](https://golang.org/pkg/time/#ParseDuration).
