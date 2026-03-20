package dialect

// Dialect abstracts SQL differences between databases.
// Implement this interface to add support for a custom database.
type Dialect interface {
	// Placeholder returns the bind parameter for the given 1-based index.
	Placeholder(index int) string
	// SchemaSQL returns DDL for the given table prefix.
	SchemaSQL(prefix string) string
	// DateAddSQL returns "timestamp + N seconds" expression.
	DateAddSQL(col, secondsExpr string) string
}
