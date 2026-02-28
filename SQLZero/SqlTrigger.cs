namespace SQLZero
{
    internal class SqlTrigger
    {
        public string Name = "";
        public string TableName = "";
        public string Timing = "";                // "BEFORE" | "AFTER"
        public string Event = "";                 // "INSERT" | "UPDATE" | "DELETE"
        public List<TriggerStmt> Statements = []; // pre-parsed body
        public string? OriginalSql;              // stored for JSON round-trip
    }
}