namespace SQLZero
{
    /// <summary>Any DML statement whose NEW./OLD. references are substituted at runtime.</summary>
    internal class DmlTriggerStmt : TriggerStmt
    {
        public List<SqlToken> Tokens = [];
    }
}