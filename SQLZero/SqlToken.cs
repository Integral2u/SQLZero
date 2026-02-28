namespace SQLZero
{
    public class SqlToken(SqlTokenType t, string? v)
    {
        public SqlTokenType Type = t;
        public string Value = v ?? string.Empty;
        public override string ToString() => $"[{Type}:{Value}]";
    }
}