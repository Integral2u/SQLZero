namespace SQLZero
{
    // ============================================================
    //  EXPRESSION EVALUATOR
    // ============================================================

    public class EvalContext
    {
        public required Dictionary<string, object?> Row;
        public required Dictionary<string, SqlFunction> Functions;
        public required Dictionary<string, SQLTable> Tables;
        /// <summary>User add-ins registered on the database, threaded through for expression evaluation.</summary>
        public IReadOnlyDictionary<string, ISqlAddIn>? AddIns;

        public object? GetColumn(string name)
        {
            if (Row.TryGetValue(name, out var v)) return v;
            var key = Row.Keys.FirstOrDefault(k => k.Equals(name, StringComparison.OrdinalIgnoreCase));
            return key != null ? Row[key] : null;
        }
    }
}