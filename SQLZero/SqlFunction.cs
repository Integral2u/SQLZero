namespace SQLZero
{
    // ============================================================
    //  INTERNAL QUERY STRUCTURES
    // ============================================================

    public class SqlFunction
    {
        public required string Name;
        public List<(string Name, Type Type)> Parameters = [];
        public Type ReturnType = typeof(object);
        public Func<object?[], object?>? CompiledFunc;
    }
}