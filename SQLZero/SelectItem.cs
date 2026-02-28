namespace SQLZero
{
    internal class SelectItem
    {
        public string Name = "";
        public string? Alias;
        public List<SqlToken> ExprTokens = [];
        public bool IsStar;
        public string? TableFilter;  // for table.*
        public bool IsAggregate;
        public string? AggFunc;
        public string? AggCol;
        public bool AggDistinct;

        public string OutputName => Alias ?? Name ?? "expr";
    }
}