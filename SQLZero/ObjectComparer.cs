namespace SQLZero
{
    internal class ObjectComparer : IComparer<object?>
    {
        public static readonly ObjectComparer Instance = new();
        public int Compare(object? a, object? b) => SqlExpr.ObjCmp(a, b);
    }
}