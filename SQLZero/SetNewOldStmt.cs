namespace SQLZero
{
    /// <summary>SET NEW.col = expr  or  SET OLD.col = expr</summary>
    internal class SetNewOldStmt : TriggerStmt
    {
        public bool IsNew;           // true → NEW, false → OLD
        public string Column = "";
        public List<SqlToken> Expr = [];
    }
}