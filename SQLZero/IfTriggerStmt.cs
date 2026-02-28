namespace SQLZero
{
    /// <summary>IF cond THEN body [ELSEIF cond THEN body]* [ELSE body] END IF</summary>
    internal class IfTriggerStmt : TriggerStmt
    {
        public List<(List<SqlToken> Cond, List<TriggerStmt> Body)> Branches = [];
        public List<TriggerStmt>? ElseBranch;
    }
}