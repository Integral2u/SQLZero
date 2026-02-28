using System.Data;
namespace SQLZero
{
    // ============================================================
    //  SQL EXECUTOR  (parser + evaluation engine)
    // ============================================================

    internal class SqlExecutor(List<SqlToken> tokens, Dictionary<string, SQLTable> tables,
                       Dictionary<string, SqlFunction> functions,
                       Dictionary<string, SqlTrigger> triggers,
                       IReadOnlyDictionary<string, ISqlAddIn>? addIns = null)
    {
        private readonly List<SqlToken> _t = tokens;
        private int _p = 0;
        private readonly Dictionary<string, SQLTable> _tables = tables;
        private readonly Dictionary<string, SqlFunction> _functions = functions;
        private readonly Dictionary<string, SqlTrigger> _triggers = triggers;
        private readonly IReadOnlyDictionary<string, ISqlAddIn>? _addIns = addIns;

        private SqlToken Cur => _p < _t.Count ? _t[_p] : new SqlToken(SqlTokenType.EOF, "");
        private SqlToken Peek(int n = 1) => _p + n < _t.Count ? _t[_p + n] : new SqlToken(SqlTokenType.EOF, "");

        private SqlToken Consume() => _t[_p++];
        private void Expect(string v)
        {
            if (!Cur.Value.Equals(v, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected '{v}' but got '{Cur.Value}'.");
            _p++;
        }
        private bool Match(string v)
        { if (Cur.Value.Equals(v, StringComparison.OrdinalIgnoreCase)) { _p++; return true; } return false; }

        // ── Dispatch ─────────────────────────────────────────────────────
        public int RunNonQuery()
        {
            return Cur.Value.ToUpperInvariant() switch
            {
                "INSERT" => DoInsert(),
                "UPDATE" => DoUpdate(),
                "DELETE" => DoDelete(),
                "CREATE" => DoCreate(),
                "ALTER" => DoAlter(),
                "DROP" => DoDrop(),
                _ => throw new InvalidOperationException($"Unsupported statement: '{Cur.Value}'.")
            };
        }

        public object?[,] RunReader()
        {
            if (!Cur.Value.Equals("SELECT", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected SELECT, got '{Cur.Value}'.");
            return DoSelect();
        }

        /// <summary>
        /// Executes a SELECT, checking <paramref name="ct"/> between each row evaluation.
        /// Returns headers and a jagged row array separately so the async public API can
        /// surface headers immediately and stream data rows without allocating a 2-D array.
        /// </summary>
        public (string[] Headers, object?[][] Rows) RunReaderWithHeaders(CancellationToken ct)
        {
            if (!Cur.Value.Equals("SELECT", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected SELECT, got '{Cur.Value}'.");
            return DoSelectWithHeaders(ct);
        }

        // ============================================================
        //  INSERT
        // ============================================================
        private int DoInsert()
        {
            Expect("INSERT"); Expect("INTO");
            string tname = Consume().Value;
            var table = RequireTable(tname);
            string[]? colNames = null;

            if (Cur.Value == "(")
            {
                _p++;
                var cols = new List<string>();
                while (Cur.Value != ")")
                { cols.Add(Consume().Value); Match(","); }
                _p++; // )
                colNames = [.. cols];
            }

            Expect("VALUES");
            int rows = 0;
            var tableCols = table.Columns;

            while (Cur.Value == "(" || rows == 0)
            {
                if (Cur.Value != "(") break;
                _p++;
                var vals = new List<object?>();
                while (Cur.Value != ")")
                {
                    var ctx = MakeCtx([]);
                    vals.Add(SqlExpr.Eval(_t, ref _p, ctx));
                    Match(",");
                }
                _p++; // )

                object?[] fullRow;
                if (colNames != null)
                {
                    fullRow = new object[tableCols.Length];
                    for (int i = 0; i < tableCols.Length; i++)
                    {
                        int ci = Array.FindIndex(colNames, c => c.Equals(tableCols[i], StringComparison.OrdinalIgnoreCase));
                        fullRow[i] = ci >= 0 ? vals[ci] : null;
                    }
                }
                else fullRow = [.. vals];

                // Build NEW row dict for trigger access
                var newRow = BuildRowDict(tableCols, fullRow);

                // ── BEFORE INSERT ──
                FireTriggers(tname, "BEFORE", "INSERT", null, newRow, tableCols);

                // Apply any BEFORE-trigger modifications back to fullRow
                for (int i = 0; i < tableCols.Length; i++)
                    if (newRow.TryGetValue(tableCols[i], out var v)) fullRow[i] = v;

                table.AddRow(fullRow);
                rows++;

                // ── AFTER INSERT ──
                FireTriggers(tname, "AFTER", "INSERT", null, BuildRowDict(tableCols, fullRow), tableCols);

                Match(","); if (Cur.Value != "(") break;
            }
            return rows;
        }

        // ============================================================
        //  UPDATE
        // ============================================================
        private int DoUpdate()
        {
            Expect("UPDATE");
            string tname = Consume().Value;
            var table = RequireTable(tname);
            Expect("SET");

            var assignments = new List<(string Col, List<SqlToken> Expr)>();
            do
            {
                string col = Consume().Value;
                Expect("=");
                var expr = ReadExprUntilCommaOrClause();
                assignments.Add((col, expr));
            } while (Match(","));

            var whereTokens = Match("WHERE") ? ReadUntilClause() : null;

            int affected = 0;
            string[] cols = table.Columns;

            for (int r = 0; r < table.Count; r++)
            {
                var rowDict = RowDict(table, r, tname, cols);
                if (!PassesWhere(whereTokens, rowDict)) continue;

                // Snapshot OLD row (unqualified column keys only)
                var oldRow = cols.ToDictionary(c => c, c => table.GetValue(r, c), StringComparer.OrdinalIgnoreCase);

                // Compute intended NEW values from assignment expressions
                var newRow = new Dictionary<string, object?>(oldRow, StringComparer.OrdinalIgnoreCase);
                foreach (var (col, expr) in assignments)
                {
                    var ctx = MakeCtx(rowDict);
                    int ep = 0; var exprCopy = expr.ToList();
                    newRow[col] = SqlExpr.Eval(exprCopy, ref ep, ctx);
                }

                // ── BEFORE UPDATE ──
                FireTriggers(tname, "BEFORE", "UPDATE", oldRow, newRow, cols);

                // Write the (possibly trigger-modified) new values to the table
                foreach (var col in cols)
                    if (newRow.TryGetValue(col, out var v)) table.SetValue(r, col, v);

                // ── AFTER UPDATE ── (re-snapshot actual row after write)
                var actualNew = cols.ToDictionary(c => c, c => table.GetValue(r, c), StringComparer.OrdinalIgnoreCase);
                FireTriggers(tname, "AFTER", "UPDATE", oldRow, actualNew, cols);

                affected++;
            }
            return affected;
        }

        // ============================================================
        //  DELETE
        // ============================================================
        private int DoDelete()
        {
            Expect("DELETE"); Expect("FROM");
            string tname = Consume().Value;
            var table = RequireTable(tname);
            var whereTokens = Match("WHERE") ? ReadUntilClause() : null;

            string[] cols = table.Columns;
            int affected = 0;
            for (int r = table.Count - 1; r >= 0; r--)
            {
                if (!PassesWhere(whereTokens, RowDict(table, r, tname, cols))) continue;

                // Snapshot OLD row before deletion
                var oldRow = cols.ToDictionary(c => c, c => table.GetValue(r, c), StringComparer.OrdinalIgnoreCase);

                // ── BEFORE DELETE ──
                FireTriggers(tname, "BEFORE", "DELETE", oldRow, null, cols);

                table.DeleteRow(r);

                // ── AFTER DELETE ──
                FireTriggers(tname, "AFTER", "DELETE", oldRow, null, cols);

                affected++;
            }
            return affected;
        }

        // ============================================================
        //  CREATE
        // ============================================================
        private int DoCreate()
        {
            Expect("CREATE");
            string what = Consume().Value.ToUpperInvariant();
            if (what == "TABLE") return CreateTable();
            if (what == "FUNCTION") return CreateFunction();
            if (what == "TRIGGER") return CreateTrigger();
            throw new InvalidOperationException($"Unsupported CREATE {what}.");
        }

        private int CreateTable()
        {
            string tname = Consume().Value;
            var colDefs = new List<(string Name, Type Type)>();

            if (Cur.Value == "(")
            {
                _p++;
                while (Cur.Value != ")")
                {
                    // Skip inline table constraints
                    if (IsIn(Cur.Value, "PRIMARY", "UNIQUE", "FOREIGN", "CONSTRAINT", "INDEX", "KEY"))
                    { while (Cur.Value != "," && Cur.Value != ")") Consume(); Match(","); continue; }

                    string colName = Consume().Value;
                    string typeName = Cur.Type == SqlTokenType.EOF ? "TEXT" : Consume().Value;

                    // precision/scale e.g. VARCHAR(255) or DECIMAL(10,2)
                    if (Cur.Value == "(") { while (Cur.Value != ")") Consume(); _p++; }

                    // Skip per-column constraints (IDENTITY, NOT NULL, DEFAULT, PRIMARY KEY, UNIQUE, NULL, AUTO_INCREMENT)
                    while (Cur.Value != "," && Cur.Value != ")" && !IsConstraintStart())
                        Consume();
                    if (IsConstraintStart())
                        while (Cur.Value != "," && Cur.Value != ")") Consume();

                    colDefs.Add((colName, SqlTypeToClr(typeName)));
                    Match(",");
                }
                _p++; // )
            }

            var table = new SQLTable(tname, [.. colDefs.Select(c => c.Name)]);
            foreach (var (cn, ct) in colDefs) table.SetColumnType(cn, ct);
            _tables[tname] = table;
            return 0;
        }

        private bool IsConstraintStart() =>
            IsIn(Cur.Value, "PRIMARY", "UNIQUE", "FOREIGN", "CONSTRAINT", "REFERENCES", "DEFAULT",
                 "NOT", "NULL", "IDENTITY", "AUTO_INCREMENT", "CHECK");

        private int CreateFunction()
        {
            string fname = Consume().Value;
            var parms = new List<(string Name, Type Type)>();

            if (Cur.Value == "(")
            {
                _p++;
                while (Cur.Value != ")")
                {
                    string pname = Consume().Value; // @param
                    string ptype = Consume().Value;
                    parms.Add((pname, SqlTypeToClr(ptype)));
                    Match(",");
                }
                _p++;
            }

            Type retType = typeof(object);
            if (Match("RETURNS")) retType = SqlTypeToClr(Consume().Value);
            Match("AS");
            Match("BEGIN");

            // Collect body tokens up to END
            var body = new List<SqlToken>();
            while (Cur.Type != SqlTokenType.EOF && !Cur.Value.Equals("END", StringComparison.OrdinalIgnoreCase))
                body.Add(Consume());
            Match("END");

            // Find RETURN statement
            int ri = body.FindIndex(x => x.Value.Equals("RETURN", StringComparison.OrdinalIgnoreCase));
            List<SqlToken>? retExpr = null;
            if (ri >= 0)
            {
                retExpr = [.. body.Skip(ri + 1).TakeWhile(x => x.Value != ";")];
                retExpr.Add(new SqlToken(SqlTokenType.EOF, ""));
            }

            var capturedParms = parms.ToList();
            var capturedFuncs = _functions;
            var capturedTables = _tables;
            var capturedAddIns = _addIns;

            var fn = new SqlFunction { Name = fname, Parameters = parms, ReturnType = retType };
            if (retExpr != null)
            {
                fn.CompiledFunc = args =>
                {
                    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    for (int i = 0; i < capturedParms.Count && i < args.Length; i++)
                        row[capturedParms[i].Name] = args[i];
                    int ep = 0;
                    var exprCopy = retExpr.ToList();
                    return SqlExpr.Eval(exprCopy, ref ep, new EvalContext { Row = row, Functions = capturedFuncs, Tables = capturedTables, AddIns = capturedAddIns });
                };
            }
            _functions[fname] = fn;
            return 0;
        }

        // ============================================================
        //  CREATE TRIGGER
        //
        //  Syntax supported:
        //    CREATE TRIGGER name
        //      BEFORE | AFTER  INSERT | UPDATE | DELETE  ON table
        //      [FOR EACH ROW]
        //      BEGIN
        //        [SET NEW.col = expr ;]
        //        [IF cond THEN ... [ELSEIF cond THEN ...] [ELSE ...] END IF ;]
        //        [<any DML with NEW.col / OLD.col references> ;]
        //      END
        // ============================================================
        private int CreateTrigger()
        {
            string trName = Consume().Value;
            string timing = Consume().Value.ToUpperInvariant(); // BEFORE | AFTER
            string @event = Consume().Value.ToUpperInvariant(); // INSERT | UPDATE | DELETE
            Expect("ON");
            string tableName = Consume().Value;
            Match("FOR"); Match("EACH"); Match("ROW");
            Match("AS");
            Match("BEGIN");

            // Collect all tokens between BEGIN and the closing END
            var bodyTokens = new List<SqlToken>();
            int depth = 1; // track nested BEGIN/END (e.g. inside IF blocks there are none here, but be safe)
            while (Cur.Type != SqlTokenType.EOF)
            {
                if (Cur.Value.Equals("BEGIN", StringComparison.OrdinalIgnoreCase)) depth++;
                if (Cur.Value.Equals("END", StringComparison.OrdinalIgnoreCase))
                {
                    depth--;
                    if (depth == 0) { Consume(); break; }
                }
                bodyTokens.Add(Consume());
            }
            Match(";"); // optional trailing semicolon after END

            // Pre-parse body into structured statement list
            int bp = 0;
            var statements = ParseTriggerStatements(bodyTokens, ref bp);

            _triggers[trName] = new SqlTrigger
            {
                Name = trName,
                TableName = tableName,
                Timing = timing,
                Event = @event,
                Statements = statements
                // OriginalSql is set by SQLDatabase after execution
            };
            return 0;
        }

        // ── Trigger body pre-parser ───────────────────────────────────────

        private static bool IsNewOldRef(List<SqlToken> t, int pos) =>
            pos + 2 < t.Count &&
            (t[pos].Value.Equals("NEW", StringComparison.OrdinalIgnoreCase) ||
             t[pos].Value.Equals("OLD", StringComparison.OrdinalIgnoreCase)) &&
            t[pos + 1].Value == "." &&
            t[pos + 2].Type == SqlTokenType.Identifier;

        private static List<SqlToken> ReadTriggerTokensUntilSemi(List<SqlToken> t, ref int pos)
        {
            var result = new List<SqlToken>();
            int depth = 0;
            while (pos < t.Count && t[pos].Type != SqlTokenType.EOF)
            {
                string v = t[pos].Value;
                if (v == "(") depth++;
                else if (v == ")") depth--;
                else if (v == ";" && depth == 0) { pos++; break; }
                else if (depth == 0 &&
                         (v.Equals("END", StringComparison.OrdinalIgnoreCase) ||
                          v.Equals("ELSEIF", StringComparison.OrdinalIgnoreCase) ||
                          v.Equals("ELSE", StringComparison.OrdinalIgnoreCase))) break;
                result.Add(t[pos++]);
            }
            return result;
        }

        private static List<SqlToken> ReadTriggerTokensUntilKeyword(List<SqlToken> t, ref int pos, string keyword)
        {
            var result = new List<SqlToken>();
            while (pos < t.Count &&
                   !t[pos].Value.Equals(keyword, StringComparison.OrdinalIgnoreCase) &&
                   t[pos].Type != SqlTokenType.EOF)
                result.Add(t[pos++]);
            return result;
        }

        private List<TriggerStmt> ParseTriggerStatements(List<SqlToken> t, ref int pos)
        {
            var stmts = new List<TriggerStmt>();
            while (pos < t.Count && t[pos].Type != SqlTokenType.EOF)
            {
                // Skip semicolons
                while (pos < t.Count && t[pos].Value == ";") pos++;
                if (pos >= t.Count || t[pos].Type == SqlTokenType.EOF) break;

                string v = t[pos].Value.ToUpperInvariant();

                // Stop at block terminators
                if (v == "END" || v == "ELSEIF" || v == "ELSE") break;

                if (v == "IF")
                {
                    pos++; // consume IF
                    stmts.Add(ParseIfTriggerStmt(t, ref pos));
                }
                else if (v == "SET" && IsNewOldRef(t, pos + 1))
                {
                    pos++; // consume SET
                    bool isNew = t[pos].Value.Equals("NEW", StringComparison.OrdinalIgnoreCase);
                    pos++; // consume NEW/OLD
                    pos++; // consume .
                    string col = t[pos++].Value;
                    pos++; // consume =
                    var expr = ReadTriggerTokensUntilSemi(t, ref pos);
                    stmts.Add(new SetNewOldStmt { IsNew = isNew, Column = col, Expr = expr });
                }
                else
                {
                    var dml = ReadTriggerTokensUntilSemi(t, ref pos);
                    if (dml.Count > 0)
                        stmts.Add(new DmlTriggerStmt { Tokens = dml });
                }
            }
            return stmts;
        }

        private IfTriggerStmt ParseIfTriggerStmt(List<SqlToken> t, ref int pos)
        {
            var ifStmt = new IfTriggerStmt();

            // First IF branch
            var cond = ReadTriggerTokensUntilKeyword(t, ref pos, "THEN");
            if (pos < t.Count) pos++; // consume THEN
            var body = ParseTriggerStatements(t, ref pos);
            ifStmt.Branches.Add((cond, body));

            // ELSEIF branches
            while (pos < t.Count && t[pos].Value.Equals("ELSEIF", StringComparison.OrdinalIgnoreCase))
            {
                pos++; // consume ELSEIF
                cond = ReadTriggerTokensUntilKeyword(t, ref pos, "THEN");
                if (pos < t.Count) pos++; // consume THEN
                body = ParseTriggerStatements(t, ref pos);
                ifStmt.Branches.Add((cond, body));
            }

            // ELSE
            if (pos < t.Count && t[pos].Value.Equals("ELSE", StringComparison.OrdinalIgnoreCase))
            {
                pos++; // consume ELSE
                ifStmt.ElseBranch = ParseTriggerStatements(t, ref pos);
            }

            // Consume END IF (or just END)
            while (pos < t.Count && t[pos].Value == ";") pos++;
            if (pos < t.Count && t[pos].Value.Equals("END", StringComparison.OrdinalIgnoreCase)) pos++;
            while (pos < t.Count && t[pos].Value == ";") pos++;
            if (pos < t.Count && t[pos].Value.Equals("IF", StringComparison.OrdinalIgnoreCase)) pos++;
            while (pos < t.Count && t[pos].Value == ";") pos++;

            return ifStmt;
        }

        // ── Trigger firing ────────────────────────────────────────────────

        /// <summary>
        /// Fires all matching triggers for the given table/timing/event.
        /// For BEFORE INSERT/UPDATE, modifications to <paramref name="newRow"/> are applied back.
        /// </summary>
        private void FireTriggers(string tableName, string timing, string @event,
                                  Dictionary<string, object?>? oldRow,
                                  Dictionary<string, object?>? newRow,
                                  string[] tableCols)
        {
            if (_triggers.Count == 0) return;

            // Build the trigger evaluation context:  NEW.col  OLD.col  and bare col keys
            var tctx = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            if (newRow != null) foreach (var kv in newRow) { tctx[$"NEW.{kv.Key}"] = kv.Value; tctx[kv.Key] = kv.Value; }
            if (oldRow != null) foreach (var kv in oldRow) tctx[$"OLD.{kv.Key}"] = kv.Value;

            foreach (var trigger in _triggers.Values)
            {
                if (!trigger.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)) continue;
                if (!trigger.Timing.Equals(timing, StringComparison.OrdinalIgnoreCase)) continue;
                if (!trigger.Event.Equals(@event, StringComparison.OrdinalIgnoreCase)) continue;

                ExecuteTriggerStmts(trigger.Statements, tctx);
            }

            // Push any NEW.col modifications made by BEFORE triggers back into newRow
            if (newRow != null && timing.Equals("BEFORE", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var col in tableCols)
                    if (tctx.TryGetValue($"NEW.{col}", out var v)) newRow[col] = v;
            }
        }

        private void ExecuteTriggerStmts(List<TriggerStmt> stmts, Dictionary<string, object?> tctx)
        {
            foreach (var stmt in stmts)
            {
                switch (stmt)
                {
                    case SetNewOldStmt s:
                        {
                            var exprCopy = s.Expr.ToList();
                            exprCopy.Add(new SqlToken(SqlTokenType.EOF, ""));
                            int ep = 0;
                            var evalCtx = new EvalContext { Row = tctx, Functions = _functions, Tables = _tables, AddIns = _addIns };
                            object? val = SqlExpr.Eval(exprCopy, ref ep, evalCtx);
                            string prefix = s.IsNew ? "NEW" : "OLD";
                            tctx[$"{prefix}.{s.Column}"] = val;
                            // Also update the bare key so expressions referencing it directly work
                            if (s.IsNew) tctx[s.Column] = val;
                            break;
                        }

                    case IfTriggerStmt ifs:
                        ExecuteIfTriggerStmt(ifs, tctx);
                        break;

                    case DmlTriggerStmt d:
                        {
                            // Substitute NEW.col / OLD.col tokens with their current runtime values
                            var resolved = SubstituteNewOld([.. d.Tokens], tctx);
                            resolved.Add(new SqlToken(SqlTokenType.EOF, ""));
                            try { new SqlExecutor(resolved, _tables, _functions, _triggers, _addIns).RunNonQuery(); }
                            catch { /* Trigger DML errors do not abort the outer statement */ }
                            break;
                        }
                }
            }
        }

        private void ExecuteIfTriggerStmt(IfTriggerStmt ifs, Dictionary<string, object?> tctx)
        {
            foreach (var (cond, body) in ifs.Branches)
            {
                var condCopy = cond.ToList();
                condCopy.Add(new SqlToken(SqlTokenType.EOF, ""));
                int ep = 0;
                var evalCtx = new EvalContext { Row = tctx, Functions = _functions, Tables = _tables, AddIns = _addIns };
                if (SqlExpr.AsBool(SqlExpr.Eval(condCopy, ref ep, evalCtx)))
                {
                    ExecuteTriggerStmts(body, tctx);
                    return;
                }
            }
            if (ifs.ElseBranch != null)
                ExecuteTriggerStmts(ifs.ElseBranch, tctx);
        }

        /// <summary>Replace NEW.col / OLD.col token triples with literal value tokens.</summary>
        private static List<SqlToken> SubstituteNewOld(List<SqlToken> tokens, Dictionary<string, object?> tctx)
        {
            var result = new List<SqlToken>(tokens.Count);
            int i = 0;
            while (i < tokens.Count)
            {
                if (i + 2 < tokens.Count &&
                    (tokens[i].Value.Equals("NEW", StringComparison.OrdinalIgnoreCase) ||
                     tokens[i].Value.Equals("OLD", StringComparison.OrdinalIgnoreCase)) &&
                    tokens[i + 1].Value == "." &&
                    tokens[i + 2].Type == SqlTokenType.Identifier)
                {
                    string key = $"{tokens[i].Value}.{tokens[i + 2].Value}";
                    tctx.TryGetValue(key, out var v);
                    result.Add(ObjectToToken(v));
                    i += 3;
                }
                else result.Add(tokens[i++]);
            }
            return result;
        }

        private static SqlToken ObjectToToken(object? val) => val switch
        {
            null => new SqlToken(SqlTokenType.Keyword, "NULL"),
            bool b => new SqlToken(SqlTokenType.Keyword, b ? "TRUE" : "FALSE"),
            long l => new SqlToken(SqlTokenType.Number, l.ToString()),
            int i => new SqlToken(SqlTokenType.Number, i.ToString()),
            double d => new SqlToken(SqlTokenType.Number, d.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            float f => new SqlToken(SqlTokenType.Number, f.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            DateTime dt => new SqlToken(SqlTokenType.StringLiteral, dt.ToString("O")),
            _ => new SqlToken(SqlTokenType.StringLiteral, val.ToString())
        };

        // ── Row dict helper used by trigger fire points ───────────────────

        private static Dictionary<string, object?> BuildRowDict(string[] cols, object?[] values)
        {
            var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < cols.Length && i < values.Length; i++) d[cols[i]] = values[i];
            return d;
        }
        private int DoAlter()
        {
            Expect("ALTER"); Expect("TABLE");
            string tname = Consume().Value;
            var table = RequireTable(tname);
            string action = Consume().Value.ToUpperInvariant();

            if (action == "ADD")
            {
                Match("COLUMN");
                string cn = Consume().Value;
                string tp = Consume().Value;
                // precision
                if (Cur.Value == "(") { while (Cur.Value != ")") Consume(); _p++; }
                table.AddColumn(cn, SqlTypeToClr(tp));
            }
            else if (action == "DROP")
            {
                Match("COLUMN");
                table.DropColumn(Consume().Value);
            }
            else if (action == "RENAME")
            {
                if (Match("TO") || Match("COLUMN")) { /* handled below */ }
                // We silently ignore unsupported ALTER variants
            }
            return 0;
        }

        // ============================================================
        //  DROP
        // ============================================================
        private int DoDrop()
        {
            Expect("DROP");
            string what = Consume().Value.ToUpperInvariant();

            if (what == "TABLE")
            {
                bool ifExists = false;
                if (Match("IF")) { Expect("EXISTS"); ifExists = true; }
                string tname = Consume().Value;
                if (!_tables.Remove(tname) && !ifExists)
                    throw new KeyNotFoundException($"Table '{tname}' not found.");
            }
            else if (what == "FUNCTION")
            {
                string fname = Consume().Value;
                _functions.Remove(fname);
            }
            else if (what == "TRIGGER")
            {
                bool ifExists = false;
                if (Match("IF")) { Expect("EXISTS"); ifExists = true; }
                string trname = Consume().Value;
                if (!_triggers.Remove(trname) && !ifExists)
                    throw new KeyNotFoundException($"Trigger '{trname}' not found.");
            }
            return 0;
        }

        // ============================================================
        //  SELECT
        // ============================================================
        // Parsed SELECT clauses — value type keeps allocation minimal
        private record struct SelectClauses(
            List<SelectItem> Items,
            List<(string Name, string Alias)> FromTables,
            List<(string Type, string Name, string Alias, List<SqlToken>? On)> Joins,
            List<SqlToken>? Where,
            List<string>? GroupBy,
            List<SqlToken>? Having,
            List<(List<SqlToken> Expr, bool Asc)>? OrderBy,
            bool Distinct, int? Limit, int? Offset);

        /// <summary>
        /// Parses all SELECT clauses from the current token stream.
        /// Pure parse — no row evaluation. Called by both the sync and async execution paths.
        /// </summary>
        private SelectClauses ParseSelectClauses()
        {
            Expect("SELECT");
            bool distinct = Match("DISTINCT");

            int? topN = null;
            if (Match("TOP"))
            {
                var ctx0 = MakeCtx([]);
                topN = (int)SqlExpr.ToNum(SqlExpr.Eval(_t, ref _p, ctx0));
            }

            var selectItems = ParseSelectList();

            var fromTables = new List<(string Name, string Alias)>();
            var joins = new List<(string Type, string Name, string Alias, List<SqlToken>? On)>();

            if (Match("FROM"))
            {
                do
                {
                    string tn = Consume().Value;
                    string ta = tn;
                    Match("AS");
                    if (Cur.Type == SqlTokenType.Identifier && !IsClause(Cur.Value) && !IsJoin(Cur.Value))
                        ta = Consume().Value;
                    fromTables.Add((tn, ta));
                } while (Match(","));

                while (IsJoin(Cur.Value))
                {
                    string jtype = "INNER";
                    if (IsIn(Cur.Value, "LEFT", "RIGHT", "FULL", "CROSS", "INNER"))
                    { jtype = Consume().Value.ToUpperInvariant(); Match("OUTER"); }
                    Expect("JOIN");
                    string jn = Consume().Value, ja = jn;
                    Match("AS");
                    if (Cur.Type == SqlTokenType.Identifier && !IsClause(Cur.Value)) ja = Consume().Value;
                    List<SqlToken>? on = null;
                    if (Match("ON")) on = ReadUntilClause();
                    joins.Add((jtype, jn, ja, on));
                }
            }

            var where = Match("WHERE") ? ReadUntilClause() : null;

            List<string>? groupBy = null;
            if (Match("GROUP")) { Expect("BY"); groupBy = ParseCommaSeparatedExprs(); }

            var having = Match("HAVING") ? ReadUntilClause() : null;

            List<(List<SqlToken> Expr, bool Asc)>? orderBy = null;
            if (Match("ORDER"))
            {
                Expect("BY");
                orderBy = [];
                do
                {
                    var ob = ReadOrderExpr();
                    bool asc = true;
                    if (Match("DESC")) asc = false; else Match("ASC");
                    orderBy.Add((ob, asc));
                } while (Match(","));
            }

            int? limitN = topN;
            if (Match("LIMIT"))
            { var ctx0 = MakeCtx([]); limitN = (int)SqlExpr.ToNum(SqlExpr.Eval(_t, ref _p, ctx0)); }

            int? offsetN = null;
            if (Match("OFFSET"))
            { var ctx0 = MakeCtx([]); offsetN = (int)SqlExpr.ToNum(SqlExpr.Eval(_t, ref _p, ctx0)); }

            return new SelectClauses(selectItems, fromTables, joins, where, groupBy, having, orderBy, distinct, limitN, offsetN);
        }

        private object?[,] DoSelect()
        {
            var c = ParseSelectClauses();
            return ExecSelect(c.Items, c.FromTables, c.Joins, c.Where, c.GroupBy,
                              c.Having, c.OrderBy, c.Distinct, c.Limit, c.Offset);
        }

        private (string[] Headers, object?[][] Rows) DoSelectWithHeaders(CancellationToken ct)
        {
            var c = ParseSelectClauses();
            return ExecSelectWithHeaders(c.Items, c.FromTables, c.Joins, c.Where, c.GroupBy,
                                         c.Having, c.OrderBy, c.Distinct, c.Limit, c.Offset, ct);
        }

        private object?[,] ExecSelect(
            List<SelectItem> sel,
            List<(string Name, string Alias)> froms,
            List<(string Type, string Name, string Alias, List<SqlToken>? On)> joins,
            List<SqlToken>? where,
            List<string>? groupBy,
            List<SqlToken>? having,
            List<(List<SqlToken> Expr, bool Asc)>? orderBy,
            bool distinct, int? limit, int? offset)
        {
            // ── Build row set ─────────────────────────────────────────────
            List<Dictionary<string, object?>> rows;

            if (froms.Count == 0)
            {
                rows = [new(StringComparer.OrdinalIgnoreCase)];
            }
            else
            {
                var (fn, fa) = froms[0];
                rows = TableRows(RequireTable(fn), fa);

                for (int i = 1; i < froms.Count; i++)
                {
                    var (tn, ta) = froms[i];
                    rows = CrossJoin(rows, TableRows(RequireTable(tn), ta));
                }

                foreach (var (jt, jn, ja, on) in joins)
                    rows = ApplyJoin(rows, TableRows(RequireTable(jn), ja), jt, on);
            }

            // ── WHERE ─────────────────────────────────────────────────────
            if (where != null)
                rows = [.. rows.Where(r => PassesWhere(where, r))];

            // ── GROUP BY / aggregates ─────────────────────────────────────
            bool hasAgg = sel.Any(s => s.IsAggregate);
            if (hasAgg || groupBy != null)
                rows = ApplyGroupBy(rows, sel, groupBy, having);
            else if (having != null)
                rows = [.. rows.Where(r => PassesWhere(having, r))];

            // ── ORDER BY ──────────────────────────────────────────────────
            if (orderBy != null && orderBy.Count > 0)
                rows = ApplyOrderBy(rows, orderBy);

            // ── OFFSET / LIMIT ────────────────────────────────────────────
            if (offset.HasValue) rows = [.. rows.Skip(offset.Value)];
            if (limit.HasValue) rows = [.. rows.Take(limit.Value)];

            // ── DISTINCT ──────────────────────────────────────────────────
            if (distinct) rows = ApplyDistinct(rows, sel);

            return BuildResult(rows, sel, froms);
        }

        /// <summary>
        /// Async-path counterpart to <see cref="ExecSelect"/>. Runs the same pipeline —
        /// row set build, WHERE, GROUP BY, ORDER BY, LIMIT — then calls
        /// <see cref="ResolveHeaders"/> and <see cref="BuildRows"/> (with cancellation)
        /// instead of <see cref="BuildResult"/>, returning headers and data separately.
        /// </summary>
        private (string[] Headers, object?[][] Rows) ExecSelectWithHeaders(
            List<SelectItem> sel,
            List<(string Name, string Alias)> froms,
            List<(string Type, string Name, string Alias, List<SqlToken>? On)> joins,
            List<SqlToken>? where,
            List<string>? groupBy,
            List<SqlToken>? having,
            List<(List<SqlToken> Expr, bool Asc)>? orderBy,
            bool distinct, int? limit, int? offset,
            CancellationToken ct)
        {
            // ── Build row set (identical to ExecSelect) ───────────────────
            List<Dictionary<string, object?>> rows;

            if (froms.Count == 0)
            {
                rows = [new(StringComparer.OrdinalIgnoreCase)];
            }
            else
            {
                var (fn, fa) = froms[0];
                rows = TableRows(RequireTable(fn), fa);
                for (int i = 1; i < froms.Count; i++)
                {
                    var (tn, ta) = froms[i];
                    rows = CrossJoin(rows, TableRows(RequireTable(tn), ta));
                }
                foreach (var (jt, jn, ja, on) in joins)
                    rows = ApplyJoin(rows, TableRows(RequireTable(jn), ja), jt, on);
            }

            if (where != null)
                rows = [.. rows.Where(r => PassesWhere(where, r))];

            bool hasAgg = sel.Any(s => s.IsAggregate);
            if (hasAgg || groupBy != null)
                rows = ApplyGroupBy(rows, sel, groupBy, having);
            else if (having != null)
                rows = [.. rows.Where(r => PassesWhere(having, r))];

            if (orderBy != null && orderBy.Count > 0)
                rows = ApplyOrderBy(rows, orderBy);

            if (offset.HasValue) rows = [.. rows.Skip(offset.Value)];
            if (limit.HasValue) rows = [.. rows.Take(limit.Value)];
            if (distinct) rows = ApplyDistinct(rows, sel);

            // ── Resolve headers then evaluate rows with cancellation ──────
            var (headers, finalItems) = ResolveHeaders(rows, sel, froms);
            var dataRows = BuildRows(rows, finalItems, ct);
            return (headers, dataRows);
        }

        // ── Join helpers ─────────────────────────────────────────────────

        private static List<Dictionary<string, object?>> TableRows(SQLTable table, string alias)
        {
            var cols = table.Columns;
            var result = new List<Dictionary<string, object?>>(table.Count);
            for (int r = 0; r < table.Count; r++)
            {
                var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                foreach (var c in cols)
                {
                    object? v = table.GetValue(r, c);
                    d[$"{alias}.{c}"] = v;
                    if (!d.ContainsKey(c)) d[c] = v; // unqualified wins for first table
                }
                result.Add(d);
            }
            return result;
        }

        private static List<Dictionary<string, object?>> CrossJoin(
            List<Dictionary<string, object?>> left, List<Dictionary<string, object?>> right)
        {
            var res = new List<Dictionary<string, object?>>(left.Count * right.Count);
            foreach (var l in left)
                foreach (var r in right)
                {
                    var m = new Dictionary<string, object?>(l, StringComparer.OrdinalIgnoreCase);
                    foreach (var kv in r) m[kv.Key] = kv.Value;
                    res.Add(m);
                }
            return res;
        }

        private List<Dictionary<string, object?>> ApplyJoin(
            List<Dictionary<string, object?>> left,
            List<Dictionary<string, object?>> right,
            string jtype, List<SqlToken>? cond)
        {
            var res = new List<Dictionary<string, object?>>();
            var matchedRight = new HashSet<int>();

            for (int l = 0; l < left.Count; l++)
            {
                bool matched = false;
                for (int r = 0; r < right.Count; r++)
                {
                    var m = new Dictionary<string, object?>(left[l], StringComparer.OrdinalIgnoreCase);
                    foreach (var kv in right[r]) m[kv.Key] = kv.Value;

                    bool pass = cond == null || PassesWhere(cond, m);
                    if (pass) { res.Add(m); matched = true; matchedRight.Add(r); }
                }

                if (!matched && (jtype == "LEFT" || jtype == "FULL"))
                {
                    var nul = new Dictionary<string, object?>(left[l], StringComparer.OrdinalIgnoreCase);
                    if (right.Count > 0) foreach (var k in right[0].Keys) nul[k] = null;
                    res.Add(nul);
                }
            }

            if (jtype == "RIGHT" || jtype == "FULL")
            {
                for (int r = 0; r < right.Count; r++)
                {
                    if (matchedRight.Contains(r)) continue;
                    var nul = new Dictionary<string, object?>(right[r], StringComparer.OrdinalIgnoreCase);
                    if (left.Count > 0) foreach (var k in left[0].Keys) if (!nul.ContainsKey(k)) nul[k] = null;
                    res.Add(nul);
                }
            }

            return res;
        }

        // ── GROUP BY ─────────────────────────────────────────────────────

        private List<Dictionary<string, object?>> ApplyGroupBy(
            List<Dictionary<string, object?>> rows,
            List<SelectItem> sel,
            List<string>? groupByCols,
            List<SqlToken>? having)
        {
            Func<Dictionary<string, object?>, string> key;
            if (groupByCols == null || groupByCols.Count == 0)
                key = _ => "";
            else
                key = row => string.Join("\x00", groupByCols.Select(gc =>
                {
                    var toks = SqlTokenizer.Tokenize(gc);
                    int ep = 0;
                    return SqlExpr.Eval(toks, ref ep, MakeCtx(row))?.ToString() ?? "NULL";
                }));

            var res = new List<Dictionary<string, object?>>();
            foreach (var grp in rows.GroupBy(key))
            {
                var grpList = grp.ToList();
                var outRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

                // Compute each select item over group
                foreach (var item in sel)
                {
                    if (item.IsStar) continue;
                    object? val = item.IsAggregate
                        ? ComputeAggregate(item.AggFunc, item.AggCol, grpList, item.AggDistinct)
                        : EvalExpr(item.ExprTokens, grpList[0]);
                    outRow[item.OutputName] = val;

                    // Also store under the bare function-expression key so HAVING
                    // can resolve COUNT(*), SUM(col) etc. regardless of alias.
                    if (item.IsAggregate)
                    {
                        string exprKey = $"{item.AggFunc}({(item.AggDistinct ? "DISTINCT " : "")}{item.AggCol})";
                        if (!outRow.ContainsKey(exprKey)) outRow[exprKey] = val;
                        // Also bare FUNC(col) without spacing variants
                        string exprKeyUpper = exprKey.ToUpperInvariant();
                        if (!outRow.ContainsKey(exprKeyUpper)) outRow[exprKeyUpper] = val;
                    }
                }

                // Also compute non-aggregate columns for ORDER BY / HAVING lookup
                if (groupByCols != null)
                    foreach (var gc in groupByCols)
                    {
                        if (!outRow.ContainsKey(gc))
                            outRow[gc] = EvalExpr(SqlTokenizer.Tokenize(gc), grpList[0]);
                    }

                if (having != null && !PassesWhere(having, outRow)) continue;
                res.Add(outRow);
            }
            return res;
        }

        private object? ComputeAggregate(string? func, string? col, List<Dictionary<string, object?>> rows, bool distinct)
        {
            func ??= string.Empty;
            object? get(Dictionary<string, object?> row)
            {
                if (col == "*") return 1L;
                var toks = SqlTokenizer.Tokenize(col);
                int ep = 0;
                return SqlExpr.Eval(toks, ref ep, MakeCtx(row));
            }

            var vals = rows.Select(get).ToList();
            if (distinct) vals = [.. vals.Distinct(new ObjEqualityComparer())];

            return func.ToUpperInvariant() switch
            {
                "COUNT" => (long)(col == "*" ? rows.Count : vals.Count(v => v != null)),
                "SUM" => vals.Where(v => v != null).Select(SqlExpr.ToNum).DefaultIfEmpty(0).Sum(),
                "AVG" => vals.Where(v => v != null).Select(SqlExpr.ToNum).DefaultIfEmpty(0).Average(),
                "MIN" => vals.Where(v => v != null).OrderBy(v => v, ObjectComparer.Instance).FirstOrDefault(),
                "MAX" => vals.Where(v => v != null).OrderByDescending(v => v, ObjectComparer.Instance).FirstOrDefault(),
                _ => null
            };
        }

        private class ObjEqualityComparer : IEqualityComparer<object?>
        {
            public new bool Equals(object? a, object? b) => SqlExpr.ObjEq(a, b);
            public int GetHashCode(object o) => o?.ToString()?.ToUpperInvariant()?.GetHashCode() ?? 0;
        }

        // ── ORDER BY ─────────────────────────────────────────────────────

        private List<Dictionary<string, object?>> ApplyOrderBy(
            List<Dictionary<string, object?>> rows,
            List<(List<SqlToken> Expr, bool Asc)> orderBy)
        {
            if (rows.Count == 0) return rows;
            IOrderedEnumerable<Dictionary<string, object?>>? ordered = null;

            for (int i = 0; i < orderBy.Count; i++)
            {
                var exprToks = orderBy[i].Expr.ToList();
                exprToks.Add(new SqlToken(SqlTokenType.EOF, ""));
                bool asc = orderBy[i].Asc;

                object? kf(Dictionary<string, object?> row) { int ep = 0; return SqlExpr.Eval(exprToks, ref ep, MakeCtx(row)); }

                ordered = i == 0
                    ? (asc ? rows.OrderBy(kf, ObjectComparer.Instance) : rows.OrderByDescending(kf, ObjectComparer.Instance))
                    : (asc ? ordered!.ThenBy(kf, ObjectComparer.Instance) : ordered!.ThenByDescending(kf, ObjectComparer.Instance));
            }

            return ordered?.ToList() ?? rows;
        }

        // ── DISTINCT ─────────────────────────────────────────────────────

        private static List<Dictionary<string, object?>> ApplyDistinct(List<Dictionary<string, object?>> rows, List<SelectItem> sel)
        {
            var seen = new HashSet<string>();
            var res = new List<Dictionary<string, object?>>();
            foreach (var row in rows)
            {
                var k = string.Join("\x00", sel.Where(s => !s.IsStar)
                    .Select(s => row.TryGetValue(s.OutputName, out var v) ? v?.ToString() ?? "NULL" : "NULL"));
                if (seen.Add(k)) res.Add(row);
            }
            return res;
        }

        // ── Result building ──────────────────────────────────────────────

        private object?[,] BuildResult(List<Dictionary<string, object?>> rows, List<SelectItem> sel,
                                       List<(string Name, string Alias)>? froms = null)
        {
            var (headers, finalItems) = ResolveHeaders(rows, sel, froms);
            var dataRows = BuildRows(rows, finalItems, CancellationToken.None);
            int numCols = finalItems.Count;
            var result = new object?[dataRows.Length + 1, numCols];
            for (int c = 0; c < numCols; c++)
                result[0, c] = headers[c];
            for (int r = 0; r < dataRows.Length; r++)
                for (int c = 0; c < numCols; c++)
                    result[r + 1, c] = dataRows[r][c];
            return result;
        }

        /// <summary>
        /// Resolves the final SELECT column list, expanding <c>*</c> and <c>table.*</c>.
        /// Returns both the header name array and the resolved <see cref="SelectItem"/> list.
        /// Called by both the sync and async paths so star-expansion logic lives in one place.
        /// </summary>
        private (string[] Headers, List<SelectItem> FinalItems) ResolveHeaders(
            List<Dictionary<string, object?>> rows,
            List<SelectItem> sel,
            List<(string Name, string Alias)>? froms)
        {
            var finalItems = new List<SelectItem>();
            foreach (var item in sel)
            {
                if (!item.IsStar) { finalItems.Add(item); continue; }

                if (rows.Count == 0)
                {
                    if (froms != null)
                    {
                        var seenEmpty = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        foreach (var (tn, ta) in froms)
                        {
                            if (!_tables.TryGetValue(tn, out var tbl)) continue;
                            foreach (var col in tbl.Columns)
                            {
                                if (item.TableFilter != null &&
                                    !ta.Equals(item.TableFilter, StringComparison.OrdinalIgnoreCase)) continue;
                                if (!seenEmpty.Add(col)) continue;
                                finalItems.Add(new SelectItem { Name = col, ExprTokens = SqlTokenizer.Tokenize(col) });
                            }
                        }
                    }
                    continue;
                }

                var firstRow = rows[0];
                var seenCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var k in firstRow.Keys)
                {
                    if (!k.Contains('.')) continue;
                    string colPart = k[(k.IndexOf('.') + 1)..];
                    string tablePart = k[..k.IndexOf('.')];
                    if (item.TableFilter != null &&
                        !tablePart.Equals(item.TableFilter, StringComparison.OrdinalIgnoreCase)) continue;
                    if (!seenCols.Add(colPart)) continue;
                    finalItems.Add(new SelectItem { Name = colPart, ExprTokens = SqlTokenizer.Tokenize(colPart) });
                }
            }

            if (finalItems.Count == 0 && rows.Count > 0)
            {
                var seenCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var k in rows[0].Keys)
                {
                    if (k.Contains('.') || !seenCols.Add(k)) continue;
                    finalItems.Add(new SelectItem { Name = k, ExprTokens = SqlTokenizer.Tokenize(k) });
                }
            }

            return (finalItems.Select(i => i.OutputName).ToArray(), finalItems);
        }

        /// <summary>
        /// Evaluates each row against the resolved SELECT items and returns a jagged array.
        /// <paramref name="ct"/> is checked <b>between rows</b>. A blocking add-in already
        /// executing within a row will run to completion before the check fires — callers
        /// requiring a hard timeout should cancel and abandon the parent <see cref="Task"/>.
        /// </summary>
        private object?[][] BuildRows(
            List<Dictionary<string, object?>> rows,
            List<SelectItem> finalItems,
            CancellationToken ct)
        {
            int numCols = finalItems.Count;
            var result = new object?[rows.Count][];
            for (int r = 0; r < rows.Count; r++)
            {
                ct.ThrowIfCancellationRequested();
                var rowResult = new object?[numCols];
                for (int c = 0; c < numCols; c++)
                {
                    var item = finalItems[c];
                    if (rows[r].TryGetValue(item.OutputName, out var pre)) { rowResult[c] = pre; continue; }
                    if (item.ExprTokens != null)
                    {
                        var exprCopy = item.ExprTokens.ToList();
                        exprCopy.Add(new SqlToken(SqlTokenType.EOF, ""));
                        int ep = 0;
                        rowResult[c] = SqlExpr.Eval(exprCopy, ref ep, MakeCtx(rows[r]));
                    }
                    else
                    {
                        rows[r].TryGetValue(item.Name ?? "", out rowResult[c]);
                    }
                }
                result[r] = rowResult;
            }
            return result;
        }

        // ── SELECT list parsing ──────────────────────────────────────────

        private List<SelectItem> ParseSelectList()
        {
            var items = new List<SelectItem>();
            while (!IsClause(Cur.Value) && Cur.Type != SqlTokenType.EOF)
            {
                if (Cur.Value == "*") { _p++; items.Add(new SelectItem { IsStar = true, Name = "*" }); }
                else if (Cur.Type == SqlTokenType.Identifier && Peek().Value == "." && Peek(2).Value == "*")
                {
                    string tf = Consume().Value; _p++; _p++; // table . *
                    items.Add(new SelectItem { IsStar = true, Name = $"{tf}.*", TableFilter = tf });
                }
                else
                {
                    var exprToks = ReadSelectExpr();
                    string? alias = null;
                    if (Match("AS")) alias = Consume().Value;
                    else if (Cur.Type == SqlTokenType.Identifier && !IsClause(Cur.Value)) alias = Consume().Value;
                    items.Add(BuildSelectItem(exprToks, alias));
                }
                Match(",");
            }
            return items;
        }

        private static SelectItem BuildSelectItem(List<SqlToken> toks, string? alias)
        {
            var item = new SelectItem { ExprTokens = toks, Alias = alias };

            // Determine display name
            if (toks.Count == 1) item.Name = toks[0].Value;
            else if (toks.Count == 3 && toks[1].Value == ".") item.Name = toks[2].Value;
            else item.Name = alias ?? string.Concat(toks.Select(x => x.Value));

            // Detect aggregates: FUNC(...)
            if (toks.Count >= 3 && toks[1].Value == "(")
            {
                string fn = toks[0].Value.ToUpperInvariant();
                if (fn is "COUNT" or "SUM" or "AVG" or "MIN" or "MAX")
                {
                    item.IsAggregate = true;
                    item.AggFunc = fn;
                    var inner = toks.Skip(2).TakeWhile(x => x.Value != ")").ToList();
                    if (inner.Count > 0 && inner[0].Value.Equals("DISTINCT", StringComparison.OrdinalIgnoreCase))
                    { item.AggDistinct = true; inner = [.. inner.Skip(1)]; }
                    item.AggCol = inner.Count == 0 || inner[0].Value == "*" ? "*"
                                : string.Join(" ", inner.Select(x => x.Value));
                    if (toks.Count >= 3 && toks[2].Value == "*") item.AggCol = "*";
                    if (alias == null) item.Alias = $"{fn}({item.AggCol})";
                }
            }

            return item;
        }

        // ── Token reading helpers ────────────────────────────────────────

        /// Read expression tokens up to: comma (only if next looks like assignment), WHERE, or clause keyword
        private List<SqlToken> ReadExprUntilCommaOrClause()
        {
            var toks = new List<SqlToken>();
            int depth = 0;
            while (Cur.Type != SqlTokenType.EOF)
            {
                if (IsClause(Cur.Value) && depth == 0) break;
                if (Cur.Value == "," && depth == 0 && NextIsAssignment()) break;
                if (Cur.Value == "(") depth++;
                else if (Cur.Value == ")") { if (depth == 0) break; depth--; }
                toks.Add(Consume());
            }
            toks.Add(new SqlToken(SqlTokenType.EOF, ""));
            return toks;
        }

        private bool NextIsAssignment()
        {
            // Current pos is at ","
            return _p + 2 < _t.Count &&
                   (_t[_p + 1].Type == SqlTokenType.Identifier || _t[_p + 1].Type == SqlTokenType.Keyword) &&
                   _t[_p + 2].Value == "=";
        }

        /// Read expression tokens up to a clause keyword (GROUP, ORDER, HAVING, LIMIT, etc.)
        private List<SqlToken> ReadUntilClause()
        {
            var toks = new List<SqlToken>();
            int depth = 0;
            while (Cur.Type != SqlTokenType.EOF)
            {
                if (Cur.Value == "(") depth++;
                else if (Cur.Value == ")") { if (depth == 0) break; depth--; }
                else if (depth == 0 && IsClause(Cur.Value)) break;
                toks.Add(Consume());
            }
            toks.Add(new SqlToken(SqlTokenType.EOF, ""));
            return toks;
        }

        /// Read a SELECT expression (stops at comma at depth 0, or clause keywords)
        private List<SqlToken> ReadSelectExpr()
        {
            var toks = new List<SqlToken>();
            int depth = 0;
            while (Cur.Type != SqlTokenType.EOF)
            {
                if (Cur.Value == "(") depth++;
                else if (Cur.Value == ")") { if (depth == 0) break; depth--; }
                else if (depth == 0 && Cur.Value == ",") break;
                else if (depth == 0 && IsClause(Cur.Value)) break;
                else if (depth == 0 && Cur.Value.Equals("AS", StringComparison.OrdinalIgnoreCase)) break;
                toks.Add(Consume());
            }
            return toks;
        }

        /// Read ORDER BY expression (stops at comma, ASC, DESC, or clause)
        private List<SqlToken> ReadOrderExpr()
        {
            var toks = new List<SqlToken>();
            int depth = 0;
            while (Cur.Type != SqlTokenType.EOF)
            {
                if (Cur.Value == "(") depth++;
                else if (Cur.Value == ")") { if (depth == 0) break; depth--; }
                else if (depth == 0 && Cur.Value == ",") break;
                else if (depth == 0 && IsIn(Cur.Value, "ASC", "DESC")) break;
                else if (depth == 0 && IsClause(Cur.Value)) break;
                toks.Add(Consume());
            }
            return toks;
        }

        private List<string> ParseCommaSeparatedExprs()
        {
            var result = new List<string>();
            while (!IsClause(Cur.Value) && Cur.Type != SqlTokenType.EOF)
            {
                var toks = new List<SqlToken>();
                int depth = 0;
                while (Cur.Type != SqlTokenType.EOF)
                {
                    if (Cur.Value == "(") depth++;
                    else if (Cur.Value == ")") { if (depth == 0) break; depth--; }
                    else if (depth == 0 && Cur.Value == ",") break;
                    else if (depth == 0 && IsClause(Cur.Value)) break;
                    toks.Add(Consume());
                }
                result.Add(string.Join(" ", toks.Select(x => x.Value)));
                Match(",");
            }
            return result;
        }

        // ── Misc helpers ─────────────────────────────────────────────────

        private bool PassesWhere(List<SqlToken>? toks, Dictionary<string, object?> row)
        {
            if (toks == null) return true;
            int wp = 0; var exprCopy = toks.ToList();
            return SqlExpr.AsBool(SqlExpr.Eval(exprCopy, ref wp, MakeCtx(row)));
        }

        private object? EvalExpr(List<SqlToken> toks, Dictionary<string, object?> row)
        {
            if (toks == null) return null;
            var exprCopy = toks.ToList();
            exprCopy.Add(new SqlToken(SqlTokenType.EOF, ""));
            int ep = 0;
            return SqlExpr.Eval(exprCopy, ref ep, MakeCtx(row));
        }

        private EvalContext MakeCtx(Dictionary<string, object?> row) =>
            new() { Row = row, Functions = _functions, Tables = _tables, AddIns = _addIns };

        private SQLTable RequireTable(string name)
        {
            if (_tables.TryGetValue(name, out var t)) return t;
            throw new KeyNotFoundException($"Table '{name}' not found.");
        }

        private static Dictionary<string, object?> RowDict(SQLTable table, int ri, string alias, string[] cols)
        {
            var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in cols)
            {
                object? v = table.GetValue(ri, c);
                d[c] = v;
                d[$"{alias}.{c}"] = v;
            }
            return d;
        }

        private static bool IsClause(string v) => IsIn(v,
            "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "INTO");

        private static bool IsJoin(string v) => IsIn(v,
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS");

        private static bool IsIn(string v, params string[] options) =>
            options.Any(o => o.Equals(v, StringComparison.OrdinalIgnoreCase));

        internal static Type SqlTypeToClr(string sqlType) => sqlType.ToUpperInvariant() switch
        {
            "INT" or "INTEGER" or "SMALLINT" or "TINYINT" or "BIGINT" => typeof(long),
            "FLOAT" or "REAL" or "DOUBLE" or "DECIMAL" or "NUMERIC" or "MONEY" => typeof(double),
            "BIT" or "BOOL" or "BOOLEAN" => typeof(bool),
            "DATETIME" or "DATE" or "TIME" or "DATETIME2" or "SMALLDATETIME" => typeof(DateTime),
            "UNIQUEIDENTIFIER" => typeof(Guid),
            _ => typeof(string)
        };
    }
}