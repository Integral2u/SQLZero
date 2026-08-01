using PMExcel.Utilities;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace PMExcel.Automation
{
    // ============================================================
    //  TOKENIZER
    // ============================================================

    public enum SqlTokenType { Keyword, Identifier, StringLiteral, Number, Operator, Punctuation, EOF }

    public class SqlToken
    {
        public SqlTokenType Type;
        public string Value;
        public SqlToken(SqlTokenType t, string v) { Type = t; Value = v; }
        public override string ToString() => $"[{Type}:{Value}]";
    }

    internal class CommonTableExpression
    {
        public string Name;
        public List<string> Columns; // optional column list
        public List<SqlToken> SelectTokens; // the SELECT query defining the CTE
    }
    public static class SqlTokenizer
    {
        private static readonly HashSet<string> Keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT","FROM","WHERE","INSERT","INTO","VALUES","UPDATE","SET","DELETE",
        "CREATE","DROP","ALTER","TABLE","COLUMN","ADD","VIEW","INDEX",
        "INNER","LEFT","RIGHT","FULL","OUTER","JOIN","ON","CROSS",
        "GROUP","BY","ORDER","HAVING","DISTINCT","AS","ALL","TOP",
        "AND","OR","NOT","IN","LIKE","BETWEEN","IS","NULL","EXISTS",
        "CASE","WHEN","THEN","ELSE","ELSEIF","END",
        "ASC","DESC","LIMIT","OFFSET",
        "FUNCTION","RETURNS","BEGIN","RETURN","DECLARE",
        "TRIGGER","BEFORE","AFTER","EACH","ROW","FOR",
        "PRIMARY","KEY","FOREIGN","REFERENCES","UNIQUE","DEFAULT","CONSTRAINT","IF",
        "INT","INTEGER","BIGINT","SMALLINT","TINYINT","FLOAT","DOUBLE","REAL",
        "DECIMAL","NUMERIC","MONEY","VARCHAR","NVARCHAR","CHAR","TEXT","NTEXT","STRING",
        "BIT","BOOL","BOOLEAN","DATETIME","DATE","TIME","DATETIME2","UNIQUEIDENTIFIER",
        "TRUE","FALSE","CAST","CONVERT","UNION","INTERSECT","EXCEPT","IDENTITY","AUTO_INCREMENT",
        "WITH"
    };

        public static List<SqlToken> Tokenize(string sql)
        {
            var tokens = new List<SqlToken>();
            int i = 0;

            while (i < sql.Length)
            {
                // Whitespace
                if (char.IsWhiteSpace(sql[i])) { i++; continue; }

                // Single-line comment
                if (i + 1 < sql.Length && sql[i] == '-' && sql[i + 1] == '-')
                { while (i < sql.Length && sql[i] != '\n') i++; continue; }

                // Multi-line comment
                if (i + 1 < sql.Length && sql[i] == '/' && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < sql.Length && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    i += 2; continue;
                }

                // String literal ' or "
                if (sql[i] == '\'' || sql[i] == '"')
                {
                    char q = sql[i++];
                    var sb = new StringBuilder();
                    while (i < sql.Length && sql[i] != q)
                    {
                        if (i + 1 < sql.Length && sql[i] == q && sql[i + 1] == q) { sb.Append(q); i += 2; }
                        else sb.Append(sql[i++]);
                    }
                    if (i < sql.Length) i++;
                    tokens.Add(new SqlToken(SqlTokenType.StringLiteral, sb.ToString()));
                    continue;
                }

                // Quoted identifier [name] or `name`
                if (sql[i] == '[' || sql[i] == '`')
                {
                    char close = sql[i] == '[' ? ']' : '`'; i++;
                    var sb = new StringBuilder();
                    while (i < sql.Length && sql[i] != close) sb.Append(sql[i++]);
                    if (i < sql.Length) i++;
                    tokens.Add(new SqlToken(SqlTokenType.Identifier, sb.ToString()));
                    continue;
                }

                // Number (including potential negative handled in expression layer)
                if (char.IsDigit(sql[i]))
                {
                    var sb = new StringBuilder();
                    while (i < sql.Length && (char.IsDigit(sql[i]) || sql[i] == '.')) sb.Append(sql[i++]);
                    // Optional E notation
                    if (i < sql.Length && (sql[i] == 'e' || sql[i] == 'E'))
                    {
                        sb.Append(sql[i++]);
                        if (i < sql.Length && (sql[i] == '+' || sql[i] == '-')) sb.Append(sql[i++]);
                        while (i < sql.Length && char.IsDigit(sql[i])) sb.Append(sql[i++]);
                    }
                    tokens.Add(new SqlToken(SqlTokenType.Number, sb.ToString()));
                    continue;
                }

                // Two-char operators
                if (i + 1 < sql.Length)
                {
                    string two = sql.Substring(i, 2);
                    if (two == "<>" || two == "!=" || two == "<=" || two == ">=" || two == ":=")
                    { tokens.Add(new SqlToken(SqlTokenType.Operator, two)); i += 2; continue; }
                }

                // Single-char operators
                if ("=<>+-*/%^".Contains(sql[i]))
                { tokens.Add(new SqlToken(SqlTokenType.Operator, sql[i].ToString())); i++; continue; }

                // Punctuation
                if ("(),;.".Contains(sql[i]))
                { tokens.Add(new SqlToken(SqlTokenType.Punctuation, sql[i].ToString())); i++; continue; }

                // Identifier or keyword
                if (char.IsLetter(sql[i]) || sql[i] == '_' || sql[i] == '@' || sql[i] == '#')
                {
                    var sb = new StringBuilder();
                    while (i < sql.Length && (char.IsLetterOrDigit(sql[i]) || sql[i] == '_' || sql[i] == '@' || sql[i] == '#'))
                        sb.Append(sql[i++]);
                    string word = sb.ToString();
                    tokens.Add(new SqlToken(Keywords.Contains(word) ? SqlTokenType.Keyword : SqlTokenType.Identifier, word));
                    continue;
                }

                i++; // skip unknown
            }

            tokens.Add(new SqlToken(SqlTokenType.EOF, ""));
            return tokens;
        }


    }

    // ============================================================
    //  SQL TABLE
    // ============================================================

    public class SQLTable
    {
        public readonly string Name;
        private readonly Dictionary<string, List<object>> DataColumns;
        private readonly Dictionary<int, string> ColumnIndex;
        private readonly Dictionary<string, Type> ColumnTypes;
        private int RowCount = 0;

        public SQLTable(string name, string[] columns = null, object[,] data = null)
        {
            Name = name;
            DataColumns = new Dictionary<string, List<object>>(StringComparer.OrdinalIgnoreCase);
            ColumnIndex = new Dictionary<int, string>();
            ColumnTypes = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

            foreach (var column in columns ?? Array.Empty<string>())
            {
                if (!DataColumns.ContainsKey(column)) DataColumns.Add(column, new List<object>());
                ColumnIndex.Add(ColumnIndex.Count, column);
            }

            if (data == null) return;
            if (data.GetLength(0) == 0) return;
            if (data.GetLength(1) != DataColumns.Count) throw new DataMisalignedException();

            for (int row = 0; row < data.GetLength(0); row++)
            {
                for (int col = 0; col < data.GetLength(1); col++)
                {
                    string colName = ColumnIndex[col];
                    object val = data[row, col];
                    if (!ColumnTypes.ContainsKey(colName))
                        ColumnTypes[colName] = val?.GetType() ?? typeof(object);
                    else if (val != null && val.GetType() != ColumnTypes[colName])
                        throw new InvalidDataException($"Type mismatch in column '{colName}' at row {row}.");
                    DataColumns[colName].Add(val);
                }
                RowCount++;
            }
        }

        // ── Public surface ──────────────────────────────────────────────
        public string[] Columns => ColumnIndex.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToArray();
        public int Count => RowCount;

        public void AddColumn(string name, Type type)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (type == null) throw new ArgumentNullException(nameof(type));
            if (DataColumns.ContainsKey(name)) throw new DuplicateNameException($"Column '{name}' already exists.");

            DataColumns[name] = new List<object>(RowCount);
            ColumnIndex[ColumnIndex.Count] = name;
            ColumnTypes[name] = type;

            object defaultVal = type.IsValueType ? Activator.CreateInstance(type) : null;
            for (int i = 0; i < RowCount; i++)
                DataColumns[name].Add(defaultVal);
        }

        public void AddRow(object[] row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));
            if (row.Length != DataColumns.Count)
                throw new DataMisalignedException($"Expected {DataColumns.Count} column(s), got {row.Length}.");

            for (int i = 0; i < row.Length; i++)
            {
                string colName = ColumnIndex[i];
                object val = row[i];

                if (val == null) continue; // nulls are always allowed

                if (!ColumnTypes.ContainsKey(colName))
                {
                    ColumnTypes[colName] = val.GetType();
                }
                else if (ColumnTypes[colName] != typeof(object))
                {
                    Type expected = ColumnTypes[colName];
                    if (!expected.IsInstanceOfType(val))
                    {
                        try { row[i] = Convert.ChangeType(val, expected); }
                        catch
                        {
                            throw new InvalidDataException(
                                $"Cannot store {val.GetType().Name} in column '{colName}' (expected {expected.Name}).");
                        }
                    }
                }
            }

            for (int i = 0; i < row.Length; i++)
                DataColumns[ColumnIndex[i]].Add(row[i]);
            RowCount++;
        }

        // ── Internal helpers used by the query engine ────────────────────
        internal void DropColumn(string name)
        {
            if (!DataColumns.ContainsKey(name)) throw new KeyNotFoundException($"Column '{name}' not found.");
            // Rebuild ColumnIndex without this column
            var orderedCols = ColumnIndex.OrderBy(kv => kv.Key).Select(kv => kv.Value)
                                         .Where(c => !c.Equals(name, StringComparison.OrdinalIgnoreCase)).ToList();
            DataColumns.Remove(name);
            ColumnTypes.Remove(name);
            ColumnIndex.Clear();
            for (int i = 0; i < orderedCols.Count; i++) ColumnIndex[i] = orderedCols[i];
        }

        internal void SetColumnType(string name, Type type)
        {
            if (!DataColumns.ContainsKey(name)) throw new KeyNotFoundException($"Column '{name}' not found.");
            ColumnTypes[name] = type;
        }

        internal bool HasColumn(string name) => DataColumns.ContainsKey(name);

        internal Type GetColumnType(string name) =>
            ColumnTypes.TryGetValue(name, out var t) ? t : typeof(object);

        public object GetValue(int rowIndex, string columnName)
        {
            if (!DataColumns.TryGetValue(columnName, out var col))
                throw new KeyNotFoundException($"Column '{columnName}' not found.");
            return col[rowIndex];
        }

        internal void SetValue(int rowIndex, string columnName, object value)
        {
            if (!DataColumns.TryGetValue(columnName, out var col))
                throw new KeyNotFoundException($"Column '{columnName}' not found.");
            col[rowIndex] = value;
        }

        internal void DeleteRow(int rowIndex)
        {
            foreach (var col in DataColumns.Values) col.RemoveAt(rowIndex);
            RowCount--;
        }

        internal object[] GetRow(int rowIndex)
        {
            var cols = Columns;
            var row = new object[cols.Length];
            for (int c = 0; c < cols.Length; c++) row[c] = DataColumns[cols[c]][rowIndex];
            return row;
        }

        // ── JSON Serialization ────────────────────────────────────────────

        /// <summary>
        /// Serializes the table to a JSON string.
        /// <para>Format:</para>
        /// <code>
        /// {
        ///   "name":    "Products",
        ///   "columns": [ { "name": "Id", "type": "Int64" }, ... ],
        ///   "rows":    [ [1, "Hammer", 12.99], ... ]
        /// }
        /// </code>
        /// Supported CLR types: <c>null</c>, <c>bool</c>, <c>long</c>, <c>double</c>,
        /// <c>string</c>, <c>DateTime</c> (ISO-8601), <c>Guid</c>.
        /// Any other type is stored via <c>ToString()</c> as a string.
        /// </summary>
        public string ToJson(bool indented = true)
        {
            var opts = new JsonWriterOptions { Indented = indented };
            using (var ms = new System.IO.MemoryStream())
            {
                using (var w = new Utf8JsonWriter(ms, opts))
                {
                    var cols = Columns;

                    w.WriteStartObject();

                    w.WriteString("name", Name);

                    // ── columns metadata ─────────────────────────────────────────
                    w.WriteStartArray("columns");
                    foreach (var col in cols)
                    {
                        w.WriteStartObject();
                        w.WriteString("name", col);
                        w.WriteString("type", GetColumnType(col).Name);
                        w.WriteEndObject();
                    }
                    w.WriteEndArray();

                    // ── row data ─────────────────────────────────────────────────
                    w.WriteStartArray("rows");
                    for (int r = 0; r < RowCount; r++)
                    {
                        w.WriteStartArray();
                        for (int c = 0; c < cols.Length; c++)
                            WriteJsonValue(w, DataColumns[cols[c]][r]);
                        w.WriteEndArray();
                    }
                    w.WriteEndArray();

                    w.WriteEndObject();
                    w.Flush();
                    return Encoding.UTF8.GetString(ms.ToArray());
                }
            }
        }

        /// <summary>
        /// Deserializes a JSON string produced by <see cref="ToJson"/> back into a new <see cref="SQLTable"/>.
        /// </summary>
        /// <exception cref="JsonException">The JSON is malformed or missing required properties.</exception>
        public static SQLTable FromJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json)) throw new ArgumentNullException(nameof(json));
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;

                string name = root.GetProperty("name").GetString()
                              ?? throw new JsonException("Missing 'name' property.");

                // ── columns ──────────────────────────────────────────────────
                var colDefs = new List<(string Name, Type ClrType)>();
                foreach (var colEl in root.GetProperty("columns").EnumerateArray())
                {
                    string colName = colEl.GetProperty("name").GetString();
                    string typeName = colEl.GetProperty("type").GetString() ?? "String";
                    colDefs.Add((colName, ClrTypeFromName(typeName)));
                }

                var table = new SQLTable(name, colDefs.Select(c => c.Name).ToArray());
                foreach (var (cn, ct) in colDefs) table.SetColumnType(cn, ct);

                // ── rows ─────────────────────────────────────────────────────
                foreach (var rowEl in root.GetProperty("rows").EnumerateArray())
                {
                    var cells = rowEl.EnumerateArray().ToArray();
                    var row = new object[cells.Length];
                    for (int c = 0; c < cells.Length; c++)
                        row[c] = ReadJsonValue(cells[c], c < colDefs.Count ? colDefs[c].ClrType : typeof(object));
                    table.AddRow(row);
                }

                return table;
            }
        }

        // ── JSON helpers ─────────────────────────────────────────────────

        private static void WriteJsonValue(Utf8JsonWriter w, object value)
        {
            switch (value)
            {
                case null: w.WriteNullValue(); break;
                case bool b: w.WriteBooleanValue(b); break;
                case long l: w.WriteNumberValue(l); break;
                case int i: w.WriteNumberValue(i); break;
                case double d: w.WriteNumberValue(d); break;
                case float f: w.WriteNumberValue(f); break;
                case decimal m: w.WriteNumberValue(m); break;
                case DateTime dt: w.WriteStringValue(dt.ToString("O")); break;
                case Guid g: w.WriteStringValue(g.ToString()); break;
                default: w.WriteStringValue(value.ToString()); break;
            }
        }

        private static object ReadJsonValue(JsonElement el, Type hint)
        {
            if (el.ValueKind == JsonValueKind.Null) return null;
            if (el.ValueKind == JsonValueKind.True) return true;
            if (el.ValueKind == JsonValueKind.False) return false;

            if (el.ValueKind == JsonValueKind.Number)
            {
                if (hint == typeof(double) || hint == typeof(float) || hint == typeof(decimal))
                    return el.GetDouble();
                if (hint == typeof(bool)) return el.GetInt64() != 0;
                return el.GetInt64();       // long by default
            }

            if (el.ValueKind == JsonValueKind.String)
            {
                string s = el.GetString();
                if (hint == typeof(DateTime) && DateTime.TryParse(s, null,
                        System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return dt;
                if (hint == typeof(Guid) && Guid.TryParse(s, out var g)) return g;
                if (hint == typeof(long) && long.TryParse(s, out var l)) return l;
                if (hint == typeof(double) && double.TryParse(s,
                        System.Globalization.NumberStyles.Any,
                        System.Globalization.CultureInfo.InvariantCulture, out var d)) return d;
                if (hint == typeof(bool))
                {
                    switch (s)
                    {
                        case "1":
                        case "true":
                        case "True":
                            return true;
                        default:
                            return false;
                    }
                }
                return s;
            }

            return el.GetRawText();
        }

        private static Type ClrTypeFromName(string name)
        {
            switch (name)
            {
                case "Int64":
                case "Int32":
                case "Int16":
                case "Byte":
                case "SByte":
                    return typeof(long);
                case "Double":
                case "Single":
                case "Decimal":
                    return typeof(double);
                case "Boolean":
                    return typeof(bool);
                case "DateTime":
                    return typeof(DateTime);
                case "Guid":
                    return typeof(Guid);
                default: return typeof(string);
            }
        }
    }

    // ============================================================
    //  EXPRESSION EVALUATOR
    // ============================================================

    public class EvalContext
    {
        public Dictionary<string, object> Row;
        public Dictionary<string, SqlFunction> Functions;
        public Dictionary<string, SQLTable> Tables;

        public object GetColumn(string name)
        {
            if (Row.TryGetValue(name, out var v)) return v;
            var key = Row.Keys.FirstOrDefault(k => k.Equals(name, StringComparison.OrdinalIgnoreCase));
            return key != null ? Row[key] : null;
        }
    }

    public static class SqlExpr
    {
        // ── Entry point ──────────────────────────────────────────────────
        public static object Eval(List<SqlToken> tokens, ref int pos, EvalContext ctx)
            => ParseOr(tokens, ref pos, ctx);

        // ── Boolean layers ───────────────────────────────────────────────
        private static object ParseOr(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            var left = ParseAnd(t, ref p, ctx);
            while (Is(t, p, "OR")) { p++; left = AsBool(left) | AsBool(ParseAnd(t, ref p, ctx)); }
            return left;
        }

        private static object ParseAnd(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            var left = ParseNot(t, ref p, ctx);
            while (Is(t, p, "AND")) { p++; left = AsBool(left) & AsBool(ParseNot(t, ref p, ctx)); }
            return left;
        }

        private static object ParseNot(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            if (Is(t, p, "NOT")) { p++; return !AsBool(ParseComparison(t, ref p, ctx)); }
            return ParseComparison(t, ref p, ctx);
        }

        // ── Comparisons ──────────────────────────────────────────────────
        private static object ParseComparison(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            var left = ParseAddSub(t, ref p, ctx);
            if (p >= t.Count) return left;

            // IS [NOT] NULL
            if (Is(t, p, "IS"))
            {
                p++;
                bool neg = false;
                if (Is(t, p, "NOT")) { neg = true; p++; }
                if (Is(t, p, "NULL")) { p++; return neg ? left != null : left == null; }
            }

            // [NOT] BETWEEN / IN / LIKE
            bool notPre = false;
            if (Is(t, p, "NOT")) { notPre = true; p++; }

            if (Is(t, p, "BETWEEN"))
            {
                p++;
                var lo = ParseAddSub(t, ref p, ctx);
                if (Is(t, p, "AND")) p++;
                var hi = ParseAddSub(t, ref p, ctx);
                bool r = ObjCmp(left, lo) >= 0 && ObjCmp(left, hi) <= 0;
                return notPre ? !r : r;
            }

            if (Is(t, p, "IN"))
            {
                p++;
                ConsumeIf(t, ref p, "(");
                var vals = new List<object>();
                while (p < t.Count && t[p].Value != ")")
                {
                    vals.Add(ParseAddSub(t, ref p, ctx));
                    ConsumeIf(t, ref p, ",");
                }
                ConsumeIf(t, ref p, ")");
                bool r = vals.Any(v => ObjEq(left, v));
                return notPre ? !r : r;
            }

            if (Is(t, p, "LIKE"))
            {
                p++;
                var pat = ParseAddSub(t, ref p, ctx);
                bool r = SqlLike(left?.ToString() ?? "", pat?.ToString() ?? "");
                return notPre ? !r : r;
            }

            if (notPre) return !AsBool(left); // bare NOT expr

            // Standard operators
            if (p < t.Count && t[p].Type == SqlTokenType.Operator)
            {
                string op = t[p].Value;
                if (op == "=" || op == "<>" || op == "!=" || op == "<" || op == ">" || op == "<=" || op == ">=")
                {
                    p++;
                    var right = ParseAddSub(t, ref p, ctx);
                    int cmp = ObjCmp(left, right);
                    switch (op)
                    {
                        case "=": return ObjEq(left, right);
                        case "<>":
                        case "!=": return !ObjEq(left, right);
                        case "<": return cmp < 0;
                        case ">": return cmp > 0;
                        case "<=": return cmp <= 0;
                        case ">=": return cmp >= 0;
                        default: return false;
                    }
                }
            }

            return left;
        }

        // ── Arithmetic ───────────────────────────────────────────────────
        private static object ParseAddSub(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            var left = ParseMulDiv(t, ref p, ctx);
            while (p < t.Count && t[p].Type == SqlTokenType.Operator && (t[p].Value == "+" || t[p].Value == "-"))
            {
                string op = t[p++].Value;
                var right = ParseMulDiv(t, ref p, ctx);
                if (op == "+")
                    left = (left is string || right is string)
                        ? (object)((left?.ToString() ?? "") + (right?.ToString() ?? ""))
                        : ToNum(left) + ToNum(right);
                else
                    left = ToNum(left) - ToNum(right);
            }
            return left;
        }

        private static object ParseMulDiv(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            var left = ParseUnary(t, ref p, ctx);
            while (p < t.Count && t[p].Type == SqlTokenType.Operator && (t[p].Value == "*" || t[p].Value == "/" || t[p].Value == "%"))
            {
                string op = t[p++].Value;
                var right = ParseUnary(t, ref p, ctx);
                switch (op)
                {
                    case "*":
                        {
                            left = ToNum(left) * ToNum(right);
                            break;
                        }
                    case "/":
                        {
                            left = ToNum(right) == 0 ? throw new DivideByZeroException() : ToNum(left) / ToNum(right);
                            break;
                        }
                    case "%":
                        {
                            left = ToNum(left) % ToNum(right);
                            break;
                        }
                }
            }
            return left;
        }

        private static object ParseUnary(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            if (p < t.Count && t[p].Type == SqlTokenType.Operator && t[p].Value == "-")
            { p++; return -ToNum(ParsePrimary(t, ref p, ctx)); }
            return ParsePrimary(t, ref p, ctx);
        }

        // ── Primary ──────────────────────────────────────────────────────
        private static object ParsePrimary(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            if (p >= t.Count || t[p].Type == SqlTokenType.EOF) return null;
            var tok = t[p];

            // Parenthesised expression
            if (tok.Value == "(")
            {
                p++;
                var v = Eval(t, ref p, ctx);
                ConsumeIf(t, ref p, ")");
                return v;
            }

            // Literals
            if (Is(t, p, "NULL")) { p++; return null; }
            if (Is(t, p, "TRUE")) { p++; return true; }
            if (Is(t, p, "FALSE")) { p++; return false; }
            if (tok.Type == SqlTokenType.StringLiteral) { p++; return tok.Value; }
            if (tok.Type == SqlTokenType.Number)
            {
                p++;
                return tok.Value.Contains('.') || tok.Value.Contains('e') || tok.Value.Contains('E')
                    ? (object)double.Parse(tok.Value, System.Globalization.CultureInfo.InvariantCulture)
                    : long.Parse(tok.Value);
            }

            // CASE
            if (Is(t, p, "CASE")) { p++; return ParseCase(t, ref p, ctx); }

            // CAST(expr AS type)
            if (Is(t, p, "CAST") && p + 1 < t.Count && t[p + 1].Value == "(")
            {
                p += 2;
                var val = Eval(t, ref p, ctx);
                if (Is(t, p, "AS")) p++;
                string typeName = p < t.Count ? t[p++].Value : "TEXT";
                if (p < t.Count && t[p].Value == "(") { while (p < t.Count && t[p].Value != ")") p++; ConsumeIf(t, ref p, ")"); }
                ConsumeIf(t, ref p, ")");
                return CastValue(val, typeName);
            }

            if (tok.Type == SqlTokenType.Identifier || tok.Type == SqlTokenType.Keyword)
            {
                // Function call: name(
                if (p + 1 < t.Count && t[p + 1].Value == "(")
                {
                    string fname = tok.Value; p += 2;
                    var args = new List<object>();
                    bool isStar = false;
                    bool isDistinct = false;

                    // ── Pre-computed aggregate lookup (for HAVING / post-GROUP-BY evaluation) ──
                    // Peek at the raw inner tokens to build the lookup key without consuming
                    // anything, then check ctx.Row before falling through to CallBuiltin.
                    if (ctx?.Row != null && IsAggFuncName(fname))
                    {
                        string innerText;
                        if (p < t.Count && t[p].Value == "*")
                        {
                            innerText = "*";
                        }
                        else
                        {
                            // Scan inner tokens (respecting parens) without moving p
                            var innerParts = new List<string>();
                            bool seenDistinct = false;
                            int depth = 0, scan = p;
                            while (scan < t.Count)
                            {
                                string sv = t[scan].Value;
                                if (sv == "(") depth++;
                                else if (sv == ")") { if (depth == 0) break; depth--; }
                                else if (sv == "," && depth == 0) { innerParts.Add(","); scan++; continue; }
                                else if (sv.Equals("DISTINCT", StringComparison.OrdinalIgnoreCase)) { seenDistinct = true; }
                                else innerParts.Add(sv);
                                scan++;
                            }
                            innerText = (seenDistinct ? "DISTINCT " : "") + string.Join("", innerParts);
                        }
                        string exprKey = $"{fname.ToUpperInvariant()}({innerText})";
                        if (ctx.Row.TryGetValue(exprKey, out var precomp))
                        {
                            // Consume the args properly so the parser position advances
                            if (p < t.Count && t[p].Value == "*") p++;
                            else { int depth = 0; while (p < t.Count) { if (t[p].Value == "(") depth++; else if (t[p].Value == ")") { if (depth == 0) break; depth--; } p++; } }
                            ConsumeIf(t, ref p, ")");
                            return precomp;
                        }
                    }

                    if (p < t.Count && t[p].Value == "*") { isStar = true; p++; }
                    else
                    {
                        while (p < t.Count && t[p].Value != ")")
                        {
                            if (Is(t, p, "DISTINCT")) { isDistinct = true; p++; continue; }
                            args.Add(Eval(t, ref p, ctx));
                            ConsumeIf(t, ref p, ",");
                        }
                    }
                    ConsumeIf(t, ref p, ")");
                    return CallBuiltin(fname, args, isStar, ctx);
                }

                // table.column
                if (p + 2 < t.Count && t[p + 1].Value == "." &&
                    (t[p + 2].Type == SqlTokenType.Identifier || t[p + 2].Type == SqlTokenType.Keyword))
                {
                    string tbl = tok.Value, col = t[p + 2].Value; p += 3;
                    return ctx.GetColumn($"{tbl}.{col}") ?? ctx.GetColumn(col);
                }

                // Column reference
                p++;
                return ctx.GetColumn(tok.Value);
            }

            p++; return null;
        }

        private static object ParseCase(List<SqlToken> t, ref int p, EvalContext ctx)
        {
            bool isSearched = Is(t, p, "WHEN");
            object pivot = isSearched ? null : Eval(t, ref p, ctx);
            object result = null; bool matched = false;

            while (Is(t, p, "WHEN"))
            {
                p++;
                var when = Eval(t, ref p, ctx);
                if (Is(t, p, "THEN")) p++;
                var then = Eval(t, ref p, ctx);
                if (!matched && (isSearched ? AsBool(when) : ObjEq(pivot, when)))
                { result = then; matched = true; }
            }
            if (Is(t, p, "ELSE")) { p++; var el = Eval(t, ref p, ctx); if (!matched) result = el; }
            if (Is(t, p, "END")) p++;
            return result;
        }

        // ── Built-in functions ───────────────────────────────────────────
        private static object CallBuiltin(string name, List<object> args, bool isStar, EvalContext ctx)
        {
            // User-defined functions
            if (ctx?.Functions != null && ctx.Functions.TryGetValue(name, out var udf) && udf.CompiledFunc != null)
                return udf.CompiledFunc(args.ToArray());

            switch (name.ToUpperInvariant())
            {
                // Aggregates (per-row placeholders; actual aggregation happens in GroupBy)
                case "COUNT": return isStar ? 1L : (args.Count > 0 && args[0] != null ? 1L : 0L);
                case "SUM": return args.Count > 0 ? (object)ToNum(args[0]) : null;
                case "AVG": return args.Count > 0 ? (object)ToNum(args[0]) : null;
                case "MIN": return args.Count > 0 ? args[0] : null;
                case "MAX": return args.Count > 0 ? args[0] : null;

                // String functions
                case "UPPER": case "UCASE": return args.Count > 0 ? args[0]?.ToString()?.ToUpperInvariant() : null;
                case "LOWER": case "LCASE": return args.Count > 0 ? args[0]?.ToString()?.ToLowerInvariant() : null;
                case "LEN": case "LENGTH": return args.Count > 0 ? (object)(long)(args[0]?.ToString()?.Length ?? 0) : null;
                case "LTRIM": return args.Count > 0 ? args[0]?.ToString()?.TrimStart() : null;
                case "RTRIM": return args.Count > 0 ? args[0]?.ToString()?.TrimEnd() : null;
                case "TRIM": return args.Count > 0 ? args[0]?.ToString()?.Trim() : null;
                case "REVERSE": return args.Count > 0 ? new string(args[0]?.ToString()?.Reverse().ToArray()) : null;
                case "CONCAT": return string.Concat(args.Select(a => a?.ToString() ?? ""));
                case "CONCAT_WS":
                    {
                        if (args.Count < 1) return null;
                        string sep = args[0]?.ToString() ?? "";
                        return string.Join(sep, args.Skip(1).Where(a => a != null).Select(a => a.ToString()));
                    }
                case "REPLACE":
                    return args.Count < 3 ? null : args[0]?.ToString()?.Replace(args[1]?.ToString() ?? "", args[2]?.ToString() ?? "");
                case "SUBSTRING":
                case "SUBSTR":
                case "MID":
                    {
                        if (args.Count < 2) return null;
                        string s = args[0]?.ToString() ?? "";
                        int st = Math.Max(0, (int)ToNum(args[1]) - 1);
                        if (st >= s.Length) return "";
                        return args.Count >= 3
                            ? s.Substring(st, Math.Min((int)ToNum(args[2]), s.Length - st))
                            : s.Substring(st);
                    }
                case "LEFT":
                    {
                        if (args.Count < 2) return null;
                        string s = args[0]?.ToString() ?? "";
                        int n = Math.Min((int)ToNum(args[1]), s.Length);
                        return n <= 0 ? "" : s.Substring(0, n);
                    }
                case "RIGHT":
                    {
                        if (args.Count < 2) return null;
                        string s = args[0]?.ToString() ?? "";
                        int n = Math.Min((int)ToNum(args[1]), s.Length);
                        return n <= 0 ? "" : s.Substring(s.Length - n);
                    }
                case "CHARINDEX":
                case "LOCATE":
                    {
                        if (args.Count < 2) return null;
                        string needle = args[0]?.ToString() ?? "", haystack = args[1]?.ToString() ?? "";
                        int idx = haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase);
                        return (long)(idx + 1); // SQL is 1-based; 0 means not found
                    }
                case "INSTR":
                    {
                        if (args.Count < 2) return null;
                        string haystack = args[0]?.ToString() ?? "", needle = args[1]?.ToString() ?? "";
                        int idx = haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase);
                        return (long)(idx + 1);
                    }
                case "PATINDEX":
                    {
                        if (args.Count < 2) return null;
                        string pat = args[0]?.ToString() ?? "", hay = args[1]?.ToString() ?? "";
                        var rx = SqlLikeToRegex(pat);
                        var m = Regex.Match(hay, rx, RegexOptions.IgnoreCase);
                        return m.Success ? (long)(m.Index + 1) : 0L;
                    }
                case "REPLICATE":
                case "REPEAT":
                    return args.Count < 2 ? null : string.Concat(Enumerable.Repeat(args[0]?.ToString() ?? "", (int)ToNum(args[1])));
                case "SPACE":
                    return args.Count > 0 ? new string(' ', (int)ToNum(args[0])) : null;
                case "STR":
                case "TOSTRING":
                case "TO_CHAR":
                    return args.Count > 0 ? args[0]?.ToString() : null;
                case "ASCII":
                    return args.Count > 0 && !string.IsNullOrEmpty(args[0]?.ToString()) ? (object)(long)args[0].ToString()[0] : null;
                case "CHAR":
                    return args.Count > 0 ? ((char)(int)ToNum(args[0])).ToString() : null;

                // Numeric functions
                case "ABS": return args.Count > 0 ? (object)Math.Abs(ToNum(args[0])) : null;
                case "ROUND":
                    {
                        if (args.Count == 0) return null;
                        int dig = args.Count > 1 ? (int)ToNum(args[1]) : 0;
                        return Math.Round(ToNum(args[0]), dig, MidpointRounding.AwayFromZero);
                    }
                case "FLOOR": return args.Count > 0 ? (object)Math.Floor(ToNum(args[0])) : null;
                case "CEILING": case "CEIL": return args.Count > 0 ? (object)Math.Ceiling(ToNum(args[0])) : null;
                case "POWER": case "POW": return args.Count >= 2 ? (object)Math.Pow(ToNum(args[0]), ToNum(args[1])) : null;
                case "SQRT": return args.Count > 0 ? (object)Math.Sqrt(ToNum(args[0])) : null;
                case "EXP": return args.Count > 0 ? (object)Math.Exp(ToNum(args[0])) : null;
                case "LOG": case "LN": return args.Count > 0 ? (object)Math.Log(ToNum(args[0]), args.Count > 1 ? ToNum(args[1]) : Math.E) : null;
                case "LOG10": return args.Count > 0 ? (object)Math.Log10(ToNum(args[0])) : null;
                case "SIGN": return args.Count > 0 ? (object)(double)Math.Sign(ToNum(args[0])) : null;
                case "MOD": return args.Count >= 2 ? (object)(ToNum(args[0]) % ToNum(args[1])) : null;
                case "RAND": case "RANDOM": return new Random().NextDouble();
                case "PI": return Math.PI;

                // Null-handling
                case "COALESCE":
                case "NVL":
                case "IFNULL":
                case "ISNULL":
                    return args.FirstOrDefault(a => a != null);
                case "NULLIF":
                    return args.Count >= 2 && ObjEq(args[0], args[1]) ? null : (args.Count > 0 ? args[0] : null);

                // Date functions
                case "NOW": case "GETDATE": case "CURRENT_TIMESTAMP": return DateTime.Now;
                case "GETUTCDATE": case "UTC_TIMESTAMP": return DateTime.UtcNow;
                case "YEAR": return args.Count > 0 && args[0] is DateTime dy ? (object)(long)dy.Year : null;
                case "MONTH": return args.Count > 0 && args[0] is DateTime dm ? (object)(long)dm.Month : null;
                case "DAY": return args.Count > 0 && args[0] is DateTime dd ? (object)(long)dd.Day : null;
                case "DATEDIFF":
                    {
                        if (args.Count < 3 || !(args[1] is DateTime d1) || !(args[2] is DateTime d2)) return null;
                        string part = args[0]?.ToString()?.ToUpperInvariant() ?? "DAY";
                        switch (part)
                        {
                            case "YEAR": return (object)(long)(d2.Year - d1.Year);
                            case "MONTH": return (long)((d2.Year - d1.Year) * 12 + d2.Month - d1.Month);
                            case "DAY": return (long)(d2 - d1).TotalDays;
                            case "HOUR": return (long)(d2 - d1).TotalHours;
                            case "MINUTE": return (long)(d2 - d1).TotalMinutes;
                            case "SECOND": return (long)(d2 - d1).TotalSeconds;
                            default: return (long)(d2 - d1).TotalDays;
                        }
                        ;
                    }

                // Misc
                case "NEWID": case "UUID": case "NEWGUID": return Guid.NewGuid().ToString();
                case "IIF":
                case "IF":
                    return args.Count >= 3 ? (AsBool(args[0]) ? args[1] : args[2]) : null;

                default:
                    return null;
            }
        }

        private static object CastValue(object val, string sqlType)
        {
            if (val == null) return null;
            switch (sqlType.ToUpperInvariant())
            {
                case "INT":
                case "INTEGER":
                case "BIGINT":
                case "SMALLINT":
                case "TINYINT": return (object)Convert.ToInt64(val);
                case "FLOAT":
                case "DOUBLE":
                case "REAL":
                case "DECIMAL":
                case "NUMERIC": return Convert.ToDouble(val);
                case "BIT":
                case "BOOL":
                case "BOOLEAN": return AsBool(val);
                case "VARCHAR":
                case "NVARCHAR":
                case "CHAR":
                case "TEXT":
                case "NTEXT":
                case "STRING": return val.ToString();
                case "DATETIME":
                case "DATE": return DateTime.TryParse(val.ToString(), out var dt) ? (object)dt : val;
                default: return val;
            }
        }

        // ── LIKE helper ──────────────────────────────────────────────────
        private static bool SqlLike(string value, string pattern)
            => Regex.IsMatch(value, SqlLikeToRegex(pattern), RegexOptions.IgnoreCase | RegexOptions.Singleline);

        private static string SqlLikeToRegex(string pattern)
        {
            var sb = new StringBuilder("^");
            foreach (char c in pattern)
            {
                if (c == '%') sb.Append(".*");
                else if (c == '_') sb.Append('.');
                else sb.Append(Regex.Escape(c.ToString()));
            }
            sb.Append('$');
            return sb.ToString();
        }

        // ── Helpers ──────────────────────────────────────────────────────
        internal static bool AsBool(object v)
        {
            if (v == null) return false;
            if (v is bool b) return b;
            if (v is long l) return l != 0;
            if (v is double d) return d != 0;
            if (v is int i) return i != 0;
            if (v is string s) return s.Length > 0;
            return true;
        }

        public static double ToNum(object v)
        {
            switch (v)
            {
                case null: return 0;
                case double d: return d;
                case long l: return l;
                case int i: return i;
                case float f: return f;
                case decimal m: return (double)m;
                case bool b: return b ? 1 : 0;
                default:
                    return double.TryParse(v.ToString(),
                         System.Globalization.NumberStyles.Any,
                         System.Globalization.CultureInfo.InvariantCulture, out double r) ? r : 0;
            }
        }

        internal static bool ObjEq(object a, object b)
        {
            if (a == null && b == null) return true;
            if (a == null || b == null) return false;
            if (a is string sa && b is string sb2) return sa.Equals(sb2, StringComparison.OrdinalIgnoreCase);
            if (IsNum(a) && IsNum(b)) return ToNum(a) == ToNum(b);
            return a.Equals(b);
        }

        internal static int ObjCmp(object a, object b)
        {
            if (a == null && b == null) return 0;
            if (a == null) return -1;
            if (b == null) return 1;
            if (IsNum(a) && IsNum(b)) return ToNum(a).CompareTo(ToNum(b));
            if (a is DateTime da && b is DateTime db) return da.CompareTo(db);
            return string.Compare(a.ToString(), b.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsNum(object v) => v is long || v is double || v is int || v is float || v is decimal || v is bool;

        private static bool IsAggFuncName(string name) =>
            name.Equals("COUNT", StringComparison.OrdinalIgnoreCase) ||
            name.Equals("SUM", StringComparison.OrdinalIgnoreCase) ||
            name.Equals("AVG", StringComparison.OrdinalIgnoreCase) ||
            name.Equals("MIN", StringComparison.OrdinalIgnoreCase) ||
            name.Equals("MAX", StringComparison.OrdinalIgnoreCase);

        private static bool Is(List<SqlToken> t, int p, string word)
            => p < t.Count && t[p].Value.Equals(word, StringComparison.OrdinalIgnoreCase);

        private static void ConsumeIf(List<SqlToken> t, ref int p, string val)
        { if (p < t.Count && t[p].Value == val) p++; }
    }

    // ============================================================
    //  INTERNAL QUERY STRUCTURES
    // ============================================================

    public class SqlFunction
    {
        public string Name;
        public List<(string Name, Type Type)> Parameters = new List<(string Name, Type Type)>();
        public Type ReturnType = typeof(object);
        public Func<object[], object> CompiledFunc;
    }

    internal class SelectItem
    {
        public string Name;
        public string Alias;
        public List<SqlToken> ExprTokens;
        public bool IsStar;
        public string TableFilter;  // for table.*
        public bool IsAggregate;
        public string AggFunc;
        public string AggCol;
        public bool AggDistinct;

        public string OutputName => Alias ?? Name ?? "expr";
    }

    internal class ObjectComparer : IComparer<object>
    {
        public static readonly ObjectComparer Instance = new ObjectComparer();
        public int Compare(object a, object b) => SqlExpr.ObjCmp(a, b);
    }

    // ============================================================
    //  TRIGGER STATEMENT AST
    //  Pre-parsed during CREATE TRIGGER so execution is clean
    // ============================================================

    internal abstract class TriggerStmt { }

    /// <summary>SET NEW.col = expr  or  SET OLD.col = expr</summary>
    internal class SetNewOldStmt : TriggerStmt
    {
        public bool IsNew;           // true → NEW, false → OLD
        public string Column;
        public List<SqlToken> Expr;
    }

    /// <summary>IF cond THEN body [ELSEIF cond THEN body]* [ELSE body] END IF</summary>
    internal class IfTriggerStmt : TriggerStmt
    {
        public List<(List<SqlToken> Cond, List<TriggerStmt> Body)> Branches = new List<(List<SqlToken> Cond, List<TriggerStmt> Body)>();
        public List<TriggerStmt> ElseBranch;
    }

    /// <summary>Any DML statement whose NEW./OLD. references are substituted at runtime.</summary>
    internal class DmlTriggerStmt : TriggerStmt
    {
        public List<SqlToken> Tokens;
    }

    internal class SqlTrigger
    {
        public string Name;
        public string TableName;
        public string Timing;                // "BEFORE" | "AFTER"
        public string Event;                 // "INSERT" | "UPDATE" | "DELETE"
        public List<TriggerStmt> Statements; // pre-parsed body
        public string OriginalSql;           // stored for JSON round-trip
    }

    // ============================================================
    //  SQL EXECUTOR  (parser + evaluation engine)
    // ============================================================

    internal class SqlExecutor
    {
        private List<SqlToken> _t;
        private int _p;
        private Dictionary<string, SQLTable> _tables;
        private Dictionary<string, SqlFunction> _functions;
        private Dictionary<string, SqlTrigger> _triggers;
        private Dictionary<string, CommonTableExpression> _ctes;
        public SqlExecutor(List<SqlToken> tokens, Dictionary<string, SQLTable> tables,
                           Dictionary<string, SqlFunction> functions,
                           Dictionary<string, SqlTrigger> triggers)
        { _t = tokens; _p = 0; _tables = tables; _functions = functions; _triggers = triggers;
            _ctes = new Dictionary<string, CommonTableExpression>(StringComparer.OrdinalIgnoreCase);
        }

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
            switch (Cur.Value.ToUpperInvariant())
            {
                case "INSERT": return DoInsert();
                case "UPDATE": return DoUpdate();
                case "DELETE": return DoDelete();
                case "CREATE": return DoCreate();
                case "ALTER": return DoAlter();
                case "DROP": return DoDrop();
                default: throw new InvalidOperationException($"Unsupported statement: '{Cur.Value}'.");
            }
        }

        public object[,] RunReaderOld()
        {
            if (!Cur.Value.Equals("SELECT", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected SELECT, got '{Cur.Value}'.");
            return DoSelect();
        }
        public object[,] RunReader()
        {
            // Parse CTEs if present
            if (Cur.Value.Equals("WITH", StringComparison.OrdinalIgnoreCase))
                ParseWithClause();

            if (!Cur.Value.Equals("SELECT", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected SELECT, got '{Cur.Value}'.");
            return DoSelect();
        }
        private void ParseWithClause()
        {
            Expect("WITH");

            do
            {
                string cteName = Consume().Value;
                var cte = new CommonTableExpression { Name = cteName, Columns = new List<string>() };

                // Optional column list: cte_name (col1, col2, ...)
                if (Cur.Value == "(")
                {
                    _p++;
                    // Check if this is a column list or the SELECT
                    if (Cur.Type == SqlTokenType.Identifier && Peek().Value != "SELECT")
                    {
                        // Column list
                        while (Cur.Value != ")")
                        {
                            cte.Columns.Add(Consume().Value);
                            Match(",");
                        }
                        _p++; // consume )
                    }
                    else
                    {
                        // No column list, rewind
                        _p--;
                    }
                }

                Expect("AS");
                Expect("(");

                // Read SELECT tokens until matching )
                var selectTokens = new List<SqlToken>();
                int depth = 1;
                while (_p < _t.Count && depth > 0)
                {
                    if (_t[_p].Value == "(") depth++;
                    else if (_t[_p].Value == ")") { depth--; if (depth == 0) break; }
                    selectTokens.Add(_t[_p++]);
                }
                Expect(")");

                selectTokens.Add(new SqlToken(SqlTokenType.EOF, ""));
                cte.SelectTokens = selectTokens;
                _ctes[cteName] = cte;

            } while (Match(","));
        }
        // ============================================================
        //  INSERT
        // ============================================================
        private int DoInsert()
        {
            Expect("INSERT"); Expect("INTO");
            string tname = Consume().Value;
            var table = RequireTable(tname);
            string[] colNames = null;

            if (Cur.Value == "(")
            {
                _p++;
                var cols = new List<string>();
                while (Cur.Value != ")")
                { cols.Add(Consume().Value); Match(","); }
                _p++; // )
                colNames = cols.ToArray();
            }

            Expect("VALUES");
            int rows = 0;
            var tableCols = table.Columns;

            while (Cur.Value == "(" || rows == 0)
            {
                if (Cur.Value != "(") break;
                _p++;
                var vals = new List<object>();
                while (Cur.Value != ")")
                {
                    var ctx = MakeCtx(new Dictionary<string, object>());
                    vals.Add(SqlExpr.Eval(_t, ref _p, ctx));
                    Match(",");
                }
                _p++; // )

                object[] fullRow;
                if (colNames != null)
                {
                    fullRow = new object[tableCols.Length];
                    for (int i = 0; i < tableCols.Length; i++)
                    {
                        int ci = Array.FindIndex(colNames, c => c.Equals(tableCols[i], StringComparison.OrdinalIgnoreCase));
                        fullRow[i] = ci >= 0 ? vals[ci] : null;
                    }
                }
                else fullRow = vals.ToArray();

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
                var newRow = new Dictionary<string, object>(oldRow, StringComparer.OrdinalIgnoreCase);
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

            var table = new SQLTable(tname, colDefs.Select(c => c.Name).ToArray());
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
            List<SqlToken> retExpr = null;
            if (ri >= 0)
            {
                retExpr = body.Skip(ri + 1).TakeWhile(x => x.Value != ";").ToList();
                retExpr.Add(new SqlToken(SqlTokenType.EOF, ""));
            }

            var capturedParms = parms.ToList();
            var capturedFuncs = _functions;
            var capturedTables = _tables;

            var fn = new SqlFunction { Name = fname, Parameters = parms, ReturnType = retType };
            if (retExpr != null)
            {
                fn.CompiledFunc = args =>
                {
                    var row = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    for (int i = 0; i < capturedParms.Count && i < args.Length; i++)
                        row[capturedParms[i].Name] = args[i];
                    int ep = 0;
                    var exprCopy = retExpr.ToList();
                    return SqlExpr.Eval(exprCopy, ref ep, new EvalContext { Row = row, Functions = capturedFuncs, Tables = capturedTables });
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
                                  Dictionary<string, object> oldRow,
                                  Dictionary<string, object> newRow,
                                  string[] tableCols)
        {
            if (_triggers.Count == 0) return;

            // Build the trigger evaluation context:  NEW.col  OLD.col  and bare col keys
            var tctx = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
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

        private void ExecuteTriggerStmts(List<TriggerStmt> stmts, Dictionary<string, object> tctx)
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
                            var evalCtx = new EvalContext { Row = tctx, Functions = _functions, Tables = _tables };
                            object val = SqlExpr.Eval(exprCopy, ref ep, evalCtx);
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
                            var resolved = SubstituteNewOld(d.Tokens.ToList(), tctx);
                            resolved.Add(new SqlToken(SqlTokenType.EOF, ""));
                            try { new SqlExecutor(resolved, _tables, _functions, _triggers).RunNonQuery(); }
                            catch { /* Trigger DML errors do not abort the outer statement */ }
                            break;
                        }
                }
            }
        }

        private void ExecuteIfTriggerStmt(IfTriggerStmt ifs, Dictionary<string, object> tctx)
        {
            foreach (var (cond, body) in ifs.Branches)
            {
                var condCopy = cond.ToList();
                condCopy.Add(new SqlToken(SqlTokenType.EOF, ""));
                int ep = 0;
                var evalCtx = new EvalContext { Row = tctx, Functions = _functions, Tables = _tables };
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
        private static List<SqlToken> SubstituteNewOld(List<SqlToken> tokens, Dictionary<string, object> tctx)
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

        private static SqlToken ObjectToToken(object val)
        {
            switch (val)
            {
                case null: return new SqlToken(SqlTokenType.Keyword, "NULL");
                case bool b: return new SqlToken(SqlTokenType.Keyword, b ? "TRUE" : "FALSE");
                case long l: return new SqlToken(SqlTokenType.Number, l.ToString());
                case int i: return new SqlToken(SqlTokenType.Number, i.ToString());
                case double d: return new SqlToken(SqlTokenType.Number, d.ToString(System.Globalization.CultureInfo.InvariantCulture));
                case float f: return new SqlToken(SqlTokenType.Number, f.ToString(System.Globalization.CultureInfo.InvariantCulture));
                case DateTime dt: return new SqlToken(SqlTokenType.StringLiteral, dt.ToString("O"));
                default: return new SqlToken(SqlTokenType.StringLiteral, val.ToString());
            }
        }

        // ── Row dict helper used by trigger fire points ───────────────────

        private static Dictionary<string, object> BuildRowDict(string[] cols, object[] values)
        {
            var d = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
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
        private object[,] DoSelect()
        {
            Expect("SELECT");
            bool distinct = Match("DISTINCT");

            int? topN = null;
            if (Match("TOP"))
            {
                var ctx0 = MakeCtx(new Dictionary<string, object>());
                topN = (int)SqlExpr.ToNum(SqlExpr.Eval(_t, ref _p, ctx0));
            }

            var selectItems = ParseSelectList();

            var fromTables = new List<(string Name, string Alias)>();
            var joins = new List<(string Type, string Name, string Alias, List<SqlToken> On)>();

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

                // JOINs
                while (IsJoin(Cur.Value))
                {
                    string jtype = "INNER";
                    if (IsIn(Cur.Value, "LEFT", "RIGHT", "FULL", "CROSS", "INNER"))
                    { jtype = Consume().Value.ToUpperInvariant(); Match("OUTER"); }
                    Expect("JOIN");
                    string jn = Consume().Value, ja = jn;
                    Match("AS");
                    if (Cur.Type == SqlTokenType.Identifier && !IsClause(Cur.Value)) ja = Consume().Value;
                    List<SqlToken> on = null;
                    if (Match("ON")) on = ReadUntilClause();
                    joins.Add((jtype, jn, ja, on));
                }
            }

            var where = Match("WHERE") ? ReadUntilClause() : null;

            List<string> groupBy = null;
            if (Match("GROUP")) { Expect("BY"); groupBy = ParseCommaSeparatedExprs(); }

            var having = Match("HAVING") ? ReadUntilClause() : null;

            List<(List<SqlToken> Expr, bool Asc)> orderBy = null;
            if (Match("ORDER"))
            {
                Expect("BY");
                orderBy = new List<(List<SqlToken>, bool)>();
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
            { var ctx0 = MakeCtx(new Dictionary<string, object>()); limitN = (int)SqlExpr.ToNum(SqlExpr.Eval(_t, ref _p, ctx0)); }

            int? offsetN = null;
            if (Match("OFFSET"))
            { var ctx0 = MakeCtx(new Dictionary<string, object>()); offsetN = (int)SqlExpr.ToNum(SqlExpr.Eval(_t, ref _p, ctx0)); }

            return ExecSelect(selectItems, fromTables, joins, where, groupBy, having, orderBy, distinct, limitN, offsetN);
        }

        private object[,] ExecSelect(
            List<SelectItem> sel,
            List<(string Name, string Alias)> froms,
            List<(string Type, string Name, string Alias, List<SqlToken> On)> joins,
            List<SqlToken> where,
            List<string> groupBy,
            List<SqlToken> having,
            List<(List<SqlToken> Expr, bool Asc)> orderBy,
            bool distinct, int? limit, int? offset)
        {
            // ── Materialize CTEs ──────────────────────────────────────────────
            foreach (var cte in _ctes.Values)
            {
                // Execute the CTE's SELECT query
                var cteExecutor = new SqlExecutor(cte.SelectTokens, _tables, _functions, _triggers);
                object[,] cteResult = cteExecutor.RunReader();

                // Create a temporary table from the result
                int cols = cteResult.GetLength(1);

                string[] columnNames;
                if (cte.Columns.Count > 0)
                    columnNames = cte.Columns.ToArray();
                else
                    columnNames = Enumerable.Range(0, cols).Select(i => cteResult[0, i].ToString()).ToArray();

                var tempTable = new SQLTable(cte.Name, columnNames);

                // Populate the temp table
                for (int r = 1; r < cteResult.GetLength(0); r++)
                {
                    var row = new object[cols];
                    for (int c = 0; c < cols; c++)
                        row[c] = cteResult[r, c];
                    tempTable.AddRow(row);
                }

                // Add to tables for this query execution
                _tables[cte.Name] = tempTable;
            }
            // ── Build row set ─────────────────────────────────────────────
            List<Dictionary<string, object>> rows;

            if (froms.Count == 0)
            {
                rows = new List<Dictionary<string, object>> { new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase) };
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
                rows = rows.Where(r => PassesWhere(where, r)).ToList();

            // ── GROUP BY / aggregates ─────────────────────────────────────
            bool hasAgg = sel.Any(s => s.IsAggregate);
            if (hasAgg || groupBy != null)
                rows = ApplyGroupBy(rows, sel, groupBy, having);
            else if (having != null)
                rows = rows.Where(r => PassesWhere(having, r)).ToList();

            // ── ORDER BY ──────────────────────────────────────────────────
            if (orderBy != null && orderBy.Count > 0)
                rows = ApplyOrderBy(rows, orderBy);

            // ── OFFSET / LIMIT ────────────────────────────────────────────
            if (offset.HasValue) rows = rows.Skip(offset.Value).ToList();
            if (limit.HasValue) rows = rows.Take(limit.Value).ToList();

            // ── DISTINCT ──────────────────────────────────────────────────
            if (distinct) rows = ApplyDistinct(rows, sel);

            return BuildResult(rows, sel, froms);
        }

        // ── Join helpers ─────────────────────────────────────────────────

        private List<Dictionary<string, object>> TableRows(SQLTable table, string alias)
        {
            var cols = table.Columns;
            var result = new List<Dictionary<string, object>>(table.Count);
            for (int r = 0; r < table.Count; r++)
            {
                var d = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var c in cols)
                {
                    object v = table.GetValue(r, c);
                    d[$"{alias}.{c}"] = v;
                    if (!d.ContainsKey(c)) d[c] = v; // unqualified wins for first table
                }
                result.Add(d);
            }
            return result;
        }

        private static List<Dictionary<string, object>> CrossJoin(
            List<Dictionary<string, object>> left, List<Dictionary<string, object>> right)
        {
            var res = new List<Dictionary<string, object>>(left.Count * right.Count);
            foreach (var l in left)
                foreach (var r in right)
                {
                    var m = new Dictionary<string, object>(l, StringComparer.OrdinalIgnoreCase);
                    foreach (var kv in r) m[kv.Key] = kv.Value;
                    res.Add(m);
                }
            return res;
        }

        private List<Dictionary<string, object>> ApplyJoin(
            List<Dictionary<string, object>> left,
            List<Dictionary<string, object>> right,
            string jtype, List<SqlToken> cond)
        {
            var res = new List<Dictionary<string, object>>();
            var matchedRight = new HashSet<int>();

            for (int l = 0; l < left.Count; l++)
            {
                bool matched = false;
                for (int r = 0; r < right.Count; r++)
                {
                    var m = new Dictionary<string, object>(left[l], StringComparer.OrdinalIgnoreCase);
                    foreach (var kv in right[r]) m[kv.Key] = kv.Value;

                    bool pass = cond == null || PassesWhere(cond, m);
                    if (pass) { res.Add(m); matched = true; matchedRight.Add(r); }
                }

                if (!matched && (jtype == "LEFT" || jtype == "FULL"))
                {
                    var nul = new Dictionary<string, object>(left[l], StringComparer.OrdinalIgnoreCase);
                    if (right.Count > 0) foreach (var k in right[0].Keys) nul[k] = null;
                    res.Add(nul);
                }
            }

            if (jtype == "RIGHT" || jtype == "FULL")
            {
                for (int r = 0; r < right.Count; r++)
                {
                    if (matchedRight.Contains(r)) continue;
                    var nul = new Dictionary<string, object>(right[r], StringComparer.OrdinalIgnoreCase);
                    if (left.Count > 0) foreach (var k in left[0].Keys) if (!nul.ContainsKey(k)) nul[k] = null;
                    res.Add(nul);
                }
            }

            return res;
        }

        // ── GROUP BY ─────────────────────────────────────────────────────

        private List<Dictionary<string, object>> ApplyGroupBy(
            List<Dictionary<string, object>> rows,
            List<SelectItem> sel,
            List<string> groupByCols,
            List<SqlToken> having)
        {
            Func<Dictionary<string, object>, string> key;
            if (groupByCols == null || groupByCols.Count == 0)
                key = _ => "";
            else
                key = row => string.Join("\x00", groupByCols.Select(gc =>
                {
                    var toks = SqlTokenizer.Tokenize(gc);
                    int ep = 0;
                    return SqlExpr.Eval(toks, ref ep, MakeCtx(row))?.ToString() ?? "NULL";
                }));

            var res = new List<Dictionary<string, object>>();
            foreach (var grp in rows.GroupBy(key))
            {
                var grpList = grp.ToList();
                var outRow = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                // Compute each select item over group
                foreach (var item in sel)
                {
                    if (item.IsStar) continue;
                    object val = item.IsAggregate
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

        private object ComputeAggregate(string func, string col, List<Dictionary<string, object>> rows, bool distinct)
        {
            Func<Dictionary<string, object>, object> get = row =>
            {
                if (col == "*") return 1L;
                var toks = SqlTokenizer.Tokenize(col);
                int ep = 0;
                return SqlExpr.Eval(toks, ref ep, MakeCtx(row));
            };

            var vals = rows.Select(get).ToList();
            if (distinct) vals = vals.Distinct(new ObjEqualityComparer()).ToList();

            switch (func.ToUpperInvariant())
            {
                case "COUNT": return (object)(long)(col == "*" ? rows.Count : vals.Count(v => v != null));
                case "SUM": return vals.Where(v => v != null).Select(SqlExpr.ToNum).DefaultIfEmpty(0).Sum();
                case "AVG": return vals.Where(v => v != null).Select(SqlExpr.ToNum).DefaultIfEmpty(0).Average();
                case "MIN": return vals.Where(v => v != null).OrderBy(v => v, ObjectComparer.Instance).FirstOrDefault();
                case "MAX": return vals.Where(v => v != null).OrderByDescending(v => v, ObjectComparer.Instance).FirstOrDefault();
                default: return null;
            }
        }

        private class ObjEqualityComparer : IEqualityComparer<object>
        {
            public new bool Equals(object a, object b) => SqlExpr.ObjEq(a, b);
            public int GetHashCode(object o) => o?.ToString()?.ToUpperInvariant()?.GetHashCode() ?? 0;
        }

        // ── ORDER BY ─────────────────────────────────────────────────────

        private List<Dictionary<string, object>> ApplyOrderBy(
            List<Dictionary<string, object>> rows,
            List<(List<SqlToken> Expr, bool Asc)> orderBy)
        {
            if (rows.Count == 0) return rows;
            IOrderedEnumerable<Dictionary<string, object>> ordered = null;

            for (int i = 0; i < orderBy.Count; i++)
            {
                var exprToks = orderBy[i].Expr.ToList();
                exprToks.Add(new SqlToken(SqlTokenType.EOF, ""));
                bool asc = orderBy[i].Asc;

                Func<Dictionary<string, object>, object> kf = row =>
                { int ep = 0; return SqlExpr.Eval(exprToks, ref ep, MakeCtx(row)); };

                ordered = i == 0
                    ? (asc ? rows.OrderBy(kf, ObjectComparer.Instance) : rows.OrderByDescending(kf, ObjectComparer.Instance))
                    : (asc ? ordered.ThenBy(kf, ObjectComparer.Instance) : ordered.ThenByDescending(kf, ObjectComparer.Instance));
            }

            return ordered?.ToList() ?? rows;
        }

        // ── DISTINCT ─────────────────────────────────────────────────────

        private List<Dictionary<string, object>> ApplyDistinct(List<Dictionary<string, object>> rows, List<SelectItem> sel)
        {
            var seen = new HashSet<string>();
            var res = new List<Dictionary<string, object>>();
            foreach (var row in rows)
            {
                var k = string.Join("\x00", sel.Where(s => !s.IsStar)
                    .Select(s => row.TryGetValue(s.OutputName, out var v) ? v?.ToString() ?? "NULL" : "NULL"));
                if (seen.Add(k)) res.Add(row);
            }
            return res;
        }

        // ── Result building ──────────────────────────────────────────────

        private object[,] BuildResult(List<Dictionary<string, object>> rows, List<SelectItem> sel,
                                       List<(string Name, string Alias)> froms = null)
        {
            // Expand * items
            var finalItems = new List<SelectItem>();
            foreach (var item in sel)
            {
                if (!item.IsStar) { finalItems.Add(item); continue; }

                if (rows.Count == 0)
                {
                    // No rows — resolve column names directly from the table schema
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

                // Preserve table column order: iterate qualified keys to get order
                var seenCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var k in firstRow.Keys)
                {
                    if (!k.Contains('.')) continue; // qualified keys drive order
                    string colPart = k.Substring(k.IndexOf('.') + 1);
                    string tablePart = k.Substring(0, k.IndexOf('.'));

                    // Filter for table.*
                    if (item.TableFilter != null && !tablePart.Equals(item.TableFilter, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (!seenCols.Add(colPart)) continue;

                    finalItems.Add(new SelectItem
                    {
                        Name = colPart,
                        ExprTokens = SqlTokenizer.Tokenize(colPart)
                    });
                }
            }

            if (finalItems.Count == 0 && rows.Count > 0)
            {
                // Fallback: all unqualified keys
                var seenCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var k in rows[0].Keys)
                {
                    if (k.Contains('.') || !seenCols.Add(k)) continue;
                    finalItems.Add(new SelectItem { Name = k, ExprTokens = SqlTokenizer.Tokenize(k) });
                }
            }

            int numCols = finalItems.Count;
            var result = new object[rows.Count + 1, numCols];

            // Header row
            for (int c = 0; c < numCols; c++)
                result[0, c] = finalItems[c].OutputName;

            // Data rows
            for (int r = 0; r < rows.Count; r++)
            {
                for (int c = 0; c < numCols; c++)
                {
                    var item = finalItems[c];
                    // Pre-computed (aggregate) value
                    if (rows[r].TryGetValue(item.OutputName, out var pre)) { result[r + 1, c] = pre; continue; }

                    // Evaluate expression
                    if (item.ExprTokens != null)
                    {
                        var exprCopy = item.ExprTokens.ToList();
                        exprCopy.Add(new SqlToken(SqlTokenType.EOF, ""));
                        int ep = 0;
                        result[r + 1, c] = SqlExpr.Eval(exprCopy, ref ep, MakeCtx(rows[r]));
                    }
                    else
                    {
                        rows[r].TryGetValue(item.Name ?? "", out result[r + 1, c]);
                    }
                }
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
                    string alias = null;
                    if (Match("AS")) alias = Consume().Value;
                    else if (Cur.Type == SqlTokenType.Identifier && !IsClause(Cur.Value)) alias = Consume().Value;
                    items.Add(BuildSelectItem(exprToks, alias));
                }
                Match(",");
            }
            return items;
        }

        private SelectItem BuildSelectItem(List<SqlToken> toks, string alias)
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
                switch (fn)
                {
                    case "COUNT":
                    case "SUM":
                    case "AVG":
                    case "MIN":
                    case "MAX":
                        {
                            item.IsAggregate = true;
                            item.AggFunc = fn;
                            var inner = toks.Skip(2).TakeWhile(x => x.Value != ")").ToList();
                            if (inner.Count > 0 && inner[0].Value.Equals("DISTINCT", StringComparison.OrdinalIgnoreCase))
                            { item.AggDistinct = true; inner = inner.Skip(1).ToList(); }
                            item.AggCol = inner.Count == 0 || inner[0].Value == "*" ? "*"
                                        : string.Join(" ", inner.Select(x => x.Value));
                            if (toks.Count >= 3 && toks[2].Value == "*") item.AggCol = "*";
                            if (alias == null) item.Alias = $"{fn}({item.AggCol})";
                            break;
                        }
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

        private bool PassesWhere(List<SqlToken> toks, Dictionary<string, object> row)
        {
            if (toks == null) return true;
            int wp = 0; var exprCopy = toks.ToList();
            return SqlExpr.AsBool(SqlExpr.Eval(exprCopy, ref wp, MakeCtx(row)));
        }

        private object EvalExpr(List<SqlToken> toks, Dictionary<string, object> row)
        {
            if (toks == null) return null;
            var exprCopy = toks.ToList();
            exprCopy.Add(new SqlToken(SqlTokenType.EOF, ""));
            int ep = 0;
            return SqlExpr.Eval(exprCopy, ref ep, MakeCtx(row));
        }

        private EvalContext MakeCtx(Dictionary<string, object> row) =>
            new EvalContext { Row = row, Functions = _functions, Tables = _tables };

        private SQLTable RequireTable(string name)
        {
            if (_tables.TryGetValue(name, out var t)) return t;
            throw new KeyNotFoundException($"Table '{name}' not found.");
        }

        private static Dictionary<string, object> RowDict(SQLTable table, int ri, string alias, string[] cols)
        {
            var d = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in cols)
            {
                object v = table.GetValue(ri, c);
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

        internal static Type SqlTypeToClr(string sqlType)
        {
            switch (sqlType.ToUpperInvariant())
            {
                case "INT":
                case "INTEGER":
                case "SMALLINT":
                case "TINYINT":
                case "BIGINT": return typeof(long);
                case "FLOAT":
                case "REAL":
                case "DOUBLE":
                case "DECIMAL":
                case "NUMERIC":
                case "MONEY": return typeof(double);
                case "BIT":
                case "BOOL":
                case "BOOLEAN": return typeof(bool);
                case "DATETIME":
                case "DATE":
                case "TIME":
                case "DATETIME2":
                case "SMALLDATETIME": return typeof(DateTime);
                case "UNIQUEIDENTIFIER": return typeof(Guid);
                default: return typeof(string);
            }
        }
    }

    // ============================================================
    //  PUBLIC API
    // ============================================================
    // Updated SQLDatabase.cs to add WITH (CTE) support

    // Adding 'WITH' keyword to tokenizer Keywords set
    // Modifying SqlExecutor to parse CTE definitions before SELECT and create temporary tables from them
    // Adding ParseCTE method to handle CTE syntax and a method to handle CTE references in FROM clauses

    // Example Implementation
    public class SQLDatabase
    {
        private readonly Dictionary<string, SQLTable> Tables =
            new Dictionary<string, SQLTable>(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, SqlFunction> Functions =
            new Dictionary<string, SqlFunction>(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, SqlTrigger> Triggers =
            new Dictionary<string, SqlTrigger>(StringComparer.OrdinalIgnoreCase);

        /// <summary>Add a pre-built table to the database.</summary>
        public void AddTable(SQLTable table)
        {
            if (Tables.ContainsKey(table.Name))
                throw new DuplicateNameException($"Table '{table.Name}' already exists.");
            Tables.Add(table.Name, table);
        }

        /// <summary>
        /// Execute INSERT / UPDATE / DELETE / CREATE / ALTER / DROP.
        /// Returns number of rows affected (0 for DDL).
        /// </summary>
        public int ExecuteNonQuery(string sql)
        {
            var trimmed = sql.Trim();
            var tokens = SqlTokenizer.Tokenize(trimmed);
            int result = new SqlExecutor(tokens, Tables, Functions, Triggers).RunNonQuery();

            // After a successful CREATE TRIGGER, store the original SQL for JSON serialization.
            if (tokens.Count > 2 &&
                tokens[0].Value.Equals("CREATE", StringComparison.OrdinalIgnoreCase) &&
                tokens[1].Value.Equals("TRIGGER", StringComparison.OrdinalIgnoreCase))
            {
                string trName = tokens[2].Value;
                if (Triggers.TryGetValue(trName, out var tr) && tr.OriginalSql == null)
                    tr.OriginalSql = trimmed;
            }

            return result;
        }

        /// <summary>
        /// Execute a SELECT query.
        /// Returns a 2-D array where row 0 contains column headers (strings)
        /// and rows 1..N contain the data values.
        /// </summary>
        public object[,] ExecuteReader(string sql)
        {
            var tokens = SqlTokenizer.Tokenize(sql.Trim());
            return new SqlExecutor(tokens, Tables, Functions, Triggers).RunReader();
        }


        /// <summary>
        /// Execute a SELECT and return the value of the first column of the first row,
        /// or null if no rows were returned.  Wraps ExecuteNonQuery for DML statements,
        /// returning the row-count as a boxed long.
        /// </summary>
        public object ExecuteScalar(string sql)
        {
            string verb = sql.TrimStart().Split(new[] { ' ', '\t', '\r', '\n' }, 2)[0].ToUpperInvariant();
            if (verb == "SELECT")
            {
                var grid = ExecuteReader(sql);
                return grid.GetLength(0) > 1 && grid.GetLength(1) > 0 ? grid[1, 0] : null;
            }
            return (object)(long)ExecuteNonQuery(sql);
        }

 
        // ── JSON Serialization ────────────────────────────────────────────

        /// <summary>
        /// Serializes every table in the database to a JSON string.
        /// User-defined functions (compiled delegates) are not serialized.
        /// <para>Format:</para>
        /// <code>
        /// {
        ///   "tables": [
        ///     { "name": "...", "columns": [...], "rows": [...] },
        ///     ...
        ///   ]
        /// }
        /// </code>
        /// </summary>
        /// <param name="indented">Pretty-print the JSON when <c>true</c> (default).</param>
        public string ToJson(bool indented = true)
        {
            var opts = new JsonWriterOptions { Indented = indented };
            using (var ms = new System.IO.MemoryStream())
            {
                using (var w = new Utf8JsonWriter(ms, opts))
                {

                    w.WriteStartObject();

                    // ── tables ───────────────────────────────────────────────────
                    w.WriteStartArray("tables");
                    foreach (var table in Tables.Values)
                    {
                        using (var tableDoc = JsonDocument.Parse(table.ToJson(indented: false)))
                        {
                            tableDoc.RootElement.WriteTo(w);
                        }
                    }
                    w.WriteEndArray();

                    // ── triggers  (stored as original CREATE TRIGGER SQL strings) ─
                    w.WriteStartArray("triggers");
                    foreach (var trigger in Triggers.Values)
                    {
                        if (trigger.OriginalSql == null) continue;
                        w.WriteStartObject();
                        w.WriteString("name", trigger.Name);
                        w.WriteString("sql", trigger.OriginalSql);
                        w.WriteEndObject();
                    }
                    w.WriteEndArray();

                    w.WriteEndObject();
                    w.Flush();
                    return Encoding.UTF8.GetString(ms.ToArray());
                }
            }
        }

        /// <summary>
        /// Saves the serialized database to a file at <paramref name="path"/>.
        /// </summary>
        public void SaveJson(string path, bool indented = true)
            => System.IO.File.WriteAllText(path, ToJson(indented), Encoding.UTF8);

        /// <summary>
        /// Deserializes a JSON string produced by <see cref="ToJson"/> into a new <see cref="SQLDatabaseInMemory"/>.
        /// All tables and their data are restored.  User-defined functions are not restored
        /// (re-run their <c>CREATE FUNCTION</c> statements if needed).
        /// </summary>
        /// <exception cref="JsonException">The JSON is malformed or missing required properties.</exception>
        public static SQLDatabase FromJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json)) throw new ArgumentNullException(nameof(json));

            var db = new SQLDatabase();
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;

                // Restore tables
                foreach (var tableEl in root.GetProperty("tables").EnumerateArray())
                    db.AddTable(SQLTable.FromJson(tableEl.GetRawText()));

                // Restore triggers by re-executing their original CREATE TRIGGER SQL
                if (root.TryGetProperty("triggers", out var triggersEl))
                {
                    foreach (var trigEl in triggersEl.EnumerateArray())
                    {
                        string sql = trigEl.GetProperty("sql").GetString();
                        if (!string.IsNullOrWhiteSpace(sql))
                            db.ExecuteNonQuery(sql);
                    }
                }
            }
            return db;
        }

        /// <summary>
        /// Loads a database from a JSON file previously written by <see cref="SaveJson"/>.
        /// </summary>
        public static SQLDatabase LoadJson(string path)
            => FromJson(System.IO.File.ReadAllText(path, Encoding.UTF8));

        /// <summary>
        /// Merges tables and triggers from a JSON string into this existing database.
        /// Existing tables/triggers are skipped unless <paramref name="overwrite"/> is <c>true</c>.
        /// </summary>
        public void MergeJson(string json, bool overwrite = false)
        {
            if (string.IsNullOrWhiteSpace(json)) throw new ArgumentNullException(nameof(json));

            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;

                // Merge tables
                foreach (var tableEl in root.GetProperty("tables").EnumerateArray())
                {
                    var table = SQLTable.FromJson(tableEl.GetRawText());
                    if (Tables.ContainsKey(table.Name))
                    {
                        if (!overwrite) continue;
                        Tables.Remove(table.Name);
                    }
                    Tables[table.Name] = table;
                }

                // Merge triggers
                if (root.TryGetProperty("triggers", out var triggersEl))
                {
                    foreach (var trigEl in triggersEl.EnumerateArray())
                    {
                        string name = trigEl.GetProperty("name").GetString();
                        string sql = trigEl.GetProperty("sql").GetString();
                        if (Triggers.ContainsKey(name) && !overwrite) continue;
                        if (!string.IsNullOrWhiteSpace(sql))
                            ExecuteNonQuery(sql);
                    }
                }
            }
        }
    }

    public static class SQLHelpers
    {
        /// <summary>Returns the value at (row, col) from a reader result (1-based for data rows).</summary>
        public static object Cell(object[,] grid, int dataRow, int col) => grid[dataRow, col];

        /// <summary>Number of data rows (excludes header row 0).</summary>
        public static int DataRows(object[,] grid) => grid.GetLength(0) - 1;

        /// <summary>Column count.</summary>
        public static int ColCount(object[,] grid) => grid.GetLength(1);

        /// <summary>Header string for a column index.</summary>
        public static string Header(object[,] grid, int col) => grid[0, col]?.ToString();
    }

}