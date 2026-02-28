using System.Data;
using System.Text;
using System.Text.RegularExpressions;
namespace SQLZero
{
    public static class SqlExpr
    {
        // ── Entry point ──────────────────────────────────────────────────
        public static object? Eval(List<SqlToken> tokens, ref int pos, EvalContext? ctx)
            => ParseOr(tokens, ref pos, ctx);

        // ── Boolean layers ───────────────────────────────────────────────
        private static object? ParseOr(List<SqlToken> t, ref int p, EvalContext? ctx)
        {
            var left = ParseAnd(t, ref p, ctx);
            while (Is(t, p, "OR")) { p++; left = AsBool(left) | AsBool(ParseAnd(t, ref p, ctx)); }
            return left;
        }

        private static object? ParseAnd(List<SqlToken> t, ref int p, EvalContext? ctx)
        {
            var left = ParseNot(t, ref p, ctx);
            while (Is(t, p, "AND")) { p++; left = AsBool(left) & AsBool(ParseNot(t, ref p, ctx)); }
            return left;
        }

        private static object? ParseNot(List<SqlToken> t, ref int p, EvalContext? ctx)
        {
            if (Is(t, p, "NOT")) { p++; return !AsBool(ParseComparison(t, ref p, ctx)); }
            return ParseComparison(t, ref p, ctx);
        }

        // ── Comparisons ──────────────────────────────────────────────────
        private static object? ParseComparison(List<SqlToken> t, ref int p, EvalContext? ctx)
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
                var vals = new List<object?>();
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
                    return op switch
                    {
                        "=" => ObjEq(left, right),
                        "<>" or "!=" => !ObjEq(left, right),
                        "<" => cmp < 0,
                        ">" => cmp > 0,
                        "<=" => cmp <= 0,
                        ">=" => cmp >= 0,
                        _ => false
                    };
                }
            }
            return left;
        }

        // ── Arithmetic ───────────────────────────────────────────────────
        private static object? ParseAddSub(List<SqlToken> t, ref int p, EvalContext? ctx)
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

        private static object? ParseMulDiv(List<SqlToken> t, ref int p, EvalContext? ctx)
        {
            var left = ParseUnary(t, ref p, ctx);
            while (p < t.Count && t[p].Type == SqlTokenType.Operator && (t[p].Value == "*" || t[p].Value == "/" || t[p].Value == "%"))
            {
                string op = t[p++].Value;
                var right = ParseUnary(t, ref p, ctx);
                left = op switch
                {
                    "*" => ToNum(left) * ToNum(right),
                    "/" => ToNum(right) == 0 ? throw new DivideByZeroException() : ToNum(left) / ToNum(right),
                    "%" => ToNum(left) % ToNum(right),
                    _ => left
                };
            }
            return left;
        }

        private static object? ParseUnary(List<SqlToken> t, ref int p, EvalContext? ctx)
        {
            if (p < t.Count && t[p].Type == SqlTokenType.Operator && t[p].Value == "-")
            { p++; return -ToNum(ParsePrimary(t, ref p, ctx)); }
            return ParsePrimary(t, ref p, ctx);
        }

        // ── Primary ──────────────────────────────────────────────────────
        private static object? ParsePrimary(List<SqlToken> t, ref int p, EvalContext? ctx)
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
                    var args = new List<object?>();
                    bool isStar = false;

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
                            if (Is(t, p, "DISTINCT")) { p++; continue; }
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
                    return ctx?.GetColumn($"{tbl}.{col}") ?? ctx?.GetColumn(col);
                }

                // Column reference
                p++;
                return ctx?.GetColumn(tok.Value);
            }

            p++; return null;
        }

        private static object? ParseCase(List<SqlToken> t, ref int p, EvalContext? ctx)
        {
            bool isSearched = Is(t, p, "WHEN");
            object? pivot = isSearched ? null : Eval(t, ref p, ctx);
            object? result = null;
            bool matched = false;

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
        private static object? CallBuiltin(string name, List<object?> args, bool isStar, EvalContext? ctx)
        {
            // User-defined SQL functions (CREATE FUNCTION)
            if (ctx?.Functions != null && ctx.Functions.TryGetValue(name, out var udf) && udf.CompiledFunc != null)
                return udf.CompiledFunc([.. args]);

            // User add-ins — resolved BEFORE built-ins so they can override if needed
            if (ctx?.AddIns != null && ctx.AddIns.TryGetValue(name, out var addIn))
                return CallUserAddIn(addIn, args);

            switch (name.ToUpperInvariant())
            {
                // Aggregates (per-row placeholders; actual aggregation happens in GroupBy)
                case "COUNT": return isStar ? 1L : (args.Count > 0 && args[0] != null ? 1L : 0L);
                case "SUM": return args.Count > 0 ? ToNum(args[0]) : null;
                case "AVG": return args.Count > 0 ? ToNum(args[0]) : null;
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
                        return string.Join(sep, args.Skip(1).Where(a => a != null).Select(a => a?.ToString())); //a is never null
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
                            : s[st..];
                    }
                case "LEFT":
                    {
                        if (args.Count < 2) return null;
                        string s = args[0]?.ToString() ?? "";
                        int n = Math.Min((int)ToNum(args[1]), s.Length);
                        return n <= 0 ? "" : s[..n];
                    }
                case "RIGHT":
                    {
                        if (args.Count < 2) return null;
                        string s = args[0]?.ToString() ?? "";
                        int n = Math.Min((int)ToNum(args[1]), s.Length);
                        return n <= 0 ? "" : s[^n..];
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
                    var a = args.Count > 0 ? args[0]?.ToString() : null;
                    return string.IsNullOrEmpty(a) ? null : (long)a[0];                    
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
                        if (args.Count < 3 || args[1] is not DateTime d1 || args[2] is not DateTime d2) return null;
                        string part = args[0]?.ToString()?.ToUpperInvariant() ?? "DAY";
                        return part switch
                        {
                            "YEAR" => (object)(long)(d2.Year - d1.Year),
                            "MONTH" => (long)((d2.Year - d1.Year) * 12 + d2.Month - d1.Month),
                            "DAY" => (long)(d2 - d1).TotalDays,
                            "HOUR" => (long)(d2 - d1).TotalHours,
                            "MINUTE" => (long)(d2 - d1).TotalMinutes,
                            "SECOND" => (long)(d2 - d1).TotalSeconds,
                            _ => (long)(d2 - d1).TotalDays
                        };
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

        /// <summary>
        /// Dispatches a call to a user-registered <see cref="ISqlAddIn"/>.
        /// Separated from <c>CallBuiltin</c> so the call site is clear in stack traces.
        /// </summary>
        private static object? CallUserAddIn(ISqlAddIn addIn, List<object?> args)
            => addIn.Invoke([.. args]);

        private static object? CastValue(object? val, string? sqlType)
        {
            if (val == null || sqlType == null) return null;
            return sqlType.ToUpperInvariant() switch
            {
                "INT" or "INTEGER" or "BIGINT" or "SMALLINT" or "TINYINT"
                    => (object)Convert.ToInt64(val),
                "FLOAT" or "DOUBLE" or "REAL" or "DECIMAL" or "NUMERIC"
                    => Convert.ToDouble(val),
                "BIT" or "BOOL" or "BOOLEAN"
                    => AsBool(val),
                "VARCHAR" or "NVARCHAR" or "CHAR" or "TEXT" or "NTEXT" or "STRING"
                    => val.ToString(),
                "DATETIME" or "DATE"
                    => DateTime.TryParse(val.ToString(), out var dt) ? (object)dt : val,
                _ => val
            };
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
        internal static bool AsBool(object? v)
        {
            if (v == null) return false;
            if (v is bool b) return b;
            if (v is long l) return l != 0;
            if (v is double d) return d != 0;
            if (v is int i) return i != 0;
            if (v is string s) return s.Length > 0;
            return true;
        }

        public static double ToNum(object? v)
        {
            return v switch
            {
                null => 0,
                double d => d,
                long l => l,
                int i => i,
                float f => f,
                decimal m => (double)m,
                bool b => b ? 1 : 0,
                _ => double.TryParse(v.ToString(),
                         System.Globalization.NumberStyles.Any,
                         System.Globalization.CultureInfo.InvariantCulture, out double r) ? r : 0
            };
        }

        internal static bool ObjEq(object? a, object? b)
        {
            if (a == null && b == null) return true;
            if (a == null || b == null) return false;
            if (a is string sa && b is string sb2) return sa.Equals(sb2, StringComparison.OrdinalIgnoreCase);
            if (IsNum(a) && IsNum(b)) return ToNum(a) == ToNum(b);
            return a.Equals(b);
        }

        internal static int ObjCmp(object? a, object? b)
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
}