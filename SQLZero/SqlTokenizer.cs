using System.Text;
namespace SQLZero
{
    public static class SqlTokenizer
    {
        private static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
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
        "TRUE","FALSE","CAST","CONVERT","UNION","INTERSECT","EXCEPT","IDENTITY","AUTO_INCREMENT"
    };

        public static List<SqlToken> Tokenize(string? sql)
        {
            sql ??= string.Empty;
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
}