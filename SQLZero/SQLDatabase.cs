using System.Data;
using System.Text;
using System.Text.Json;
namespace SQLZero
{
    // ============================================================
    //  PUBLIC API
    // ============================================================
    public class SQLDatabase
    {
        private readonly Dictionary<string, SQLTable> Tables =
            new(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, SqlFunction> Functions =
            new(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, SqlTrigger> Triggers = new(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<string, ISqlAddIn> AddIns = new(StringComparer.OrdinalIgnoreCase);

        private static readonly char[] separators = [' ', '\t', '\r', '\n'];

        // ── Add-in registration ───────────────────────────────────────────

        /// <summary>
        /// Registers an <see cref="ISqlAddIn"/> so its <see cref="ISqlAddIn.FunctionName"/>
        /// can be called from any SQL expression evaluated by this database instance.
        /// Add-ins are resolved before the built-in function table.
        /// </summary>
        public void RegisterAddIn(ISqlAddIn addIn)
        {
            ArgumentNullException.ThrowIfNull(addIn);
            AddIns[addIn.FunctionName] = addIn;
        }

        /// <summary>
        /// Registers a delegate as a named SQL add-in function.
        /// Equivalent to implementing <see cref="ISqlAddIn"/> inline.
        /// </summary>
        /// <param name="name">SQL function name (case-insensitive).</param>
        /// <param name="fn">Delegate to invoke. Receives evaluated SQL arguments; may return <c>null</c>.</param>
        public void RegisterAddIn(string name, Func<object?[], object?> fn)
        {
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(fn);
            AddIns[name] = new DelegateAddIn(name, fn);
        }

        /// <summary>Removes a previously registered add-in by name.</summary>
        public bool UnregisterAddIn(string name) => AddIns.Remove(name);

        /// <summary>Returns all currently registered add-in names.</summary>
        public IReadOnlyCollection<string> RegisteredAddIns => AddIns.Keys;

        /// <summary>Internal shim that wraps a plain delegate as an <see cref="ISqlAddIn"/>.</summary>
        private sealed class DelegateAddIn(string name, Func<object?[], object?> fn) : ISqlAddIn
        {
            public string FunctionName => name;
            public object? Invoke(object?[] args) => fn(args);
        }

        /// <summary>Add a pre-built table to the database.</summary>
        public void AddTable(SQLTable table)
        {
            if (!Tables.TryAdd(table.Name, table))
                throw new DuplicateNameException($"Table '{table.Name}' already exists.");
        }

        /// <summary>
        /// Execute INSERT / UPDATE / DELETE / CREATE / ALTER / DROP.
        /// Returns number of rows affected (0 for DDL).
        /// </summary>
        public int ExecuteNonQuery(string sql)
        {
            var trimmed = sql.Trim();
            var tokens = SqlTokenizer.Tokenize(trimmed);
            int result = new SqlExecutor(tokens, Tables, Functions, Triggers, AddIns).RunNonQuery();

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
        /// Asynchronously executes a non-query SQL statement on a thread-pool thread.
        /// Returns number of rows affected (0 for DDL).
        /// </summary>
        /// <remarks>
        /// Any <see cref="ISqlAddIn"/> implementations that perform blocking I/O
        /// will safely run on the thread-pool thread rather than the calling thread.
        /// </remarks>
        public Task<int> ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken = default)
            => Task.Run(() => ExecuteNonQuery(sql), cancellationToken);

        /// <summary>
        /// Execute a SELECT query.
        /// Returns a 2-D array where row 0 contains column headers (strings)
        /// and rows 1..N contain the data values.
        /// </summary>
        public object?[,] ExecuteReader(string sql)
        {
            var tokens = SqlTokenizer.Tokenize(sql.Trim());
            return new SqlExecutor(tokens, Tables, Functions, Triggers, AddIns).RunReader();
        }

        /// <summary>
        /// Asynchronously executes a SELECT query on a thread-pool thread and returns the
        /// column headers together with a lazily-streamed sequence of data rows.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>Execution model:</b> all row evaluation happens on a single thread-pool thread
        /// (via <see cref="Task.Run"/>). The returned <see cref="IAsyncEnumerable{T}"/> streams
        /// rows that are already in memory — the async surface exists so callers can
        /// <c>await foreach</c> and interleave other async work between rows.
        /// </para>
        /// <para>
        /// <b>Cancellation:</b> <paramref name="ct"/> is checked between each row evaluation.
        /// An add-in that is already executing within a row will run to completion before the
        /// check fires. To abandon a truly stuck add-in, cancel the token and discard the task —
        /// the thread-pool thread will eventually observe the cancellation at the next row boundary.
        /// </para>
        /// <para>
        /// <b>Ordering / grouping:</b> queries with <c>ORDER BY</c>, <c>GROUP BY</c>, or
        /// <c>DISTINCT</c> must materialise all rows before the first result can be yielded.
        /// </para>
        /// </remarks>
        /// <param name="sql">The SELECT statement to execute.</param>
        /// <param name="ct">Token used to cancel the compute phase between row evaluations.</param>
        /// <returns>
        /// A tuple of:
        /// <list type="bullet">
        ///   <item><description><c>Headers</c> — column names in SELECT list order.</description></item>
        ///   <item><description><c>Rows</c> — async sequence of data rows; each element is
        ///   an <c>object?[]</c> aligned with <c>Headers</c>.</description></item>
        /// </list>
        /// </returns>
        public async Task<(string[] Headers, IAsyncEnumerable<object?[]> Rows)> ExecuteReaderAsync(
            string sql,
            CancellationToken ct = default)
        {
            var tokens = SqlTokenizer.Tokenize(sql.Trim());

            // Full compute on the thread pool — cancellable between row evaluations.
            var (headers, rows) = await Task.Run(
                () => new SqlExecutor(tokens, Tables, Functions, Triggers, AddIns)
                          .RunReaderWithHeaders(ct),
                ct);

            return (headers, StreamRows(rows, ct));
        }

        /// <summary>
        /// Streams a pre-computed jagged row array as an <see cref="IAsyncEnumerable{T}"/>.
        /// Cancellation is checked before each row is yielded.
        /// </summary>
        private static async IAsyncEnumerable<object?[]> StreamRows(
            object?[][] rows,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            foreach (var row in rows)
            {
                ct.ThrowIfCancellationRequested();
                yield return row;
                await Task.Yield(); // allow other async work to interleave between rows
            }
        }

        /// <summary>
        /// Execute a SELECT and return the value of the first column of the first row,
        /// or null if no rows were returned. Wraps ExecuteNonQuery for DML statements,
        /// returning the row-count as a boxed long.
        /// </summary>
        public object? ExecuteScalar(string sql)
        {
            string verb = sql.TrimStart().Split(separators, 2)[0].ToUpperInvariant();
            if (verb == "SELECT")
            {
                var grid = ExecuteReader(sql);
                return grid.GetLength(0) > 1 && grid.GetLength(1) > 0 ? grid[1, 0] : null;
            }
            return (long)ExecuteNonQuery(sql);
        }

        /// <summary>
        /// Asynchronously executes a scalar query on a thread-pool thread.
        /// Returns the value of the first column of the first row, or null.
        /// </summary>
        public Task<object?> ExecuteScalarAsync(string sql, CancellationToken cancellationToken = default)
            => Task.Run(() => ExecuteScalar(sql), cancellationToken);

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
            using var ms = new System.IO.MemoryStream();
            using var w = new Utf8JsonWriter(ms, opts);

            w.WriteStartObject();

            // ── tables ───────────────────────────────────────────────────
            w.WriteStartArray("tables");
            foreach (var table in Tables.Values)
            {
                using var tableDoc = JsonDocument.Parse(table.ToJson(indented: false));
                tableDoc.RootElement.WriteTo(w);
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

        /// <summary>
        /// Saves the serialized database to a file at <paramref name="path"/>.
        /// </summary>
        public void SaveJson(string path, bool indented = true)
            => System.IO.File.WriteAllText(path, ToJson(indented), Encoding.UTF8);

        /// <summary>
        /// Deserializes a JSON string produced by <see cref="ToJson"/> into a new <see cref="SQLDatabase"/>.
        /// All tables and their data are restored.  User-defined functions are not restored
        /// (re-run their <c>CREATE FUNCTION</c> statements if needed).
        /// </summary>
        /// <exception cref="JsonException">The JSON is malformed or missing required properties.</exception>
        public static SQLDatabase FromJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json)) throw new ArgumentNullException(nameof(json));

            var db = new SQLDatabase();
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // Restore tables
            foreach (var tableEl in root.GetProperty("tables").EnumerateArray())
                db.AddTable(SQLTable.FromJson(tableEl.GetRawText()));

            // Restore triggers by re-executing their original CREATE TRIGGER SQL
            if (root.TryGetProperty("triggers", out var triggersEl))
            {
                foreach (var trigEl in triggersEl.EnumerateArray())
                {
                    string? sql = trigEl.GetProperty("sql").GetString();
                    if (!string.IsNullOrWhiteSpace(sql))
                        db.ExecuteNonQuery(sql);
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

            using var doc = JsonDocument.Parse(json);
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
                    string? name = trigEl.GetProperty("name").GetString();
                    if (string.IsNullOrWhiteSpace(name)) continue;
                    string? sql = trigEl.GetProperty("sql").GetString();
                    if (Triggers.ContainsKey(name) && !overwrite) continue;
                    if (!string.IsNullOrWhiteSpace(sql))
                        ExecuteNonQuery(sql);
                }
            }
        }
    }
}