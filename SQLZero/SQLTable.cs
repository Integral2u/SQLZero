using System.Data;
using System.Text;
using System.Text.Json;
namespace SQLZero
{
    // ============================================================
    //  SQL TABLE
    // ============================================================

    public class SQLTable
    {
        public readonly string Name;
        private readonly Dictionary<string, List<object?>> DataColumns;
        private readonly Dictionary<int, string> ColumnIndex;
        private readonly Dictionary<string, Type> ColumnTypes;
        private int RowCount = 0;

        public SQLTable(string name, string[]? columns = null, object?[,]? data = null)
        {
            Name = name;
            DataColumns = new Dictionary<string, List<object?>>(StringComparer.OrdinalIgnoreCase);
            ColumnIndex = [];
            ColumnTypes = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

            foreach (var column in columns ?? [])
            {
                DataColumns.TryAdd(column, []);
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
                    object? val = data[row, col];
                    if (!ColumnTypes.TryGetValue(colName, out Type? value))
                        ColumnTypes[colName] = val?.GetType() ?? typeof(object);
                    else if (val != null && val.GetType() != value)
                        throw new InvalidDataException($"Type mismatch in column '{colName}' at row {row}.");
                    DataColumns[colName].Add(val);
                }
                RowCount++;
            }
        }

        // ── Public surface ──────────────────────────────────────────────
        public string[] Columns => [.. ColumnIndex.OrderBy(kv => kv.Key).Select(kv => kv.Value)];
        public int Count => RowCount;

        public void AddColumn(string name, Type type)
        {
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(type);
            if (DataColumns.ContainsKey(name)) throw new DuplicateNameException($"Column '{name}' already exists.");

            DataColumns[name] = new List<object?>(RowCount);
            ColumnIndex[ColumnIndex.Count] = name;
            ColumnTypes[name] = type;

            object? defaultVal = type.IsValueType ? Activator.CreateInstance(type) : null;
            for (int i = 0; i < RowCount; i++)
                DataColumns[name].Add(defaultVal);
        }

        public void AddRow(object?[] row)
        {
            ArgumentNullException.ThrowIfNull(row);
            if (row.Length != DataColumns.Count)
                throw new DataMisalignedException($"Expected {DataColumns.Count} column(s), got {row.Length}.");

            for (int i = 0; i < row.Length; i++)
            {
                string colName = ColumnIndex[i];
                object? val = row[i];

                if (val == null) continue; // nulls are always allowed

                if (!ColumnTypes.TryGetValue(colName, out Type? expected))
                {
                    ColumnTypes[colName] = val.GetType();
                }
                else if (expected != typeof(object))
                {
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

        public object? GetValue(int rowIndex, string columnName)
        {
            if (!DataColumns.TryGetValue(columnName, out var col))
                throw new KeyNotFoundException($"Column '{columnName}' not found.");
            return col[rowIndex];
        }

        internal void SetValue(int rowIndex, string columnName, object? value)
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

        internal object?[] GetRow(int rowIndex)
        {
            var cols = Columns;
            var row = new object?[cols.Length];
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
            using var ms = new System.IO.MemoryStream();
            using var w = new Utf8JsonWriter(ms, opts);
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

        /// <summary>
        /// Deserializes a JSON string produced by <see cref="ToJson"/> back into a new <see cref="SQLTable"/>.
        /// </summary>
        /// <exception cref="JsonException">The JSON is malformed or missing required properties.</exception>
        public static SQLTable FromJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json)) throw new ArgumentNullException(nameof(json));
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            string name = root.GetProperty("name").GetString()
                          ?? throw new JsonException("Missing 'name' property.");

            // ── columns ──────────────────────────────────────────────────
            var colDefs = new List<(string Name, Type ClrType)>();
            foreach (var colEl in root.GetProperty("columns").EnumerateArray())
            {
                string colName = colEl.GetProperty("name").GetString()!;
                string typeName = colEl.GetProperty("type").GetString() ?? "String";
                colDefs.Add((colName, ClrTypeFromName(typeName)));
            }

            var table = new SQLTable(name, [.. colDefs.Select(c => c.Name)]);
            foreach (var (cn, ct) in colDefs) table.SetColumnType(cn, ct);

            // ── rows ─────────────────────────────────────────────────────
            foreach (var rowEl in root.GetProperty("rows").EnumerateArray())
            {
                var cells = rowEl.EnumerateArray().ToArray();
                var row = new object[cells.Length];
                for (int c = 0; c < cells.Length; c++)
                {
                    var r = ReadJsonValue(cells[c], c < colDefs.Count ? colDefs[c].ClrType : typeof(object));
                    if(r is not null) row[c] = r;
                }
                table.AddRow(row);
            }

            return table;
        }

        // ── JSON helpers ─────────────────────────────────────────────────

        private static void WriteJsonValue(Utf8JsonWriter w, object? value)
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

        private static object? ReadJsonValue(JsonElement el, Type hint)
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
                string s = el.GetString()!;
                if (hint == typeof(DateTime) && DateTime.TryParse(s, null,
                        System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return dt;
                if (hint == typeof(Guid) && Guid.TryParse(s, out var g)) return g;
                if (hint == typeof(long) && long.TryParse(s, out var l)) return l;
                if (hint == typeof(double) && double.TryParse(s,
                        System.Globalization.NumberStyles.Any,
                        System.Globalization.CultureInfo.InvariantCulture, out var d)) return d;
                if (hint == typeof(bool)) return s is "1" or "true" or "True";
                return s;
            }

            return el.GetRawText();
        }

        private static Type ClrTypeFromName(string name) => name switch
        {
            "Int64" or "Int32" or "Int16" or "Byte" or "SByte" => typeof(long),
            "Double" or "Single" => typeof(double),
            "Decimal" => typeof(double),
            "Boolean" => typeof(bool),
            "DateTime" => typeof(DateTime),
            "Guid" => typeof(Guid),
            _ => typeof(string)
        };
    }
}