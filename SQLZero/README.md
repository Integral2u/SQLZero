# SQLite.Zero — In-Memory SQL Engine for C#

A self-contained, zero-dependency, in-memory SQL engine written in pure C#.  
Drop a single file into any project and write SQL against strongly-typed in-memory tables — no Entity Framework, no SQLite, no server required.  
Extend it with your own callable functions via the add-in system, and query asynchronously with full cancellation support.

[![NuGet](https://img.shields.io/nuget/v/SQLZero.svg)](https://www.nuget.org/packages/SQLZero)
[![NuGet Downloads](https://img.shields.io/nuget/dt/SQLZero.svg)](https://www.nuget.org/packages/SQLZero)

---

## Features

| Area | Supported |
|---|---|
| **DML** | `INSERT`, `UPDATE`, `DELETE` |
| **DDL** | `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE FUNCTION`, `DROP FUNCTION`, `CREATE TRIGGER`, `DROP TRIGGER` |
| **SELECT** | `DISTINCT`, `TOP n`, `*`, `table.*`, column aliases, arbitrary expressions |
| **Joins** | `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS JOIN`, implicit cross-join |
| **Filtering** | `WHERE`, `HAVING`, `AND/OR/NOT`, `IS [NOT] NULL`, `BETWEEN`, `IN`, `NOT IN`, `LIKE` (`%` `_`) |
| **Aggregates** | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` + `GROUP BY` + `HAVING` + `DISTINCT` |
| **Ordering & paging** | `ORDER BY … ASC/DESC`, multi-column, `LIMIT`, `OFFSET`, `TOP` |
| **Expressions** | Arithmetic, string concatenation, `CASE/WHEN/THEN/ELSE/END`, `CAST(x AS type)` |
| **Built-in functions** | 50+ built-ins — string, numeric, date, null-handling (see below) |
| **User-defined functions** | `CREATE FUNCTION … RETURNS … BEGIN RETURN expr; END` |
| **Triggers** | `BEFORE`/`AFTER` × `INSERT`/`UPDATE`/`DELETE`, `NEW`/`OLD` row access, `IF/ELSEIF/ELSE` in body |
| **Add-ins** | Register C# delegates or `ISqlAddIn` implementations callable directly from SQL |
| **Async** | `ExecuteNonQueryAsync`, `ExecuteReaderAsync`, `ExecuteScalarAsync` — cancellable |
| **Persistence** | JSON serialization/deserialization for tables, triggers, and the full database |
| **Comments** | `-- single line` and `/* multi-line */` |

---

## Installation

Copy **`SQLDatabase.cs`** into your project.  The file has no external dependencies beyond the .NET BCL.

Targets **C# 12 / .NET 8** or later (uses nullable reference types, primary constructors, and collection expressions).

Add the namespace:
```csharp
using SQLZero;
```

---

## Quick Start

```csharp
var db = new SQLDatabase();

db.ExecuteNonQuery(@"
    CREATE TABLE Employees (
        Id      INT,
        Name    VARCHAR(100),
        Dept    VARCHAR(50),
        Salary  DECIMAL
    )");

db.ExecuteNonQuery("INSERT INTO Employees VALUES (1, 'Alice', 'Engineering', 95000)");
db.ExecuteNonQuery("INSERT INTO Employees VALUES (2, 'Bob',   'Marketing',   72000)");
db.ExecuteNonQuery("INSERT INTO Employees VALUES (3, 'Carol', 'Engineering', 110000)");

// result[0,*] is the header row; result[1..,*] is data
object?[,] result = db.ExecuteReader(
    "SELECT Name, Salary FROM Employees ORDER BY Salary DESC");

int rows = result.GetLength(0), cols = result.GetLength(1);
for (int r = 0; r < rows; r++)
{
    for (int c = 0; c < cols; c++) Console.Write($"{result[r, c],-20}");
    Console.WriteLine();
}
```

Output:
```
Name                Salary
Carol               110000
Alice               95000
Bob                 72000
```

---

## API Reference

### `SQLDatabase`

#### Synchronous

```csharp
// Execute INSERT / UPDATE / DELETE / CREATE / ALTER / DROP
// Returns rows affected (0 for DDL)
int ExecuteNonQuery(string sql);

// Execute SELECT — returns object?[,]
// Row 0 contains column header strings; rows 1..N contain data
object?[,] ExecuteReader(string sql);

// Execute SELECT and return result[1,0], or null if no rows
// For DML, returns affected row count as a boxed long
object? ExecuteScalar(string sql);

// Add a pre-built SQLTable (e.g. populated from a CSV loader or JSON)
void AddTable(SQLTable table);
```

#### Asynchronous

All compute runs on a thread-pool thread via `Task.Run`. Cancellation is checked **between row evaluations** — a blocking add-in that is already executing a row will finish before the token is observed.

```csharp
// Async INSERT / UPDATE / DELETE / CREATE / etc.
Task<int> ExecuteNonQueryAsync(string sql, CancellationToken ct = default);

// Async SELECT — returns headers immediately; rows stream as IAsyncEnumerable
Task<(string[] Headers, IAsyncEnumerable<object?[]> Rows)>
    ExecuteReaderAsync(string sql, CancellationToken ct = default);

// Async scalar
Task<object?> ExecuteScalarAsync(string sql, CancellationToken ct = default);
```

**`ExecuteReaderAsync` usage:**
```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

var (headers, rows) = await db.ExecuteReaderAsync(sql, cts.Token);
Console.WriteLine(string.Join(", ", headers));

await foreach (var row in rows.WithCancellation(cts.Token))
    Console.WriteLine(string.Join(", ", row));
```

> **Note:** Queries with `ORDER BY`, `GROUP BY`, or `DISTINCT` must materialise all rows before streaming can begin. Simple `SELECT … WHERE` queries yield rows progressively.

#### Add-ins

Register C# functions callable by name from any SQL expression:

```csharp
// Interface-based
void RegisterAddIn(ISqlAddIn addIn);

// Delegate shorthand
void RegisterAddIn(string name, Func<object?[], object?> fn);

// Lifecycle
bool UnregisterAddIn(string name);
IReadOnlyCollection<string> RegisteredAddIns { get; }
```

#### JSON Persistence

```csharp
// Full database → JSON string (tables + triggers)
string ToJson(bool indented = true);

// JSON string → new SQLDatabase (tables + triggers restored)
static SQLDatabase FromJson(string json);

// Save/load to file
void SaveJson(string path, bool indented = true);
static SQLDatabase LoadJson(string path);

// Merge another database's JSON into this one
void MergeJson(string json, bool overwrite = false);
```

---

### `SQLTable`

```csharp
// Construct with optional column names and seed data
var table = new SQLTable("Products",
    columns: new[] { "Id", "Name", "Price" },
    data: new object?[,] {
        { 1L, "Widget", 9.99 },
        { 2L, "Gadget", 24.99 }
    });

// Properties
string   Name    { get; }
string[] Columns { get; }
int      Count   { get; }

// Methods
void     AddColumn(string name, Type type);  // existing rows filled with type default
void     AddRow(object?[] row);              // type-checked; coercion attempted
object?  GetValue(int rowIndex, string columnName);

// JSON round-trip
string           ToJson(bool indented = true);
static SQLTable  FromJson(string json);
```

---

### `ISqlAddIn`

Implement this interface to register a C# function callable from SQL:

```csharp
public interface ISqlAddIn
{
    // Name as it appears in SQL — case-insensitive
    string FunctionName { get; }

    // Called with already-evaluated argument values; return null for SQL NULL
    object? Invoke(object?[] args);
}
```

Add-ins are resolved **before** the built-in function table, so they can shadow any built-in if needed. An add-in that calls an external API or performs blocking I/O should be used via `ExecuteReaderAsync` so the blocking work happens on the thread pool, not the calling thread.

---

## Built-in Functions

### String
`UPPER` · `LOWER` · `LEN` / `LENGTH` · `TRIM` · `LTRIM` · `RTRIM` · `REVERSE` ·
`CONCAT` · `CONCAT_WS` · `REPLACE` · `SUBSTRING` / `SUBSTR` / `MID` · `LEFT` · `RIGHT` ·
`CHARINDEX` / `LOCATE` · `INSTR` · `PATINDEX` · `REPLICATE` / `REPEAT` · `SPACE` ·
`STR` / `TOSTRING` / `TO_CHAR` · `ASCII` · `CHAR`

### Numeric
`ABS` · `ROUND` · `FLOOR` · `CEILING` / `CEIL` · `POWER` / `POW` · `SQRT` · `EXP` ·
`LOG` / `LN` · `LOG10` · `SIGN` · `MOD` · `RAND` / `RANDOM` · `PI`

### Date & Time
`NOW` / `GETDATE` / `CURRENT_TIMESTAMP` · `GETUTCDATE` / `UTC_TIMESTAMP` ·
`YEAR` · `MONTH` · `DAY` · `DATEDIFF(part, start, end)`

### Null Handling
`COALESCE` / `NVL` / `IFNULL` / `ISNULL` · `NULLIF`

### Conversion & Flow
`CAST(expr AS type)` · `IIF(cond, true_val, false_val)` · `CASE WHEN … THEN … ELSE … END`

### Misc
`NEWID` / `UUID` / `NEWGUID`

---

## Examples

### Programmatic table loading

```csharp
var db = new SQLDatabase();
var table = new SQLTable("Products",
    columns: new[] { "Id", "Name", "Category", "Price", "Stock" },
    data: new object?[,] {
        { 1L, "Hammer",     "Tools",     12.99, 200L },
        { 2L, "Wrench",     "Tools",     19.99,  85L },
        { 3L, "Drill",      "Tools",    149.99,  32L },
        { 4L, "Paint",      "Supplies",   8.49, 500L },
        { 5L, "Paintbrush", "Supplies",   3.99, 1200L }
    });
db.AddTable(table);
```

### Filtering and expressions

```csharp
var result = db.ExecuteReader(@"
    SELECT Name,
           Price * 1.1 AS PriceWithTax,
           CASE WHEN Stock > 100 THEN 'In Stock' ELSE 'Low Stock' END AS Availability
    FROM   Products
    WHERE  Category = 'Tools'
      AND  Price BETWEEN 10 AND 200
    ORDER  BY Price ASC");
```

### Aggregates and GROUP BY

```csharp
var result = db.ExecuteReader(@"
    SELECT   Category,
             COUNT(*)   AS ItemCount,
             AVG(Price) AS AvgPrice,
             SUM(Stock) AS TotalStock
    FROM     Products
    GROUP BY Category
    HAVING   COUNT(*) > 1
    ORDER BY AvgPrice DESC");
```

### JOIN

```csharp
db.ExecuteNonQuery(@"
    CREATE TABLE Orders (
        OrderId    INT,
        CustomerId INT,
        ProductId  INT,
        Qty        INT
    )");

db.ExecuteNonQuery("INSERT INTO Orders VALUES (1, 10, 1, 3)");
db.ExecuteNonQuery("INSERT INTO Orders VALUES (2, 11, 3, 1)");

var result = db.ExecuteReader(@"
    SELECT o.OrderId,
           p.Name      AS Product,
           p.Price * o.Qty AS LineTotal
    FROM   Orders o
    INNER JOIN Products p ON o.ProductId = p.Id
    ORDER  BY LineTotal DESC");
```

### CREATE FUNCTION

```csharp
db.ExecuteNonQuery(@"
    CREATE FUNCTION DiscountedPrice(@price FLOAT, @pct FLOAT)
    RETURNS FLOAT
    AS BEGIN
        RETURN @price * (1 - @pct / 100);
    END");

var result = db.ExecuteReader(@"
    SELECT Name, Price, DiscountedPrice(Price, 15) AS AfterDiscount
    FROM   Products
    WHERE  Category = 'Tools'");
```

### Triggers

Triggers fire `BEFORE` or `AFTER` `INSERT`, `UPDATE`, or `DELETE`, once per affected row. The `NEW` pseudo-row gives access to incoming values; `OLD` gives the previous values. `BEFORE` triggers can modify `NEW` values before they are written.

```csharp
// Audit log trigger
db.ExecuteNonQuery(@"
    CREATE TRIGGER trg_ProductUpdate
    AFTER UPDATE ON Products
    FOR EACH ROW
    BEGIN
        INSERT INTO AuditLog VALUES ('UPDATE', 'Products', OLD.Name, NEW.Name);
    END");

// BEFORE trigger — enforce a price floor
db.ExecuteNonQuery(@"
    CREATE TRIGGER trg_MinPrice
    BEFORE INSERT ON Products
    FOR EACH ROW
    BEGIN
        IF NEW.Price < 1.0 THEN
            SET NEW.Price = 1.0;
        END IF;
    END");

// Remove a trigger
db.ExecuteNonQuery("DROP TRIGGER trg_MinPrice");
db.ExecuteNonQuery("DROP TRIGGER IF EXISTS NonExistent");  // safe
```

**Trigger body supports:**
- `SET NEW.col = expr` / `SET OLD.col = expr`
- `IF cond THEN … ELSEIF cond THEN … ELSE … END IF`
- Any DML statement with `NEW.col` / `OLD.col` substitution
- Calls to user-defined functions and add-ins

**Triggers survive JSON round-trips** — `SaveJson`/`LoadJson` and `ToJson`/`FromJson` preserve and restore all triggers automatically.

### Add-ins (callable from SQL)

```csharp
// Delegate shorthand — register a C# function by name
db.RegisterAddIn("TaxRate", args =>
{
    string country = args[0]?.ToString() ?? "";
    return country.ToUpper() switch {
        "AU" => 0.10,
        "NZ" => 0.15,
        "UK" => 0.20,
        _    => 0.0
    };
});

// Now callable directly from SQL
var result = db.ExecuteReader(@"
    SELECT Name, Price, Price * TaxRate(Country) AS Tax
    FROM   Products");

// Interface-based add-in (better for complex/testable logic)
public class CurrencyConvertAddIn : ISqlAddIn
{
    public string FunctionName => "ConvertCurrency";

    public object? Invoke(object?[] args)
    {
        double amount = SqlExpr.ToNum(args[0]);
        string from   = args[1]?.ToString() ?? "USD";
        string to     = args[2]?.ToString() ?? "USD";
        // ... conversion logic ...
        return converted;
    }
}

db.RegisterAddIn(new CurrencyConvertAddIn());
```

> **Async add-ins:** If your add-in calls an external service, keep `Invoke` synchronous but use `ExecuteReaderAsync` so the blocking work runs on the thread pool rather than the UI thread. If the add-in loops forever, cancel the token — the engine will observe it at the next row boundary.

### Async query with cancellation

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

var (headers, rows) = await db.ExecuteReaderAsync(
    "SELECT * FROM Products WHERE Category = 'Tools'",
    cts.Token);

Console.WriteLine(string.Join(" | ", headers));

await foreach (var row in rows.WithCancellation(cts.Token))
    Console.WriteLine(string.Join(" | ", row));
```

### JSON persistence

```csharp
// Save to file
db.SaveJson("snapshot.json");

// Restore — tables, data, and triggers all preserved
var restored = SQLDatabase.LoadJson("snapshot.json");

// Merge two databases (skip existing tables)
db.MergeJson(otherDb.ToJson(), overwrite: false);

// Overwrite on conflict
db.MergeJson(otherDb.ToJson(), overwrite: true);

// SQLTable round-trip independently
string json = table.ToJson();
var clone   = SQLTable.FromJson(json);
```

### LIMIT / OFFSET paging

```csharp
// Page 2, 2 items per page
var result = db.ExecuteReader(@"
    SELECT Id, Name, Price
    FROM   Products
    ORDER  BY Price DESC
    LIMIT  2 OFFSET 2");
```

### UPDATE and DELETE

```csharp
int updated = db.ExecuteNonQuery(@"
    UPDATE Products
    SET    Price = Price * 0.9
    WHERE  Category = 'Supplies'");

int deleted = db.ExecuteNonQuery(
    "DELETE FROM Products WHERE Stock > 1000");
```

### ALTER TABLE

```csharp
db.ExecuteNonQuery("ALTER TABLE Products ADD COLUMN Discontinued BIT");
db.ExecuteNonQuery("UPDATE Products SET Discontinued = 0");
db.ExecuteNonQuery("ALTER TABLE Products DROP COLUMN Stock");
```

---

## Limitations

- **No persistence by default** — all data is lost when the object goes out of scope unless serialized via `SaveJson`.
- **No transactions** — `BEGIN TRANSACTION / COMMIT / ROLLBACK` are not implemented.
- **No indexes** — all scans are O(n); not suitable for large datasets.
- **No correlated subqueries** — subqueries in `WHERE` clauses are not parsed.
- **Single statement per call** — semicolon-separated batches are not split automatically.
- **Soft type system** — values are stored as CLR `object?`; implicit coercions happen on insert.
- **`CREATE FUNCTION` body** — only a single `RETURN expr;` statement is executed; multi-statement bodies are not supported.
- **Stuck add-ins** — cancellation is checked between row evaluations, not inside them. An add-in in an infinite loop will not be interrupted by the token; cancelling and discarding the task is the only recourse.

---

## Architecture Overview

```
SQLDatabase
 ├── ExecuteNonQuery / ExecuteNonQueryAsync
 ├── ExecuteReader   / ExecuteReaderAsync  ← tuple (string[] Headers, IAsyncEnumerable<object?[]> Rows)
 ├── ExecuteScalar   / ExecuteScalarAsync
 ├── RegisterAddIn(ISqlAddIn | Func<object?[], object?>)
 ├── ToJson / FromJson / SaveJson / LoadJson / MergeJson
 └── SqlTokenizer          — hand-written lexer
      └── SqlExecutor      — recursive-descent parser + executor
           ├── ParseSelectClauses  — pure parse, shared by sync and async paths
           ├── DoSelect / DoSelectWithHeaders
           ├── ExecSelect / ExecSelectWithHeaders  — WHERE → JOIN → GROUP BY → HAVING → ORDER → LIMIT
           ├── ResolveHeaders      — * expansion, shared by sync and async paths
           ├── BuildRows(ct)       — row evaluation with cancellation between rows
           ├── DoInsert / DoUpdate / DoDelete
           ├── DoCreate (TABLE / FUNCTION / TRIGGER)
           ├── DoAlter / DoDrop
           └── SqlExpr             — expression evaluator (Pratt-style precedence)
                ├── CallUserAddIn  — ISqlAddIn dispatch (before built-ins)
                └── CallBuiltin    — 50+ built-in function implementations

SQLTable
 ├── ToJson / FromJson      — independent table serialization
 └── GetValue / AddRow / AddColumn
```

---

## Running the Tests

Tests use **NUnit 3.x**. Add the NuGet packages `NUnit` and `NUnit3TestAdapter` to a test project, include `SQLDatabase.cs`, and run:

```bash
dotnet test
```

The test suite covers DDL, DML, all join types, aggregates, triggers (including JSON round-trips), add-ins, async cancellation, and JSON persistence.

---

## License

MIT — do whatever you like with it.
