// SQLDatabaseTests.cs
// Requires NUnit 3.x  (install via NuGet: NUnit + NUnit3TestAdapter)
// Add a reference to SQLDatabase.cs in the same project.

using System.Data;
using SQLZero;

namespace SQLZeroTests
{
    [TestFixture]
    public class SQLDatabaseTests
    {
        // ──────────────────────────────────────────────────────────────────────────
        //  Helpers
        // ──────────────────────────────────────────────────────────────────────────

        /// <summary>Returns the value at (row, col) from a reader result (1-based for data rows).</summary>
        private static object? Cell(object?[,] grid, int dataRow, int col) => grid[dataRow, col];

        /// <summary>Number of data rows (excludes header row 0).</summary>
        private static int DataRows(object?[,] grid) => grid.GetLength(0) - 1;

        /// <summary>Column count.</summary>
        private static int ColCount(object?[,] grid) => grid.GetLength(1);

        /// <summary>Header string for a column index.</summary>
        private static string? Header(object?[,] grid, int col) => grid[0, col]?.ToString();

        // ──────────────────────────────────────────────────────────────────────────
        //  SQLTable — construction and AddRow / AddColumn
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void SQLTable_Constructor_WithColumnsAndData_SetsRowCount()
        {
            var table = new SQLTable("Test",
                columns: ["Id", "Name"],
                data: new object?[,] { { 1, "Alice" }, { 2, "Bob" } });

            var expected = new[] { "Id", "Name" };
            Assert.That(table.Count, Is.EqualTo(2));
            Assert.That(table.Columns, Is.EquivalentTo(expected));
        }

        [Test]
        public void SQLTable_AddRow_IncreasesCount()
        {
            var table = new SQLTable("T", ["X"]);
            table.AddRow([42L]);
            Assert.That(table.Count, Is.EqualTo(1));
        }

        [Test]
        public void SQLTable_AddRow_WrongColumnCount_Throws()
        {
            var table = new SQLTable("T", ["A", "B"]);
            Assert.Throws<DataMisalignedException>(() => table.AddRow([1L]));
        }

        [Test]
        public void SQLTable_AddColumn_AppearsInColumns()
        {
            var table = new SQLTable("T", ["Id"]);
            table.AddRow([1L]);
            table.AddColumn("Score", typeof(double));

            Assert.Multiple(() =>
            {
                Assert.That(table.Columns, Contains.Item("Score"));
                Assert.That(table.Count, Is.EqualTo(1));  // existing row gets default (0.0)
            });
        }

        [Test]
        public void SQLTable_AddColumn_Duplicate_Throws()
        {
            var table = new SQLTable("T", ["Id"]);
            Assert.Throws<DuplicateNameException>(() => table.AddColumn("Id", typeof(int)));
        }

        [Test]
        public void SQLTable_Constructor_ColumnCountMismatch_Throws()
        {
            Assert.Throws<DataMisalignedException>(() =>
                new SQLTable("T",
                    columns: ["A", "B"],
                    data: new object?[,] { { 1 } }));  // only 1 col, expected 2
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  SQLDatabaseInMemory.AddTable
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void AddTable_Duplicate_Throws()
        {
            var db = new SQLZero.SQLDatabase();
            db.AddTable(new SQLTable("T"));
            Assert.Throws<DuplicateNameException>(() => db.AddTable(new SQLTable("T")));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  CREATE TABLE
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void CreateTable_Basic_SucceedsAndIsQueryable()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE Widgets (Id INT, Name VARCHAR(100), Price FLOAT)");
            var result = db.ExecuteReader("SELECT * FROM Widgets");

            Assert.Multiple(() =>
            {
                Assert.That(DataRows(result), Is.EqualTo(0));
                Assert.That(ColCount(result), Is.EqualTo(3));
                Assert.That(Header(result, 0), Is.EqualTo("Id"));
            });
        }

        [Test]
        public void CreateTable_WithConstraints_DoesNotThrow()
        {
            var db = new SQLZero.SQLDatabase();
            Assert.DoesNotThrow(() => db.ExecuteNonQuery(@"
            CREATE TABLE Orders (
                OrderId   INT         NOT NULL PRIMARY KEY,
                CustomerId INT        NOT NULL,
                Total     DECIMAL(10,2) DEFAULT 0
            )"));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  INSERT
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Insert_SingleRow_ReturnsOne()
        {
            var db = BuildProductDb();
            int n = db.ExecuteNonQuery("INSERT INTO Products VALUES (10, 'Widget', 'Tools', 9.99, 100)");
            Assert.That(n, Is.EqualTo(1));
        }

        [Test]
        public void Insert_MultiRow_ReturnsCorrectCount()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE N (Val INT)");
            int n = db.ExecuteNonQuery("INSERT INTO N VALUES (1), (2), (3)");
            Assert.That(n, Is.EqualTo(3));
        }

        [Test]
        public void Insert_NamedColumns_CorrectlyPopulates()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE P (A INT, B VARCHAR(10), C INT)");
            db.ExecuteNonQuery("INSERT INTO P (C, A) VALUES (99, 7)");

            var r = db.ExecuteReader("SELECT A, B, C FROM P");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(7));
                Assert.That(Cell(r, 1, 1), Is.Null);
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 2)), Is.EqualTo(99));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  SELECT — basic
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Select_Star_ReturnsAllColumnsAndRows()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT * FROM Products");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(5));
                Assert.That(ColCount(r), Is.EqualTo(5));
            });
        }

        [Test]
        public void Select_SpecificColumns_ReturnsCorrectSubset()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name, Price FROM Products");
            Assert.Multiple(() =>
            {
                Assert.That(ColCount(r), Is.EqualTo(2));
                Assert.That(Header(r, 0), Is.EqualTo("Name"));
                Assert.That(Header(r, 1), Is.EqualTo("Price"));
            });
        }

        [Test]
        public void Select_WithAlias_HeaderMatchesAlias()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name AS ProductName, Price AS Cost FROM Products LIMIT 1");
            Assert.Multiple(() =>
            {
                Assert.That(Header(r, 0), Is.EqualTo("ProductName"));
                Assert.That(Header(r, 1), Is.EqualTo("Cost"));
            });
        }

        [Test]
        public void Select_NoFrom_ExpressionOnly()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT 1 + 1 AS Two, 'hello' AS Greeting");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(2));
                Assert.That(Cell(r, 1, 1), Is.EqualTo("hello"));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  WHERE
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Where_Equality_FiltersCorrectly()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name FROM Products WHERE Category = 'Tools'");
            Assert.That(DataRows(r), Is.EqualTo(3));
        }

        [Test]
        public void Where_AndOr_CombinesCorrectly()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            SELECT Name FROM Products
            WHERE  Category = 'Tools' AND Price < 50");
            Assert.That(DataRows(r), Is.EqualTo(2));
        }

        [Test]
        public void Where_Between_FiltersRange()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name FROM Products WHERE Price BETWEEN 5 AND 20");
            Assert.That(DataRows(r), Is.EqualTo(3));
        }

        [Test]
        public void Where_In_FiltersList()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name FROM Products WHERE Id IN (1, 3, 5)");
            Assert.That(DataRows(r), Is.EqualTo(3));
        }

        [Test]
        public void Where_Like_PercentWildcard()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name FROM Products WHERE Name LIKE '%r%'");
            // Hammer, Wrench, Drill, Paintbrush all contain 'r'
            Assert.That(DataRows(r), Is.EqualTo(4));
        }

        [Test]
        public void Where_Like_UnderscoreWildcard()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name FROM Products WHERE Name LIKE 'Dr__l'");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Drill"));
            });
        }

        [Test]
        public void Where_IsNull_DetectsNulls()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE T (A INT, B VARCHAR(10))");
            db.ExecuteNonQuery("INSERT INTO T (A) VALUES (1)");
            db.ExecuteNonQuery("INSERT INTO T VALUES (2, 'hello')");

            var r = db.ExecuteReader("SELECT A FROM T WHERE B IS NULL");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(1));
            });
        }

        [Test]
        public void Where_IsNotNull_ExcludesNulls()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE T (A INT, B VARCHAR(10))");
            db.ExecuteNonQuery("INSERT INTO T (A) VALUES (1)");
            db.ExecuteNonQuery("INSERT INTO T VALUES (2, 'hello')");

            var r = db.ExecuteReader("SELECT A FROM T WHERE B IS NOT NULL");
            Assert.That(DataRows(r), Is.EqualTo(1));
        }

        [Test]
        public void Where_NotIn_ExcludesList()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name FROM Products WHERE Id NOT IN (1, 2, 3)");
            Assert.That(DataRows(r), Is.EqualTo(2));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  DISTINCT
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Distinct_DeduplicatesRows()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT DISTINCT Category FROM Products ORDER BY Category");
            Assert.That(DataRows(r), Is.EqualTo(2));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  TOP / LIMIT / OFFSET
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Limit_RestrictsRowCount()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name FROM Products LIMIT 2");
            Assert.That(DataRows(r), Is.EqualTo(2));
        }

        [Test]
        public void Limit_WithOffset_SkipsRows()
        {
            var db = BuildProductDb();
            // Products ordered by Id: 1,2,3,4,5 → skip 2, take 2 → ids 3,4
            var r = db.ExecuteReader("SELECT Id FROM Products ORDER BY Id LIMIT 2 OFFSET 2");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(2));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(3));
                Assert.That(SqlExpr.ToNum(Cell(r, 2, 0)), Is.EqualTo(4));
            });
        }

        [Test]
        public void Top_RestrictsRowCount()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT TOP 3 Name FROM Products ORDER BY Price DESC");
            Assert.That(DataRows(r), Is.EqualTo(3));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  ORDER BY
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void OrderBy_Ascending_CorrectOrder()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Price FROM Products ORDER BY Price ASC");
            double prev = double.MinValue;
            for (int i = 1; i <= DataRows(r); i++)
            {
                double v = SqlExpr.ToNum(Cell(r, i, 0));
                Assert.That(v, Is.GreaterThanOrEqualTo(prev));
                prev = v;
            }
        }

        [Test]
        public void OrderBy_Descending_CorrectOrder()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Price FROM Products ORDER BY Price DESC");
            double prev = double.MaxValue;
            for (int i = 1; i <= DataRows(r); i++)
            {
                double v = SqlExpr.ToNum(Cell(r, i, 0));
                Assert.That(v, Is.LessThanOrEqualTo(prev));
                prev = v;
            }
        }

        [Test]
        public void OrderBy_MultiColumn_SortsCorrectly()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Category, Price FROM Products ORDER BY Category ASC, Price DESC");

            // Within each category prices should be descending
            string? lastCat = null; double lastPrice = double.MaxValue;
            for (int i = 1; i <= DataRows(r); i++)
            {
                string? cat = Cell(r, i, 0)?.ToString();
                double price = SqlExpr.ToNum(Cell(r, i, 1));
                if (cat != lastCat) { lastCat = cat; lastPrice = double.MaxValue; }
                Assert.That(price, Is.LessThanOrEqualTo(lastPrice));
                lastPrice = price;
            }
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Aggregate functions + GROUP BY + HAVING
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Aggregate_Count_Star()
        {
            var db = BuildProductDb();
            object val = db.ExecuteScalar("SELECT COUNT(*) FROM Products");
            Assert.That(SqlExpr.ToNum(val), Is.EqualTo(5));
        }

        [Test]
        public void Aggregate_Sum_ReturnsTotal()
        {
            var db = BuildProductDb();
            object val = db.ExecuteScalar("SELECT SUM(Price) FROM Products WHERE Category = 'Tools'");
            // 12.99 + 19.99 + 149.99 = 182.97
            Assert.That(SqlExpr.ToNum(val), Is.EqualTo(182.97).Within(0.001));
        }

        [Test]
        public void Aggregate_Avg_ReturnsAverage()
        {
            var db = BuildProductDb();
            object val = db.ExecuteScalar("SELECT AVG(Price) FROM Products WHERE Category = 'Supplies'");
            // (8.49 + 3.99) / 2 = 6.24
            Assert.That(SqlExpr.ToNum(val), Is.EqualTo(6.24).Within(0.001));
        }

        [Test]
        public void Aggregate_MinMax_ReturnsBounds()
        {
            var db = BuildProductDb();
            object min = db.ExecuteScalar("SELECT MIN(Price) FROM Products");
            object max = db.ExecuteScalar("SELECT MAX(Price) FROM Products");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(min), Is.EqualTo(3.99).Within(0.001));
                Assert.That(SqlExpr.ToNum(max), Is.EqualTo(149.99).Within(0.001));
            });
        }

        [Test]
        public void GroupBy_AggregateCounts_PerGroup()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            SELECT   Category, COUNT(*) AS Cnt
            FROM     Products
            GROUP BY Category
            ORDER BY Category ASC");

            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(2));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Supplies"));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(2));
                Assert.That(Cell(r, 2, 0), Is.EqualTo("Tools"));
                Assert.That(SqlExpr.ToNum(Cell(r, 2, 1)), Is.EqualTo(3));
            });
        }

        [Test]
        public void Having_FiltersGroups()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            SELECT   Category, COUNT(*) AS Cnt
            FROM     Products
            GROUP BY Category
            HAVING   COUNT(*) > 2");

            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Tools"));
            });
        }

        [Test]
        public void Aggregate_CountDistinct()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE T (Val INT)");
            foreach (var v in new[] { 1, 1, 2, 2, 3 })
                db.ExecuteNonQuery($"INSERT INTO T VALUES ({v})");

            object val = db.ExecuteScalar("SELECT COUNT(DISTINCT Val) FROM T");
            Assert.That(SqlExpr.ToNum(val), Is.EqualTo(3));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Expressions and CASE
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Expression_Arithmetic_EvaluatesCorrectly()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Price * 2 AS Doubled FROM Products WHERE Id = 1");
            Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(25.98).Within(0.001));
        }

        [Test]
        public void Expression_StringConcat_WithPlus()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader("SELECT Name + ' [' + Category + ']' AS Label FROM Products WHERE Id = 1");
            Assert.That(Cell(r, 1, 0)?.ToString(), Is.EqualTo("Hammer [Tools]"));
        }

        [Test]
        public void Case_Searched_ReturnsCorrectBranch()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            SELECT Name,
                   CASE WHEN Price < 10  THEN 'Budget'
                        WHEN Price < 50  THEN 'Mid'
                        ELSE 'Premium'
                   END AS Tier
            FROM   Products
            ORDER  BY Id");

            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 1), Is.EqualTo("Mid"));     // Hammer 12.99
                Assert.That(Cell(r, 2, 1), Is.EqualTo("Mid"));     // Wrench 19.99
                Assert.That(Cell(r, 3, 1), Is.EqualTo("Premium")); // Drill  149.99
                Assert.That(Cell(r, 4, 1), Is.EqualTo("Budget"));  // Paint  8.49
            });
        }

        [Test]
        public void Case_Simple_MatchesByValue()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            SELECT CASE Category
                     WHEN 'Tools'    THEN 'T'
                     WHEN 'Supplies' THEN 'S'
                     ELSE '?'
                   END AS Code
            FROM   Products WHERE Id = 1");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("T"));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Built-in string functions
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Fn_Upper_Lower()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT UPPER('hello') AS U, LOWER('WORLD') AS L");
            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 0), Is.EqualTo("HELLO"));
                Assert.That(Cell(r, 1, 1), Is.EqualTo("world"));
            });
        }

        [Test]
        public void Fn_Len()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT LEN('abcde') AS N");
            Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(5));
        }

        [Test]
        public void Fn_Trim_Variants()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT TRIM('  hi  ') AS T, LTRIM('  hi') AS L, RTRIM('hi  ') AS R");
            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 0), Is.EqualTo("hi"));
                Assert.That(Cell(r, 1, 1), Is.EqualTo("hi"));
                Assert.That(Cell(r, 1, 2), Is.EqualTo("hi"));
            });
        }

        [Test]
        public void Fn_Substring()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT SUBSTRING('Hello World', 7, 5) AS S");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("World"));
        }

        [Test]
        public void Fn_Replace()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT REPLACE('foo bar foo', 'foo', 'baz') AS S");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("baz bar baz"));
        }

        [Test]
        public void Fn_Left_Right()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT LEFT('ABCDEF', 3) AS L, RIGHT('ABCDEF', 3) AS R");
            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 0), Is.EqualTo("ABC"));
                Assert.That(Cell(r, 1, 1), Is.EqualTo("DEF"));
            });
        }

        [Test]
        public void Fn_Concat_Ws()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT CONCAT_WS('-', '2024', '01', '15') AS D");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("2024-01-15"));
        }

        [Test]
        public void Fn_Reverse()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT REVERSE('abc') AS R");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("cba"));
        }

        [Test]
        public void Fn_Replicate()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT REPLICATE('ab', 3) AS R");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("ababab"));
        }

        [Test]
        public void Fn_CharIndex_And_Instr()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT CHARINDEX('lo', 'Hello World') AS C, INSTR('Hello World', 'World') AS I");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(4)); // 1-based
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(7));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Built-in numeric functions
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Fn_Abs_Sign()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT ABS(-42) AS A, SIGN(-5) AS S, SIGN(3) AS P");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(42));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(-1));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 2)), Is.EqualTo(1));
            });
        }

        [Test]
        public void Fn_Round_Floor_Ceiling()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT ROUND(3.567, 2) AS R, FLOOR(3.9) AS F, CEILING(3.1) AS C");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(3.57).Within(0.001));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(3));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 2)), Is.EqualTo(4));
            });
        }

        [Test]
        public void Fn_Power_Sqrt()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT POWER(2, 10) AS P, SQRT(144) AS S");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(1024).Within(0.001));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(12).Within(0.001));
            });
        }

        [Test]
        public void Fn_Mod_And_Pi()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT MOD(17, 5) AS M, ROUND(PI(), 5) AS Pi");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(2));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(3.14159).Within(0.00001));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Null-handling functions
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Fn_Coalesce_ReturnsFirstNonNull()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT COALESCE(NULL, NULL, 'found') AS C");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("found"));
        }

        [Test]
        public void Fn_NullIf_ReturnsNullOnMatch()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT NULLIF(5, 5) AS A, NULLIF(5, 6) AS B");
            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 0), Is.Null);
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(5));
            });
        }

        [Test]
        public void Fn_IIF_ReturnsCorrectBranch()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT IIF(1 > 2, 'yes', 'no') AS A, IIF(2 > 1, 'yes', 'no') AS B");
            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 0), Is.EqualTo("no"));
                Assert.That(Cell(r, 1, 1), Is.EqualTo("yes"));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  CAST
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Cast_IntToString()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT CAST(42 AS VARCHAR) AS S");
            Assert.That(Cell(r, 1, 0), Is.EqualTo("42"));
        }

        [Test]
        public void Cast_StringToFloat()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader("SELECT CAST('3.14' AS FLOAT) AS N");
            Assert.That(SqlExpr.ToNum(Cell(r, 1, 0)), Is.EqualTo(3.14).Within(0.001));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  UPDATE
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Update_SpecificRows_AffectedCountCorrect()
        {
            var db = BuildProductDb();
            int n = db.ExecuteNonQuery("UPDATE Products SET Price = Price * 0.9 WHERE Category = 'Supplies'");
            Assert.That(n, Is.EqualTo(2));
        }

        [Test]
        public void Update_ChangesValues_VerifiedBySelect()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("UPDATE Products SET Name = 'Mega Drill' WHERE Id = 3");
            object name = db.ExecuteScalar("SELECT Name FROM Products WHERE Id = 3");
            Assert.That(name, Is.EqualTo("Mega Drill"));
        }

        [Test]
        public void Update_MultipleColumns()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("UPDATE Products SET Price = 0, Category = 'Clearance' WHERE Id = 5");
            var r = db.ExecuteReader("SELECT Category, Price FROM Products WHERE Id = 5");
            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Clearance"));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(0));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  DELETE
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Delete_WithWhere_RemovesMatchingRows()
        {
            var db = BuildProductDb();
            int n = db.ExecuteNonQuery("DELETE FROM Products WHERE Category = 'Supplies'");
            Assert.That(n, Is.EqualTo(2));

            object cnt = db.ExecuteScalar("SELECT COUNT(*) FROM Products");
            Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(3));
        }

        [Test]
        public void Delete_AllRows_EmptiesTable()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("DELETE FROM Products");
            object cnt = db.ExecuteScalar("SELECT COUNT(*) FROM Products");
            Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(0));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  DROP TABLE
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Drop_Table_RemovesFromDb()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("DROP TABLE Products");
            Assert.Throws<KeyNotFoundException>(() => db.ExecuteReader("SELECT * FROM Products"));
        }

        [Test]
        public void Drop_Table_IfExists_DoesNotThrowWhenMissing()
        {
            var db = new SQLZero.SQLDatabase();
            Assert.DoesNotThrow(() => db.ExecuteNonQuery("DROP TABLE IF EXISTS NonExistent"));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  ALTER TABLE
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Alter_AddColumn_AppearsInSelect()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("ALTER TABLE Products ADD COLUMN Discontinued BIT");
            var r = db.ExecuteReader("SELECT Discontinued FROM Products LIMIT 1");
            Assert.Multiple(() =>
            {
                Assert.That(ColCount(r), Is.EqualTo(1));
                Assert.That(Header(r, 0), Is.EqualTo("Discontinued"));
            });
        }

        [Test]
        public void Alter_DropColumn_DisappearsFromSelect()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("ALTER TABLE Products DROP COLUMN Stock");
            var r = db.ExecuteReader("SELECT * FROM Products LIMIT 1");
            for (int c = 0; c < ColCount(r); c++)
                Assert.That(Header(r, c), Is.Not.EqualTo("Stock"));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  JOIN
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void InnerJoin_ReturnsMatchingRows()
        {
            var (db, _) = BuildOrderDb();
            var r = db.ExecuteReader(@"
            SELECT o.OrderId, p.Name
            FROM   Orders o
            INNER JOIN Products p ON o.ProductId = p.Id
            ORDER BY o.OrderId");

            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(3));
                Assert.That(Cell(r, 1, 1), Is.EqualTo("Hammer"));
                Assert.That(Cell(r, 2, 1), Is.EqualTo("Drill"));
            });
        }

        [Test]
        public void LeftJoin_IncludesUnmatchedLeft()
        {
            var (db, _) = BuildOrderDb();
            var r = db.ExecuteReader(@"
            SELECT p.Name, o.OrderId
            FROM   Products p
            LEFT JOIN Orders o ON p.Id = o.ProductId
            ORDER BY p.Id");

            // 5 products, some with multiple orders, some with none
            // Products 2 (Wrench) and 4 (Paint) have no orders → OrderId should be null
            bool foundNull = false;
            for (int i = 1; i <= DataRows(r); i++)
                if (Cell(r, i, 1) == null) { foundNull = true; break; }
            Assert.That(foundNull, Is.True);
        }

        [Test]
        public void CrossJoin_ProducesCartesianProduct()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE A (X INT)");
            db.ExecuteNonQuery("INSERT INTO A VALUES (1)");
            db.ExecuteNonQuery("INSERT INTO A VALUES (2)");
            db.ExecuteNonQuery("CREATE TABLE B (Y INT)");
            db.ExecuteNonQuery("INSERT INTO B VALUES (10)");
            db.ExecuteNonQuery("INSERT INTO B VALUES (20)");
            db.ExecuteNonQuery("INSERT INTO B VALUES (30)");

            var r = db.ExecuteReader("SELECT A.X, B.Y FROM A CROSS JOIN B");
            Assert.That(DataRows(r), Is.EqualTo(6)); // 2 × 3
        }

        [Test]
        public void Join_ExpressionInOn_Works()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE LHS (Id INT, Val INT)");
            db.ExecuteNonQuery("CREATE TABLE RHS (Id INT, Val INT)");
            db.ExecuteNonQuery("INSERT INTO LHS VALUES (1, 10), (2, 20)");
            db.ExecuteNonQuery("INSERT INTO RHS VALUES (1, 10), (1, 99)");

            // Join on both Id AND Val being equal
            var r = db.ExecuteReader(@"
            SELECT LHS.Id, LHS.Val
            FROM LHS INNER JOIN RHS ON LHS.Id = RHS.Id AND LHS.Val = RHS.Val");
            Assert.That(DataRows(r), Is.EqualTo(1));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  CREATE FUNCTION / user-defined functions
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void CreateFunction_ScalarReturnsCorrectValue()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery(@"
            CREATE FUNCTION Square(@n FLOAT)
            RETURNS FLOAT
            AS BEGIN
                RETURN @n * @n;
            END");

            object r = db.ExecuteScalar("SELECT Square(7)");
            Assert.That(SqlExpr.ToNum(r), Is.EqualTo(49).Within(0.001));
        }

        [Test]
        public void CreateFunction_UsedInWhere()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery(@"
            CREATE FUNCTION IsExpensive(@price FLOAT)
            RETURNS BIT
            AS BEGIN
                RETURN @price > 50;
            END");

            var r = db.ExecuteReader("SELECT Name FROM Products WHERE IsExpensive(Price) = 1");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Drill"));
            });
        }

        [Test]
        public void CreateFunction_MultiParam()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery(@"
            CREATE FUNCTION Discount(@price FLOAT, @pct FLOAT)
            RETURNS FLOAT
            AS BEGIN
                RETURN @price * (1 - @pct / 100);
            END");

            object r = db.ExecuteScalar("SELECT Discount(200, 25)");
            Assert.That(SqlExpr.ToNum(r), Is.EqualTo(150).Within(0.001));
        }

        [Test]
        public void DropFunction_RemovesFromDb()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery(@"
            CREATE FUNCTION Dbl(@x FLOAT)
            RETURNS FLOAT
            AS BEGIN RETURN @x * 2; END");
            db.ExecuteNonQuery("DROP FUNCTION Dbl");

            // Function no longer resolves — should return null rather than throw
            object r = db.ExecuteScalar("SELECT Dbl(5)");
            Assert.That(r, Is.Null);
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  ExecuteScalar
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void ExecuteScalar_NoRows_ReturnsNull()
        {
            var db = BuildProductDb();
            object r = db.ExecuteScalar("SELECT Name FROM Products WHERE Id = 9999");
            Assert.That(r, Is.Null);
        }

        [Test]
        public void ExecuteScalar_DmlReturnsAffectedCount()
        {
            var db = BuildProductDb();
            object r = db.ExecuteScalar("UPDATE Products SET Price = Price + 1 WHERE Category = 'Tools'");
            Assert.That(SqlExpr.ToNum(r), Is.EqualTo(3));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Pre-loaded table (SQLDatabaseInMemory.AddTable)
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void AddTable_Programmatic_IsQueryable()
        {
            var db = new SQLZero.SQLDatabase();
            var tbl = new SQLTable("Inventory",
                columns: ["Sku", "Qty"],
                data: new object?[,] { { "A001", 10 }, { "B002", 25 } });
            db.AddTable(tbl);

            object total = db.ExecuteScalar("SELECT SUM(Qty) FROM Inventory");
            Assert.That(SqlExpr.ToNum(total), Is.EqualTo(35));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Comments in SQL
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Comments_SingleLine_Ignored()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            -- Fetch only tools
            SELECT Name FROM Products -- filter below
            WHERE Category = 'Tools'");
            Assert.That(DataRows(r), Is.EqualTo(3));
        }

        [Test]
        public void Comments_MultiLine_Ignored()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            /* Returns all product names */
            SELECT Name FROM Products /* limit to cheap */ WHERE Price < 15");
            Assert.That(DataRows(r), Is.EqualTo(3));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Edge cases
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void Select_EmptyTable_ReturnsHeaderOnly()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE Empty (A INT, B VARCHAR(10))");
            var r = db.ExecuteReader("SELECT * FROM Empty");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(0));
                Assert.That(ColCount(r), Is.EqualTo(2));
            });
        }

        [Test]
        public void CaseInsensitiveTableAndColumnNames()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE MyTable (MyCol INT)");
            db.ExecuteNonQuery("INSERT INTO MYTABLE VALUES (42)");
            object r = db.ExecuteScalar("SELECT mycol FROM mytable");
            Assert.That(SqlExpr.ToNum(r), Is.EqualTo(42));
        }

        [Test]
        public void Select_LiteralExpressionsWithoutTable()
        {
            var db = new SQLZero.SQLDatabase();
            var r = db.ExecuteReader(@"
            SELECT UPPER('hello')        AS U,
                   ABS(-99)             AS A,
                   ROUND(3.14159, 2)    AS Pi,
                   COALESCE(NULL, 'ok') AS C");
            Assert.Multiple(() =>
            {
                Assert.That(Cell(r, 1, 0), Is.EqualTo("HELLO"));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(99));
                Assert.That(SqlExpr.ToNum(Cell(r, 1, 2)), Is.EqualTo(3.14).Within(0.001));
                Assert.That(Cell(r, 1, 3), Is.EqualTo("ok"));
            });
        }

        [Test]
        public void Nested_Aggregate_In_Having_FiltersCorrectly()
        {
            var db = BuildProductDb();
            var r = db.ExecuteReader(@"
            SELECT Category, AVG(Price) AS AvgP
            FROM   Products
            GROUP BY Category
            HAVING AVG(Price) > 30");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Tools"));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  JSON Serialization — SQLTable
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void SQLTable_ToJson_ProducesValidJson()
        {
            var table = new SQLTable("Items",
                columns: ["Id", "Name", "Price"],
                data: new object?[,] { { 1L, "Widget", 9.99 } });

            string json = table.ToJson();

            using var doc = System.Text.Json.JsonDocument.Parse(json);
            Assert.Multiple(() =>
            {
                Assert.That(doc.RootElement.GetProperty("name").GetString(), Is.EqualTo("Items"));
                Assert.That(doc.RootElement.GetProperty("columns").GetArrayLength(), Is.EqualTo(3));
                Assert.That(doc.RootElement.GetProperty("rows").GetArrayLength(), Is.EqualTo(1));
            });
        }

        [Test]
        public void SQLTable_ToJson_ContainsCorrectColumnMetadata()
        {
            var table = new SQLTable("T",
                columns: ["Flag", "Score", "Label"],
                data: new object?[,] { { true, 3.14, "hi" } });

            string json = table.ToJson();
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var cols = doc.RootElement.GetProperty("columns").EnumerateArray().ToArray();

            Assert.Multiple(() =>
            {
                Assert.That(cols[0].GetProperty("name").GetString(), Is.EqualTo("Flag"));
                Assert.That(cols[0].GetProperty("type").GetString(), Is.EqualTo("Boolean"));
                Assert.That(cols[1].GetProperty("type").GetString(), Is.EqualTo("Double"));
                Assert.That(cols[2].GetProperty("type").GetString(), Is.EqualTo("String"));
            });
        }

        [Test]
        public void SQLTable_FromJson_RoundTrip_PreservesRowCount()
        {
            // Re-create as table from programmatic source
            var table = new SQLTable("Products",
                columns: ["Id", "Name", "Category", "Price", "Stock"],
                data: new object?[,]
                {
                { 1L, "Hammer", "Tools", 12.99, 200L },
                { 2L, "Wrench", "Tools", 19.99,  85L },
                });

            string json = table.ToJson();
            var restored = SQLTable.FromJson(json);

            Assert.Multiple(() =>
            {
                Assert.That(restored.Name, Is.EqualTo("Products"));
                Assert.That(restored.Count, Is.EqualTo(2));
            });
            Assert.That(restored.Columns, Is.EquivalentTo(table.Columns));
        }

        [Test]
        public void SQLTable_FromJson_RoundTrip_PreservesValues()
        {
            var table = new SQLTable("T",
                columns: ["Id", "Name", "Price", "Active"],
                data: new object?[,] { { 7L, "Alpha", 42.5, true } });

            var restored = SQLTable.FromJson(table.ToJson());

            Assert.Multiple(() =>
            {
                Assert.That(restored.GetValue(0, "Id"), Is.EqualTo(7L));
                Assert.That(restored.GetValue(0, "Name"), Is.EqualTo("Alpha"));
                Assert.That(SqlExpr.ToNum(restored.GetValue(0, "Price")), Is.EqualTo(42.5).Within(0.001));
                Assert.That(restored.GetValue(0, "Active"), Is.EqualTo(true));
            });
        }

        [Test]
        public void SQLTable_FromJson_NullValues_Preserved()
        {
            var table = new SQLTable("T", ["A", "B"]);
            table.AddRow([1L, null]);

            var restored = SQLTable.FromJson(table.ToJson());
            Assert.That(restored.GetValue(0, "B"), Is.Null);
        }

        [Test]
        public void SQLTable_FromJson_DateTimeColumn_RoundTrips()
        {
            var now = new DateTime(2024, 6, 15, 12, 30, 0, DateTimeKind.Utc);
            var t2 = new SQLTable("Events",
                columns: ["Id", "Occurred"],
                data: new object?[,] { { 1L, now } });

            var restored = SQLTable.FromJson(t2.ToJson());
            var restoredDt = (DateTime)restored.GetValue(0, "Occurred");
            Assert.That(restoredDt, Is.EqualTo(now).Within(TimeSpan.FromSeconds(1)));
        }

        [Test]
        public void SQLTable_FromJson_RestoredTableIsQueryable()
        {
            var original = new SQLTable("Numbers",
                columns: ["N"],
                data: new object?[,] { { 1L }, { 2L }, { 3L } });

            var restored = SQLTable.FromJson(original.ToJson());

            var db = new SQLZero.SQLDatabase();
            db.AddTable(restored);
            object sum = db.ExecuteScalar("SELECT SUM(N) FROM Numbers");
            Assert.That(SqlExpr.ToNum(sum), Is.EqualTo(6));
        }

        [Test]
        public void SQLTable_ToJson_EmptyTable_Valid()
        {
            var table = new SQLTable("Empty", ["X", "Y"]);
            string json = table.ToJson();
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            Assert.That(doc.RootElement.GetProperty("rows").GetArrayLength(), Is.EqualTo(0));
        }

        [Test]
        public void SQLTable_ToJson_NotIndented_IsCompact()
        {
            var table = new SQLTable("T",
                columns: ["Id"],
                data: new object?[,] { { 1L } });
            string compact = table.ToJson(indented: false);
            Assert.That(compact, Does.Not.Contain("\n"));
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  JSON Serialization — SQLDatabaseInMemory
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void SQLDatabaseInMemory_ToJson_ContainsAllTables()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("CREATE TABLE Extras (Tag VARCHAR(20))");

            string json = db.ToJson();
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var names = doc.RootElement.GetProperty("tables")
                           .EnumerateArray()
                           .Select(t => t.GetProperty("name").GetString())
                           .ToList();

            Assert.That(names, Contains.Item("Products"));
            Assert.That(names, Contains.Item("Extras"));
        }

        [Test]
        public void SQLDatabaseInMemory_FromJson_RoundTrip_RestoresTableCount()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("CREATE TABLE Tags (Id INT, Tag VARCHAR(20))");
            db.ExecuteNonQuery("INSERT INTO Tags VALUES (1, 'sale')");

            var restored = SQLZero.SQLDatabase.FromJson(db.ToJson());

            object prodCount = restored.ExecuteScalar("SELECT COUNT(*) FROM Products");
            object tagCount = restored.ExecuteScalar("SELECT COUNT(*) FROM Tags");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(prodCount), Is.EqualTo(5));
                Assert.That(SqlExpr.ToNum(tagCount), Is.EqualTo(1));
            });
        }

        [Test]
        public void SQLDatabaseInMemory_FromJson_RestoredDb_SupportsInsert()
        {
            var db = BuildProductDb();
            var restored = SQLZero.SQLDatabase.FromJson(db.ToJson());

            // Should be able to insert new rows into the restored table
            Assert.DoesNotThrow(() =>
                restored.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Screwdriver', 'Tools', 7.49, 300)"));

            object count = restored.ExecuteScalar("SELECT COUNT(*) FROM Products");
            Assert.That(SqlExpr.ToNum(count), Is.EqualTo(6));
        }

        [Test]
        public void SQLDatabaseInMemory_FromJson_RestoredDb_SupportsJoins()
        {
            var (db, _) = BuildOrderDb();
            var restored = SQLZero.SQLDatabase.FromJson(db.ToJson());

            var r = restored.ExecuteReader(@"
            SELECT o.OrderId, p.Name
            FROM   Orders o
            INNER JOIN Products p ON o.ProductId = p.Id
            ORDER BY o.OrderId");

            Assert.That(DataRows(r), Is.EqualTo(3));
        }

        [Test]
        public void SQLDatabaseInMemory_FromJson_RestoresNumericTypes()
        {
            var db = BuildProductDb();
            var restored = SQLZero.SQLDatabase.FromJson(db.ToJson());

            // Arithmetic should still work (requires numeric type, not string)
            object total = restored.ExecuteScalar(
                "SELECT SUM(Price * Stock) FROM Products WHERE Category = 'Tools'");
            // 12.99*200 + 19.99*85 + 149.99*32 = 2598 + 1699.15 + 4799.68 = 9096.83
            Assert.That(SqlExpr.ToNum(total), Is.EqualTo(9096.83).Within(0.1));
        }

        [Test]
        public void SQLDatabaseInMemory_FromJson_EmptyDatabase_RoundTrips()
        {
            var db = new SQLZero.SQLDatabase();
            var restored = SQLZero.SQLDatabase.FromJson(db.ToJson());
            // Should not throw when querying a non-existent table
            Assert.Throws<KeyNotFoundException>(() => restored.ExecuteReader("SELECT * FROM Ghost"));
        }

        [Test]
        public void SQLDatabaseInMemory_MergeJson_AddsNewTables()
        {
            var db1 = new SQLZero.SQLDatabase();
            db1.ExecuteNonQuery("CREATE TABLE A (X INT)");
            db1.ExecuteNonQuery("INSERT INTO A VALUES (1)");

            var db2 = new SQLZero.SQLDatabase();
            db2.ExecuteNonQuery("CREATE TABLE B (Y INT)");
            db2.ExecuteNonQuery("INSERT INTO B VALUES (99)");

            db1.MergeJson(db2.ToJson());

            Assert.Multiple(() =>
            {
                // db1 should now have both A and B
                Assert.That(SqlExpr.ToNum(db1.ExecuteScalar("SELECT X FROM A")), Is.EqualTo(1));
                Assert.That(SqlExpr.ToNum(db1.ExecuteScalar("SELECT Y FROM B")), Is.EqualTo(99));
            });
        }

        [Test]
        public void SQLDatabaseInMemory_MergeJson_NoOverwrite_KeepsOriginal()
        {
            var db1 = new SQLZero.SQLDatabase();
            db1.ExecuteNonQuery("CREATE TABLE T (Val INT)");
            db1.ExecuteNonQuery("INSERT INTO T VALUES (1)");

            var db2 = new SQLZero.SQLDatabase();
            db2.ExecuteNonQuery("CREATE TABLE T (Val INT)");
            db2.ExecuteNonQuery("INSERT INTO T VALUES (999)");

            db1.MergeJson(db2.ToJson(), overwrite: false);

            object val = db1.ExecuteScalar("SELECT Val FROM T");
            Assert.That(SqlExpr.ToNum(val), Is.EqualTo(1)); // original preserved
        }

        [Test]
        public void SQLDatabaseInMemory_MergeJson_Overwrite_ReplacesTable()
        {
            var db1 = new SQLZero.SQLDatabase();
            db1.ExecuteNonQuery("CREATE TABLE T (Val INT)");
            db1.ExecuteNonQuery("INSERT INTO T VALUES (1)");

            var db2 = new SQLZero.SQLDatabase();
            db2.ExecuteNonQuery("CREATE TABLE T (Val INT)");
            db2.ExecuteNonQuery("INSERT INTO T VALUES (999)");

            db1.MergeJson(db2.ToJson(), overwrite: true);

            object val = db1.ExecuteScalar("SELECT Val FROM T");
            Assert.That(SqlExpr.ToNum(val), Is.EqualTo(999)); // replaced
        }

        [Test]
        public void SQLDatabaseInMemory_SaveAndLoadJson_RoundTripsViaFile()
        {
            var db = BuildProductDb();
            string path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"sqldb_{Guid.NewGuid():N}.json");

            try
            {
                db.SaveJson(path);
                Assert.That(System.IO.File.Exists(path), Is.True);

                var restored = SQLZero.SQLDatabase.LoadJson(path);
                object count = restored.ExecuteScalar("SELECT COUNT(*) FROM Products");
                Assert.That(SqlExpr.ToNum(count), Is.EqualTo(5));
            }
            finally
            {
                if (System.IO.File.Exists(path)) System.IO.File.Delete(path);
            }
        }

        [Test]
        public void SQLDatabaseInMemory_ToJson_NotIndented_IsCompact()
        {
            var db = BuildProductDb();
            string json = db.ToJson(indented: false);
            Assert.That(json, Does.Not.Contain("\n"));
        }

        [Test]
        public void SQLDatabaseInMemory_FromJson_InvalidJson_Throws()
        {
            Assert.Catch<System.Text.Json.JsonException>(() => SQLZero.SQLDatabase.FromJson("not-json"));
        }

        [Test]
        public void SQLDatabaseInMemory_FromJson_EmptyString_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => SQLZero.SQLDatabase.FromJson(""));
        }

        /// <summary>
        /// Creates a database with a Products table:
        ///   Id | Name        | Category | Price  | Stock
        ///   1  | Hammer      | Tools    | 12.99  | 200
        ///   2  | Wrench      | Tools    | 19.99  |  85
        ///   3  | Drill       | Tools    | 149.99 |  32
        ///   4  | Paint       | Supplies |  8.49  | 500
        ///   5  | Paintbrush  | Supplies |  3.99  | 1200
        /// </summary>
        private static SQLZero.SQLDatabase BuildProductDb()
        {
            var db = new SQLZero.SQLDatabase();
            db.AddTable(new SQLTable("Products",
                columns: ["Id", "Name", "Category", "Price", "Stock"],
                data: new object?[,]
                {
                { 1L, "Hammer",     "Tools",    12.99,  200L },
                { 2L, "Wrench",     "Tools",    19.99,   85L },
                { 3L, "Drill",      "Tools",   149.99,   32L },
                { 4L, "Paint",      "Supplies",  8.49,  500L },
                { 5L, "Paintbrush", "Supplies",  3.99, 1200L }
                }));
            return db;
        }

        /// <summary>
        /// Adds an Orders table to a product database:
        ///   OrderId | CustomerId | ProductId | Qty
        ///   1       | 10         | 1         | 3      (Hammer)
        ///   2       | 11         | 3         | 1      (Drill)
        ///   3       | 10         | 5         | 10     (Paintbrush)
        /// </summary>
        private static (SQLZero.SQLDatabase db, SQLTable orders) BuildOrderDb()
        {
            var db = BuildProductDb();
            var orders = new SQLTable("Orders",
                columns: ["OrderId", "CustomerId", "ProductId", "Qty"],
                data: new object?[,]
                {
                { 1L, 10L, 1L,  3L },
                { 2L, 11L, 3L,  1L },
                { 3L, 10L, 5L, 10L }
                });
            db.AddTable(orders);
            return (db, orders);
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  TRIGGERS
        // ──────────────────────────────────────────────────────────────────────────

        private static SQLZero.SQLDatabase BuildAuditDb()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery(@"
            CREATE TABLE AuditLog (
                Action    VARCHAR(10),
                TableName VARCHAR(50),
                OldVal    VARCHAR(100),
                NewVal    VARCHAR(100)
            )");
            return db;
        }

        [Test]
        public void Trigger_AfterInsert_LogsNewRow()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_ProductInsert
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('INSERT', 'Products', NULL, NEW.Name);
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Screwdriver', 'Tools', 7.49, 300)");

            object logged = db.ExecuteScalar("SELECT NewVal FROM AuditLog WHERE Action = 'INSERT'");
            Assert.That(logged, Is.EqualTo("Screwdriver"));
        }

        [Test]
        public void Trigger_AfterInsert_MultiRow_FiresForEachRow()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_ins
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('INSERT', 'Products', NULL, NEW.Name);
            END");

            db.ExecuteNonQuery(@"
            INSERT INTO Products VALUES
                (6, 'Bolt',  'Hardware', 0.99, 5000),
                (7, 'Screw', 'Hardware', 0.49, 8000)");

            object cnt = db.ExecuteScalar("SELECT COUNT(*) FROM AuditLog");
            Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(2));
        }

        [Test]
        public void Trigger_BeforeInsert_CanModifyNewRow()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_MinPrice
            BEFORE INSERT ON Products
            FOR EACH ROW
            BEGIN
                IF NEW.Price < 1.0 THEN
                    SET NEW.Price = 1.0;
                END IF;
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Freebie', 'Samples', 0.0, 10)");

            object price = db.ExecuteScalar("SELECT Price FROM Products WHERE Id = 6");
            Assert.That(SqlExpr.ToNum(price), Is.EqualTo(1.0).Within(0.001));
        }

        [Test]
        public void Trigger_BeforeInsert_UpperCasesName()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_UpperName
            BEFORE INSERT ON Products
            FOR EACH ROW
            BEGIN
                SET NEW.Name = UPPER(NEW.Name);
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'gadget', 'Tools', 5.00, 50)");

            object name = db.ExecuteScalar("SELECT Name FROM Products WHERE Id = 6");
            Assert.That(name, Is.EqualTo("GADGET"));
        }

        [Test]
        public void Trigger_AfterUpdate_AccessesOldAndNewValues()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_ProductUpdate
            AFTER UPDATE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('UPDATE', 'Products', OLD.Name, NEW.Name);
            END");

            db.ExecuteNonQuery("UPDATE Products SET Name = 'Mega Hammer' WHERE Id = 1");

            var r = db.ExecuteReader("SELECT OldVal, NewVal FROM AuditLog WHERE Action = 'UPDATE'");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(1));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Hammer"));
                Assert.That(Cell(r, 1, 1), Is.EqualTo("Mega Hammer"));
            });
        }

        [Test]
        public void Trigger_AfterUpdate_FiresOncePerAffectedRow()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_upd
            AFTER UPDATE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('UPDATE', 'Products', OLD.Category, NEW.Category);
            END");

            db.ExecuteNonQuery("UPDATE Products SET Category = 'PowerTools' WHERE Category = 'Tools'");

            object cnt = db.ExecuteScalar("SELECT COUNT(*) FROM AuditLog");
            Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(3));
        }

        [Test]
        public void Trigger_BeforeUpdate_CapsPrice()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_MaxPrice
            BEFORE UPDATE ON Products
            FOR EACH ROW
            BEGIN
                IF NEW.Price > 999 THEN
                    SET NEW.Price = 999;
                END IF;
            END");

            db.ExecuteNonQuery("UPDATE Products SET Price = 99999 WHERE Id = 3");

            object price = db.ExecuteScalar("SELECT Price FROM Products WHERE Id = 3");
            Assert.That(SqlExpr.ToNum(price), Is.EqualTo(999).Within(0.001));
        }

        [Test]
        public void Trigger_BeforeUpdate_OldValueIsPreserved()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_CheckOld
            BEFORE UPDATE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('CHECK', 'Products', OLD.Name, NEW.Name);
            END");

            db.ExecuteNonQuery("UPDATE Products SET Name = 'Changed' WHERE Id = 2");

            object oldVal = db.ExecuteScalar("SELECT OldVal FROM AuditLog WHERE Action = 'CHECK'");
            Assert.That(oldVal, Is.EqualTo("Wrench"));
        }

        [Test]
        public void Trigger_AfterDelete_AccessesOldRow()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_ProductDelete
            AFTER DELETE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('DELETE', 'Products', OLD.Name, NULL);
            END");

            db.ExecuteNonQuery("DELETE FROM Products WHERE Id = 1");

            object logged = db.ExecuteScalar("SELECT OldVal FROM AuditLog WHERE Action = 'DELETE'");
            Assert.That(logged, Is.EqualTo("Hammer"));
        }

        [Test]
        public void Trigger_AfterDelete_FiresForEachDeletedRow()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_del
            AFTER DELETE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('DELETE', 'Products', OLD.Name, NULL);
            END");

            db.ExecuteNonQuery("DELETE FROM Products WHERE Category = 'Supplies'");

            object cnt = db.ExecuteScalar("SELECT COUNT(*) FROM AuditLog");
            Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(2));
        }

        [Test]
        public void Trigger_BeforeDelete_WritesToAnotherTable()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_BeforeDelete
            BEFORE DELETE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('PRE-DELETE', 'Products', OLD.Name, NULL);
            END");

            db.ExecuteNonQuery("DELETE FROM Products WHERE Id = 5");

            object logged = db.ExecuteScalar("SELECT OldVal FROM AuditLog WHERE Action = 'PRE-DELETE'");
            Assert.That(logged, Is.EqualTo("Paintbrush"));
        }

        [Test]
        public void Trigger_IfElseif_SelectsCorrectBranch()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_Tier
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                IF NEW.Price > 100 THEN
                    INSERT INTO AuditLog VALUES ('INSERT', 'Products', 'Premium', NEW.Name);
                ELSEIF NEW.Price > 10 THEN
                    INSERT INTO AuditLog VALUES ('INSERT', 'Products', 'Mid', NEW.Name);
                ELSE
                    INSERT INTO AuditLog VALUES ('INSERT', 'Products', 'Budget', NEW.Name);
                END IF;
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Diamond', 'Gems',    500.0, 1)");
            db.ExecuteNonQuery("INSERT INTO Products VALUES (7, 'Keyring', 'Gifting',  15.0, 50)");
            db.ExecuteNonQuery("INSERT INTO Products VALUES (8, 'Sticker', 'Gifting',   0.5, 1000)");

            var r = db.ExecuteReader("SELECT OldVal, NewVal FROM AuditLog ORDER BY NewVal");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(3));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("Premium")); // Diamond
                Assert.That(Cell(r, 2, 0), Is.EqualTo("Mid"));     // Keyring
                Assert.That(Cell(r, 3, 0), Is.EqualTo("Budget"));  // Sticker
            });
        }

        [Test]
        public void Trigger_MultipleTriggers_SameEvent_BothFire()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("CREATE TABLE Counter (N INT)");
            db.ExecuteNonQuery("INSERT INTO Counter VALUES (0)");

            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_A
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                UPDATE Counter SET N = N + 1;
            END");

            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_B
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                UPDATE Counter SET N = N + 10;
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Widget', 'Tools', 1.0, 1)");

            object n = db.ExecuteScalar("SELECT N FROM Counter");
            Assert.That(SqlExpr.ToNum(n), Is.EqualTo(11));
        }

        [Test]
        public void Trigger_BeforeAndAfter_BothFireInOrder()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_Before
            BEFORE INSERT ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('BEFORE', 'Products', NULL, NEW.Name);
            END");

            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_After
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('AFTER', 'Products', NULL, NEW.Name);
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Gadget', 'Tech', 49.99, 10)");

            var r = db.ExecuteReader("SELECT Action FROM AuditLog ORDER BY rowid");
            Assert.Multiple(() =>
            {
                Assert.That(DataRows(r), Is.EqualTo(2));
                Assert.That(Cell(r, 1, 0), Is.EqualTo("BEFORE"));
                Assert.That(Cell(r, 2, 0), Is.EqualTo("AFTER"));
            });
        }

        [Test]
        public void Trigger_Drop_StopsFiring()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_Drop
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('INSERT', 'Products', NULL, NEW.Name);
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Before', 'Test', 1.0, 1)");
            db.ExecuteNonQuery("DROP TRIGGER trg_Drop");
            db.ExecuteNonQuery("INSERT INTO Products VALUES (7, 'After', 'Test', 1.0, 1)");

            object cnt = db.ExecuteScalar("SELECT COUNT(*) FROM AuditLog");
            Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(1));
        }

        [Test]
        public void Trigger_DropIfExists_NoThrowWhenMissing()
        {
            var db = new SQLZero.SQLDatabase();
            Assert.DoesNotThrow(() => db.ExecuteNonQuery("DROP TRIGGER IF EXISTS NonExistent"));
        }

        [Test]
        public void Trigger_ExpressionOnNewColumns_EvaluatesCorrectly()
        {
            var db = BuildProductDb();
            db.ExecuteNonQuery("CREATE TABLE Totals (ProductId INT, LineTotal FLOAT)");

            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_LineTotal
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO Totals VALUES (NEW.Id, NEW.Price * NEW.Stock);
            END");

            db.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Widget', 'Tools', 10.0, 50)");

            object total = db.ExecuteScalar("SELECT LineTotal FROM Totals WHERE ProductId = 6");
            Assert.That(SqlExpr.ToNum(total), Is.EqualTo(500).Within(0.001));
        }

        [Test]
        public void Trigger_UpdateWhereNoMatch_DoesNotFire()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_NeverFire
            AFTER UPDATE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('UPDATE', 'Products', OLD.Name, NEW.Name);
            END");

            int affected = db.ExecuteNonQuery("UPDATE Products SET Name = 'X' WHERE Id = 9999");
            object cnt = db.ExecuteScalar("SELECT COUNT(*) FROM AuditLog");

            Assert.Multiple(() =>
            {
                Assert.That(affected, Is.EqualTo(0));
                Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(0));
            });
        }

        [Test]
        public void Trigger_JsonRoundTrip_TriggerRestoredAndFires()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_Persist
            AFTER INSERT ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('INSERT', 'Products', NULL, NEW.Name);
            END");

            var restored = SQLZero.SQLDatabase.FromJson(db.ToJson());
            restored.ExecuteNonQuery("INSERT INTO Products VALUES (6, 'Gadget', 'Tech', 9.99, 5)");

            object cnt = restored.ExecuteScalar("SELECT COUNT(*) FROM AuditLog");
            object name = restored.ExecuteScalar("SELECT NewVal FROM AuditLog");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(cnt), Is.EqualTo(1));
                Assert.That(name, Is.EqualTo("Gadget"));
            });
        }

        [Test]
        public void Trigger_SaveAndLoadJson_TriggerFires()
        {
            var db = BuildAuditDb();
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_FilePersist
            AFTER DELETE ON Products
            FOR EACH ROW
            BEGIN
                INSERT INTO AuditLog VALUES ('DELETE', 'Products', OLD.Name, NULL);
            END");

            string path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"trigdb_{Guid.NewGuid():N}.json");
            try
            {
                db.SaveJson(path);
                var restored = SQLZero.SQLDatabase.LoadJson(path);
                restored.ExecuteNonQuery("DELETE FROM Products WHERE Id = 2");
                object val = restored.ExecuteScalar("SELECT OldVal FROM AuditLog");
                Assert.That(val, Is.EqualTo("Wrench"));
            }
            finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
        }

        [Test]
        public void Trigger_ToJson_ContainsTriggersArray()
        {
            var db = new SQLZero.SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE T (X INT)");
            db.ExecuteNonQuery(@"
            CREATE TRIGGER trg_Json
            AFTER INSERT ON T
            FOR EACH ROW
            BEGIN
                INSERT INTO T VALUES (NEW.X + 1);
            END");

            string json = db.ToJson();
            using var doc = System.Text.Json.JsonDocument.Parse(json);

            Assert.Multiple(() =>
            {
                Assert.That(doc.RootElement.TryGetProperty("triggers", out var tArr), Is.True);
                Assert.That(tArr.GetArrayLength(), Is.EqualTo(1));
                Assert.That(tArr[0].GetProperty("name").GetString(), Is.EqualTo("trg_Json"));
                Assert.That(tArr[0].GetProperty("sql").GetString(), Does.Contain("CREATE TRIGGER"));
            });
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Add-ins
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public void AddIn_Delegate_CallableFromSql()
        {
            var db = new SQLDatabase();
            db.RegisterAddIn("Double", args => (object?)(SqlExpr.ToNum(args[0]) * 2));

            object? result = db.ExecuteScalar("SELECT Double(21)");
            Assert.That(SqlExpr.ToNum(result), Is.EqualTo(42).Within(0.001));
        }

        [Test]
        public void AddIn_Interface_CallableFromSql()
        {
            var db = BuildProductDb();
            db.RegisterAddIn(new TaxAddIn());

            var r = db.ExecuteReader("SELECT Name, TaxAmount(Price) AS Tax FROM Products WHERE Id = 1");
            // Hammer: 12.99 * 0.1 = 1.299
            Assert.That(SqlExpr.ToNum(Cell(r, 1, 1)), Is.EqualTo(1.299).Within(0.001));
        }

        [Test]
        public void AddIn_MultiArg_ReceivesAllArgs()
        {
            var db = new SQLDatabase();
            db.RegisterAddIn("Clamp", args =>
            {
                double val = SqlExpr.ToNum(args[0]);
                double lo = SqlExpr.ToNum(args[1]);
                double hi = SqlExpr.ToNum(args[2]);
                return (object?)Math.Max(lo, Math.Min(hi, val));
            });

            object? low = db.ExecuteScalar("SELECT Clamp(5, 10, 20)");
            object? mid = db.ExecuteScalar("SELECT Clamp(15, 10, 20)");
            object? high = db.ExecuteScalar("SELECT Clamp(25, 10, 20)");
            Assert.Multiple(() =>
            {
                Assert.That(SqlExpr.ToNum(low), Is.EqualTo(10));
                Assert.That(SqlExpr.ToNum(mid), Is.EqualTo(15));
                Assert.That(SqlExpr.ToNum(high), Is.EqualTo(20));
            });
        }

        [Test]
        public void AddIn_NullReturn_IsSqlNull()
        {
            var db = new SQLDatabase();
            db.RegisterAddIn("AlwaysNull", _ => null);

            object? result = db.ExecuteScalar("SELECT AlwaysNull(1)");
            Assert.That(result, Is.Null);
        }

        [Test]
        public void AddIn_ShadowsBuiltin()
        {
            var db = new SQLDatabase();
            // Override UPPER to return a fixed marker instead of uppercasing
            db.RegisterAddIn("UPPER", _ => (object?)"__OVERRIDDEN__");

            object? result = db.ExecuteScalar("SELECT UPPER('hello')");
            Assert.That(result, Is.EqualTo("__OVERRIDDEN__"));
        }

        [Test]
        public void AddIn_Unregister_ReturnsNullThereafter()
        {
            var db = new SQLDatabase();
            db.RegisterAddIn("Ping", _ => (object?)"pong");
            db.UnregisterAddIn("Ping");

            // After unregistering, the name is unknown — engine returns null
            object? result = db.ExecuteScalar("SELECT Ping()");
            Assert.That(result, Is.Null);
        }

        [Test]
        public void AddIn_RegisteredAddIns_ReflectsCurrentRegistrations()
        {
            var db = new SQLDatabase();
            db.RegisterAddIn("Alpha", _ => null);
            db.RegisterAddIn("Beta", _ => null);

            Assert.That(db.RegisteredAddIns, Has.Member("Alpha"));
            Assert.That(db.RegisteredAddIns, Has.Member("Beta"));
            Assert.That(db.RegisteredAddIns.Count, Is.EqualTo(2));

            db.UnregisterAddIn("Alpha");
            Assert.That(db.RegisteredAddIns, Has.No.Member("Alpha"));
            Assert.That(db.RegisteredAddIns.Count, Is.EqualTo(1));
        }

        [Test]
        public void AddIn_UsedInWhereClause()
        {
            var db = BuildProductDb();
            // Returns 1 if the product is expensive (> 50), 0 otherwise
            db.RegisterAddIn("Expensive", args => (object?)(SqlExpr.ToNum(args[0]) > 50 ? 1L : 0L));

            var r = db.ExecuteReader("SELECT Name FROM Products WHERE Expensive(Price) = 1");
            Assert.That(DataRows(r), Is.EqualTo(1));
            Assert.That(Cell(r, 1, 0), Is.EqualTo("Drill"));
        }

        [Test]
        public void AddIn_UsedInOrderBy()
        {
            var db = BuildProductDb();
            // Returns the last character of a string for ordering
            db.RegisterAddIn("LastChar", args =>
                (object?)(args[0]?.ToString() is { Length: > 0 } s ? s[^1..] : ""));

            var r = db.ExecuteReader("SELECT Name FROM Products ORDER BY LastChar(Name) ASC LIMIT 1");
            // Paintbrush ends in 'h', Drill ends in 'l', Hammer ends in 'r', Wrench ends in 'h', Paint ends in 't'
            // 'h' is the lowest alphabetically (Paintbrush or Wrench)
            Assert.That(DataRows(r), Is.EqualTo(1));
        }

        /// <summary>ISqlAddIn implementation used by add-in tests.</summary>
        private sealed class TaxAddIn : ISqlAddIn
        {
            public string FunctionName => "TaxAmount";
            public object? Invoke(object?[] args) =>
                (object?)(SqlExpr.ToNum(args[0]) * 0.1);
        }

        // ──────────────────────────────────────────────────────────────────────────
        //  Async API
        // ──────────────────────────────────────────────────────────────────────────

        [Test]
        public async Task Async_ExecuteNonQueryAsync_ReturnsAffectedCount()
        {
            var db = BuildProductDb();
            int n = await db.ExecuteNonQueryAsync(
                "UPDATE Products SET Price = Price * 0.9 WHERE Category = 'Tools'");
            Assert.That(n, Is.EqualTo(3));
        }

        [Test]
        public async Task Async_ExecuteNonQueryAsync_DdlReturnsZero()
        {
            var db = new SQLDatabase();
            int n = await db.ExecuteNonQueryAsync("CREATE TABLE AsyncTable (Id INT)");
            Assert.That(n, Is.EqualTo(0));
        }

        [Test]
        public async Task Async_ExecuteScalarAsync_ReturnsCorrectValue()
        {
            var db = BuildProductDb();
            object? result = await db.ExecuteScalarAsync(
                "SELECT COUNT(*) FROM Products WHERE Category = 'Tools'");
            Assert.That(SqlExpr.ToNum(result), Is.EqualTo(3));
        }

        [Test]
        public async Task Async_ExecuteScalarAsync_NoRows_ReturnsNull()
        {
            var db = BuildProductDb();
            object? result = await db.ExecuteScalarAsync(
                "SELECT Name FROM Products WHERE Id = 9999");
            Assert.That(result, Is.Null);
        }

        [Test]
        public async Task Async_ExecuteReaderAsync_ReturnsCorrectHeaders()
        {
            var db = BuildProductDb();
            var (headers, _) = await db.ExecuteReaderAsync(
                "SELECT Name, Price FROM Products LIMIT 1");
            Assert.Multiple(() =>
            {
                Assert.That(headers, Has.Length.EqualTo(2));
                Assert.That(headers[0], Is.EqualTo("Name"));
                Assert.That(headers[1], Is.EqualTo("Price"));
            });
        }

        [Test]
        public async Task Async_ExecuteReaderAsync_ReturnsCorrectRowCount()
        {
            var db = BuildProductDb();
            var (_, rows) = await db.ExecuteReaderAsync(
                "SELECT Name FROM Products WHERE Category = 'Tools'");

            int count = 0;
            await foreach (var _ in rows) count++;
            Assert.That(count, Is.EqualTo(3));
        }

        [Test]
        public async Task Async_ExecuteReaderAsync_RowValuesCorrect()
        {
            var db = BuildProductDb();
            var (headers, rows) = await db.ExecuteReaderAsync(
                "SELECT Name, Price FROM Products ORDER BY Price DESC LIMIT 1");

            Assert.That(headers[0], Is.EqualTo("Name"));

            var collected = new List<object?[]>();
            await foreach (var row in rows) collected.Add(row);

            Assert.That(collected.Count, Is.EqualTo(1));
            Assert.That(collected[0][0], Is.EqualTo("Drill"));
            Assert.That(SqlExpr.ToNum(collected[0][1]), Is.EqualTo(149.99).Within(0.001));
        }

        [Test]
        public async Task Async_ExecuteReaderAsync_EmptyResult_ReturnsEmptyRows()
        {
            var db = BuildProductDb();
            var (headers, rows) = await db.ExecuteReaderAsync(
                "SELECT Name FROM Products WHERE Id = 9999");

            Assert.That(headers, Has.Length.EqualTo(1));

            int count = 0;
            await foreach (var _ in rows) count++;
            Assert.That(count, Is.EqualTo(0));
        }

        [Test]
        public async Task Async_ExecuteReaderAsync_WithAddIn_Works()
        {
            var db = BuildProductDb();
            db.RegisterAddIn("Doubled", args => (object?)(SqlExpr.ToNum(args[0]) * 2));

            var (headers, rows) = await db.ExecuteReaderAsync(
                "SELECT Name, Doubled(Price) AS DP FROM Products WHERE Id = 1");

            Assert.That(headers[1], Is.EqualTo("DP"));

            var collected = new List<object?[]>();
            await foreach (var row in rows) collected.Add(row);
            Assert.That(SqlExpr.ToNum(collected[0][1]), Is.EqualTo(25.98).Within(0.001));
        }

        [Test]
        public async Task Async_ExecuteReaderAsync_CancellationToken_CancelsBeforeCompletion()
        {
            // Build a large table so there are enough rows to make cancellation
            // observable between row evaluations
            var db = new SQLDatabase();
            db.ExecuteNonQuery("CREATE TABLE Big (N INT)");
            for (int i = 0; i < 500; i++)
                db.ExecuteNonQuery($"INSERT INTO Big VALUES ({i})");

            using var cts = new CancellationTokenSource();
            cts.Cancel(); // cancel immediately

            // ThrowsAsync checks exact type; CatchAsync handles subclasses (TaskCanceledException
            // is a subclass of OperationCanceledException and is what Task.Run surfaces on cancel)
            Assert.CatchAsync<OperationCanceledException>(async () =>
            {
                var (_, rows) = await db.ExecuteReaderAsync("SELECT * FROM Big", cts.Token);
                await foreach (var _ in rows.WithCancellation(cts.Token)) { }
            });
        }
    }
}