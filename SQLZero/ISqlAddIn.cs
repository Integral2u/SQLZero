namespace SQLZero
{
    // ============================================================
    //  ADD-IN CONTRACT
    // ============================================================

    /// <summary>
    /// Contract for user-defined add-in functions that are callable directly
    /// from SQL expressions — e.g. <c>SELECT MyAddIn(col1, col2) FROM t</c>.
    /// <para>
    /// Register implementations via
    /// <see cref="SQLDatabase.RegisterAddIn(ISqlAddIn)"/> or the
    /// convenience overload that accepts a plain delegate.
    /// Add-ins are resolved <b>before</b> the built-in function table, so they
    /// can shadow any built-in if needed.
    /// </para>
    /// <para>
    /// <b>Async add-ins:</b> if your implementation performs I/O (e.g. calling
    /// an external API), call <c>.GetAwaiter().GetResult()</c> internally and
    /// invoke the query via <see cref="SQLDatabase.ExecuteReaderAsync"/>
    /// so the blocking work happens on a thread-pool thread, not the UI thread.
    /// </para>
    /// </summary>
    public interface ISqlAddIn
    {
        /// <summary>
        /// The function name exactly as it will appear in SQL (case-insensitive).
        /// </summary>
        string FunctionName { get; }

        /// <summary>
        /// Invokes the add-in with the already-evaluated SQL argument values.
        /// Return <c>null</c> to represent a SQL <c>NULL</c> result.
        /// </summary>
        /// <param name="args">Evaluated argument values. May contain <c>null</c> entries.</param>
        object? Invoke(object?[] args);
    }
}