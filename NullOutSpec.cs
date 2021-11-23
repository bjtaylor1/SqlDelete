namespace SqlDelete
{
    public class NullOutSpec : ModifySpec
    {
        private readonly string column;

        public NullOutSpec(string database, string table, string column, string condition) : base(database, table, condition)
        {
            this.column = column;
        }

        public override string GetSql() => $"UPDATE [{Table}] SET [{column}] = null WHERE {Condition}";
    }
}
