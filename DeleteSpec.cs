namespace SqlDelete
{
    public class DeleteSpec : ModifySpec
    {
        public DeleteSpec(string database, string table, string condition) : base(database, table, condition)
        {
        }

        public override string GetSql() => $"DELETE [{Table}] WHERE {Condition}";
    }
}
