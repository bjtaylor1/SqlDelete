namespace SqlDelete
{
    public class ConflictDeleteResult : ModifyResult
    {
        public ConflictDeleteResult(string constraint, string database, string table, string column)
        {
            Constraint = constraint;
            Database = database;
            Table = table;
            Column = column;
        }

        public string Constraint{get;set;}
        public string Database{get;set;}
        public string Table{get;set;}
        public string Column{get;set;}
    }
}
