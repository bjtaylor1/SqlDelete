using System.Text.RegularExpressions;

namespace SqlDelete
{
    public abstract class ModifySpec
    {
        public ModifySpec(string database, string table, string condition)
        {
            Database = database;
            Table = Regex.Replace(table, @"^dbo\.", "");
            Condition = condition;
        }

        public string Database{get;set;}
        public string Table{get;set;}
        public string Condition{get;set;}

        public abstract string GetSql();
    }
}
