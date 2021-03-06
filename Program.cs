using System;
using System.Data.SqlClient;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Ultimate.ORM;

namespace SqlDelete
{
    class Program
    {
        static CancellationTokenSource cts = new CancellationTokenSource();
        static Program()
        {
            Console.CancelKeyPress += (sender, args) => cts.Cancel();
        }
        
        static Lazy<ObjectMapper> objectMapper = new Lazy<ObjectMapper>();
        static async Task<int> Main(string[] args)
        {
            try
            {
                if(args.Length != 3) 
                {
                    await Console.Out.WriteLineAsync($"Usage: SqlDelete [database] [table] \"[condition]\"");
                    return 1;
                }
                var database = args[0];
                var table = args[1];
                var condition = args[2];
                await DoDelete(new DeleteSpec(database, table, condition));
                return 0;
            }
            catch(Exception ex)
            {
                await Console.Out.WriteLineAsync(ex.ToString());
                return 1;
            }
        }

        static async Task DoDelete(ModifySpec deleteSpec)
        {
            // hardcode to local - mustn't run on shared dbs!
            var deleteResult = await TryModify(deleteSpec);
            if(deleteResult is ConflictDeleteResult conflictDeleteResult)
            {
                var blocker = await GetBlocker(deleteSpec, conflictDeleteResult);
                await DoDelete(blocker);
                await DoDelete(deleteSpec);
            }

            //The DELETE statement conflicted with the REFERENCE constraint "FK_LeaveRequest_StatusUpdatedByUserId". The conflict occurred in database "Payroll_Shard4", table "dbo.LeaveRequest", column 'StatusUpdatedByUserId'.
        }

        static async Task<ModifySpec> GetBlocker(ModifySpec originalSpec, ConflictDeleteResult conflictDeleteResult)
        {
            var constraintInfo = await GetConstraintInfo(conflictDeleteResult);
            var condition = $"{conflictDeleteResult.Column} IN (SELECT [{constraintInfo.ReferencedColumnName}] FROM [{originalSpec.Table}] WHERE {originalSpec.Condition})";
            if(constraintInfo.ParentColumnIsNullable)
            {
                return new NullOutSpec(conflictDeleteResult.Database, conflictDeleteResult.Table, conflictDeleteResult.Column, condition);
            }
            else
            {
                return new DeleteSpec(conflictDeleteResult.Database, conflictDeleteResult.Table, condition);
            }
        }

        static async Task<ConstraintDetails> GetConstraintInfo(ConflictDeleteResult conflictDeleteResult)
        {
            await using var sqlConnection = new SqlConnection($"Data Source=localhost\\sqlexpress;Initial Catalog={conflictDeleteResult.Database};Integrated Security=SSPI");
            await sqlConnection.OpenAsync(cts.Token);
            await using var sqlCommand = new SqlCommand(@$"
                select rc.name ReferencedColumnName, pc.is_nullable ParentColumnIsNullable
                from sys.foreign_key_columns fk
                join sys.columns rc on fk.referenced_column_id = rc.column_id and fk.referenced_object_id = rc.object_id
                join sys.columns pc on fk.parent_column_id = pc.column_id and fk.parent_object_id = pc.object_id
                where fk.constraint_object_id = object_id(@constraintName)
                ", sqlConnection);
            sqlCommand.Parameters.AddWithValue("constraintName", conflictDeleteResult.Constraint);
            return await objectMapper.Value.ToSingleObject<ConstraintDetails>(sqlCommand);
        }

        //^The DELETE statement conflicted with the REFERENCE constraint "(?<constraint>.+)". The conflict occurred in database "(?<database>.+)", table "(?<table>.+)", column '(?<column>.+)'.$
        static Regex conflictException = new Regex(@"The DELETE statement conflicted with the (?:SAME TABLE )?REFERENCE constraint ""(?<constraint>.+)"". The conflict occurred in database ""(?<database>.+)"", table ""(?<table>.+)"", column '(?<column>.+)'.");
        static async Task<ModifyResult> TryModify(ModifySpec modifySpec)
        {
            try
            {
                await using var sqlConnection = new SqlConnection($"Data Source=localhost\\sqlexpress;Initial Catalog={modifySpec.Database};Integrated Security=SSPI");
                await sqlConnection.OpenAsync(cts.Token);

                string sql = modifySpec.GetSql();
                await using var sqlCommand = new SqlCommand(sql, sqlConnection) { CommandTimeout = 0 };
                await sqlCommand.ExecuteNonQueryAsync(cts.Token);
                await Console.Out.WriteLineAsync(sql);
                return new SuccessModifyResult();
            }
            catch(SqlException ex)
            {
                var conflictExceptionMatch = conflictException.Match(ex.Message);
                if(conflictExceptionMatch.Success)
                {
                    var constraint = conflictExceptionMatch.Groups["constraint"].Value;
                    var database = conflictExceptionMatch.Groups["database"].Value;
                    var table = conflictExceptionMatch.Groups["table"].Value;
                    var column = conflictExceptionMatch.Groups["column"].Value;
                    return new ConflictDeleteResult(constraint, database, table, column);
                }
                else throw ex;
            }
        }
    }
}
