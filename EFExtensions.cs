using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace PrebookingModuleCore
{
    public static class EFExtensions
    {
        /// <summary>
        /// Creates an initial DbCommand object based on a sql command 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public static DbCommand LoadSqlCommand(this DbContext context, string sqlCommand)
        {
            var cmd = context.Database.GetDbConnection().CreateCommand();
            cmd.CommandText = sqlCommand;
            cmd.CommandType = System.Data.CommandType.Text;
            return cmd;
        }

        /// <summary>
        /// Creates an initial DbCommand object based on a stored procedure name
        /// </summary>
        /// <param name="context"></param>
        /// <param name="storedProcName"></param>
        /// <returns></returns>
        public static DbCommand LoadStoredProcedure(this DbContext context, string storedProcName)
        {
            var cmd = context.Database.GetDbConnection().CreateCommand();
            cmd.CommandText = storedProcName;
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            return cmd;
        }

        /// <summary>
        /// Creates a DbParameter object and adds it to a DbCommand
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="paramName"></param>
        /// <param name="paramValue"></param>
        /// <returns></returns>
        public static DbCommand WithSqlParam(this DbCommand cmd, string paramName, object paramValue)
        {
            if (string.IsNullOrEmpty(cmd.CommandText) && cmd.CommandType != System.Data.CommandType.Text && cmd.CommandType != System.Data.CommandType.StoredProcedure)
                throw new InvalidOperationException("Call LoadSqlCommand or LoadStoredProceudre before using this method");

            var param = cmd.CreateParameter();
            param.ParameterName = paramName;
            param.Value = paramValue;
            return AddSqlParam(cmd, param);
        }

        public static DbCommand WithSqlParam(this DbCommand cmd, DbParameter param)
        {
            if (string.IsNullOrEmpty(cmd.CommandText) && cmd.CommandType != System.Data.CommandType.Text && cmd.CommandType != System.Data.CommandType.StoredProcedure)
                throw new InvalidOperationException("Call LoadSqlCommand or LoadStoredProceudre before using this method");

            return AddSqlParam(cmd, param);
        }

        /// <summary>
        /// Executes a DbDataReader and returns a list of mapped column values to the properties of <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        public static IList<T> ExecuteSqlCommand<T>(this DbCommand command)
        {
            using (command)
            {
                if (command.Connection.State == System.Data.ConnectionState.Closed)
                    command.Connection.Open();
                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        return reader.MapToList<T>();
                    }
                }
                finally
                {
                    command.Connection.Close();
                }
            }
        }

        /// <summary>
        /// Executes a DbDataReader and returns a list of mapped column values to the properties of <typeparamref name="T"/> Asynchronous
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        public async static Task<IList<T>> ExecuteSqlCommandAsync<T>(this DbCommand command)
        {
            var result = await Task.Run(() => {
                using (command)
                {
                    if (command.Connection.State == System.Data.ConnectionState.Closed)
                        command.Connection.Open();
                    try
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            return reader.MapToList<T>();
                        }
                    }
                    finally
                    {
                        command.Connection.Close();
                    }
                }
            });

            return result;
        }

        /// <summary>
        /// Retrieves the column values from the sql command and maps them to <typeparamref name="T"/>'s properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dr"></param>
        /// <returns>IList<<typeparamref name="T"/>></returns>
        private static IList<T> MapToList<T>(this DbDataReader dr)
        {
            if(!typeof(T).IsValueType && typeof(T) != typeof(string))
            {
                return MapToListReferenceType<T>(dr);
            }
            return MapToListValueType<T>(dr);
        }

        private static IList<T> MapToListReferenceType<T>(DbDataReader dr)
        {
            var objList = new List<T>();
            var props = typeof(T).GetRuntimeProperties();

            var colMapping = dr.GetColumnSchema()
                .Where(x => props.Any(y => y.Name.ToLower() == x.ColumnName.ToLower()))
                .ToDictionary(key => key.ColumnName.ToLower());

            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    T obj = Activator.CreateInstance<T>();
                    foreach (var prop in props)
                    {
                        if (colMapping.ContainsKey(prop.Name.ToLower()))
                        {
                            var val = dr.GetValue(colMapping[prop.Name.ToLower()].ColumnOrdinal.Value);
                            prop.SetValue(obj, val == DBNull.Value ? null : val);
                        }
                        else
                        {
                            prop.SetValue(obj, GetDefault(prop.PropertyType));
                        }
                    }
                    objList.Add(obj);
                }
            }
            return objList;
        }

        private static IList<T> MapToListValueType<T>(DbDataReader dr)
        {
            var objList = new List<T>();
            if(dr.HasRows)
            {
                var column = dr.GetColumnSchema().First();
                if (column.DataType != typeof(T))
                {
                    throw new Exception("Typeparamref not match");
                }
                while (dr.Read())
                {
                    objList.Add((T)dr.GetValue(column.ColumnOrdinal.Value));
                }
            }
            return objList;
        }

        private static DbCommand AddSqlParam(DbCommand cmd, DbParameter param)
        {
            cmd.Parameters.Add(param);
            return cmd;
        }

        private static object GetDefault(Type type)
        {
            if (!type.IsValueType && !type.IsInterface)
            {
                return Activator.CreateInstance(type);
            }
            return null;
        }
    }
}
