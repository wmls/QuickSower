using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Text;

namespace DBHelper
{
    public class DbHelper
    {
        public DbHelper()
        {
            //
            // TODO: 在此处添加构造函数逻辑
            //
        }

        public static DataSet Query(string SQLString)
        {
            DataSet ds = new DataSet();
            switch (ConfigurationManager.AppSettings["DBType"])
            {
                case "MsSql":
                    ds = SQLHelper.Query(SQLString);
                    break;
                case "Oracle":
                    ds = OracleHelper.Query(SQLString);
                    break;
                default:
                    ds = SQLHelper.Query(SQLString);
                    break;
            }
            return ds;
        }

        public static int ExecuteSql(string SQLString)
        {
            int itemp;
            switch (ConfigurationManager.AppSettings["DBType"])
            {
                case "MsSql":
                    itemp = SQLHelper.ExecuteSql(SQLString);
                    break;
                case "Oracle":
                    itemp = OracleHelper.ExecuteSql(SQLString);
                    break;
                default:
                    itemp = SQLHelper.ExecuteSql(SQLString);
                    break;
            }
            return itemp;
        }

        public static object GetSingle(string SQLString)
        {
            object itemp;
            switch (ConfigurationManager.AppSettings["DBType"])
            {
                case "MsSql":
                    itemp = SQLHelper.GetSingle(SQLString);
                    break;
                case "Oracle":
                    itemp = OracleHelper.GetSingle(SQLString);
                    break;
                default:
                    itemp = SQLHelper.GetSingle(SQLString);
                    break;
            }
            return itemp;
        }
        
        public static int GetMaxID(string FieldName, string TableName)
        {
            int itemp;
            switch (ConfigurationManager.AppSettings["DBType"])
            {
                case "MsSql":
                    itemp = SQLHelper.GetMaxID(FieldName, TableName);
                    break;
                case "Oracle":
                    itemp = OracleHelper.GetMaxID(FieldName, TableName);
                    break;
                default:
                    itemp = SQLHelper.GetMaxID(FieldName, TableName);
                    break;
            }
            return itemp;
        }
    }
}
