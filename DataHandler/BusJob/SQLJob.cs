using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using DataHandler.BusDataClass;
using System.Collections.Specialized;
using DBHelper;
using System.Data.SqlClient;

namespace DataHandler.BusJob
{
    public class SQLJob
    {
        public SQLJob()
        {
        }

        #region DML字段校验
        //DML操作中需要的字段
        public enum DataType
        {
            DBBigint = 0,
            DBBinary = 1,
            DBBit = 2,
            DBChar = 3,
            DBDate = 4,
            DBDatetime = 5,
            DBDatetime2 = 6,
            DBDatetimeoffset = 7,
            DBDecimal = 8,
            DBFloat = 9,
            DBGeography = 10,
            DBGeometry = 11,
            DBHierarchyid = 12,
            DBImage = 13,
            DBInt = 14,
            DBMoney = 15,
            DBNchar = 16,
            DBNtext = 17,
            DBNumeric = 18,
            DBNvarchar = 19,
            DBReal = 20,
            DBSmalldatetime = 21,
            DBSmallint = 22,
            DBSmallmoney = 23,
            DBSql_variant = 24,
            DBSysname = 25,
            DBText = 26,
            DBTime = 27,
            DBTimestamp = 28,
            DBTinyint = 29,
            DBUniqueidentifier = 30,
            DBVarbinary = 31,
            DBVarchar = 32,
            DBXml = 33,
        }
        public enum SQLDataObjectType
        {
            Table = 0,
            View = 1,
            UnKnown = 2
        }
        public enum FieldCatagory
        {
            AllFields = 0,
            DMLFields = 1,
            PrimaryKeyFields = 2,
            UniqueFields = 3,
            AutoIncrementFields = 4,
            TimestampField = 5,
            PrimaryKeyandAutoIncrementFields = 6,
            PrimaryKeyandUniqueFields = 7,
            UniqueandAutoIncrementFields = 8,
            PrimaryKeyandUniqueandAutoIncrementFields = 9,
            MasterForeignFields = 10,
            SlaveForeignFields = 11
        }
        public struct FieldAttr
        {
            public DataType DataType;
            public bool IsAutoIncrement;
            public bool IsUniqueKey;
            public bool IsPrimaryKey;
            public bool IsNullable;
            public bool IsTimestamp;
            public string DefaultValue;
            public string MasterForeignKeys;
            public string SlaveForeignKeys;
        }
        public SQLDataObjectType GetSQLDataObjectType(string DataObjectName)
        {
            string[] arschema = DataObjectName.Split(new char[1] { '.' });
            string DB = (arschema.Length == 3 ? arschema[0] : "");
            DataObjectName = (arschema.Length == 3 ? arschema[2] : DataObjectName);
            SQLDataObjectType objType = SQLDataObjectType.UnKnown;
            string SQLString = @"SELECT LTRIM(RTRIM(type)) AS [type] FROM sys.all_objects WHERE name = '" + DataObjectName + "'";
            SQLString = (DB == string.Empty ? "" : "USE [" + DB + "]" + char.ConvertFromUtf32(13)) + SQLString;
            DataSet ds = DbHelper.Query(SQLString);
            DataTable dt = ds != null ? ds.Tables[0] : null;
            if (dt != null && dt.Rows.Count>0)
            {
                string ctype = dt.Rows[0][0].ToString().ToLower();
                objType = (ctype == "u" ? SQLDataObjectType.Table : (ctype == "v" ? SQLDataObjectType.View : SQLDataObjectType.UnKnown));
            }
            return objType;
        }
        public Dictionary<string, FieldAttr> TableFields(string DataObjectName, string MasterTableName, SQLDataObjectType DataObjectType, bool IsDML)
        {
            string[] arschema = DataObjectName.Split(new char[1] { '.' });
            string DB = (arschema.Length == 3 ? arschema[0] : "");
            DataObjectName = (arschema.Length == 3 ? arschema[2] : DataObjectName);
            Dictionary<string, FieldAttr> dmlFlds = new Dictionary<string, FieldAttr>();
            string SQLString = @"SELECT distinct " + (DataObjectType == SQLDataObjectType.View ? "TableName,OwnerTable" : "TableName") + @",FieldName,DataType,isnullable,isidentity,isunique,istimestamp,PK_Column,SlaveForeignKeys,MasterForeignKeys
                FROM(
	                SELECT " + (DataObjectType == SQLDataObjectType.View ? "v.name AS TableName, OBJECT_NAME(t.depid) AS OwnerTable" : "t.name AS TableName") + @" --, t.type
		                            ,y.NAME AS DataType
		                            ," + (DataObjectType == SQLDataObjectType.View ?
                                           @"_c.name AS FieldName, _c.type,_c.usertype,_c.length,_c.isnullable, columnproperty(_c.id,_c.name,'isidentity') AS  isidentity"
                                           :
                                           @"c.name AS FieldName, c.type,c.usertype,c.length,c.isnullable, columnproperty(c.id,c.name,'isidentity') AS  isidentity") + @"
                                    ,isnull(fk.is_unique,0) as isunique,CONVERT(BIT,CASE y.name WHEN 'timestamp' THEN 1 ELSE 0 END) AS istimestamp,isnull(fk.is_primary_key,0) AS PK_Column
									,STUFF((SELECT ','+OBJECT_NAME(fkc.parent_object_id)+'.'+fc.name
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.parent_object_id= fc.id AND fkc.parent_column_id = fc.colid WHERE fkc.referenced_object_id=c.id and fkc.referenced_column_id=c.colid 
											FOR XML PATH('')),1,1,'') AS SlaveForeignKeys
									,STUFF((SELECT ','+OBJECT_NAME(fkc.referenced_object_id)+'.'+fc.name 
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.referenced_object_id= fc.id AND fkc.referenced_column_id = fc.colid WHERE fkc.parent_object_id=c.id and fkc.parent_column_id=c.colid
											FOR XML PATH('')),1,1,'') AS MasterForeignKeys
	                            FROM " + (DataObjectType == SQLDataObjectType.View ?
                                           @"sys.all_views v
	                            INNER JOIN sys.syscolumns _c ON _c.id = v.object_id
	                            INNER JOIN sys.types y ON _c.xtype = y.user_type_id
	                            LEFT JOIN (sys.sysdepends t
			                            INNER JOIN sys.syscolumns c ON t.depid = c.id AND t.depnumber = c.colid AND OBJECT_NAME(t.id) = '" + DataObjectName + @"'
	                            ) ON  _c.name = c.name AND _c.xtype = c.xtype AND EXISTS
										 (
											SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id
												FROM sys.index_columns a1
												INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
												INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
												INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
												WHERE o1.type = 'u' --AND  o1.name+'.'+c1.name = OBJECT_NAME(t.depid)+ '.' + c.name -- 按约定视图命名规则，与主数据表匹配
										 ) "
                                           : @"sys.all_objects t
                                INNER JOIN sys.syscolumns c ON t.object_id = c.id
	                            INNER JOIN sys.types y ON c.xtype = y.user_type_id") + @"
	                            LEFT JOIN (
		                            SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id,CONVERT(BIT, MAX(CONVERT(INT,idx.is_primary_key))) AS is_primary_key,CONVERT(BIT, MAX(CONVERT(INT,idx.is_unique))) AS is_unique 
		                            FROM sys.index_columns a1
									INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
		                            INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
		                            INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
		                            WHERE o1.type = 'u'
                                    GROUP BY o1.name, c1.object_id,c1.name,a1.column_id
	                            ) fk ON c.id = fk.object_id AND fk.column_id =  c.colid
	                            WHERE " + (DataObjectType == SQLDataObjectType.View ? "v.name" : "t.type = 'U' AND t.name") + @"
	                             = '" + DataObjectName + @"'
	                ) tbl  
				" + (DataObjectType == SQLDataObjectType.View && MasterTableName != null && MasterTableName != "" ? "WHERE (tbl.OwnerTable ='" + MasterTableName + "' AND (tbl.PK_Column = 1 OR tbl.isidentity =1 OR tbl.isunique = 1) OR tbl.PK_Column <> 1 AND tbl.isidentity <>1 AND tbl.isunique <> 1)" : "") + @"
                WHERE tbl.isidentity != 1 AND tbl.PK_Column != 1
                ORDER BY TableName, FieldName";
            SQLString = (DB == string.Empty ? "" : "USE [" + DB + "]" + char.ConvertFromUtf32(13)) + SQLString;
            DataSet ds = DbHelper.Query(SQLString);
            DataTable dt = ds != null ? ds.Tables[0] : null;
            if (dt != null)
            {
                FieldAttr fldat = new FieldAttr();
                for (int r = 0; r < dt.Rows.Count; r++)
                {
                    DataRow dr = dt.Rows[r];
                    string typename = dr["DataType"].ToString();
                    typename = "DB" + typename.Substring(0, 1).ToUpper() + typename.Substring(1);
                    fldat.DataType = (DataType)Enum.Parse(typeof(DataType), typename);
                    fldat.IsNullable = (dt.Columns["isnullable"].DataType.FullName == "System.Boolean" ? (bool)dr["isnullable"] : (dr["isnullable"].ToString() == "1" || dr["isnullable"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsAutoIncrement = (dt.Columns["isidentity"].DataType.FullName == "System.Boolean" ? (bool)dr["isidentity"] : (dr["isidentity"].ToString() == "1" || dr["isidentity"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsUniqueKey = (dt.Columns["isunique"].DataType.FullName == "System.Boolean" ? (bool)dr["isunique"] : (dr["isunique"].ToString() == "1" || dr["isunique"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsPrimaryKey = (dt.Columns["PK_Column"].DataType.FullName == "System.Boolean" ? (bool)dr["PK_Column"] : (dr["PK_Column"].ToString() == "1" || dr["PK_Column"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsTimestamp = (dt.Columns["istimestamp"].DataType.FullName == "System.Boolean" ? (bool)dr["istimestamp"] : (dr["istimestamp"].ToString() == "1" || dr["istimestamp"].ToString().ToLower() == "true" ? true : false));
                    dmlFlds.Add((DataObjectType == SQLDataObjectType.View ? (dr["OwnerTable"] == DBNull.Value || dr["OwnerTable"].ToString() == "" ? "" : dr["OwnerTable"].ToString() + ".") : "") + dr["FieldName"].ToString(), fldat);
                }
            }

            return dmlFlds;
        }

        public Dictionary<string, FieldAttr> TableFields(string DataObjectName, string MasterTableName, SQLDataObjectType DataObjectType, bool IsDML, bool IsWithUserDefaults, string MasterForeignTable, string SlaveForeignTable)
        {
            string[] arschema = DataObjectName.Split(new char[1] { '.' });
            string DB = (arschema.Length == 3 ? arschema[0] : "");
            DataObjectName = (arschema.Length == 3 ? arschema[2] : DataObjectName);
            Dictionary<string, FieldAttr> dmlFlds = new Dictionary<string, FieldAttr>();
            string SQLString = @"SELECT distinct " + (DataObjectType == SQLDataObjectType.View ? "TableName,OwnerTable" : "TableName") + @",FieldName,DataType,isnullable,isidentity,isunique,istimestamp,PK_Column,SlaveForeignKeys,MasterForeignKeys"
                + (IsWithUserDefaults ? ",def.[cDefaultValue] AS UserDefault" : "")
                + @" FROM(
	                SELECT " + (DataObjectType == SQLDataObjectType.View ? "v.name AS TableName, OBJECT_NAME(t.depid) AS OwnerTable" : "t.name AS TableName") + @" --, t.type
		                            ,y.NAME AS DataType
		                            ," + (DataObjectType == SQLDataObjectType.View ?
                                           @"_c.name AS FieldName, _c.type,_c.usertype,_c.length,_c.isnullable, columnproperty(_c.id,_c.name,'isidentity') AS  isidentity"
                                           :
                                           @"c.name AS FieldName, c.type,c.usertype,c.length,c.isnullable, columnproperty(c.id,c.name,'isidentity') AS  isidentity") + @"
                                    ,isnull(fk.is_unique,0) as isunique,CONVERT(BIT,CASE y.name WHEN 'timestamp' THEN 1 ELSE 0 END) AS istimestamp,isnull(fk.is_primary_key,0) AS PK_Column
									,STUFF((SELECT ','+OBJECT_NAME(fkc.parent_object_id)+'.'+fc.name
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.parent_object_id= fc.id AND fkc.parent_column_id = fc.colid WHERE fkc.referenced_object_id=c.id and fkc.referenced_column_id=c.colid 
											" + (SlaveForeignTable == null ? "" : " AND OBJECT_NAME(fkc.parent_object_id) = '" + SlaveForeignTable + "'") + @"
											FOR XML PATH('')),1,1,'') AS SlaveForeignKeys
									,STUFF((SELECT ','+OBJECT_NAME(fkc.referenced_object_id)+'.'+fc.name 
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.referenced_object_id= fc.id AND fkc.referenced_column_id = fc.colid WHERE fkc.parent_object_id=c.id and fkc.parent_column_id=c.colid
											" + (MasterForeignTable == null ? "" : " AND OBJECT_NAME(fkc.referenced_object_id) = '" + MasterForeignTable + "'") + @"
											FOR XML PATH('')),1,1,'') AS MasterForeignKeys
	                            FROM " + (DataObjectType == SQLDataObjectType.View ?
                                           @"sys.all_views v
	                            INNER JOIN sys.syscolumns _c ON _c.id = v.object_id
	                            INNER JOIN sys.types y ON _c.xtype = y.user_type_id
	                            LEFT JOIN (sys.sysdepends t
			                            INNER JOIN sys.syscolumns c ON t.depid = c.id AND t.depnumber = c.colid AND OBJECT_NAME(t.id) = '" + DataObjectName + @"'
	                            ) ON  _c.name = c.name AND _c.xtype = c.xtype AND EXISTS
										 (
											SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id
												FROM sys.index_columns a1
												INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
												INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
												INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
												WHERE o1.type = 'u' --AND  o1.name+'.'+c1.name = OBJECT_NAME(t.depid)+ '.' + c.name -- 按约定视图命名规则，与主数据表匹配
										 ) "
                                           : @"sys.all_objects t
                                INNER JOIN sys.syscolumns c ON t.object_id = c.id
	                            INNER JOIN sys.types y ON c.xtype = y.user_type_id") + @"
	                            LEFT JOIN (
		                            SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id,CONVERT(BIT, MAX(CONVERT(INT,idx.is_primary_key))) AS is_primary_key,CONVERT(BIT, MAX(CONVERT(INT,idx.is_unique))) AS is_unique 
		                            FROM sys.index_columns a1
									INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
		                            INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
		                            INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
		                            WHERE o1.type = 'u'
                                    GROUP BY o1.name, c1.object_id,c1.name,a1.column_id
	                            ) fk ON c.id = fk.object_id AND fk.column_id =  c.colid
	                            WHERE " + (DataObjectType == SQLDataObjectType.View ? "v.name" : "t.type = 'U' AND t.name") + @"
	                             = '" + DataObjectName + @"'
	                ) tbl  
				" + (DataObjectType == SQLDataObjectType.View && MasterTableName != null && MasterTableName != "" ? "WHERE (tbl.OwnerTable ='" + MasterTableName + "' AND (tbl.PK_Column = 1 OR tbl.isidentity =1 OR tbl.isunique = 1) OR tbl.PK_Column <> 1 AND tbl.isidentity <>1 AND tbl.isunique <> 1)" : "") + @"
                "
                + (IsWithUserDefaults ? " LEFT JOIN [dbo].[BD_DefaultValue] def ON tbl.TableName = def.[cTblName] AND tbl.FieldName = def.[cField] " : "")
                + @" WHERE tbl.isidentity != 1 AND tbl.PK_Column != 1
                ORDER BY TableName, FieldName";
            SQLString = (DB == string.Empty ? "" : "USE [" + DB + "]" + char.ConvertFromUtf32(13)) + SQLString;
            DataSet ds = DbHelper.Query(SQLString);
            DataTable dt = ds != null ? ds.Tables[0] : null;
            if (dt != null)
            {
                FieldAttr fldat = new FieldAttr();
                for (int r = 0; r < dt.Rows.Count; r++)
                {
                    DataRow dr = dt.Rows[r];
                    string typename = dr["DataType"].ToString();
                    typename = "DB" + typename.Substring(0, 1).ToUpper() + typename.Substring(1);
                    fldat.DataType = (DataType)Enum.Parse(typeof(DataType), typename);
                    fldat.IsNullable = (dt.Columns["isnullable"].DataType.FullName == "System.Boolean" ? (bool)dr["isnullable"] : (dr["isnullable"].ToString() == "1" || dr["isnullable"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsAutoIncrement = (dt.Columns["isidentity"].DataType.FullName == "System.Boolean" ? (bool)dr["isidentity"] : (dr["isidentity"].ToString() == "1" || dr["isidentity"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsUniqueKey = (dt.Columns["isunique"].DataType.FullName == "System.Boolean" ? (bool)dr["isunique"] : (dr["isunique"].ToString() == "1" || dr["isunique"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsPrimaryKey = (dt.Columns["PK_Column"].DataType.FullName == "System.Boolean" ? (bool)dr["PK_Column"] : (dr["PK_Column"].ToString() == "1" || dr["PK_Column"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsTimestamp = (dt.Columns["istimestamp"].DataType.FullName == "System.Boolean" ? (bool)dr["istimestamp"] : (dr["istimestamp"].ToString() == "1" || dr["istimestamp"].ToString().ToLower() == "true" ? true : false));
                    if (IsWithUserDefaults) fldat.DefaultValue = (dr["UserDefault"] == DBNull.Value ? string.Empty : dr["UserDefault"].ToString());
                    dmlFlds.Add((DataObjectType == SQLDataObjectType.View ? (dr["OwnerTable"] == DBNull.Value || dr["OwnerTable"].ToString() == "" ? "" : dr["OwnerTable"].ToString() + ".") : "") + dr["FieldName"].ToString(), fldat);
                }
            }

            return dmlFlds;
        }

        public Dictionary<string, FieldAttr> GetTableFields(string DataObjectName, string MasterTableName, SQLDataObjectType DataObjectType, FieldCatagory fieldtype)
        {
            string[] arschema = DataObjectName.Split(new char[1] { '.' });
            string DB = (arschema.Length == 3 ? arschema[0] : "");
            DataObjectName = (arschema.Length == 3 ? arschema[2] : DataObjectName);
            Dictionary<string, FieldAttr> dmlFlds = new Dictionary<string, FieldAttr>();
            string whereString = string.Empty;
            switch (fieldtype)
            {
                case FieldCatagory.DMLFields:
                    whereString = "WHERE tbl.isidentity <> 1 and tbl.istimestamp <> 1";
                    break;
                case FieldCatagory.PrimaryKeyFields:
                    whereString = "WHERE tbl.PK_Column = 1";
                    break;
                case FieldCatagory.AutoIncrementFields:
                    whereString = "WHERE tbl.isidentity = 1";
                    break;
                case FieldCatagory.UniqueFields:
                    whereString = "WHERE tbl.isunique = 1";
                    break;
                case FieldCatagory.TimestampField:
                    whereString = "WHERE tbl.istimestamp = 1";
                    break;
                case FieldCatagory.PrimaryKeyandAutoIncrementFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.PrimaryKeyandUniqueFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isunique = 1";
                    break;
                case FieldCatagory.UniqueandAutoIncrementFields:
                    whereString = "WHERE tbl.isunique = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.PrimaryKeyandUniqueandAutoIncrementFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isunique = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.MasterForeignFields:
                    whereString = "WHERE ISNULL(tbl.MasterForeignKeys,'') <> ''";
                    break;
                case FieldCatagory.SlaveForeignFields:
                    whereString = "WHERE ISNULL(tbl.SlaveForeignKeys,'') <> ''";
                    break;
                default:
                    whereString = "";
                    break;
            }
            string SQLString = @"SELECT distinct " + (DataObjectType == SQLDataObjectType.View ? "TableName,OwnerTable" : "TableName") + @",FieldName,DataType,isnullable,isidentity,isunique,istimestamp,PK_Column,SlaveForeignKeys,MasterForeignKeys
                FROM(
	                SELECT " + (DataObjectType == SQLDataObjectType.View ? "v.name AS TableName, OBJECT_NAME(t.depid) AS OwnerTable" : "t.name AS TableName") + @" --, t.type
		                            ,y.NAME AS DataType
		                            ," + (DataObjectType == SQLDataObjectType.View ?
                                           @"_c.name AS FieldName, _c.type,_c.usertype,_c.length,_c.isnullable, columnproperty(_c.id,_c.name,'isidentity') AS  isidentity"
                                           :
                                           @"c.name AS FieldName, c.type,c.usertype,c.length,c.isnullable, columnproperty(c.id,c.name,'isidentity') AS  isidentity")+ @"
                                    ,isnull(fk.is_unique,0) as isunique,CONVERT(BIT,CASE y.name WHEN 'timestamp' THEN 1 ELSE 0 END) AS istimestamp,isnull(fk.is_primary_key,0) AS PK_Column
									,STUFF((SELECT ','+OBJECT_NAME(fkc.parent_object_id)+'.'+fc.name
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.parent_object_id= fc.id AND fkc.parent_column_id = fc.colid WHERE fkc.referenced_object_id=c.id and fkc.referenced_column_id=c.colid 
											FOR XML PATH('')),1,1,'') AS SlaveForeignKeys
									,STUFF((SELECT ','+OBJECT_NAME(fkc.referenced_object_id)+'.'+fc.name 
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.referenced_object_id= fc.id AND fkc.referenced_column_id = fc.colid WHERE fkc.parent_object_id=c.id and fkc.parent_column_id=c.colid
											FOR XML PATH('')),1,1,'') AS MasterForeignKeys
	                            FROM " + (DataObjectType == SQLDataObjectType.View ?
                                           @"sys.all_views v
	                            INNER JOIN sys.syscolumns _c ON _c.id = v.object_id
	                            INNER JOIN sys.types y ON _c.xtype = y.user_type_id
	                            LEFT JOIN (sys.sysdepends t
			                            INNER JOIN sys.syscolumns c ON t.depid = c.id AND t.depnumber = c.colid AND OBJECT_NAME(t.id) = '" + DataObjectName + @"'
	                            ) ON  _c.name = c.name AND _c.xtype = c.xtype AND EXISTS
										 (
											SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id
												FROM sys.index_columns a1
												INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
												INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
												INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
												WHERE o1.type = 'u' --AND  o1.name+'.'+c1.name = OBJECT_NAME(t.depid)+ '.' + c.name -- 按约定视图命名规则，与主数据表匹配
										 ) "
                                           : @"sys.all_objects t
                                INNER JOIN sys.syscolumns c ON t.object_id = c.id
	                            INNER JOIN sys.types y ON c.xtype = y.user_type_id") + @"
	                            LEFT JOIN (
		                            SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id,CONVERT(BIT, MAX(CONVERT(INT,idx.is_primary_key))) AS is_primary_key,CONVERT(BIT, MAX(CONVERT(INT,idx.is_unique))) AS is_unique 
		                            FROM sys.index_columns a1
									INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
		                            INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
		                            INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
		                            WHERE o1.type = 'u'
                                    GROUP BY o1.name, c1.object_id,c1.name,a1.column_id
	                            ) fk ON c.id = fk.object_id AND fk.column_id =  c.colid
	                            WHERE " + (DataObjectType == SQLDataObjectType.View ? "v.name" : "t.type = 'U' AND t.name") + @"
	                             = '" + DataObjectName + @"'
	                ) tbl  
				" + (DataObjectType == SQLDataObjectType.View && MasterTableName != null && MasterTableName != "" ? "WHERE (tbl.OwnerTable ='" + MasterTableName + "' AND (tbl.PK_Column = 1 OR tbl.isidentity =1 OR tbl.isunique = 1) OR tbl.PK_Column <> 1 AND tbl.isidentity <>1 AND tbl.isunique <> 1)" : "") + @"
                " + whereString + @"
                ORDER BY TableName, FieldName";
            SQLString = (DB == string.Empty ? "" : "USE [" + DB + "]" + char.ConvertFromUtf32(13)) + SQLString;
            DataSet ds = DbHelper.Query(SQLString);
            DataTable dt = ds != null ? ds.Tables[0] : null;
            if (dt != null)
            {
                FieldAttr fldat = new FieldAttr();
                for (int r = 0; r < dt.Rows.Count; r++)
                {
                    DataRow dr = dt.Rows[r];
                    string typename = dr["DataType"].ToString();
                    typename = "DB" + typename.Substring(0, 1).ToUpper() + typename.Substring(1);
                    fldat.DataType = (DataType)Enum.Parse(typeof(DataType), typename);
                    fldat.IsNullable = (dt.Columns["isnullable"].DataType.FullName == "System.Boolean" ? (bool)dr["isnullable"] : (dr["isnullable"].ToString() == "1" || dr["isnullable"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsAutoIncrement = (dt.Columns["isidentity"].DataType.FullName == "System.Boolean" ? (bool)dr["isidentity"] : (dr["isidentity"].ToString() == "1" || dr["isidentity"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsUniqueKey = (dt.Columns["isunique"].DataType.FullName == "System.Boolean" ? (bool)dr["isunique"] : (dr["isunique"].ToString() == "1" || dr["isunique"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsPrimaryKey = (dt.Columns["PK_Column"].DataType.FullName == "System.Boolean" ? (bool)dr["PK_Column"] : (dr["PK_Column"].ToString() == "1" || dr["PK_Column"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsTimestamp = (dt.Columns["istimestamp"].DataType.FullName == "System.Boolean" ? (bool)dr["istimestamp"] : (dr["istimestamp"].ToString() == "1" || dr["istimestamp"].ToString().ToLower() == "true" ? true : false));
                    fldat.MasterForeignKeys = (dr["MasterForeignKeys"] == DBNull.Value ? "" : dr["MasterForeignKeys"].ToString());
                    fldat.SlaveForeignKeys = (dr["SlaveForeignKeys"] == DBNull.Value ? "" : dr["SlaveForeignKeys"].ToString());
                    dmlFlds.Add((DataObjectType == SQLDataObjectType.View ? (dr["OwnerTable"] == DBNull.Value || dr["OwnerTable"].ToString() == "" ? "" : dr["OwnerTable"].ToString() + ".") : "") + dr["FieldName"].ToString(), fldat);
                }
            }


            return dmlFlds;
        }

        public Dictionary<string, FieldAttr> GetTableFields(string DataObjectName, string MasterTableName, SQLDataObjectType DataObjectType, FieldCatagory fieldtype, bool IsWithUserDefaults)
        {
            string[] arschema = DataObjectName.Split(new char[1] { '.' });
            string DB = (arschema.Length == 3 ? arschema[0] : "");
            DataObjectName = (arschema.Length == 3 ? arschema[2] : DataObjectName);
            Dictionary<string, FieldAttr> dmlFlds = new Dictionary<string, FieldAttr>();
            string whereString = string.Empty;
            switch (fieldtype)
            {
                case FieldCatagory.DMLFields:
                    whereString = "WHERE tbl.isidentity <> 1 and tbl.istimestamp <> 1";
                    break;
                case FieldCatagory.PrimaryKeyFields:
                    whereString = "WHERE tbl.PK_Column = 1";
                    break;
                case FieldCatagory.AutoIncrementFields:
                    whereString = "WHERE tbl.isidentity = 1";
                    break;
                case FieldCatagory.UniqueFields:
                    whereString = "WHERE tbl.isunique = 1";
                    break;
                case FieldCatagory.TimestampField:
                    whereString = "WHERE tbl.istimestamp = 1";
                    break;
                case FieldCatagory.PrimaryKeyandAutoIncrementFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.PrimaryKeyandUniqueFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isunique = 1";
                    break;
                case FieldCatagory.UniqueandAutoIncrementFields:
                    whereString = "WHERE tbl.isunique = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.PrimaryKeyandUniqueandAutoIncrementFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isunique = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.MasterForeignFields:
                    whereString = "WHERE ISNULL(tbl.MasterForeignKeys,'') <> ''";
                    break;
                case FieldCatagory.SlaveForeignFields:
                    whereString = "WHERE ISNULL(tbl.SlaveForeignKeys,'') <> ''";
                    break;
                default:
                    whereString = "";
                    break;
            }
            string SQLString = @"SELECT distinct " + (DataObjectType == SQLDataObjectType.View ? "TableName,OwnerTable" : "TableName") + @",FieldName,DataType,isnullable,isidentity,isunique,istimestamp,PK_Column,SlaveForeignKeys,MasterForeignKeys"
                + (IsWithUserDefaults ? ",def.[cDefaultValue] AS UserDefault" : "")
                + @" FROM(
	                SELECT " + (DataObjectType == SQLDataObjectType.View ? "v.name AS TableName, OBJECT_NAME(t.depid) AS OwnerTable" : "t.name AS TableName") + @" --, t.type
		                            ,y.NAME AS DataType
		                            ," + (DataObjectType == SQLDataObjectType.View ?
                                           @"_c.name AS FieldName, _c.type,_c.usertype,_c.length,_c.isnullable, columnproperty(_c.id,_c.name,'isidentity') AS  isidentity"
                                           :
                                           @"c.name AS FieldName, c.type,c.usertype,c.length,c.isnullable, columnproperty(c.id,c.name,'isidentity') AS  isidentity") + @"
                                    ,isnull(fk.is_unique,0) as isunique,CONVERT(BIT,CASE y.name WHEN 'timestamp' THEN 1 ELSE 0 END) AS istimestamp,isnull(fk.is_primary_key,0) AS PK_Column
									,STUFF((SELECT ','+OBJECT_NAME(fkc.parent_object_id)+'.'+fc.name
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.parent_object_id= fc.id AND fkc.parent_column_id = fc.colid WHERE fkc.referenced_object_id=c.id and fkc.referenced_column_id=c.colid 
											FOR XML PATH('')),1,1,'') AS SlaveForeignKeys
									,STUFF((SELECT ','+OBJECT_NAME(fkc.referenced_object_id)+'.'+fc.name 
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.referenced_object_id= fc.id AND fkc.referenced_column_id = fc.colid WHERE fkc.parent_object_id=c.id and fkc.parent_column_id=c.colid
											FOR XML PATH('')),1,1,'') AS MasterForeignKeys
	                            FROM " + (DataObjectType == SQLDataObjectType.View ?
                                           @"sys.all_views v
	                            INNER JOIN sys.syscolumns _c ON _c.id = v.object_id
	                            INNER JOIN sys.types y ON _c.xtype = y.user_type_id
	                            LEFT JOIN (sys.sysdepends t
			                            INNER JOIN sys.syscolumns c ON t.depid = c.id AND t.depnumber = c.colid AND OBJECT_NAME(t.id) = '" + DataObjectName + @"'
	                            ) ON  _c.name = c.name AND _c.xtype = c.xtype AND EXISTS
										 (
											SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id
												FROM sys.index_columns a1
												INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
												INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
												INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
												WHERE o1.type = 'u' --AND  o1.name+'.'+c1.name = OBJECT_NAME(t.depid)+ '.' + c.name -- 按约定视图命名规则，与主数据表匹配
										 ) "
                                           : @"sys.all_objects t
                                INNER JOIN sys.syscolumns c ON t.object_id = c.id
	                            INNER JOIN sys.types y ON c.xtype = y.user_type_id") + @"
	                            LEFT JOIN (
		                            SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id,CONVERT(BIT, MAX(CONVERT(INT,idx.is_primary_key))) AS is_primary_key,CONVERT(BIT, MAX(CONVERT(INT,idx.is_unique))) AS is_unique 
		                            FROM sys.index_columns a1
									INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
		                            INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
		                            INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
		                            WHERE o1.type = 'u'
                                    GROUP BY o1.name, c1.object_id,c1.name,a1.column_id
	                            ) fk ON c.id = fk.object_id AND fk.column_id =  c.colid
	                            WHERE " + (DataObjectType == SQLDataObjectType.View ? "v.name" : "t.type = 'U' AND t.name") + @"
	                             = '" + DataObjectName + @"'
	                ) tbl 
				" + (DataObjectType == SQLDataObjectType.View && MasterTableName != null && MasterTableName != "" ? "WHERE (tbl.OwnerTable ='" + MasterTableName + "' AND (tbl.PK_Column = 1 OR tbl.isidentity =1 OR tbl.isunique = 1) OR tbl.PK_Column <> 1 AND tbl.isidentity <>1 AND tbl.isunique <> 1)" : "") + @"
                "
                + (IsWithUserDefaults ? " LEFT JOIN [dbo].[BD_DefaultValue] def ON tbl.TableName = def.[cTblName] AND tbl.FieldName = def.[cField] " : "")
                + whereString + @"
                ORDER BY TableName, FieldName";
            SQLString = (DB == string.Empty ? "" : "USE [" + DB + "]" + char.ConvertFromUtf32(13)) + SQLString;
            DataSet ds = DbHelper.Query(SQLString);
            DataTable dt = ds != null ? ds.Tables[0] : null;
            if (dt != null)
            {
                FieldAttr fldat = new FieldAttr();
                for (int r = 0; r < dt.Rows.Count; r++)
                {
                    DataRow dr = dt.Rows[r];
                    string typename = dr["DataType"].ToString();
                    typename = "DB" + typename.Substring(0, 1).ToUpper() + typename.Substring(1);
                    fldat.DataType = (DataType)Enum.Parse(typeof(DataType), typename);
                    fldat.IsNullable = (dt.Columns["isnullable"].DataType.FullName == "System.Boolean" ? (bool)dr["isnullable"] : (dr["isnullable"].ToString() == "1" || dr["isnullable"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsAutoIncrement = (dt.Columns["isidentity"].DataType.FullName == "System.Boolean" ? (bool)dr["isidentity"] : (dr["isidentity"].ToString() == "1" || dr["isidentity"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsUniqueKey = (dt.Columns["isunique"].DataType.FullName == "System.Boolean" ? (bool)dr["isunique"] : (dr["isunique"].ToString() == "1" || dr["isunique"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsPrimaryKey = (dt.Columns["PK_Column"].DataType.FullName == "System.Boolean" ? (bool)dr["PK_Column"] : (dr["PK_Column"].ToString() == "1" || dr["PK_Column"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsTimestamp = (dt.Columns["istimestamp"].DataType.FullName == "System.Boolean" ? (bool)dr["istimestamp"] : (dr["istimestamp"].ToString() == "1" || dr["istimestamp"].ToString().ToLower() == "true" ? true : false));
                    if (IsWithUserDefaults) fldat.DefaultValue = (dr["UserDefault"] == DBNull.Value ? string.Empty : dr["UserDefault"].ToString());
                    fldat.MasterForeignKeys = (dr["MasterForeignKeys"] == DBNull.Value ? "" : dr["MasterForeignKeys"].ToString());
                    fldat.SlaveForeignKeys = (dr["SlaveForeignKeys"] == DBNull.Value ? "" : dr["SlaveForeignKeys"].ToString());
                    dmlFlds.Add((DataObjectType == SQLDataObjectType.View ? (dr["OwnerTable"] == DBNull.Value || dr["OwnerTable"].ToString() == "" ? "" : dr["OwnerTable"].ToString() + ".") : "") + dr["FieldName"].ToString(), fldat);
                }
            }


            return dmlFlds;
        }

        public Dictionary<string, FieldAttr> GetTableFields(string DataObjectName, string MasterTableName, SQLDataObjectType DataObjectType, FieldCatagory fieldtype, bool IsWithUserDefaults, string MasterForeignTable, string SlaveForeignTable)
        {
            string[] arschema = DataObjectName.Split(new char[1] { '.' });
            string DB = (arschema.Length == 3 ? arschema[0] : "");
            DataObjectName = (arschema.Length == 3 ? arschema[2] : DataObjectName);
            Dictionary<string, FieldAttr> dmlFlds = new Dictionary<string, FieldAttr>();
            string whereString = string.Empty;
            switch (fieldtype)
            {
                case FieldCatagory.DMLFields:
                    whereString = "WHERE tbl.isidentity <> 1 and tbl.istimestamp <> 1";
                    break;
                case FieldCatagory.PrimaryKeyFields:
                    whereString = "WHERE tbl.PK_Column = 1";
                    break;
                case FieldCatagory.AutoIncrementFields:
                    whereString = "WHERE tbl.isidentity = 1";
                    break;
                case FieldCatagory.UniqueFields:
                    whereString = "WHERE tbl.isunique = 1";
                    break;
                case FieldCatagory.TimestampField:
                    whereString = "WHERE tbl.istimestamp = 1";
                    break;
                case FieldCatagory.PrimaryKeyandAutoIncrementFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.PrimaryKeyandUniqueFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isunique = 1";
                    break;
                case FieldCatagory.UniqueandAutoIncrementFields:
                    whereString = "WHERE tbl.isunique = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.PrimaryKeyandUniqueandAutoIncrementFields:
                    whereString = "WHERE tbl.PK_Column = 1 or tbl.isunique = 1 or tbl.isidentity = 1";
                    break;
                case FieldCatagory.MasterForeignFields:
                    whereString = "WHERE ISNULL(tbl.MasterForeignKeys,'') <> ''";
                    break;
                case FieldCatagory.SlaveForeignFields:
                    whereString = "WHERE ISNULL(tbl.SlaveForeignKeys,'') <> ''";
                    break;
                default:
                    whereString = "";
                    break;
            }
            if (MasterForeignTable != null && MasterForeignTable != "") whereString += (whereString == "" ? "WHERE " : " AND ") + ("','+ISNULL(tbl.MasterForeignKeys,'') LIKE '%," + MasterForeignTable + ".%'");
            if (SlaveForeignTable != null && SlaveForeignTable != "") whereString += (whereString == "" ? "WHERE " : " AND ") + ("','+ISNULL(tbl.SlaveForeignKeys,'') LIKE '%," + SlaveForeignTable + ".%'");
            string SQLString = @"SELECT distinct " + (DataObjectType == SQLDataObjectType.View ? "TableName,OwnerTable" : "TableName") + @",FieldName,DataType,isnullable,isidentity,isunique,istimestamp,PK_Column,SlaveForeignKeys,MasterForeignKeys"
                + (IsWithUserDefaults ? ",def.[cDefaultValue] AS UserDefault" : "")
                + @" FROM(
	                SELECT " + (DataObjectType == SQLDataObjectType.View ? "v.name AS TableName, OBJECT_NAME(t.depid) AS OwnerTable" : "t.name AS TableName") + @" --, t.type
		                            ,y.NAME AS DataType
		                            ," + (DataObjectType == SQLDataObjectType.View ?
                                           @"_c.name AS FieldName, _c.type,_c.usertype,_c.length,_c.isnullable, columnproperty(_c.id,_c.name,'isidentity') AS  isidentity"
                                           :
                                           @"c.name AS FieldName, c.type,c.usertype,c.length,c.isnullable, columnproperty(c.id,c.name,'isidentity') AS  isidentity") + @"
                                    ,isnull(fk.is_unique,0) as isunique,CONVERT(BIT,CASE y.name WHEN 'timestamp' THEN 1 ELSE 0 END) AS istimestamp,isnull(fk.is_primary_key,0) AS PK_Column
									,STUFF((SELECT ','+OBJECT_NAME(fkc.parent_object_id)+'.'+fc.name
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.parent_object_id= fc.id AND fkc.parent_column_id = fc.colid WHERE fkc.referenced_object_id=c.id and fkc.referenced_column_id=c.colid 
											" + (SlaveForeignTable == null ? "" : " AND OBJECT_NAME(fkc.parent_object_id) = '" + SlaveForeignTable + "'") + @"
											FOR XML PATH('')),1,1,'') AS SlaveForeignKeys
									,STUFF((SELECT ','+OBJECT_NAME(fkc.referenced_object_id)+'.'+fc.name 
											FROM sys.foreign_key_columns fkc 
											INNER JOIN sys.syscolumns fc ON fkc.referenced_object_id= fc.id AND fkc.referenced_column_id = fc.colid WHERE fkc.parent_object_id=c.id and fkc.parent_column_id=c.colid
											" + (MasterForeignTable == null ? "" : " AND OBJECT_NAME(fkc.referenced_object_id) = '" + MasterForeignTable + "'") + @"
											FOR XML PATH('')),1,1,'') AS MasterForeignKeys
	                            FROM " + (DataObjectType == SQLDataObjectType.View ?
                                           @"sys.all_views v
	                            INNER JOIN sys.syscolumns _c ON _c.id = v.object_id
	                            INNER JOIN sys.types y ON _c.xtype = y.user_type_id
	                            LEFT JOIN (sys.sysdepends t
			                            INNER JOIN sys.syscolumns c ON t.depid = c.id AND t.depnumber = c.colid AND OBJECT_NAME(t.id) = '" + DataObjectName + @"'
	                            ) ON  _c.name = c.name AND _c.xtype = c.xtype AND EXISTS
										 (
											SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id
												FROM sys.index_columns a1
												INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
												INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
												INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
												WHERE o1.type = 'u' --AND  o1.name+'.'+c1.name = OBJECT_NAME(t.depid)+ '.' + c.name -- 按约定视图命名规则，与主数据表匹配
										 ) "
                                           : @"sys.all_objects t
                                INNER JOIN sys.syscolumns c ON t.object_id = c.id
	                            INNER JOIN sys.types y ON c.xtype = y.user_type_id") + @"
	                            LEFT JOIN (
		                            SELECT o1.name, c1.object_id,c1.name AS colname,a1.column_id,CONVERT(BIT, MAX(CONVERT(INT,idx.is_primary_key))) AS is_primary_key,CONVERT(BIT, MAX(CONVERT(INT,idx.is_unique))) AS is_unique 
		                            FROM sys.index_columns a1
									INNER JOIN sys.indexes idx ON idx.object_id = a1.object_id AND idx.index_id = a1.index_id
		                            INNER JOIN sys.columns c1 ON a1.object_id = c1.object_id AND a1.column_id = c1.column_id
		                            INNER JOIN sys.objects o1 ON c1.object_id = o1.object_id
		                            WHERE o1.type = 'u'
                                    GROUP BY o1.name, c1.object_id,c1.name,a1.column_id
	                            ) fk ON c.id = fk.object_id AND fk.column_id =  c.colid
	                            WHERE " + (DataObjectType == SQLDataObjectType.View ? "v.name" : "t.type = 'U' AND t.name") + @"
	                             = '" + DataObjectName + @"'
	                ) tbl 
				" + (DataObjectType == SQLDataObjectType.View && MasterTableName != null && MasterTableName != "" ? "WHERE (tbl.OwnerTable ='" + MasterTableName + "' AND (tbl.PK_Column = 1 OR tbl.isidentity =1 OR tbl.isunique = 1) OR tbl.PK_Column <> 1 AND tbl.isidentity <>1 AND tbl.isunique <> 1)" : "") + @"
                "
                + (IsWithUserDefaults ? " LEFT JOIN [dbo].[BD_DefaultValue] def ON tbl.TableName = def.[cTblName] AND tbl.FieldName = def.[cField] " : "")
                + whereString + @"
                ORDER BY TableName, FieldName";
            SQLString = (DB == string.Empty ? "" : "USE [" + DB + "]" + char.ConvertFromUtf32(13)) + SQLString;
            DataSet ds = DbHelper.Query(SQLString);
            DataTable dt = ds != null ? ds.Tables[0] : null;
            if (dt != null)
            {
                FieldAttr fldat = new FieldAttr();
                for (int r = 0; r < dt.Rows.Count; r++)
                {
                    DataRow dr = dt.Rows[r];
                    string typename = dr["DataType"].ToString();
                    typename = "DB" + typename.Substring(0, 1).ToUpper() + typename.Substring(1);
                    fldat.DataType = (DataType)Enum.Parse(typeof(DataType), typename);
                    fldat.IsNullable = (dt.Columns["isnullable"].DataType.FullName == "System.Boolean" ? (bool)dr["isnullable"] : (dr["isnullable"].ToString() == "1" || dr["isnullable"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsAutoIncrement = (dt.Columns["isidentity"].DataType.FullName == "System.Boolean" ? (bool)dr["isidentity"] : (dr["isidentity"].ToString() == "1" || dr["isidentity"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsUniqueKey = (dt.Columns["isunique"].DataType.FullName == "System.Boolean" ? (bool)dr["isunique"] : (dr["isunique"].ToString() == "1" || dr["isunique"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsPrimaryKey = (dt.Columns["PK_Column"].DataType.FullName == "System.Boolean" ? (bool)dr["PK_Column"] : (dr["PK_Column"].ToString() == "1" || dr["PK_Column"].ToString().ToLower() == "true" ? true : false));
                    fldat.IsTimestamp = (dt.Columns["istimestamp"].DataType.FullName == "System.Boolean" ? (bool)dr["istimestamp"] : (dr["istimestamp"].ToString() == "1" || dr["istimestamp"].ToString().ToLower() == "true" ? true : false));
                    if (IsWithUserDefaults) fldat.DefaultValue = (dr["UserDefault"] == DBNull.Value ? string.Empty : dr["UserDefault"].ToString());
                    fldat.MasterForeignKeys = (dr["MasterForeignKeys"] == DBNull.Value ? "" : dr["MasterForeignKeys"].ToString());
                    fldat.SlaveForeignKeys = (dr["SlaveForeignKeys"] == DBNull.Value ? "" : dr["SlaveForeignKeys"].ToString());
                    dmlFlds.Add((DataObjectType == SQLDataObjectType.View ? (dr["OwnerTable"] == DBNull.Value || dr["OwnerTable"].ToString() == "" ? "" : dr["OwnerTable"].ToString() + ".") : "") + dr["FieldName"].ToString(), fldat);
                }
            }


            return dmlFlds;
        }
        
        #endregion

        public DataSet QueryDataSet_Job(string TableName, string FieldsList, JobDataFilter Filter)
        {
            DataSet ds = null;
            string SQLString = string.Empty;
            try
            {
                string whereStr = string.Empty;

                if (Filter != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string valuepair = filtItem.Key + "='" + filtItem.Value.ToString() + "'";
                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }
                }
                SQLString = "select " + FieldsList + " from " + TableName + (whereStr == string.Empty ? "" : " where " + whereStr);

                ds = DbHelper.Query(SQLString);
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return ds;
        }

        public DataTable QueryDataTable_Job(string TableName, string FieldsList, JobDataFilter Filter)
        {
            DataTable dt = null;
            string SQLString = string.Empty;
            try
            {
                string whereStr = string.Empty;

                if (Filter != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string valuepair = filtItem.Key + "='" + filtItem.Value.ToString() + "'";
                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }
                }
                SQLString = "select " + FieldsList + " from " + TableName + (whereStr == string.Empty ? "" : " where " + whereStr);

                DataSet ds = DbHelper.Query(SQLString);
                if (ds != null && ds.Tables.Count > 0) dt = ds.Tables[0];
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;

            }
            return dt;
        }
        
        public DataSet QueryDataSet_Job(string Script, JobDataFilter SPParameters)
        {
            DataSet ds = null;
            string SQLString = string.Empty;
            try
            {
                SQLString = Script;

                if (SPParameters != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in SPParameters.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        fldValue = (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        SQLString = SQLString.Replace("@" + filtItem.Key, fldValue);
                    }
                }

                ds = DbHelper.Query(SQLString);
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;

            }
            return ds;
        }

        public DataTable QueryDataTable_Job(string Script, JobDataFilter SPParameters)
        {
            DataTable dt = null;
            string SQLString = string.Empty;
            try
            {
                SQLString = Script;

                if (SPParameters != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in SPParameters.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        fldValue = (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        SQLString = SQLString.Replace("@" + filtItem.Key, fldValue);
                    }
                }

                DataSet ds = DbHelper.Query(SQLString);
                if (ds != null && ds.Tables.Count > 0) dt = ds.Tables[0];
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;

            }
            return dt;
        }

        public bool Add_Job(string TableName, string FieldsList, NameValueCollection[] arFieldValues)
        {
            bool rst=true;
            string SQLString = string.Empty;
            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields);

                string[] arF = FieldsList == string.Empty ? new string[0] : FieldsList.Split(new char[1] { ',' });
                var flds = arF.Where(e => fldsAtt.ContainsKey(e));
                string[] arFields = flds.ToArray();
                
                string valuelist = string.Empty;
                FieldsList = string.Join(",", arFields);
                for (int r = 0; r < arFieldValues.Length; r++)
                {
                    string rowvalues = string.Empty;
                    NameValueCollection FieldValues = arFieldValues[r];
                    for (int i = 0; i < arFields.Length;i++)
                    {
                        string fldName = arFields[i].ToString().Replace("[","").Replace("]","");
                        string fldValue = FieldValues[fldName].ToString();
                        FieldAttr fatt = fldsAtt[fldName];
                        bool fldisnullable = fatt.IsNullable;
                        fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : "''") : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                        rowvalues += (rowvalues == string.Empty ? "(" : ",") + fldValue;
                    }
                    rowvalues = rowvalues + ")";
                    valuelist += (valuelist == string.Empty ? rowvalues : "," + rowvalues);
                }
                if (arFieldValues.Length > 0 && valuelist != "") SQLString = "insert into " + TableName + " (" + FieldsList + ") values " + valuelist;

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return rst;
        }
        public bool Add_Job(string TableName, string FieldsList, NameValueCollection[] arFieldValues, bool SetDefaults)
        {
            bool rst = true;
            string SQLString = string.Empty;
            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                if (SetDefaults)
                {
                    FieldsList = string.Empty;
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        FieldsList += (FieldsList == string.Empty ? "" : ",") + fldatt.Key;
                    }
                }

                string[] arF = FieldsList == string.Empty ? new string[0] : FieldsList.Split(new char[1] { ',' });
                var flds = arF.Where(e => fldsAtt.ContainsKey(e));
                string[] arFields = flds.ToArray();

                string valuelist = string.Empty;
                FieldsList = string.Join(",", arFields);
                for (int r = 0; r < arFieldValues.Length; r++)
                {
                    string rowvalues = string.Empty;
                    NameValueCollection FieldValues = arFieldValues[r];
                    for (int i = 0; i < arFields.Length; i++)
                    {
                        string fldName = arFields[i].ToString().Replace("[", "").Replace("]", "");
                        //string fldValue = FieldValues[fldName].ToString();
                        FieldAttr fatt = fldsAtt[fldName];
                        bool fldisnullable = fatt.IsNullable;
                        string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : fldsAtt[fldName].DefaultValue;
                        fldValue = (fldValue == "" ? (SetDefaults ? (fldsAtt[fldName].DefaultValue == string.Empty ? (fldisnullable ? "NULL" : "''") : "'" + fldsAtt[fldName].DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                        //fldValue = (fldValue == "" ? (SetDefaults ? "'" + fatt.DefaultValue.Replace("'","''") + "'" : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'","''") + "'"));
                        rowvalues += (rowvalues == string.Empty ? "(" : ",") + fldValue;
                    }
                    rowvalues = rowvalues + ")";
                    valuelist += (valuelist == string.Empty ? rowvalues : "," + rowvalues);
                }
                if (arFieldValues.Length > 0 && valuelist != "") SQLString = "insert into " + TableName + " (" + FieldsList + ") values " + valuelist;

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return rst;
        }
        public bool Add_Job(string TableName, string FieldsList, NameValueCollection[] arFieldValues, bool SetDefaults, string MasterForeignTable, string SlaveForeignTable)
        {
            bool rst = true;
            string SQLString = string.Empty;
            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                if (SetDefaults)
                {
                    FieldsList = string.Empty;
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        FieldsList += (FieldsList == string.Empty ? "" : ",") + fldatt.Key;
                    }
                }

                string[] arF = FieldsList == string.Empty ? new string[0] : FieldsList.Split(new char[1] { ',' });
                var flds = arF.Where(e => fldsAtt.ContainsKey(e));
                string[] arFields = flds.ToArray();

                string valuelist = string.Empty;
                FieldsList = string.Join(",", arFields);
                for (int r = 0; r < arFieldValues.Length; r++)
                {
                    string rowvalues = string.Empty;
                    NameValueCollection FieldValues = arFieldValues[r];
                    for (int i = 0; i < arFields.Length; i++)
                    {
                        string fldName = arFields[i].ToString().Replace("[", "").Replace("]", "");
                        //string fldValue = FieldValues[fldName].ToString();
                        FieldAttr fatt = fldsAtt[fldName];
                        bool fldisnullable = fatt.IsNullable;
                        string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : fldsAtt[fldName].DefaultValue;
                        fldValue = (fldValue == "" ? (SetDefaults ? (fldsAtt[fldName].DefaultValue == string.Empty ? (fldisnullable ? "NULL" : "''") : "'" + fldsAtt[fldName].DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                        //fldValue = (fldValue == "" ? (SetDefaults ? "'" + fatt.DefaultValue.Replace("'","''") + "'" : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'","''") + "'"));
                        rowvalues += (rowvalues == string.Empty ? "(" : ",") + fldValue;
                    }
                    rowvalues = rowvalues + ")";
                    valuelist += (valuelist == string.Empty ? rowvalues : "," + rowvalues);
                }
                if (arFieldValues.Length > 0 && valuelist != "") SQLString = "insert into " + TableName + " (" + FieldsList + ") values " + valuelist;

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return rst;
        }

        public struct UpdateDefine
        {
            public NameValueCollection FieldValues;
            public string NoSavedFields;
            public JobDataFilter Filter;
        }

        public bool BatchUpdate_Job(string TableName, UpdateDefine[] UpdateData)
        {
            bool rst = false;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields);

                for (int r = 0; r < UpdateData.Length; r++)
                {

                    UpdateDefine updaterow = UpdateData[r];
                    NameValueCollection FieldValues = updaterow.FieldValues;
                    string[] arNoSavedFields = updaterow.NoSavedFields == string.Empty ? new string[0] : updaterow.NoSavedFields.Split(new char[1] { ',' });
                    JobDataFilter Filter = updaterow.Filter;

                    //更新的条件
                    string whereStr = string.Empty;
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }

                    //更新的数据
                    string rowvalues = string.Empty;
                    foreach (KeyValuePair<string, object> valItem in FieldValues)
                    {
                        string fldName = valItem.Key;
                        string fldValue = valItem.Value.ToString();
                        if (fldsAtt.ContainsKey(fldName) && !arNoSavedFields.Contains(fldName))
                        {
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : "''") : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                        }
                    }
                    if (FieldValues.Count > 0 && rowvalues != "") SQLString += "update " + TableName + " set " + rowvalues + " where " + whereStr + ";" + char.ConvertFromUtf32(13);
                }
                DbHelper.ExecuteSql(SQLString);
                rst = true;
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return rst;
        }
        public bool BatchUpdate_Job(string TableName, UpdateDefine[] UpdateData, bool SetDefaults)
        {
            bool rst = false;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                for (int r = 0; r < UpdateData.Length; r++)
                {

                    UpdateDefine updaterow = UpdateData[r];
                    NameValueCollection FieldValues = updaterow.FieldValues;
                    string[] arNoSavedFields = updaterow.NoSavedFields == string.Empty ? new string[0] : updaterow.NoSavedFields.Split(new char[1] { ',' });
                    JobDataFilter Filter = updaterow.Filter;

                    //更新的条件
                    string whereStr = string.Empty;
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }

                    //更新的数据
                    string rowvalues = string.Empty;
                    foreach (KeyValuePair<string, object> valItem in DefFields)//FieldValues)
                    {
                        string fldName = valItem.Key;
                        string fldValue = valItem.Value.ToString();
                        if (fldsAtt.ContainsKey(fldName) && !arNoSavedFields.Contains(fldName))
                        {
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(valItem.Key) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : "''") : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                        }
                    }
                    if (FieldValues.Count > 0 && rowvalues != "") SQLString += "update " + TableName + " set " + rowvalues + " where " + whereStr + ";" + char.ConvertFromUtf32(13);
                }
                DbHelper.ExecuteSql(SQLString);
                rst = true;
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return rst;
        }
        public bool BatchUpdate_Job(string TableName, UpdateDefine[] UpdateData, bool SetDefaults, string MasterForeignTable, string SlaveForeignTable)
        {
            bool rst = false;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                for (int r = 0; r < UpdateData.Length; r++)
                {

                    UpdateDefine updaterow = UpdateData[r];
                    NameValueCollection FieldValues = updaterow.FieldValues;
                    string[] arNoSavedFields = updaterow.NoSavedFields == string.Empty ? new string[0] : updaterow.NoSavedFields.Split(new char[1] { ',' });
                    JobDataFilter Filter = updaterow.Filter;

                    //更新的条件
                    string whereStr = string.Empty;
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }

                    //更新的数据
                    string rowvalues = string.Empty;
                    foreach (KeyValuePair<string, object> valItem in DefFields)//FieldValues)
                    {
                        string fldName = valItem.Key;
                        string fldValue = valItem.Value.ToString();
                        if (fldsAtt.ContainsKey(fldName) && !arNoSavedFields.Contains(fldName))
                        {
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(valItem.Key) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : "''") : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                        }
                    }
                    if (FieldValues.Count > 0 && rowvalues != "") SQLString += "update " + TableName + " set " + rowvalues + " where " + whereStr + ";" + char.ConvertFromUtf32(13);
                }
                DbHelper.ExecuteSql(SQLString);
                rst = true;
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return rst;
        }

        public bool SingleUpdate_Job(string TableName, NameValueCollection FieldValues, JobDataFilter Filter)
        {
            bool rst = true;
            string SQLString = string.Empty;

            try
            {
                string whereStr = string.Empty;
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields);

                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                    string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }


                string rowvalues = string.Empty;
                foreach (KeyValuePair<string, object> valItem in FieldValues)
                {
                    string fldName = valItem.Key;
                    string fldValue = valItem.Value.ToString();
                    FieldAttr fatt = fldsAtt[fldName];
                    bool fldisnullable = fatt.IsNullable;
                    fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : "''") : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                    string fldPair = fldName + " = " + fldValue;
                    rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                }
                SQLString = "update " + TableName + " set " + rowvalues + " where " + whereStr;

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return rst;
        }
        public bool SingleUpdate_Job(string TableName, NameValueCollection FieldValues, JobDataFilter Filter, bool SetDefaults)
        {
            bool rst = true;
            string SQLString = string.Empty;

            try
            {
                string whereStr = string.Empty;
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                    string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }


                string rowvalues = string.Empty;
                foreach (KeyValuePair<string, object> valItem in DefFields)//FieldValues)
                {
                    string fldName = valItem.Key;
                    string fldValue = valItem.Value.ToString();
                    FieldAttr fatt = fldsAtt[fldName];
                    bool fldisnullable = fatt.IsNullable;
                    fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(valItem.Key) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : "''") : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                    string fldPair = fldName + " = " + fldValue;
                    rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                }
                SQLString = "update " + TableName + " set " + rowvalues + " where " + whereStr;

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return rst;
        }
        public bool SingleUpdate_Job(string TableName, NameValueCollection FieldValues, JobDataFilter Filter, bool SetDefaults, string MasterForeignTable, string SlaveForeignTable)
        {
            bool rst = true;
            string SQLString = string.Empty;

            try
            {
                string whereStr = string.Empty;
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                    string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }


                string rowvalues = string.Empty;
                foreach (KeyValuePair<string, object> valItem in DefFields)//FieldValues)
                {
                    string fldName = valItem.Key;
                    string fldValue = valItem.Value.ToString();
                    FieldAttr fatt = fldsAtt[fldName];
                    bool fldisnullable = fatt.IsNullable;
                    fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(valItem.Key) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : "''") : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                    string fldPair = fldName + " = " + fldValue;
                    rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                }
                SQLString = "update " + TableName + " set " + rowvalues + " where " + whereStr;

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return rst;
        }

        public bool SingleDelete_Job(string TableName, JobDataFilter Filter)
        {
            bool rst = true;
            string SQLString = string.Empty;

            try
            {
                string whereStr = string.Empty;
                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string valuepair = filtItem.Key + "='" + filtItem.Value.ToString() + "'";
                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }
                SQLString = "delete from " + TableName + " where " + whereStr;

                int idel = DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return rst;
        }
        public bool BatchDelete_Job(string TableName, NameValueCollection[] DeleteFilter)
        {
            bool rst = false;
            string SQLString = string.Empty;


            try
            {
                //删除索引条件，必须是在本表内的字段
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.AllFields);

                for (int r = 0; r < DeleteFilter.Length; r++)
                {

                    NameValueCollection deleterow = DeleteFilter[r];

                    //删除的数据条件
                    string whereStr = string.Empty;
                    foreach (string fldName in deleterow)
                    {
                        string fldValue = deleterow[fldName].ToString();
                        if (fldsAtt.ContainsKey(fldName))  //忽略本表不存在的字段
                        {
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : "''") : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            whereStr += (whereStr == string.Empty ? fldPair : " and " + fldPair);
                        }
                    }
                    SQLString += "delete from " + TableName + (whereStr == string.Empty ? "" : " where " + whereStr) + ";" + char.ConvertFromUtf32(13);
                }
                
                int idel = DbHelper.ExecuteSql(SQLString);

                rst = true;
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return rst;
        }

        public string GetScript_Add(string TableName, string FieldsList, NameValueCollection[] arFieldValues)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields);

                string[] arF = FieldsList == string.Empty ? new string[0] : FieldsList.Split(new char[1] { ',' });
                var flds = arF.Where(e => fldsAtt.ContainsKey(e));
                string[] arFields = flds.ToArray();
                FieldsList = string.Join(",", arFields);

                string valuelist = string.Empty;
                for (int r = 0; r < arFieldValues.Length; r++)
                {
                    string rowvalues = string.Empty;
                    NameValueCollection FieldValues = arFieldValues[r];
                    for (int i = 0; i < arFields.Length; i++)
                    {
                        string fldName = arFields[i].ToString().Replace("[", "").Replace("]", "");
                        string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : "";
                        FieldAttr fatt = fldsAtt[fldName];
                        bool fldisnullable = fatt.IsNullable;
                        DataType ftype = fatt.DataType;
                        string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                        fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : NonNullDefVal) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                        rowvalues += (rowvalues == string.Empty ? "(" : ",") + fldValue;
                    }
                    rowvalues = rowvalues + ")";
                    valuelist += (valuelist == string.Empty ? rowvalues : "," + rowvalues);
                }
                if (arFieldValues.Length > 0 && valuelist != "") SQLString = "insert into " + TableName + " (" + FieldsList + ") values " + valuelist;

                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return script;
        }
        public string GetScript_Add(string TableName, string FieldsList, string NoSavedFields, NameValueCollection[] arFieldValues, bool SetDefaults)
        {
            //MasterForeignValue格式：MasterForeignFieldName = FieldValue(常量/@变量)
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                Dictionary<string, FieldAttr> fldMaster = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.MasterForeignFields, false);
                //按默认值更新时，忽略FieldsList，取全部字段
                if (SetDefaults)
                {
                    FieldsList = string.Empty;
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        FieldsList += (FieldsList == string.Empty ? "" : ",") + fldatt.Key;
                    }
                }
                string[] arF = FieldsList == string.Empty ? new string[0] : FieldsList.Split(new char[1] { ',' });
                string[] arNo = NoSavedFields == string.Empty ? new string[0] : NoSavedFields.Split(new char[1] { ',' });
                var flds = arF.Where(e => (fldsAtt.ContainsKey(e) && !arNo.Contains(e)));
                string[] arFields = flds.ToArray();
                FieldsList = string.Join(",", arFields);

                string valuelist = string.Empty;
                for (int r = 0; r < arFieldValues.Length; r++)
                {
                    string rowvalues = string.Empty;
                    NameValueCollection FieldValues = arFieldValues[r];
                    for (int i = 0; i < arFields.Length; i++)
                    {
                        string fldName = arFields[i].ToString().Replace("[", "").Replace("]", "");
                        //string fldValue = FieldValues[fldName].ToString();
                        FieldAttr fatt = fldsAtt[fldName];
                        bool fldisnullable = fatt.IsNullable;
                        string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : "";
                        DataType ftype = fatt.DataType;
                        string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                        fldValue = (fldValue == "" ? (SetDefaults ? (fldsAtt[fldName].DefaultValue == string.Empty ? (fldisnullable ? "NULL" : NonNullDefVal) : "'" + fldsAtt[fldName].DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : NonNullDefVal)) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                        //fldValue = (fldValue == "" ? (SetDefaults ? "'" + fatt.DefaultValue.Replace("'","''") + "'" : (fldisnullable ? "NULL" : "''")) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'","''") + "'"));
                        rowvalues += (rowvalues == string.Empty ? "(" : ",") + fldValue;
                    }
                    rowvalues = rowvalues + ")";
                    valuelist += (valuelist == string.Empty ? rowvalues : "," + rowvalues);
                }
                if (arFieldValues.Length > 0 && valuelist != "") SQLString = "insert into " + TableName + " (" + FieldsList + ") values " + valuelist;

                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return script;
        }
        public string GetScript_Add(string TableName, string FieldsList, string NoSavedFields, NameValueCollection[] arFieldValues, bool SetDefaults, string MasterForeignTable, string SlaveForeignTable, List<string> MasterForeignKeys)
        {
            //MasterForeignValue格式：MasterForeignFieldName = FieldValue(常量/@变量)
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                Dictionary<string, FieldAttr> fldMaster = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.MasterForeignFields, false, MasterForeignTable, string.Empty);
                //按默认值更新时，忽略FieldsList，取全部字段
                if (SetDefaults)
                {
                    FieldsList = string.Empty;
                    foreach (KeyValuePair<string,FieldAttr> fldatt in fldsAtt)
                    {
                        FieldsList += (FieldsList == string.Empty ? "" : ",") + fldatt.Key;
                    }
                }
                string[] arF = FieldsList == string.Empty ? new string[0] : FieldsList.Split(new char[1] { ',' });
                string[] arNo = NoSavedFields == string.Empty ? new string[0]: NoSavedFields.Split(new char[1] { ',' });
                var flds = arF.Where(e => (fldsAtt.ContainsKey(e) && !arNo.Contains(e)));
                string[] arFields = flds.ToArray();
                FieldsList = string.Join(",", arFields);

                string valuelist = string.Empty;
                for (int r = 0; r < arFieldValues.Length; r++)
                {
                    string rowvalues = string.Empty;
                    NameValueCollection FieldValues = arFieldValues[r];
                    for (int i = 0; i < arFields.Length; i++)
                    {
                        string fldName = arFields[i].ToString().Replace("[", "").Replace("]", "");
                        FieldAttr fatt = fldsAtt[fldName];
                        bool fldisnullable = fatt.IsNullable;
                        string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : "";
                        DataType ftype = fatt.DataType;
                        string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                        fldValue = (fldValue == "" ? (SetDefaults ? (fldsAtt[fldName].DefaultValue == string.Empty ? (fldisnullable ? "NULL" : NonNullDefVal) : "'" + fldsAtt[fldName].DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : NonNullDefVal)) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                        if (fatt.MasterForeignKeys != null && fatt.MasterForeignKeys != "" && MasterForeignKeys.Contains(fldName))
                        {
                            fldValue = "{" + fldName + "}";
                        }
                        rowvalues += (rowvalues == string.Empty ? "(" : ",") + fldValue;
                    }
                    rowvalues = rowvalues + ")";
                    valuelist += (valuelist == string.Empty ? rowvalues : "," + rowvalues);
                }
                if (arFieldValues.Length > 0 && valuelist != "") SQLString = "insert into " + TableName + " (" + FieldsList + ") values " + valuelist;

                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return script;
        }
        public struct MasterForeignKeyValue
        {
            public int RowIndex;
            public NameValueCollection FieldValues;
        }
        public struct RowStatus
        {
            public int RowIndex;
            public string RowDMLType;
            public int InsertIndex;  //update:-1
            public int UpdateIndex;  //insert:-1
            public NameValueCollection RowKeyValue;
        }
        public struct TableSchema
        {
            public string TableName;
            public List<RowStatus> DataSchema;
            public DataTable GridData;
            public int CurrentRowIndex;
            public string ForeignKeyFields;
            public string CurrentRowKeyValues;
            public String GridSaveScript;
        }
        public string GetScript_DML_ReturnKeys(string TableName, string FieldsList, string NoSavedFields, NameValueCollection[] InsertValues, UpdateDefine[] UpdateValues, int CurrentRowIndex, bool SetDefaults, string MasterForeignTable, string SlaveForeignTable, string MasterForeignKeyFields, List<RowStatus> TableDataSchema, List<RowStatus> MasterDataSchema, int MasterForeignValueIndex, string MasterSelectRowKeyValues)
        {
            //计算主表的当前行的外键值，更新保存本表数据，计算本表当前行的主键值
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {

                Dictionary<string, FieldAttr> fldIdent = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.AutoIncrementFields);
                Dictionary<string, FieldAttr> fldPMKeys = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.PrimaryKeyFields);
                Dictionary<string, FieldAttr> fldMaster = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.MasterForeignFields, false, MasterForeignTable,string.Empty);
                Dictionary<string, FieldAttr> fldForeignKeys = fldPMKeys;
                if (!fldForeignKeys.ContainsKey(fldIdent.First().Key)) fldForeignKeys.Add(fldIdent.First().Key, fldIdent.First().Value);

                //主表
                string MasterCurrentRowInsertMapping = string.Empty;
                string MasterForeignRowType = "Update"; //主表当前行的DML
                int MasterInsertCounts = 0, MasterUpdateCounts = 0;
                if (MasterForeignTable != string.Empty && MasterDataSchema != null)
                {
                    foreach (RowStatus rowsts in MasterDataSchema)
                    {
                        MasterCurrentRowInsertMapping += (MasterCurrentRowInsertMapping == "" ? "" : ",") + rowsts.RowIndex.ToString() + ":" + rowsts.InsertIndex.ToString();
                        if (rowsts.InsertIndex > 0) { MasterInsertCounts++; } else { MasterUpdateCounts++; }
                        
                        if (MasterForeignValueIndex == rowsts.RowIndex)
                        {
                            if (rowsts.InsertIndex > 0)
                            {
                                MasterForeignRowType = "Insert";
                                MasterForeignValueIndex = rowsts.InsertIndex;  //更新关联主表主键ID取值行为Insert的行排序数
                            }
                            else if (rowsts.UpdateIndex > 0)
                            {
                                MasterForeignRowType = "Update";
                                MasterForeignValueIndex = rowsts.UpdateIndex;  //更新关联主表主键ID取值行为Insert的行排序数
                            }
                        }
                    }
                }
                string Script_MasterCurrentRowInsertMapping = "DECLARE @MasterCurrentRowInsertMapping NVARCHAR(MAX); SET @MasterCurrentRowInsertMapping = '" + MasterCurrentRowInsertMapping.Replace("'", "''") + "'  --此变量暂时不用";



                string MasterForeignValuesVar = "@" + MasterForeignTable + "_Table_Primary";  //替代变量占位符
                //string Script_MasterForeignValuesVar = "DECLARE @PrimaryValues NVARCHAR(MAX)=''";
                string Script_DML = "DECLARE @DML_SQLSTR NVARCHAR(MAX)";
                string Script_RowType = "DECLARE @MasterforeignRowType varchar(10); SET @MasterforeignRowType = '" + MasterForeignRowType.Replace("'", "''") + "'";
                string Script_RowIndex = "DECLARE @MasterforeignRowIndex INT; SET @MasterforeignRowIndex = " + MasterForeignValueIndex.ToString();
                string MasterForeignCurrentValueVar = "@" + MasterForeignTable + "_Table_PrimaryCurrentValue";  //替代变量占位符
                string Script_MasterSelectRowKeyValues = "DECLARE @MasterSelectRowKeyValues NVARCHAR(MAX); SET @MasterSelectRowKeyValues = '" + (MasterSelectRowKeyValues == null ? "" : MasterSelectRowKeyValues).Replace("'", "''") + "'";
                //int foreignpos = MasterForeignValue.RowIndex;
                //NameValueCollection foreignvalues = MasterForeignValue.FieldValues;

                string Script_FieldMapping = "DECLARE @FieldMapping VARCHAR(500)";
                string Script_MasterForeignKeyFilter = "DECLARE @KeyFilter NVARCHAR(MAX)";
                string MasterForeignKeyFilterStr = "";
                string FieldMappingStr = "";
                List<string> MasterKeyList = new List<string>();
                foreach (KeyValuePair<string, FieldAttr> maskey in fldMaster)
                {
                    string fldName = maskey.Key;
                    MasterKeyList.Add(fldName);
                    FieldAttr fatt = maskey.Value;
                    bool fldisnullable = fatt.IsNullable;
                    if (fatt.MasterForeignKeys != null && fatt.MasterForeignKeys != "")
                    {
                        MasterForeignKeyFilterStr += (MasterForeignKeyFilterStr == string.Empty ? "" : " AND ") + fldName + "={" + fldName + "}";
                        FieldMappingStr += (FieldMappingStr == string.Empty ? "" : ",") + (fatt.MasterForeignKeys.IndexOf('.') > 0 ? fatt.MasterForeignKeys.Substring(fatt.MasterForeignKeys.IndexOf('.') + 1) : fatt.MasterForeignKeys) + ":" + fldName;
                    }
                }
                Script_FieldMapping += ";SET @FieldMapping='" + FieldMappingStr.Replace("'", "''") + "';  --主表字段:本表字段，无配对表示名称相同";

                //主表当前行的DML
                string CurrentRowType = "Update"; 
                int InsertCounts = 0, UpdateCounts = 0;
                if (TableName != string.Empty && TableDataSchema != null)
                {
                    foreach (RowStatus rowsts in TableDataSchema)
                    {
                        if (rowsts.InsertIndex > 0) { InsertCounts++; } else { UpdateCounts++; }

                        if (CurrentRowIndex == rowsts.RowIndex)
                        {
                            if (rowsts.InsertIndex > 0)
                            {
                                CurrentRowType = "Insert";
                                CurrentRowIndex = rowsts.InsertIndex;  //更新本表主键ID取值行为Insert的行排序数
                            }
                            else if (rowsts.UpdateIndex > 0)
                            {
                                CurrentRowType = "Update";
                                CurrentRowIndex = rowsts.UpdateIndex;  //更新本表主键ID取值行为Insert的行排序数
                            }
                        }
                    }
                }
                string DMLScript = "";
                if (UpdateCounts > 0) DMLScript += GetScript_BatchUpdate(TableName, UpdateValues, true);
                if (InsertCounts > 0) DMLScript += GetScript_Add(TableName, FieldsList, NoSavedFields, InsertValues, SetDefaults, MasterForeignTable, SlaveForeignTable, MasterKeyList);
                Script_DML += "; SET @DML_SQLSTR = N'" + DMLScript.Replace("'", "''") + "'";   //Insert脚本

                //本表的INSERT行的主键取值
                string TableKeyValues = string.Empty;
                string Script_TableKeyValues = string.Empty;
                string Script_OutputTableKeyValues = string.Empty;
                string Script_Return = string.Empty;
                string autoIncreamFld = string.Empty;
                string keyfilter = string.Empty;
                string keyfldLink = string.Empty;
                string keyfldLinkDsp = string.Empty;
                //接收Insert数据的信息
                if (fldIdent.Count > 0)
                {
                    KeyValuePair<string, FieldAttr> pmkey = fldIdent.First();
                    autoIncreamFld = pmkey.Key;
                    script = @"Declare @Table_Primary" + " VARCHAR(MAX); " + char.ConvertFromUtf32(13)
                             + @"Declare @Table_PrimaryCurrentValue" + " VARCHAR(MAX); " + char.ConvertFromUtf32(13)
                             + "DECLARE @" + autoIncreamFld + "_LAST INT; SET @" + autoIncreamFld + "_LAST = ISNULL((Select MAX(" + autoIncreamFld + ") FROM " + TableName + "),0)" + char.ConvertFromUtf32(13);
                    keyfilter = " Where " + MasterForeignKeyFilterStr + (MasterForeignKeyFilterStr == string.Empty ? "" : " AND ") + autoIncreamFld + " BETWEEN @" + autoIncreamFld + "_LAST + 1 AND IDENT_CURRENT('" + TableName + "') ORDER BY " + autoIncreamFld;
                    keyfldLink = "'InsRowIndex:'+CONVERT(VARCHAR(10),ROW_NUMBER() OVER (ORDER BY " + autoIncreamFld + " ASC))+'," + autoIncreamFld + ":'+" + "CONVERT(VARCHAR(10)," + autoIncreamFld + ")";
                    if (CurrentRowType.ToLower() == "insert") keyfldLinkDsp = "ROW_NUMBER() OVER (ORDER BY " + autoIncreamFld + " ASC) AS RowIndex," + autoIncreamFld + "";
                }
                else
                {
                    script = "Declare @Table_Primary" + " VARCHAR(MAX)" + char.ConvertFromUtf32(13)
                             + @"Declare @Table_PrimaryCurrentValue" + " VARCHAR(MAX); " + char.ConvertFromUtf32(13);
                    keyfilter = (MasterForeignKeyFilterStr == string.Empty ? "" : " Where " + MasterForeignKeyFilterStr);
                    //按字段组定义的主键，一般要配合时间戳字段来执行排序，以保证数据排序的正确，忽略它可能导致后面按RowIndex定位的关联数据行发生错误
                    string orderFlds = string.Empty;
                    string valueFlds = string.Empty;
                    string valueFldsDsp = string.Empty;
                    foreach (KeyValuePair<string, FieldAttr> pmkey in fldPMKeys)
                    {
                        DataType ftype = pmkey.Value.DataType;
                        string fval = (ftype == DataType.DBChar || ftype == DataType.DBNchar || ftype == DataType.DBNtext || ftype == DataType.DBNvarchar || ftype == DataType.DBSysname || ftype == DataType.DBText || ftype == DataType.DBUniqueidentifier || ftype == DataType.DBVarchar || ftype == DataType.DBXml ?
                            "ISNULL(" + pmkey.Key + ",'')" : "CONVERT(VARCHAR(10)," + pmkey.Key + ")");
                        orderFlds += (orderFlds == string.Empty ? "" : ",") + pmkey.Key.Replace("'", "''") + " ASC";
                        valueFlds += (valueFlds == string.Empty ? "" : "','+") + "'" + pmkey.Key.Replace("'", "''") + ":'+" + fval;
                        valueFldsDsp += (valueFlds == string.Empty ? "" : ",") + pmkey.Key.Replace("'", "''");
                    }
                    keyfldLink = "'InsRowIndex:'+CONVERT(VARCHAR(10),ROW_NUMBER() OVER (ORDER BY " + orderFlds + "))+','+" + valueFlds;
                    if (CurrentRowType.ToLower() == "insert") keyfldLinkDsp = "ROW_NUMBER() OVER (ORDER BY " + orderFlds + ") AS RowIndex," + valueFldsDsp;
                }
                Script_MasterForeignKeyFilter += ";SET @KeyFilter='" + keyfilter.Replace("'", "''") + "'"; //暂无用

                //接收并返回Insert的数据行(s)
                TableKeyValues = "SET @Table_Primary" + " = STUFF((SELECT ';'+" + keyfldLink + " From " + TableName + keyfilter + " FOR XML PATH('')),1,1,'')";
                Script_TableKeyValues = "Declare @TableKeyValues" + " NVARCHAR(MAX); SET @TableKeyValues = N'" + TableKeyValues.Replace("'", "''") + "'" + char.ConvertFromUtf32(13);
                Script_OutputTableKeyValues = "EXECUTE sp_executesql @TableKeyValues,N'@" + autoIncreamFld + "_LAST INT,@Table_Primary VARCHAR(MAX) OUT',@" + autoIncreamFld + "_LAST,@Table_Primary OUT" + char.ConvertFromUtf32(13);
                Script_Return = "INSERT INTO @Returns(TableName, InsPrimaryKeyValues, SelectedRowPMKeyValue) VALUES('" + TableName + "',@Table_Primary,@Table_PrimaryCurrentValue)";

                //计算当前行的返回值
                string TablePMKeyCurrentValue = string.Empty;
                string Script_TablePMKeyCurrentValue = string.Empty;
                string Script_OutputTablePMKeyCurrentValue = string.Empty;
                keyfilter = string.Empty;
                if (CurrentRowType.ToLower() == "insert")
                {
                    //只对insert返回值，可以从返回值系列中结合当前行index获取当前行数据
                    //TablePMKeyCurrentValue = "SET @Table_PrimaryCurrentValue" + " = STUFF((SELECT ';'+" + keyfldLink + " From (SELECT " + keyfldLinkDsp + " From " + TableName + keyfilter + ") A WHERE A.RowIndex = " + CurrentRowIndex.ToString() + " FOR XML PATH('')),1,1,'')";
                    TablePMKeyCurrentValue = "DECLARE @ctmp NVARCHAR(max); SET @ctmp = SUBSTRING(@Table_Primary,CHARINDEX('InsRowIndex:'+'" + CurrentRowIndex.ToString() + "',@Table_Primary),LEN(@Table_Primary))+';'; " + char.ConvertFromUtf32(13)
                        + "SET @Table_PrimaryCurrentValue = SUBSTRING(@ctmp,1,CHARINDEX(';',@ctmp)-1)";
                    Script_TablePMKeyCurrentValue = "Declare @TablePMKeyCurrentValue" + " NVARCHAR(MAX); SET @TablePMKeyCurrentValue = N'" + TablePMKeyCurrentValue.Replace("'", "''") + "'" + char.ConvertFromUtf32(13);
                    Script_OutputTablePMKeyCurrentValue = "EXECUTE sp_executesql @TablePMKeyCurrentValue,N'@Table_Primary VARCHAR(MAX), @Table_PrimaryCurrentValue VARCHAR(MAX) OUT',@Table_Primary,@Table_PrimaryCurrentValue OUT" + char.ConvertFromUtf32(13);
                }
                else
                {
                    //update不返回值，需查询当前行数据
                    string sqlPMKeyFields = string.Empty;
                    bool isSelected = (UpdateValues.Count() > 0 && CurrentRowIndex > 0 ? true : false);
                    UpdateDefine ValueRowDef = (UpdateValues.Count() == 0 || CurrentRowIndex < 1 ? new UpdateDefine() : UpdateValues[CurrentRowIndex - 1]);
                    NameValueCollection CurrentRowDef = ValueRowDef.FieldValues;
                    foreach (KeyValuePair<string, FieldAttr> fattr in fldForeignKeys)
                    {
                        string fld = fattr.Key;
                        string ftype = fattr.Value.DataType.ToString();
                        string fvalue = (isSelected ? CurrentRowDef[fld] : "");
                        if (fvalue == "" && isSelected)
                        {
                            fvalue = (ValueRowDef.Filter.FilterParameters.ContainsKey(fld) ? ValueRowDef.Filter.FilterParameters[fld].ToString() : "");
                        }
                        //Char,Date,Image,text,variant,Sysname,binary,Xml
                        if (fvalue == "")
                        {
                            string[] artype = "Char,Date,Image,text,variant,Sysname,binary,Xml".Split(new char[1] { ',' });
                            bool ismatch = false;
                            for (int it=0;it<artype.Length;it++){
                                ismatch = (ftype.ToLower().IndexOf(artype[it].ToLower()) > -1 ? true : false);
                                if (ismatch) break;
                            }
                            if (!ismatch) fvalue = "0";
                        }
                        keyfilter += (keyfilter == string.Empty ? "" : " AND ") + fld + "='" + fvalue + "'";
                    }
                    keyfilter = " Where " + MasterForeignKeyFilterStr + (MasterForeignKeyFilterStr == string.Empty ? "" : " AND ") + keyfilter;
                    TablePMKeyCurrentValue = "SET @Table_PrimaryCurrentValue" + " = STUFF((SELECT ';'+" + keyfldLink + " From " + TableName + keyfilter + " FOR XML PATH('')),1,1,'')";
                    //TablePMKeyCurrentValue = "DECLARE @ctmp NVARCHAR(max); SET @ctmp = SUBSTRING(@Table_Primary,CHARINDEX('InsRowIndex:'+'" + CurrentRowIndex.ToString() + "',@Table_Primary),LEN(@Table_Primary))+';'; " + char.ConvertFromUtf32(13)
                    //    + "SET @Table_PrimaryCurrentValue = SUBSTRING(@ctmp,1,CHARINDEX(';',@ctmp)-1)";
                    Script_TablePMKeyCurrentValue = "Declare @TablePMKeyCurrentValue" + " NVARCHAR(MAX); SET @TablePMKeyCurrentValue = N'" + TablePMKeyCurrentValue.Replace("'", "''") + "'" + char.ConvertFromUtf32(13);
                    Script_OutputTablePMKeyCurrentValue = "EXECUTE sp_executesql @TablePMKeyCurrentValue,N'@Table_Primary VARCHAR(MAX), @Table_PrimaryCurrentValue VARCHAR(MAX) OUT',@Table_Primary,@Table_PrimaryCurrentValue OUT" + char.ConvertFromUtf32(13);
                }

                //主表的UPDATE行的外键取值（INSERT行的外键取值在SQL脚本中动态计算）
                //string[] arMasterForeignKeyFields = MasterForeignKeyFields.Split(new char[1] { ',' });
                //string sqlMasterForeignKeyFields = string.Empty;
                //if (MasterForeignRowType.ToLower() == "update")
                //{
                //    UpdateDefine MasterForeignValueRow = UpdateValues[MasterForeignValueIndex - 1];
                //    for (int f = 0; f < arMasterForeignKeyFields.Length; f++)
                //    {
                //        string fld = arMasterForeignKeyFields[f];
                //        string fvalue = MasterForeignValueRow.FieldValues.AllKeys.Contains(fld) ? MasterForeignValueRow.FieldValues[fld] : "";
                //        if (fvalue == "")
                //        {
                //            fvalue = (MasterForeignValueRow.Filter.FilterParameters.ContainsKey(fld) ? MasterForeignValueRow.Filter.FilterParameters[fld].ToString() : "");
                //        }
                //        sqlMasterForeignKeyFields += (sqlMasterForeignKeyFields == string.Empty ? "" : ",") + fld + ":" + fvalue;
                //        //sqlMasterForeignKeyFields += (sqlMasterForeignKeyFields == string.Empty ? "';'+" : "+','+") + "'" + arMasterForeignKeyFields[f] + ":'+" + arMasterForeignKeyFields[f];
                //    }
                //    //sqlMasterForeignKeyFields = "SET @MasterSelectRowKeyValues = STUFF((SELECT " + sqlMasterForeignKeyFields + " FROM " + MasterForeignTable + " FOR XML PATH('')),1,1,'')";
                //    sqlMasterForeignKeyFields = "SET @MasterSelectRowKeyValues =" + sqlMasterForeignKeyFields;
                //}
                //string Script_MasterForeignKeyFields = sqlMasterForeignKeyFields;

                #region 更新参数定义
                string Script_UpdataParas = @"
@Script_FieldMapping
--DECLARE @KeyValueTemplate VARCHAR(MAX); SET @KeyValueTemplate = 'User:{1},Location:{2},BU:{3}'
@Script_DML_SQLSTR
@Script_KeyFilter
@Script_CurrentIndex
@Script_MasterSelectRowKeyValues
@Script_RowType

"
+ (MasterForeignTable == string.Empty ? "" : 
@"---基于可能的Update操作，重新构建主表当前行外键键值串
SET @MasterSelectRowKeyValues = $MasterForeignCurrentValueVar$"
+ char.ConvertFromUtf32(13))
+ @"
--SELECT @MasterSelectRowKeyValues
DECLARE @STR VARCHAR(MAX); SET @STR = @FieldMapping
DECLARE @FldMap VARCHAR(100)
DECLARE @SFld VARCHAR(100)
DECLARE @DFld VARCHAR(100)
WHILE @STR<>''
	BEGIN
		SET @FldMap = CASE WHEN CHARINDEX(',',@STR) > 0 THEN SUBSTRING(@STR,1,CHARINDEX(',',@STR)-1) ELSE RTRIM(@STR) END
		IF CHARINDEX(':',@FldMap) > 0
			BEGIN
				SET @SFld = SUBSTRING(@FldMap,1,CHARINDEX(':',@FldMap)-1)
				SET @DFld = SUBSTRING(@FldMap,CHARINDEX(':',@FldMap)+1,LEN(@FldMap))
				SET @MasterSelectRowKeyValues = STUFF(REPLACE(','+@MasterSelectRowKeyValues, ','+@SFld+':', ','+@DFld+':'),1,1,'')
			END
		SET @STR = CASE WHEN CHARINDEX(',',@STR) > 0 THEN SUBSTRING(@STR,CHARINDEX(',',@STR)+1,LEN(@STR)) ELSE '' END
	END
--SELECT @MasterSelectRowKeyValues

@Script_TableKeyValues
@Script_TablePMKeyCurrentValue
SET @STR = @MasterSelectRowKeyValues
DECLARE @Fld VARCHAR(100)
DECLARE @Value VARCHAR(100)
WHILE @STR<>''
	BEGIN
		SET @FldMap = CASE WHEN CHARINDEX(',',@STR) > 0 THEN SUBSTRING(@STR,1,CHARINDEX(',',@STR)-1) ELSE RTRIM(@STR) END
		IF CHARINDEX(':',@FldMap) > 0
			BEGIN
				SET @Fld = CASE WHEN CHARINDEX(':',@FldMap) > 0 THEN SUBSTRING(@FldMap,1,CHARINDEX(':',@FldMap)-1) ELSE RTRIM(@FldMap) END
				SET @Value = CASE WHEN CHARINDEX(':',@FldMap) > 0 THEN SUBSTRING(@FldMap,CHARINDEX(':',@FldMap)+1,LEN(@FldMap)) ELSE '' END
				SET @Value = CASE WHEN LEFT(@Value,1) = '@' THEN @Value ELSE '''' + @Value + '''' END
				SET @DML_SQLSTR = REPLACE(@DML_SQLSTR, '{'+@Fld+'}', @Value)
				SET @KeyFilter = REPLACE(@KeyFilter, '{'+@Fld+'}', @Value)
				SET @TableKeyValues = REPLACE(@TableKeyValues, '{'+@Fld+'}', @Value)
				SET @TablePMKeyCurrentValue = REPLACE(@TablePMKeyCurrentValue, '{'+@Fld+'}', @Value)
			END
		SET @STR = CASE WHEN CHARINDEX(',',@STR) > 0 THEN SUBSTRING(@STR,CHARINDEX(',',@STR)+1,LEN(@STR)) ELSE '' END
	END
--SELECT @DML_SQLSTR
--SELECT @KeyFilter
--SET @DML_SQLSTR = REPLACE(@DML_SQLSTR,'''','''''');
EXEC(@DML_SQLSTR);
@Script_OutputTableKeyValues;
@Script_OutputTablePMKeyCurrentValue
@Script_Return;
";
                #endregion
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_Return", Script_Return);
                Script_UpdataParas = Script_UpdataParas.Replace("@Return", "$Return$");
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_FieldMapping", Script_FieldMapping);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_KeyFilter", Script_MasterForeignKeyFilter);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_CurrentIndex", Script_RowIndex);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_RowType", Script_RowType);
                //Script_UpdataParas = Script_UpdataParas.Replace("@Script_MasterForeignKeyFields", Script_MasterForeignKeyFields);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_MasterSelectRowKeyValues", Script_MasterSelectRowKeyValues);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_DML_SQLSTR", Script_DML);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_TableKeyValues", Script_TableKeyValues);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_TablePMKeyCurrentValue", Script_TablePMKeyCurrentValue);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_OutputTableKeyValues", Script_OutputTableKeyValues);
                Script_UpdataParas = Script_UpdataParas.Replace("@Script_OutputTablePMKeyCurrentValue", Script_OutputTablePMKeyCurrentValue);
                Script_UpdataParas = Script_UpdataParas.Replace("'@'", "#var#");
                Script_UpdataParas = Script_UpdataParas.Replace("@", "@" + TableName + "_");  //变量去重
                Script_UpdataParas = Script_UpdataParas.Replace("#var#", "'@'");
                Script_UpdataParas = Script_UpdataParas.Replace("$MasterForeignValuesVar$", MasterForeignValuesVar);
                Script_UpdataParas = Script_UpdataParas.Replace("$MasterForeignCurrentValueVar$", MasterForeignCurrentValueVar);
                Script_UpdataParas = Script_UpdataParas.Replace("$Return$", "@Return");
                script = script.Replace("@", "@" + TableName + "_");
                script += //char.ConvertFromUtf32(13) + DMLScript
                    //返回一个按自增值大小排列的ID字符串
                    char.ConvertFromUtf32(13) + Script_UpdataParas;
                    //+ char.ConvertFromUtf32(13) + "SET @" + TableName + "_Primary" + " = STUFF((SELECT ';'+" + keyfldLink + " From " + TableName + keyfilter + " FOR XML PATH('')),1,1,'')";
                //script = DMLScript + char.ConvertFromUtf32(13) + script;

            }
            catch (Exception exp)
            {
                Exception er = new Exception(exp.Message + ";" + char.ConvertFromUtf32(13) + "Trace: " + exp.StackTrace + ";" + char.ConvertFromUtf32(13) + " SQL Script: " + SQLString, exp);
                throw er;
            }
            return script;
        }

        public string GetScript_BatchUpdate(string TableName, UpdateDefine[] UpdateData)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields);

                for (int r = 0; r < UpdateData.Length; r++)
                {

                    UpdateDefine updaterow = UpdateData[r];
                    NameValueCollection FieldValues = updaterow.FieldValues;
                    string[] arNoSavedFields = updaterow.NoSavedFields == string.Empty ? new string[0] : updaterow.NoSavedFields.Split(new char[1] { ',' });
                    JobDataFilter Filter = updaterow.Filter;

                    //更新的条件
                    string whereStr = string.Empty;
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : (fldValue.ToLower() == "null" ? "" : "'" + fldValue.Replace("'", "''") + "'"));

                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }

                    //更新的数据
                    string rowvalues = string.Empty;
                    foreach (string fldName in FieldValues)
                    {
                        if (fldsAtt.ContainsKey(fldName) && !arNoSavedFields.Contains(fldName))
                        {
                            string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : "";
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            DataType ftype = fatt.DataType;
                            string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                            fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : NonNullDefVal) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                        }
                    }
                    if (FieldValues.Count > 0 && rowvalues != "") SQLString += "update " + TableName + " set " + rowvalues + (whereStr == string.Empty ? "" : " where " + whereStr) + ";" + char.ConvertFromUtf32(13);
                }
                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return script;
        }
        public string GetScript_BatchUpdate(string TableName, UpdateDefine[] UpdateData, bool SetDefaults)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                for (int r = 0; r < UpdateData.Length; r++)
                {

                    UpdateDefine updaterow = UpdateData[r];
                    NameValueCollection FieldValues = updaterow.FieldValues;
                    string[] arNoSavedFields = updaterow.NoSavedFields == string.Empty ? new string[0] : updaterow.NoSavedFields.Split(new char[1] { ',' });
                    JobDataFilter Filter = updaterow.Filter;

                    //更新的条件
                    string whereStr = string.Empty;
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : (fldValue.ToLower() == "null" ? "" : "'" + fldValue.Replace("'", "''") + "'"));

                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }

                    //更新的数据
                    string rowvalues = string.Empty;
                    foreach (string fldName in DefFields)
                    {
                        if (fldsAtt.ContainsKey(fldName) && !arNoSavedFields.Contains(fldName))
                        {
                            string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : "";
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            DataType ftype = fatt.DataType;
                            string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                            fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(fldName) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : NonNullDefVal) : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : NonNullDefVal)) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                        }
                    }
                    if (FieldValues.Count > 0 && rowvalues != "") SQLString += "update " + TableName + " set " + rowvalues + (whereStr == string.Empty ? "" : " where " + whereStr) + ";" + char.ConvertFromUtf32(13);
                }
                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return script;
        }
        public string GetScript_BatchUpdate(string TableName, UpdateDefine[] UpdateData, bool SetDefaults, string MasterForeignTable, string SlaveForeignTable)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                for (int r = 0; r < UpdateData.Length; r++)
                {

                    UpdateDefine updaterow = UpdateData[r];
                    NameValueCollection FieldValues = updaterow.FieldValues;
                    string[] arNoSavedFields = updaterow.NoSavedFields == string.Empty ? new string[0] : updaterow.NoSavedFields.Split(new char[1] { ',' });
                    JobDataFilter Filter = updaterow.Filter;

                    //更新的条件
                    string whereStr = string.Empty;
                    foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : (fldValue.ToLower() == "null" ? "" : "'" + fldValue.Replace("'", "''") + "'"));

                        whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                    }

                    //更新的数据
                    string rowvalues = string.Empty;
                    foreach (string fldName in DefFields)//FieldValues)
                    {
                        if (fldsAtt.ContainsKey(fldName) && !arNoSavedFields.Contains(fldName))
                        {
                            string fldValue = FieldValues.AllKeys.Contains(fldName) ? FieldValues[fldName].ToString() : "";
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            DataType ftype = fatt.DataType;
                            string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                            fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(fldName) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : NonNullDefVal) : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : NonNullDefVal)) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                        }
                    }
                    if (FieldValues.Count > 0 && rowvalues != "") SQLString += "update " + TableName + " set " + rowvalues + (whereStr == string.Empty ? "" : " where " + whereStr) + ";" + char.ConvertFromUtf32(13);
                }
                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return script;
        }
        public string GetScript_SingleUpdate(string TableName, NameValueCollection FieldValues, JobDataFilter Filter)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields);
                string whereStr = string.Empty;
                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                    string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : (fldValue.ToLower() == "null" ? "" : "'" + fldValue.Replace("'", "''") + "'"));

                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }


                string rowvalues = string.Empty;
                foreach (KeyValuePair<string, object> valItem in FieldValues)
                {
                    string fldName = valItem.Key;
                    string fldValue = valItem.Value.ToString();
                    FieldAttr fatt = fldsAtt[fldName];
                    bool fldisnullable = fatt.IsNullable;
                    DataType ftype = fatt.DataType;
                    string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                    fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : NonNullDefVal) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                    string fldPair = fldName + " = " + fldValue;
                    rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                }
                SQLString = "update " + TableName + " set " + rowvalues + " where " + whereStr;

                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return script;
        }
        public string GetScript_SingleUpdate(string TableName, NameValueCollection FieldValues, JobDataFilter Filter, bool SetDefaults)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                string whereStr = string.Empty;
                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                    string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : (fldValue.ToLower() == "null" ? "" : "'" + fldValue.Replace("'", "''") + "'"));

                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }


                string rowvalues = string.Empty;
                foreach (KeyValuePair<string, object> valItem in DefFields)//FieldValues)
                {
                    string fldName = valItem.Key;
                    string fldValue = valItem.Value.ToString();
                    FieldAttr fatt = fldsAtt[fldName];
                    bool fldisnullable = fatt.IsNullable;
                    DataType ftype = fatt.DataType;
                    string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                    fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(fldName) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : NonNullDefVal) : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : NonNullDefVal)) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                    string fldPair = fldName + " = " + fldValue;
                    rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                }
                SQLString = "update " + TableName + " set " + rowvalues + " where " + whereStr;

                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return script;
        }
        public string GetScript_SingleUpdate(string TableName, NameValueCollection FieldValues, JobDataFilter Filter, bool SetDefaults, string MasterForeignTable, string SlaveForeignTable)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.DMLFields, SetDefaults);
                //按默认值更新时，忽略FieldsList，取全部字段
                NameValueCollection DefFields = new NameValueCollection();
                if (SetDefaults)
                {
                    foreach (KeyValuePair<string, FieldAttr> fldatt in fldsAtt)
                    {
                        DefFields.Add(fldatt.Key, fldatt.Value.DefaultValue);
                    }
                }

                string whereStr = string.Empty;
                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                    string valuepair = filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : (fldValue.ToLower() == "null" ? "" : "'" + fldValue.Replace("'", "''") + "'"));

                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }


                string rowvalues = string.Empty;
                foreach (KeyValuePair<string, object> valItem in DefFields)
                {
                    string fldName = valItem.Key;
                    string fldValue = valItem.Value.ToString();
                    FieldAttr fatt = fldsAtt[fldName];
                    bool fldisnullable = fatt.IsNullable;
                    DataType ftype = fatt.DataType;
                    string NonNullDefVal = (ftype == DataType.DBBigint || ftype == DataType.DBDecimal || ftype == DataType.DBFloat || ftype == DataType.DBInt || ftype == DataType.DBMoney || ftype == DataType.DBNumeric || ftype == DataType.DBReal || ftype == DataType.DBSmallint || ftype == DataType.DBSmallmoney || ftype == DataType.DBTinyint || ftype == DataType.DBBit ? "'0'" : "''");
                    fldValue = (fldValue == "" || !FieldValues.AllKeys.Contains(fldName) ? (SetDefaults ? (fatt.DefaultValue == string.Empty ? (fldisnullable ? "NULL" : NonNullDefVal) : "'" + fatt.DefaultValue.Replace("'", "''") + "'") : (fldisnullable ? "NULL" : NonNullDefVal)) : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                    string fldPair = fldName + " = " + fldValue;
                    rowvalues += (rowvalues == string.Empty ? fldPair : "," + fldPair);
                }
                SQLString = "update " + TableName + " set " + rowvalues + " where " + whereStr;

                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return script;
        }

        public string GetScript_SingleDelete(string TableName, JobDataFilter Filter)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            try
            {
                string whereStr = string.Empty;
                foreach (KeyValuePair<string, object> filtItem in Filter.FilterParameters)
                {
                    string valuepair = filtItem.Key + "='" + filtItem.Value.ToString() + "'";
                    whereStr += (whereStr == string.Empty ? valuepair : " and " + valuepair);
                }
                SQLString = "delete from " + TableName + " where " + whereStr;

                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return script;
        }
        public string GetScript_BatchDelete(string TableName, NameValueCollection[] DeleteFilter)
        {
            string script = string.Empty;
            string SQLString = string.Empty;

            
            try
            {
                //删除索引条件，必须是在本表内的字段
                Dictionary<string, FieldAttr> fldsAtt = GetTableFields(TableName, string.Empty, SQLDataObjectType.Table, FieldCatagory.AllFields);

                for (int r = 0; r < DeleteFilter.Length; r++)
                {

                    NameValueCollection deleterow = DeleteFilter[r];

                    //删除的数据条件
                    string whereStr = string.Empty;
                    foreach (string fldName in deleterow)
                    {
                        string fldValue = deleterow[fldName].ToString();
                        if (fldsAtt.ContainsKey(fldName))  //忽略本表不存在的字段
                        {
                            FieldAttr fatt = fldsAtt[fldName];
                            bool fldisnullable = fatt.IsNullable;
                            fldValue = (fldValue == "" ? (fldisnullable ? "NULL" : "''") : (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'"));
                            string fldPair = fldName + " = " + fldValue;
                            whereStr += (whereStr == string.Empty ? fldPair : " and " + fldPair);
                        }
                    }
                    SQLString += "delete from " + TableName + (whereStr == string.Empty ? "" : " where " + whereStr) + ";" + char.ConvertFromUtf32(13);
                }
                script = SQLString;

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }

            return script;
        }


        public bool Execute_Script(string Script, JobDataFilter SPParameters)
        {
            bool rst = true;
            string SQLString = string.Empty;

            try
            {
                SQLString = Script;

                if (SPParameters != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in SPParameters.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        fldValue = (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        SQLString = SQLString.Replace("@" + filtItem.Key, fldValue);
                    }
                }

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return rst;
        }

        public string GetScript_ExecuteScript(string Script, JobDataFilter SPParameters)
        {
            string SQLString = string.Empty;

            try
            {
                SQLString = Script;

                if (SPParameters != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in SPParameters.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        fldValue = (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        SQLString = SQLString.Replace("@" + filtItem.Key, fldValue);
                    }
                }

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return SQLString;
        }

        public bool Execute_SP(string SPName, JobDataFilter SPParameters)
        {
            bool rst = true;
            string SQLString = string.Empty;

            try
            {

                string parasStr = string.Empty;
                if (SPParameters != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in SPParameters.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string parapair = "@" + filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        parasStr += (parasStr == string.Empty ? parapair : ", " + parapair);
                    }
                }
                SQLString = "execute " + SPName + " " + parasStr;

                DbHelper.ExecuteSql(SQLString);

            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return rst;
        }

        public DataSet GetDataSet_SP(string SPName, JobDataFilter SPParameters)
        {
            bool rst = true;
            string SQLString = string.Empty;

            DataSet ds = null;
            try
            {

                string parasStr = string.Empty;
                if (SPParameters != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in SPParameters.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string parapair = "@" + filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        parasStr += (parasStr == string.Empty ? parapair : ", " + parapair);
                    }
                }
                SQLString = "execute " + SPName + " " + parasStr;

                ds = DbHelper.Query(SQLString);
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return ds;
        }

        public DataTable GetDataTable_SP(string SPName, JobDataFilter SPParameters)
        {
            bool rst = true;
            string SQLString = string.Empty;

            DataTable dt = null; 
            try
            {

                string parasStr = string.Empty;
                if (SPParameters != null)
                {
                    foreach (KeyValuePair<string, object> filtItem in SPParameters.FilterParameters)
                    {
                        string fldValue = (filtItem.Value == null ? "" : filtItem.Value.ToString());
                        string parapair = "@" + filtItem.Key + "=" + (fldValue.Length > 0 && fldValue.Substring(0, 1) == "@" ? fldValue : "'" + fldValue.Replace("'", "''") + "'");
                        parasStr += (parasStr == string.Empty ? parapair : ", " + parapair);
                    }
                }
                SQLString = "execute " + SPName + " " + parasStr;

                DataSet ds = DbHelper.Query(SQLString);
                if (ds != null && ds.Tables.Count > 0) dt = ds.Tables[0];
            }
            catch (Exception exp)
            {
                Exception er = new Exception((exp.Message + "; SQL Script: " + SQLString).Replace("'","\'"), exp);
                throw er;
            }
            return dt;
        }


    }


}
