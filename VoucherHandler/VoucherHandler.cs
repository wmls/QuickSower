using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Web;
using System.Web.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using DataHandler.BusDataClass;
using DataHandler.BusJob;
using DataHandler.PubObjects;

namespace VoucherHandler
{
    public class VoucherHandler
    {
        #region 单据的增删改查审操作

        protected string GetBusData(DataHandler.BusDataClass.JobDataFilter Filter, string JobName)
        {
            string rst = "0";

            JobDataFilter getFilter = new JobDataFilter();
            SQLJob getJob = new SQLJob();
            string script;
            DataSet ds;
            DataTable dt;
            switch (JobName.ToLower())
            {
                case "getvoucher":
                    getFilter = Filter;
                    ds = GetVoucher(getFilter);

                    rst = PubFunctions.GetSerializedJson_DataSet(ds, PubFunctions.JasonGridType.EasyUIGrid);

                    break;
                case "searchvoucher":
                    getFilter = Filter;
                    ds = SearchVoucher(getFilter);

                    rst = PubFunctions.GetSerializedJson_DataSet(ds, PubFunctions.JasonGridType.EasyUIGrid);

                    break;
                case "getdatarowschema":
                    getFilter = Filter;
                    rst = GetTableRowObject(getFilter);
                    break;
                case "gettablerowdata":
                    getFilter = Filter;
                    rst = GetTableRowData(getFilter);
                    break;
                case "getpackage":
                    string iRdsID = Filter.FilterParameters["iRdsID"].ToString();

                    string cTblName = Filter.FilterParameters["tblName"].ToString();

                    script = @"SELECT * FROM " + cTblName + "  WHERE [iRdsID]= @iRdsID";

                    //保存Application的数据内容
                    getFilter.FilterParameters.Add("iRdsID", iRdsID);
                    script = PubFunctions.CreateTransactionScript(script);
                    dt = getJob.QueryDataTable_Job(script, getFilter);

                    rst = (dt != null ? PubFunctions.GetSerializedeasyDataGrid_Table(dt) : "");
                    rst = PubFunctions.GetEasyuiTableJson(rst);

                    break;
                case "getwarehousebills":
                    string ID = Filter.FilterParameters["ID"].ToString();

                    string TblName = Filter.FilterParameters["tblName"].ToString();

                    script = @"SELECT * FROM " + TblName + "  WHERE [ID]= @ID";

                    //保存Application的数据内容
                    getFilter.FilterParameters.Add("ID", ID);
                    script = PubFunctions.CreateTransactionScript(script);
                    dt = getJob.QueryDataTable_Job(script, getFilter);

                    rst = (dt != null ? PubFunctions.GetSerializedeasyDataGrid_Table(dt) : "");
                    rst = PubFunctions.GetEasyuiTableJson(rst);
                    break;

            }

            return rst;

        }
        protected string GetTableRowObject(JobDataFilter Filter)
        {
            string RowFields = string.Empty;

            SQLJob getJob = new SQLJob();

            string TableName = Filter.FilterParameters["tablename"].ToString();
            string FieldList = Filter.FilterParameters["fieldlist"].ToString();
            string MasterTableName = Filter.FilterParameters.ContainsKey("mastertable") ? Filter.FilterParameters["mastertable"].ToString() : "";
            string tblTableName = TableName;
            string nodbTableName = (TableName.IndexOf(".") > 0 ? TableName.Substring(TableName.LastIndexOf(".") + 1) : TableName);
            string nodbMasterTableName = (MasterTableName.IndexOf(".") > 0 ? MasterTableName.Substring(MasterTableName.LastIndexOf(".") + 1) : MasterTableName);
            SQLJob.SQLDataObjectType TableType = getJob.GetSQLDataObjectType("V_" + nodbTableName);
            if (TableType == SQLJob.SQLDataObjectType.View) TableName = "V_" + nodbTableName;

            //校验字段列表(主键、外键)
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allSFields = getJob.GetTableFields(TableName, nodbTableName, TableType, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
            string[] arFields = FieldList.Split(new char[1] { ',' });
            //补全当前table的主键字段
            Dictionary<string, SQLJob.FieldAttr> fldMaster = getJob.GetTableFields(tblTableName, nodbTableName, SQLJob.SQLDataObjectType.Table, SQLJob.FieldCatagory.PrimaryKeyandAutoIncrementFields);
            foreach (KeyValuePair<string, SQLJob.FieldAttr> maskey in fldMaster)
            {
                string fldName = maskey.Key.IndexOf('.') > 0 ? maskey.Key.Substring(maskey.Key.IndexOf('.') + 1) : maskey.Key;
                if (!arFields.Contains(fldName)) arFields = arFields.Concat(new string[1] { fldName }).ToArray();
            }
            //补全当前table的外键字段
            Dictionary<string, SQLJob.FieldAttr> fldForeign = getJob.GetTableFields(tblTableName, nodbTableName, SQLJob.SQLDataObjectType.Table, SQLJob.FieldCatagory.MasterForeignFields, false, nodbMasterTableName, string.Empty);
            foreach (KeyValuePair<string, SQLJob.FieldAttr> fgnkey in fldForeign)
            {
                string fldName = fgnkey.Key.IndexOf('.') > 0 ? fgnkey.Key.Substring(fgnkey.Key.IndexOf('.') + 1) : fgnkey.Key;
                if (!arFields.Contains(fldName)) arFields = arFields.Concat(new string[1] { fldName }).ToArray();
            }
            foreach (string c in arFields)
            {
                var s = from k in allSFields where (k.Key.IndexOf('.') > 0 ? k.Key.Substring(k.Key.IndexOf('.') + 1) : k.Key) == c select k;
                if (s.Count() > 0) RowFields += (RowFields == string.Empty ? "" : ",") + "\"" + c + "\":''";
            }
            RowFields = "{" + RowFields + "}";
            return RowFields;
        }
        protected string GetTableRowData(JobDataFilter Filter)
        {
            string RowData = string.Empty;

            SQLJob getJob = new SQLJob();

            string TableName = Filter.FilterParameters["tablename"].ToString();
            string FieldList = Filter.FilterParameters["fieldlist"].ToString();
            string MasterTableName = Filter.FilterParameters.ContainsKey("mastertable") ? Filter.FilterParameters["mastertable"].ToString() : "";
            string nodbMasterTableName = (MasterTableName.IndexOf(".") > 0 ? MasterTableName.Substring(MasterTableName.LastIndexOf(".") + 1) : MasterTableName);
            string tblTableName = TableName;
            string nodbTableName = (TableName.IndexOf(".") > 0 ? TableName.Substring(TableName.LastIndexOf(".") + 1) : TableName);
            SQLJob.SQLDataObjectType TableType = getJob.GetSQLDataObjectType("V_" + nodbTableName);
            if (TableType == SQLJob.SQLDataObjectType.View) TableName = "V_" + nodbTableName;

            //校验字段列表(主键、外键)
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allSFields = getJob.GetTableFields(TableName, nodbTableName, TableType, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
            string[] arFields = FieldList.Split(new char[1] { ',' });
            //补全当前table的主键字段，并构建RowFilter
            JobDataFilter RowFilter = new JobDataFilter();
            string WhereStr = string.Empty;
            Dictionary<string, SQLJob.FieldAttr> fldMaster = getJob.GetTableFields(tblTableName, nodbTableName, SQLJob.SQLDataObjectType.Table, SQLJob.FieldCatagory.PrimaryKeyandAutoIncrementFields);
            foreach (KeyValuePair<string, SQLJob.FieldAttr> maskey in fldMaster)
            {
                string fldName = maskey.Key.IndexOf('.') > 0 ? maskey.Key.Substring(maskey.Key.IndexOf('.') + 1) : maskey.Key;
                if (!arFields.Contains(fldName)) arFields = arFields.Concat(new string[1] { fldName }).ToArray();
                RowFilter.FilterParameters.Add(fldName, Filter.FilterParameters[fldName].ToString());
                WhereStr += (WhereStr == string.Empty ? "" : " AND ") + fldName + " = @" + fldName;
            }
            //过滤schema中不存在的field
            FieldList = string.Empty;

            //补全当前table的外键字段
            Dictionary<string, SQLJob.FieldAttr> fldForeign = getJob.GetTableFields(tblTableName, nodbTableName, SQLJob.SQLDataObjectType.Table, SQLJob.FieldCatagory.MasterForeignFields, false, nodbMasterTableName, string.Empty);
            foreach (KeyValuePair<string, SQLJob.FieldAttr> fgnkey in fldForeign)
            {
                string fldName = fgnkey.Key.IndexOf('.') > 0 ? fgnkey.Key.Substring(fgnkey.Key.IndexOf('.') + 1) : fgnkey.Key;
                if (!arFields.Contains(fldName)) arFields = arFields.Concat(new string[1] { fldName }).ToArray();
            }
            string RowFields = string.Empty;
            foreach (string c in arFields)
            {
                var s = from k in allSFields where (k.Key.IndexOf('.') > 0 ? k.Key.Substring(k.Key.IndexOf('.') + 1) : k.Key) == c select k;
                if (s.Count() > 0) RowFields += (RowFields == string.Empty ? "" : ",") + c;
            }

            DataTable rowdt = getJob.QueryDataTable_Job("SELECT " + RowFields + " FROM " + TableName + (WhereStr == "" ? "" : " WHERE " + WhereStr), RowFilter);
            arFields = RowFields.Split(new char[1] { ',' }); //过滤不存在的field，重置数组
            string FldValues = string.Empty;
            if (rowdt != null && rowdt.Rows.Count > 0)
            {
                foreach (string c in arFields)
                {
                    string value = rowdt.Rows[0][c] == DBNull.Value || rowdt.Rows[0][c] == null ? "" : rowdt.Rows[0][c].ToString();
                    FldValues += (FldValues == string.Empty ? "" : ",") + c + ":\"" + value + "\"";
                }
            }
            RowData = "{" + FldValues + "}";
            return RowData;
        }
        protected DataSet GetVoucher(JobDataFilter Filter)
        {
            SQLJob getJob = new SQLJob();

            DataSet ds = new DataSet();
            DataTable dt = null;

            string DataTableName = Filter.FilterParameters["mastertable"].ToString();
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> PMFields = getJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.PrimaryKeyFields);

            string Direct = Filter.FilterParameters["direct"].ToString();
            string MasterTableName = Filter.FilterParameters["mastertable"].ToString();
            string nodbMasterTableName = (MasterTableName.IndexOf(".") > 0 ? MasterTableName.Substring(MasterTableName.LastIndexOf(".") + 1) : MasterTableName);
            string tblMasterTableName = MasterTableName;
            string MasterFieldList = Filter.FilterParameters["masterfieldlist"].ToString();
            string MasterPMValues = Filter.FilterParameters["masterpmvalues"].ToString();
            string VoucherType = Filter.FilterParameters["vouchtype"].ToString();

            string PMFieldList = string.Empty;
            string VoucherFilter = (VoucherType != string.Empty ? VoucherType : string.Empty);
            string CurrentFilter = string.Empty;

            //视图要求建在本库，不能远程
            SQLJob.SQLDataObjectType MasterTableType = getJob.GetSQLDataObjectType("V_" + nodbMasterTableName);
            if (MasterTableType == SQLJob.SQLDataObjectType.View) MasterTableName = "V_" + nodbMasterTableName;

            NameValueCollection MasterCurrentRow = new NameValueCollection();
            JToken row = (JToken)JsonConvert.DeserializeObject(MasterPMValues);
            //((JValue)((JProperty)((JToken)datatbl[r]).First).Value).Value
            for (JProperty pfld = (JProperty)row.First; pfld != null; pfld = (JProperty)pfld.Next)
            {
                string fldname = pfld.Name;
                string fldvalue = pfld.Value.ToString();
                MasterCurrentRow.Add(fldname, fldvalue);
            }
            //校验字段列表
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allMFields = getJob.GetTableFields(MasterTableName, nodbMasterTableName, MasterTableType, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
            string[] arMasterFields = MasterFieldList.Split(new char[1] { ',' });
            MasterFieldList = "";
            foreach (string c in arMasterFields)
            {
                var s = from k in allMFields where (k.Key.IndexOf('.') > 0 ? k.Key.Substring(k.Key.IndexOf('.') + 1) : k.Key) == c select k;
                if (s.Count() > 0) MasterFieldList += (MasterFieldList == string.Empty ? "" : ",") + c;
            }

            foreach (KeyValuePair<string, SQLJob.FieldAttr> fld in PMFields)
            {
                PMFieldList += (PMFieldList == string.Empty ? "" : ", ") + (fld.Key.IndexOf('.') > 0 ? fld.Key.Substring(fld.Key.IndexOf('.') + 1) : fld.Key);
                if (!MasterCurrentRow.AllKeys.Contains(fld.Key)) CurrentFilter += (CurrentFilter == string.Empty ? "" : " AND ") + fld.Key + " = '" + MasterCurrentRow[fld.Key].ToString() + "'";
            }
            string SortExp = (Filter.FilterParameters.ContainsKey("sortexp") ? Filter.FilterParameters["sortexp"].ToString() : PMFieldList);  //默认按主键升序排列
            string[] arSortfld = SortExp.Split(new char[1] { ',' });
            string ReverseSortExp = "";
            foreach (string c in arSortfld)
            {
                string fld = c.Trim();
                string drt = "";
                if (fld.IndexOf(" ") > 0)
                {
                    fld = fld.Substring(0, fld.IndexOf(" "));
                    drt = fld.Substring(fld.IndexOf(" ") + 1).Trim();
                }
                drt = (drt.ToLower() == "asc" || drt == "" ? "desc" : "");
                ReverseSortExp = (ReverseSortExp == "" ? "" : ",") + fld + " " + drt;
            }
            string Script = string.Empty;
            switch (Direct.ToLower())
            {
                case "f": //first
                    Script = "SELECT TOP 1 ROW_NUMBER() OVER (ORDER BY " + SortExp + ") AS RowIndex, " + MasterFieldList + " FROM " + MasterTableName + (VoucherFilter == string.Empty ? "" : " WHERE " + VoucherFilter);
                    break;
                case "p":  //previous
                    Script = @";WITH SortTable
                                    AS
                                    (
	                                    SELECT ROW_NUMBER() OVER(ORDER BY " + ReverseSortExp + ") AS RowIndex, " + MasterFieldList + " FROM " + MasterTableName + (VoucherFilter == string.Empty ? "" : " WHERE " + VoucherFilter) + @"
                                    )
                                    SELECT TOP 1 " + MasterFieldList + @"
                                    FROM SortTable
                                    WHERE RowIndex > 
                                      ISNULL((SELECT RowIndex FROM SortTable WHERE " + CurrentFilter + "),0)";
                    break;
                case "n":  //next
                    Script = @";WITH SortTable
                                    AS
                                    (
	                                    SELECT ROW_NUMBER() OVER(ORDER BY " + SortExp + ") AS RowIndex, " + MasterFieldList + " FROM " + MasterTableName + (VoucherFilter == string.Empty ? "" : " WHERE " + VoucherFilter) + @"
                                    )
                                    SELECT TOP 1 " + MasterFieldList + @"
                                    FROM SortTable
                                    WHERE RowIndex > 
                                      ISNULL((SELECT RowIndex FROM SortTable WHERE " + CurrentFilter + "),0)";
                    break;
                case "e":   //end
                    Script = "SELECT TOP 1 ROW_NUMBER() OVER (ORDER BY " + ReverseSortExp + ") AS RowIndex, " + MasterFieldList + " FROM " + MasterTableName + (VoucherFilter == string.Empty ? "" : " WHERE " + VoucherFilter);
                    break;
                default:   //position by id
                    Script = "SELECT " + MasterFieldList + " FROM " + MasterTableName + " WHERE " + CurrentFilter + (VoucherFilter == string.Empty ? "" : " and " + VoucherFilter);
                    break;
            }

            //执行主表数据查询
            dt = getJob.QueryDataTable_Job(Script, Filter);
            dt.TableName = MasterTableName;
            ds = dt.DataSet;
            if (dt.Rows.Count > 0)
            {
                string SlaveTableNames = Filter.FilterParameters["slavetable"].ToString();
                string[] arSlaveTables = SlaveTableNames.Split(new char[1] { ',' });
                string SlaveFieldLists = Filter.FilterParameters["slavefieldlist"].ToString();
                JToken JTSlaveFieldLists = (JToken)JsonConvert.DeserializeObject(HttpUtility.UrlDecode(SlaveFieldLists));
                NameValueCollection SlaveFieldListNV = new NameValueCollection();
                //((JValue)((JProperty)((JToken)datatbl[r]).First).Value).Value
                for (JProperty pfld = (JProperty)JTSlaveFieldLists.First; pfld != null; pfld = (JProperty)pfld.Next)
                {
                    string fldname = pfld.Name;
                    string fldvalue = pfld.Value.ToString();
                    SlaveFieldListNV.Add(fldname, fldvalue);
                }
                for (int t = 0; t < arSlaveTables.Length; t++)
                {
                    string SlaveTableName = arSlaveTables[t];
                    string nodbSlaveTableName = (SlaveTableName.IndexOf(".") > 0 ? SlaveTableName.Substring(SlaveTableName.LastIndexOf(".") + 1) : SlaveTableName);
                    string tblSlaveTableName = SlaveTableName;
                    string SlaveFieldList = SlaveFieldListNV[SlaveTableName];
                    //视图要求建在本库，不能远程
                    SQLJob.SQLDataObjectType SlaveTableType = getJob.GetSQLDataObjectType("V_" + nodbSlaveTableName);
                    if (SlaveTableType == SQLJob.SQLDataObjectType.View) SlaveTableName = "V_" + nodbSlaveTableName;

                    //获取子表数据
                    DataTable childdt = null;
                    //校验字段列表
                    Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allSFields = getJob.GetTableFields(SlaveTableName, nodbSlaveTableName, SlaveTableType, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
                    string[] arSlaveFields = SlaveFieldList.Split(new char[1] { ',' });
                    Dictionary<string, SQLJob.FieldAttr> fldMaster = getJob.GetTableFields(tblSlaveTableName, nodbSlaveTableName, SQLJob.SQLDataObjectType.Table, SQLJob.FieldCatagory.AllFields, false, nodbMasterTableName, string.Empty);
                    string MasterForeignKeyFilterStr = "";
                    List<string> MasterKeyList = new List<string>();
                    foreach (KeyValuePair<string, SQLJob.FieldAttr> maskey in fldMaster)
                    {
                        string fldName = maskey.Key.IndexOf('.') > 0 ? maskey.Key.Substring(maskey.Key.IndexOf('.') + 1) : maskey.Key;
                        MasterKeyList.Add(fldName);
                        SQLJob.FieldAttr fatt = maskey.Value;
                        string foreignkey = fatt.MasterForeignKeys;
                        string[] arfkey = foreignkey.Split(new char[1] { ',' });
                        foreignkey = "";
                        for (int f = 0; f < arfkey.Length; f++)
                        {
                            string tbl = (arfkey[f].IndexOf(".") > 0 ? arfkey[f].Substring(0, arfkey[f].IndexOf(".")) : arfkey[f]).Trim();
                            string fld = (arfkey[f].IndexOf(".") > 0 ? arfkey[f].Substring(arfkey[f].IndexOf(".") + 1) : "").Trim();
                            if (tbl.ToLower() == nodbMasterTableName.ToLower())
                            {
                                foreignkey = fld;
                                break;
                            }
                        }
                        if (foreignkey != "")
                        {
                            MasterForeignKeyFilterStr += (MasterForeignKeyFilterStr == string.Empty ? "" : " AND ") + fldName + "='" + dt.Rows[0][foreignkey].ToString() + "'";
                        }
                        //补全当前table的外键字段
                        if (!arSlaveFields.Contains(fldName)) arSlaveFields = arSlaveFields.Concat(new string[1] { fldName }).ToArray();
                    }
                    SlaveFieldList = "";
                    foreach (string c in arSlaveFields)
                    {
                        var s = from k in allSFields where (k.Key.IndexOf('.') > 0 ? k.Key.Substring(k.Key.IndexOf('.') + 1) : k.Key) == c select k;
                        if (s.Count() > 0) SlaveFieldList += (SlaveFieldList == string.Empty ? "" : ",") + c;
                    }
                    Script = "SELECT " + SlaveFieldList + " FROM " + SlaveTableName + " WHERE " + MasterForeignKeyFilterStr;
                    childdt = getJob.QueryDataTable_Job(Script, Filter);
                    childdt.TableName = SlaveTableName;

                    ds.Tables.Add(childdt.Copy());
                }
            }
            return ds;
        }
        protected DataSet SearchVoucher(JobDataFilter Filter)
        {
            SQLJob getJob = new SQLJob();

            DataSet ds = new DataSet();
            DataTable dt = null;

            string DataTableName = Filter.FilterParameters["mastertable"].ToString();
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> PMFields = getJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.PrimaryKeyFields);

            string MasterTableName = Filter.FilterParameters["mastertable"].ToString();
            string nodbMasterTableName = (MasterTableName.IndexOf(".") > 0 ? MasterTableName.Substring(MasterTableName.LastIndexOf(".") + 1) : MasterTableName);
            string tblMasterTableName = MasterTableName;
            string Direct = Filter.FilterParameters["direct"].ToString();
            string MasterFieldList = Filter.FilterParameters["masterfieldlist"].ToString();
            string CodeFieldName = Filter.FilterParameters["codefieldname"].ToString();
            string cCode = Filter.FilterParameters["code"].ToString();

            string PMFieldList = string.Empty;
            SQLJob.SQLDataObjectType MasterTableType = getJob.GetSQLDataObjectType("V_" + nodbMasterTableName);
            if (MasterTableType == SQLJob.SQLDataObjectType.View) MasterTableName = "V_" + nodbMasterTableName;


            //校验字段列表
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allMFields = getJob.GetTableFields(MasterTableName, nodbMasterTableName, MasterTableType, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
            string[] arMasterFields = MasterFieldList.Split(new char[1] { ',' });
            MasterFieldList = "";
            foreach (string c in arMasterFields)
            {
                var s = from k in allMFields where (k.Key.IndexOf('.') > 0 ? k.Key.Substring(k.Key.IndexOf('.') + 1) : k.Key) == c select k;
                if (s.Count() > 0) MasterFieldList += (MasterFieldList == string.Empty ? "" : ",") + c;
            }

            foreach (KeyValuePair<string, SQLJob.FieldAttr> fld in PMFields)
            {
                PMFieldList += (PMFieldList == string.Empty ? "" : ", ") + (fld.Key.IndexOf('.') > 0 ? fld.Key.Substring(fld.Key.IndexOf('.') + 1) : fld.Key);
            }

            string Script = string.Empty;
            Script = "SELECT top 1 " + MasterFieldList + " FROM " + MasterTableName + " WHERE " + CodeFieldName + " like '%" + cCode + "%'";

            //执行主表数据查询
            dt = getJob.QueryDataTable_Job(Script, Filter);
            dt.TableName = MasterTableName;
            ds = dt.DataSet;

            string SlaveTableNames = Filter.FilterParameters["slavetable"].ToString();
            string[] arSlaveTables = SlaveTableNames.Split(new char[1] { ',' });
            string SlaveFieldLists = Filter.FilterParameters["slavefieldlist"].ToString();
            JToken JTSlaveFieldLists = (JToken)JsonConvert.DeserializeObject(HttpUtility.UrlDecode(SlaveFieldLists));
            NameValueCollection SlaveFieldListNV = new NameValueCollection();
            //((JValue)((JProperty)((JToken)datatbl[r]).First).Value).Value
            for (JProperty pfld = (JProperty)JTSlaveFieldLists.First; pfld != null; pfld = (JProperty)pfld.Next)
            {
                string fldname = pfld.Name;
                string fldvalue = pfld.Value.ToString();
                SlaveFieldListNV.Add(fldname, fldvalue);
            }
            for (int t = 0; t < arSlaveTables.Length; t++)
            {
                string SlaveTableName = arSlaveTables[t];
                string nodbSlaveTableName = (SlaveTableName.IndexOf(".") > 0 ? SlaveTableName.Substring(SlaveTableName.LastIndexOf(".") + 1) : SlaveTableName);
                string tblSlaveTableName = SlaveTableName;
                string SlaveFieldList = SlaveFieldListNV[SlaveTableName];
                SQLJob.SQLDataObjectType SlaveTableType = getJob.GetSQLDataObjectType("V_" + nodbSlaveTableName);
                if (SlaveTableType == SQLJob.SQLDataObjectType.View) SlaveTableName = "V_" + nodbSlaveTableName;

                //获取子表数据
                DataTable childdt = null;
                //校验字段列表
                Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allSFields = getJob.GetTableFields(SlaveTableName, nodbSlaveTableName, SlaveTableType, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
                string[] arSlaveFields = SlaveFieldList.Split(new char[1] { ',' });
                Dictionary<string, SQLJob.FieldAttr> fldMaster = getJob.GetTableFields(tblSlaveTableName, nodbSlaveTableName, SQLJob.SQLDataObjectType.Table, SQLJob.FieldCatagory.MasterForeignFields, false, nodbMasterTableName, string.Empty);
                string MasterForeignKeyFilterStr = "";
                List<string> MasterKeyList = new List<string>();
                /*
                 *  foreach (KeyValuePair<string, SQLJob.FieldAttr> maskey in fldMaster)
                    {
                        string fldName = maskey.Key.IndexOf('.') > 0 ? maskey.Key.Substring(maskey.Key.IndexOf('.') + 1) : maskey.Key;
                        MasterKeyList.Add(fldName);
                        SQLJob.FieldAttr fatt = maskey.Value;
                        string foreignkey = fatt.MasterForeignKeys;
                        string[] arfkey = foreignkey.Split(new char[1] { ',' });
                        foreignkey = "";
                        for (int f = 0; f < arfkey.Length; f++)
                        {
                            string tbl = (arfkey[f].IndexOf(".") > 0 ? arfkey[f].Substring(0, arfkey[f].IndexOf(".")) : arfkey[f]).Trim();
                            string fld = (arfkey[f].IndexOf(".") > 0 ? arfkey[f].Substring(arfkey[f].IndexOf(".") + 1) : "").Trim();
                            if (tbl.ToLower() == nodbMasterTableName.ToLower())
                            {
                                foreignkey = fld;
                                break;
                            }
                        }
                        if (foreignkey != "")
                        {
                            MasterForeignKeyFilterStr += (MasterForeignKeyFilterStr == string.Empty ? "" : " AND ") + fldName + "='" + dt.Rows[0][foreignkey].ToString() + "'";
                        }
                        //补全当前table的外键字段
                        if (!arSlaveFields.Contains(fldName)) arSlaveFields = arSlaveFields.Concat(new string[1] { fldName }).ToArray();
                    }
                 * */
                foreach (KeyValuePair<string, SQLJob.FieldAttr> maskey in fldMaster)
                {
                    string fldName = maskey.Key.IndexOf('.') > 0 ? maskey.Key.Substring(maskey.Key.IndexOf('.') + 1) : maskey.Key;
                    MasterKeyList.Add(fldName);
                    SQLJob.FieldAttr fatt = maskey.Value;
                    string foreignkey = fatt.MasterForeignKeys;
                    string[] arfkey = foreignkey.Split(new char[1] { ',' });
                    foreignkey = "";
                    for (int f = 0; f < arfkey.Length; f++)
                    {
                        string tbl = (arfkey[f].IndexOf(".") > 0 ? arfkey[f].Substring(0, arfkey[f].IndexOf(".")) : arfkey[f]).Trim();
                        string fld = (arfkey[f].IndexOf(".") > 0 ? arfkey[f].Substring(arfkey[f].IndexOf(".") + 1) : "").Trim();
                        if (tbl.ToLower() == nodbMasterTableName.ToLower())
                        {
                            foreignkey = fld;
                            break;
                        }
                    }
                    if (foreignkey != "")
                    {
                        MasterForeignKeyFilterStr += (MasterForeignKeyFilterStr == string.Empty ? "" : " AND ") + fldName + "='" + dt.Rows[0][foreignkey].ToString() + "'";
                    }
                    //补全当前table的外键字段
                    if (!arSlaveFields.Contains(fldName)) arSlaveFields = arSlaveFields.Concat(new string[1] { fldName }).ToArray();
                }
                SlaveFieldList = "";
                foreach (string c in arSlaveFields)
                {
                    var s = from k in allSFields where (k.Key.IndexOf('.') > 0 ? k.Key.Substring(k.Key.IndexOf('.') + 1) : k.Key) == c select k;
                    if (s.Count() > 0) SlaveFieldList += (SlaveFieldList == string.Empty ? "" : ",") + c;
                }
                Script = "SELECT " + SlaveFieldList + " FROM " + SlaveTableName + " WHERE " + MasterForeignKeyFilterStr;
                childdt = getJob.QueryDataTable_Job(Script, Filter);
                childdt.TableName = SlaveTableName;

                ds.Tables.Add(childdt.Copy());
            }
            return ds;
        }
        protected string SaveVoucher(DataHandler.BusDataClass.JobDataFilter Filter, string JobName)
        {

            string rst = "0";


            JobDataFilter saveJobFilter = Filter;
            string fldlist = string.Empty;
            SQLJob otsaveJob = new SQLJob();
            switch (JobName.ToLower())
            {
                case "tablesdata":
                    StringBuilder savescript = new StringBuilder();
                    string Jsntables = Filter.FilterParameters["tabledata"].ToString();
                    //反序列化
                    JArray datatbls = (JArray)JsonConvert.DeserializeObject(HttpUtility.UrlDecode(Jsntables));
                    Dictionary<string, SQLJob.TableSchema> TableSchema = new System.Collections.Generic.Dictionary<string, SQLJob.TableSchema>();
                    for (int i = 0; i < datatbls.Count; i++)
                    {
                        JObject tblbag = (JObject)datatbls[i];

                        //tblbag: { tablename: '', fieldlayout: '', parenttable: '', foreignprimaryfield: {}, data: {} };
                        string tblname = string.Empty;
                        string fieldlayout = string.Empty;
                        string parenttable = string.Empty;
                        int currentrowindex = 0;
                        JObject foreignprimaryfield = null;
                        string primaryfieldvalues = string.Empty;
                        JArray arnosavedfields = null;
                        string nosavedfields = string.Empty;
                        JArray tbldata = null;
                        JobDataFilter keyfields = saveJobFilter;
                        string keylist = string.Empty;
                        string masterkeys = string.Empty;
                        bool isparameters = false;
                        bool includedeletion = false;
                        string JsnGrid = string.Empty;
                        foreach (KeyValuePair<string, Newtonsoft.Json.Linq.JToken> k in tblbag)
                        {
                            switch (k.Key.ToLower())
                            {
                                case "tablename":
                                    tblname = (k.Value == null ? "" : k.Value.ToString());
                                    break;
                                case "fieldlayout":
                                    fieldlayout = (k.Value == null ? "" : k.Value.ToString());
                                    break;
                                case "parenttable":  //关联的上级主表
                                    parenttable = (k.Value == null ? "" : k.Value.ToString());
                                    break;
                                case "isparameters":  //本表是否作为参数表？
                                    isparameters = (k.Value != null && k.Value.ToString().ToLower() == "true" ? true : false);
                                    break;
                                case "currentrowindex":  //本表的当前选定数据行，默认为1，第一行
                                    currentrowindex = (k.Value == null ? 1 : (int)k.Value);
                                    break;
                                case "foreignprimaryfield":  //用于外部输入的主表主键值，区别于事物内部生成的方式，适用于最外层业务对象; 目前已作废，整合到统一的Table行中，也就是说，若最外层对象提供主键值，那么也需要在页面上构造一个table元素封装的隐藏域对象区域，存储这些主键值
                                    foreignprimaryfield = (JObject)k.Value;
                                    foreach (KeyValuePair<string, Newtonsoft.Json.Linq.JToken> kfld in foreignprimaryfield)
                                    {
                                        masterkeys += (masterkeys == "" ? "" : ",") + kfld.Key;
                                        primaryfieldvalues += (primaryfieldvalues == "" ? "" : ",") + kfld.Key + ":" + kfld.Value.ToString();
                                        keyfields.FilterParameters.Add(kfld.Key, kfld.Value.ToString()); //将当前表的关联主表的外键值
                                    }
                                    break;
                                case "includedeletion": //是否按Filter参数表检查并执行数据删除？默认值false，table中不定义也没事，后面会根据主表定义和主表关联主键取值情况做自动化调整
                                    includedeletion = (k.Value != null && k.Value.ToString().ToLower() == "true" ? true : false);
                                    break;
                                case "nosavedfields":
                                    arnosavedfields = (JArray)k.Value;
                                    for (int j = 0; j < arnosavedfields.Count; j++)
                                    {
                                        string fn = (string)arnosavedfields[j];
                                        nosavedfields += (nosavedfields == "" ? "" : ",") + fn;
                                    }
                                    break;
                                case "data":
                                    JsnGrid = k.Value.ToString();
                                    break;
                                default:

                                    break;
                            }
                        }
                        string sqlstring = string.Empty;
                        //isparameters = true;
                        Dictionary<string, SQLJob.TableSchema> tblResult;
                        if (isparameters)
                        {
                            tblResult = PubFunctions.GetSaveJsonDataScript(TableSchema, JsnGrid, tblname, nosavedfields, includedeletion, currentrowindex, parenttable, PubFunctions.DMLType.Query, keyfields);
                            TableSchema = tblResult;
                        }
                        else
                        {
                            //调整includedeletion的取值：当本表定义了其关联主表主键的外键并对这些外键赋值时，自动将includedeletion调整为true
                            if (parenttable != null && parenttable != "" && TableSchema.ContainsKey(parenttable))
                            {
                                SQLJob.TableSchema tblschm = TableSchema[parenttable];
                                if (tblschm.CurrentRowIndex > 0 && tblschm.GridData.Rows.Count >= tblschm.CurrentRowIndex || tblschm.CurrentRowKeyValues != "")
                                {
                                    includedeletion = true;
                                }
                                else
                                {
                                    //若定义了主表，且主表在上下文环境中有多行，但却无法确定外键赋值时，抛出异常！
                                    throw (new Exception("本表的外键关联主表已被定义，这表明本表数据的DML操作将受到主表的外键值的合法限制，但目前无法确定与本表有外键关联的特定主表数据行！请检查是否指定了与主表数据行有关的参数设置？"
                                        + char.ConvertFromUtf32(13) + "例如：是否在呈现主表的列表类型控件中选中了主表关联数据行？或者在主表关联的散列字段控件元素中指定了主表数据行的主键值？"));
                                }
                            }
                            else
                            {
                                //没有定义主表并给关联外键赋值时，强制设置为false
                                if (includedeletion) includedeletion = false;
                            }
                            tblResult = PubFunctions.GetSaveJsonDataScript(TableSchema, JsnGrid, tblname, nosavedfields, includedeletion, currentrowindex, parenttable, PubFunctions.DMLType.MixedDML, keyfields);
                            TableSchema = tblResult;
                        }
                        sqlstring = char.ConvertFromUtf32(13) + tblResult[tblname].GridSaveScript;
                        sqlstring = otsaveJob.GetScript_ExecuteScript(sqlstring, keyfields);
                        savescript.Append(sqlstring);
                    }
                    string sqlscript = "DECLARE @Returns TABLE([Index] [int] IDENTITY(1,1) NOT NULL,TableName NVARCHAR(100) PRIMARY KEY, InsPrimaryKeyValues NVARCHAR(MAX), SelectedRowPMKeyValue NVARCHAR(2000))";
                    sqlscript += char.ConvertFromUtf32(13) + savescript.ToString();
                    sqlscript += char.ConvertFromUtf32(13) + "SELECT * FROM @Returns order by [Index]";
                    string transctionScript = PubFunctions.CreateTransactionScript(sqlscript);
                    //保存数据，并返回Insert的主键值
                    DataTable dt = otsaveJob.QueryDataTable_Job(transctionScript, saveJobFilter);


                    rst = (dt != null ? PubFunctions.GetSerializedJson_Table(dt) : "0|");

                    break;
                default:
                    break;
            }

            return rst;
        }
        protected string CreateBusData(DataHandler.BusDataClass.JobDataFilter Filter, string JobName)
        {
            string rtnJsn = string.Empty;
            switch (JobName.ToLower())
            {
                case "vouchercode":
                    rtnJsn = CreateVoucherCode(Filter);
                    break;
            }

            return rtnJsn;
        }
        protected string DeleteVoucher(DataHandler.BusDataClass.JobDataFilter Filter, string JobName)
        {
            if (JobName == "MF")
            {
                return DeleteVoucher(Filter);
            }
            string rtnJsn = string.Empty;

            string strSql = "";

            //获取参数
            string cType = Filter.FilterParameters.ContainsKey("cType") ? Filter.FilterParameters["cType"].ToString() : "";
            if (cType == "CSH") return "";
            string cCode, cVouchType;
            cVouchType = Filter.FilterParameters.ContainsKey("cVouchType") ? Filter.FilterParameters["cVouchType"].ToString() : "";
            cCode = Filter.FilterParameters.ContainsKey("cCode") ? Filter.FilterParameters["cCode"].ToString() : "";
            if (cCode == "")
            {
                rtnJsn = "删除参数不正确！";
                return rtnJsn;
            }
            int ID = 0;
            switch (cVouchType)
            {
                case "PU":
                    strSql = "Select Min(ID) From MES_PU_RdRecord where cCode = '" + cCode + "'";
                    break;
                case "MO":
                    strSql = "Select Min(ID) From MES_MO_RdRecord where cCode = '" + cCode + "'";
                    break;
                case "ST":
                    strSql = "Select Min(ID) From MES_ST_RdRecord where cCode = '" + cCode + "'";
                    break;
                case "SADL":
                    strSql = "Select Min(DLID) From MES_SA_DispatchList where cDLCode = '" + cCode + "'";
                    break;
                case "SARD":
                    strSql = "Select Min(ID) From MES_SA_RdRecord where cCode = '" + cCode + "'";
                    break;
                case "AT":
                    //调拨申请单 
                    strSql = "Select Min(ID) From MES_ST_AppTransVouch where cTVCode = '" + cCode + "'";
                    break;
                case "TV":
                    //调拨单 
                    strSql = "Select Min(ID) From MES_TransVouch where cTVCode = '" + cCode + "'";
                    break;
                case "AV":
                    //形态转换单 
                    strSql = "Select Min(ID) From MES_AssemVouch where cAVCode = '" + cCode + "'";
                    break;
                case "AP":
                    //货位调整单 
                    strSql = "Select Min(ID) From MES_AdjustPVouch where cVouchCode = '" + cCode + "'";
                    break;
                case "CV":
                    //盘点单 
                    strSql = "Select Min(ID) From MES_CheckVouch where cCVCode = '" + cCode + "'";
                    break;
                case "08":
                    //其他入库单 
                    strSql = "Select Min(ID) From MES_RdRecord08 where cCode = '" + cCode + "'";
                    break;
                case "09":
                    //其他入库单 
                    strSql = "Select Min(ID) From MES_RdRecord09 where cCode = '" + cCode + "'";
                    break;
            }

            try
            {
                ID = int.Parse(((DataRow)DBHelper.SQLHelper.GetSingle(strSql))[0].ToString());
            }
            catch
            {
                ID = 0;
            }
            if (ID == 0)
            {
                rtnJsn = "待删除单据号不正确！";
                return rtnJsn;
            }
            strSql = " exec usp_MES_Delete @cVouchType='" + cVouchType + "',@cCode='" + cCode + "',@cMaker='" + Module.GetLoginUser() + "'";
            string cError = "";
            try
            {
                DBHelper.SQLHelper.Query(strSql);
            }
            catch (Exception ex)
            {
                cError = ex.Message;
            }
            if (cError == "")
            {
                rtnJsn = "删除成功";
            }
            else
            {
                rtnJsn = cError;//返回给前台页面  
            }
            return rtnJsn;
        }
        protected string DeleteVoucher(DataHandler.BusDataClass.JobDataFilter Filter)
        {
            string rtnJsn = string.Empty;

            string strSql = "";

            //获取参数
            string cType = Filter.FilterParameters.ContainsKey("cType") ? Filter.FilterParameters["cType"].ToString() : "";
            if (cType == "CSH") return "";
            string cCode, cVouchType;
            cVouchType = Filter.FilterParameters.ContainsKey("cVouchType") ? Filter.FilterParameters["cVouchType"].ToString() : "";
            cCode = Filter.FilterParameters.ContainsKey("cCode") ? Filter.FilterParameters["cCode"].ToString() : "";
            if (cCode == "")
            {
                rtnJsn = "删除参数不正确！";
                return rtnJsn;
            }

            strSql = " exec usp_MES_DeleteMF @cVouchType='" + cVouchType + "',@cCode='" + cCode + "',@cMaker='" + Module.GetLoginUser() + "'";
            string cError = "";
            try
            {
                DBHelper.SQLHelper.Query(strSql);
            }
            catch (Exception ex)
            {
                cError = ex.Message;
            }
            if (cError == "")
            {
                rtnJsn = "删除成功";
            }
            else
            {
                rtnJsn = cError;//返回给前台页面  
            }
            return rtnJsn;
        }
        protected string CreateVoucherCode(DataHandler.BusDataClass.JobDataFilter Filter)
        {
            string cCode = string.Empty;

            string strSql = "";
            string cVouchType = Filter.FilterParameters.ContainsKey("cVouchType") ? Filter.FilterParameters["cVouchType"].ToString() : "";
            if (cVouchType != null && cVouchType != "")
            {
                strSql = "usp_GetNextCode";
                System.Data.SqlClient.SqlParameter pa = new System.Data.SqlClient.SqlParameter("@cVCCode", cVouchType);
                IDataParameter[] pas = new IDataParameter[1] { pa };
                cCode = ((System.Data.IDataReader)DBHelper.SQLHelper.RunProcedure(strSql, pas))[0].ToString();
            }

            return cCode;
        }
        #endregion


        #region 参照处理

        protected string SelectByRefrence(DataHandler.BusDataClass.JobDataFilter Filter, string JobName)
        {
            string strJson = string.Empty;

            JobDataFilter getFilter = new JobDataFilter();
            SQLJob getJob = new SQLJob();
            string script = string.Empty;
            DataSet ds;
            DataTable dt;

            string cType = Filter.FilterParameters["ctype"].ToString();
            string cOTCode = Filter.FilterParameters["cotcode"].ToString();
            string selKeyField = Filter.FilterParameters["selkeyfield"].ToString();
            string selValues = Filter.FilterParameters["selvalues"].ToString();
            //string addParm = Filter.FilterParameters["addParm"].ToString();
            bool CrossDB = Filter.FilterParameters.ContainsKey("CrossDB") ? (Filter.FilterParameters["CrossDB"].ToString().ToLower() == "true" || Filter.FilterParameters["CrossDB"].ToString() == "1" ? true : false) : true;
            if (cType == "CSH") return string.Empty;

            switch (JobName)
            {
                case "Delivery":  ////出库单参照

                    break;
            }
            dt = getJob.QueryDataTable_Job(script, getFilter);
            strJson = (dt != null ? PubFunctions.GetSerializedeasyDataGrid_Table(dt) : "");
            strJson = PubFunctions.GetEasyuiTableJson(strJson);
            return strJson;

        }

        #endregion

        #region 从视图中按条件取数据
        protected string GetDataFromView(DataHandler.BusDataClass.JobDataFilter Filter, string JobName)
        {
            string strJson = string.Empty;

            JobDataFilter getFilter = new JobDataFilter();
            SQLJob getJob = new SQLJob();
            string script = string.Empty;
            DataSet ds;
            DataTable dt;

            string cType = Filter.FilterParameters["ctype"].ToString();
            string cOTCode = Filter.FilterParameters["cotcode"].ToString();
            string selKeyField = Filter.FilterParameters["selkeyfield"].ToString();
            string selValues = Filter.FilterParameters["selvalues"].ToString();
            //string addParm = Filter.FilterParameters["addParm"].ToString();
            if (cType == "CSH") return string.Empty;
            //JobName --视图名称
            //cOTCode --过滤条件
            if (JobName == "" || cOTCode == "") return string.Empty;

            script = " Select * From " + JobName + " where " + cOTCode;

            dt = getJob.QueryDataTable_Job(script, getFilter);
            strJson = (dt != null ? PubFunctions.GetSerializedeasyDataGrid_Table(dt) : "");
            strJson = PubFunctions.GetEasyuiTableJson(strJson);
            return strJson;

        }
        #endregion

    }
}
