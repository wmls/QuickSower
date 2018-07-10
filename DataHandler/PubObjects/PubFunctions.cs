using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Web;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
//using System.Web.SessionState;
using DataHandler.BusJob;
using DataHandler.BusDataClass;

namespace DataHandler.PubObjects
{
    public class PubFunctions //: IHttpHandler, IRequiresSessionState
    {
        #region Start 页面参数接收与转化
        //接收url页面参数
        static public DataHandler.BusDataClass.JobDataFilter GetPageParameters(System.Web.HttpContext context)
        {
            DataHandler.BusDataClass.JobDataFilter datFilter = new DataHandler.BusDataClass.JobDataFilter();

            datFilter = (DataHandler.BusDataClass.JobDataFilter)RequestFillJobCondition(context.Request.Form, datFilter);
            datFilter = (DataHandler.BusDataClass.JobDataFilter)RequestFillJobCondition(context.Request.QueryString, datFilter);

            return datFilter;
        }

        //将参数填充到实体类中
        static public object RequestFillJobCondition(NameValueCollection Request, DataHandler.BusDataClass.JobDataFilter filter)
        {

            if (Request != null && Request.AllKeys.Length > 0)
            {
                int i = 0;
                foreach (object para in Request)
                {
                    if (para == null)
                    {  //json对象
                        //反序列化
                        string oval = HttpUtility.UrlDecode(Request[i]);
                        if (!oval.Contains('{') && !oval.Contains('}') && !oval.Contains(':'))
                        {
                            filter.FilterParameters.Add(Request[i], null);
                        }
                        else
                        {
                            object pathvalue = JsonConvert.DeserializeObject(HttpUtility.UrlDecode(Request[i]));
                            JObject databag = (JObject)pathvalue;

                            foreach (KeyValuePair<string, Newtonsoft.Json.Linq.JToken> k in databag)
                            {
                                string sValue = k.Value.ToString().Replace("'", "''");

                                if (!filter.FilterParameters.ContainsKey(k.Key))
                                {
                                    filter.FilterParameters.Add(k.Key, k.Value.ToString());
                                }

                            }
                        }

                    }
                    else
                    {
                        if (!filter.FilterParameters.ContainsKey(para.ToString()))
                        {
                            filter.FilterParameters.Add(para.ToString(), Request[para.ToString()]);
                        }
                    }

                    i++;

                }
            }
            return filter;

        }


        static public NameValueCollection TransJsontoNameValueCollection(string jsn)
        {
            NameValueCollection nvs = new NameValueCollection();

            //反序列化
            JObject databag = (JObject)JsonConvert.DeserializeObject(HttpUtility.UrlDecode(jsn));

            foreach (KeyValuePair<string, Newtonsoft.Json.Linq.JToken> k in databag)
            {
                string sValue = k.Value.ToString().Replace("'", "''");

                nvs.Add(k.Key, k.Value.ToString());

            }

            return nvs;
        }

        static public string TransJsontoTableString(string jsn)
        {
            //反序列化
            JArray dataArray = (JArray)JsonConvert.DeserializeObject(HttpUtility.UrlDecode(jsn));

            string fldlist = "";
            string valueslist = "";
            bool isgetflds = false;
            foreach (JObject item in dataArray)
            {
                string values = "";
                foreach (KeyValuePair<string, Newtonsoft.Json.Linq.JToken> k in item)
                {
                    if (!isgetflds)
                    {
                        fldlist += (fldlist == "" ? "" : ",") + k.Key;
                    }
                    values += (values == "" ? "" : ":") + k.Value;
                }
                isgetflds = true;
                valueslist += (valueslist == "" ? "" : ",") + values;

            }

            return fldlist + "|" + valueslist;
        }

        public enum JsonSection{
            rows = 0,
            fields = 1,
            total = 2
        }

        static public DataTable TransJsontoDatatable(string jsn)
        {

            //反序列化
            JObject databag = (JObject)JsonConvert.DeserializeObject(HttpUtility.UrlDecode(jsn));

            System.Data.DataTable dt = new System.Data.DataTable();
            foreach (KeyValuePair<string, Newtonsoft.Json.Linq.JToken> k in databag)
            {
                string sValue = k.Value.ToString().Replace("'", "''");

                switch (k.Key.ToLower())
                {
                    case "fields":

                        JArray fldlist = k.Value as JArray;
                        for (int n = 0; n < fldlist.Count; n++)
                        {
                            System.Data.DataColumn dc = new System.Data.DataColumn(fldlist[n].ToString(), Type.GetType("System.String"));
                            dt.Columns.Add(dc);
                        }
                        dt.AcceptChanges();

                        break;
                    case "rows":

                        JArray rowlist = k.Value as JArray;
                        for (int r = 0; r < rowlist.Count; r++)
                        {
                            JArray row = rowlist[r] as JArray;
                            System.Data.DataRow dr = dt.NewRow();
                            dt.Rows.Add(dr);
                            int c = 0;
                            foreach (System.Data.DataColumn dc in dt.Columns)
                            {
                                dr[dc.ColumnName] = (row[c].ToString().ToLower() == "true" ? 1 : (row[c].ToString().ToLower() == "false" ? 0 : row[c]));
                                c++;
                            }
                            dr.AcceptChanges();
                        }
                        dt.AcceptChanges();
                        break;
                    default:

                        break;
                }

            }

            return dt;
        }

        static public object TransJsontoDatatable(string jsn, JsonSection section)
        {

            //反序列化
            JObject databag = (JObject)JsonConvert.DeserializeObject(HttpUtility.UrlDecode(jsn));

            System.Data.DataTable dt = new System.Data.DataTable();
            foreach (KeyValuePair<string, Newtonsoft.Json.Linq.JToken> k in databag)
            {
                string sValue = k.Value.ToString().Replace("'", "''");
                switch (section)
                {
                    case JsonSection.total:
                        return k.Value.ToString();
                    case JsonSection.fields:
                        return k.Value.ToString();
                    default:
                        switch (k.Key.ToLower())
                        {
                            case "fields":

                                JArray fldlist = k.Value as JArray;
                                for (int n = 0; n < fldlist.Count; n++)
                                {
                                    System.Data.DataColumn dc = new System.Data.DataColumn(fldlist[n].ToString(), Type.GetType("System.String"));
                                    dt.Columns.Add(dc);
                                }
                                dt.AcceptChanges();

                                break;
                            case "rows":

                                JArray rowlist = k.Value as JArray;
                                for (int r = 0; r < rowlist.Count; r++)
                                {
                                    JArray row = rowlist[r] as JArray;
                                    System.Data.DataRow dr = dt.NewRow();
                                    dt.Rows.Add(dr);
                                    int c = 0;
                                    foreach (System.Data.DataColumn dc in dt.Columns)
                                    {
                                        dr[dc.ColumnName] = (row[c].ToString().ToLower() == "true" ? 1 : (row[c].ToString().ToLower() == "false" ? 0 : row[c]));
                                        c++;
                                    }
                                    dr.AcceptChanges();
                                }
                                dt.AcceptChanges();
                                break;
                        }
                        return dt;
                }
            }

            return null;
        }

        #endregion End 页面参数接收与转化

        //将一段SQL Script包装成事务Script
        static public string CreateTransactionScript(string Script)
        {

            string TransactionScript = "DECLARE @ERRMSG NVARCHAR(MAX) ; " + char.ConvertFromUtf32(13)
                + "BEGIN TRANSACTION ; " + char.ConvertFromUtf32(13)
                + "BEGIN TRY" + char.ConvertFromUtf32(13);
            TransactionScript += Script + char.ConvertFromUtf32(13);
            TransactionScript += "    COMMIT TRANSACTION" + char.ConvertFromUtf32(13);
            TransactionScript += "END TRY" + char.ConvertFromUtf32(13);
            TransactionScript += "BEGIN CATCH" + char.ConvertFromUtf32(13);
            TransactionScript += "    ROLLBACK TRANSACTION" + char.ConvertFromUtf32(13);
            TransactionScript += "    SET @ERRMSG = ERROR_MESSAGE()" + char.ConvertFromUtf32(13);
            TransactionScript += "    SELECT @ERRMSG as ErrMsg, 0 as result" + char.ConvertFromUtf32(13);
            TransactionScript += "END CATCH" + char.ConvertFromUtf32(13);

            return TransactionScript;
        }

        #region DataTable对象转化为两种格式的Grid的Data Json
        public enum JasonGridType
        {
            QinGrid = 0,
            EasyUIGrid =1
        }

        static public string GetSerializedJson_DataSet(DataSet ds, JasonGridType jgridtype)
        {
            string strjsn = "{";

            int i = 0;
            foreach (DataTable dt in ds.Tables)
            {
                i++;
                string dtjsn = "";
                dtjsn += "'" + (dt.TableName == "" ? "table" + i.ToString() : dt.TableName) + "'";
                dtjsn += ": " + (jgridtype == JasonGridType.QinGrid ? GetSerializedJson_Table(dt) : GetSerializedeasyDataGrid_Table(dt));
                //dtjsn += "";

                strjsn += (strjsn == "{" ? "" : ",") + dtjsn;
            }
            strjsn += "}";

            return strjsn;
        }

        static public string GetSerializedJson_Table(DataTable dt)
        {
            string strjsn = string.Empty;

            if (dt != null && dt.Rows.Count == 1 && dt.Columns.Contains("result") && dt.Columns.Contains("ErrMsg") && dt.Rows[0]["result"].ToString() == "0")
            {
                strjsn = "{resultType:1,data:'" + dt.Rows[0]["ErrMsg"].ToString().Replace("'","&#39;").Replace("\"","&#34;").Replace("'","\'") + "'}";
            }
            else
            {
                strjsn = "{";
                strjsn += "total:'" + (dt == null ? 0 : dt.Rows.Count).ToString() + "',";
                string collist = string.Empty;
                if (dt != null)
                {
                    foreach (DataColumn dc in dt.Columns)
                    {
                        collist += (collist == string.Empty ? "" : ",") + dc.ColumnName;
                    }
                }
                strjsn += "fields:'" + collist + "',";
                string rowvalues = "[";
                if (dt != null)
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        string rowval = "[";
                        foreach (DataColumn dc in dt.Columns)
                        {
                            //rowval += (rowval == "{" ? "" : ",") + dc.ColumnName + ":" + (dr[dc.ColumnName] == DBNull.Value ? "''" : "'" + dr[dc.ColumnName].ToString() + "'");
                            rowval += (rowval == "[" ? "" : ",") + (dr[dc.ColumnName] == DBNull.Value ? "''" : "'" + dr[dc.ColumnName].ToString().Replace("'", "&#39;").Replace(",", "&#44;") + "'");
                        }
                        rowval += "]";
                        rowvalues += (rowvalues == "[" ? "" : ",") + rowval;
                    }
                }
                rowvalues += "]";
                strjsn += "rows:" + rowvalues;
                strjsn += "}";
                strjsn = "{resultType:0,data:" + strjsn + "}";
            }
            return strjsn;

        }

        static public string GetSerializedeasyDataGrid_Table(DataTable dt)
        {
            string strjsn = string.Empty;

            if (dt != null && dt.Rows.Count == 1 && dt.Columns.Contains("result") && dt.Columns.Contains("ErrMsg") && dt.Rows[0]["result"].ToString() == "0")
            {
                strjsn = "{resultType:1,data:'" + dt.Rows[0]["ErrMsg"].ToString().Replace("'","&#39;").Replace("\"","&#34;") + "'}";
            }
            else
            {
                strjsn = "{";
                //string collist = string.Empty;
                //if (dt != null)
                //{
                //    foreach (DataColumn dc in dt.Columns)
                //    {
                //        collist += (collist == string.Empty ? "" : ",") + dc.ColumnName;
                //    }
                //}
                //strjsn += "fields:'" + collist + "',";
                string rowvalues = "[";
                if (dt != null)
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        string rowval = "{";
                        foreach (DataColumn dc in dt.Columns)
                        {
                            //rowval += (rowval == "{" ? "" : ",") + dc.ColumnName + ":" + (dr[dc.ColumnName] == DBNull.Value ? "''" : "'" + dr[dc.ColumnName].ToString() + "'");
                            rowval += (rowval == "{" ? "" : ",") + "\"" + dc.ColumnName + "\"" + ":" + (dr[dc.ColumnName] == DBNull.Value ? "''" : "'" + dr[dc.ColumnName].ToString().Replace("'", "&#39;").Replace(",", "&#44;") + "'");
                        }
                        rowval += "}";
                        rowvalues += (rowvalues == "[" ? "" : ",") + rowval;
                    }
                }
                rowvalues += "]";
                strjsn += "\"rows\":" + rowvalues + ",";
                strjsn += "\"total\":" + (dt == null ? 0 : dt.Rows.Count).ToString();
                strjsn += "}";
                strjsn = "{resultType:0,data:" + strjsn + "}";
            }
            return strjsn;

        }

        static public string GetEasyuiTableJson(string strJson)
        {
            string tbljson = strJson;
            try
            {
                string ctype = ((Newtonsoft.Json.Linq.JValue)((Newtonsoft.Json.Linq.JProperty)(((Newtonsoft.Json.Linq.JContainer)(JsonConvert.DeserializeObject(strJson))).First)).Name).Value.ToString();
                tbljson = (ctype.ToLower() == "resulttype" ?
                    ((Newtonsoft.Json.Linq.JProperty)(((Newtonsoft.Json.Linq.JContainer)(JsonConvert.DeserializeObject(strJson))).Last)).Value.ToString()
                    : strJson);
            }
            catch (Exception ex)
            {
            }
            return tbljson;
        }
        #endregion

            #region QinGrid Json的数据反序列化，已经转化为SQL DML脚本
        public enum DMLType
        {
            Query = 0,
            Insert = 1,
            Update = 2,
            MixedDML = 3,
            None = 9
        }
        //将Qin-datagrid数据保存到数据库(客户端需要将其它json格式的griddata转成QinGridData)
        static public DataTable SaveJsonGrid(string JsnGrid, string DataTableName, string MasterData_ForeignKeyFields, DMLType dmltype, BusDataClass.JobDataFilter filter)
        {
            if (MasterData_ForeignKeyFields == string.Empty)
            {
                throw new Exception("主表的外键关联字段没有定义!");
            }
            string[] arForeignKeys = MasterData_ForeignKeyFields.Split(new char[1] { ',' });
            string missingKeys = string.Empty;
            for (int i = 0; i < arForeignKeys.Length; i++)
            {
                if (!filter.FilterParameters.ContainsKey(MasterData_ForeignKeyFields))
                {
                    missingKeys += (missingKeys == string.Empty ? "" : ",") + arForeignKeys[i];
                }
            }
            if (missingKeys != string.Empty) throw new Exception("主表的外键关联字段（" + missingKeys + "）在参数表中没有提供！");

            DataTable griddata = TransJsontoDatatable(JsnGrid);
            string fldlist = TransJsontoDatatable(JsnGrid, JsonSection.fields).ToString();
            fldlist = fldlist.Replace("[", "").Replace("]", "").Replace("\r\n", "").Replace("\"", "").Replace(" ", "");
            string[] arflds = fldlist.Split(',');

            BusJob.SQLJob dmlJob = new BusJob.SQLJob();

            //将filter中的属于本表的字段扫描添加到griddata和fldlist
            BusDataClass.JobDataFilter updatebasefilter = new BusDataClass.JobDataFilter();
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allfields = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
            foreach (KeyValuePair<string,object> fld in filter.FilterParameters)
            {
                string fname = fld.Key;
                string fvalue = (fld.Value == null || fld.Value.ToString() == string.Empty ? null : "'" + fld.Value.ToString() + "'");
                if (allfields.ContainsKey(fname) && !griddata.Columns.Contains(fname))
                {
                    updatebasefilter.FilterParameters.Add(fname, fvalue);
                    //更新数据集
                    DataHandler.BusJob.SQLJob.FieldAttr fattr = allfields[fname];
                    string ftype = "System.Int32";
                    #region 转换字段的数据类型
                    switch (fattr.DataType)
                    {
                        case BusJob.SQLJob.DataType.DBBigint:
                            ftype = "System.Int64";
                            break;
                        case BusJob.SQLJob.DataType.DBBit:
                            ftype = "System.Boolean";
                            break;
                        case BusJob.SQLJob.DataType.DBDate:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetime2:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDecimal:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBFloat:
                            ftype = "System.Single";
                            break;
                        case BusJob.SQLJob.DataType.DBImage:
                            ftype = "System.Object";
                            break;
                        case BusJob.SQLJob.DataType.DBInt:
                            ftype = "System.Int32";
                            break;
                        case BusJob.SQLJob.DataType.DBMoney:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBNumeric:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBReal:
                            ftype = "System.Double";
                            break;
                        case BusJob.SQLJob.DataType.DBSmalldatetime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBSmallint:
                            ftype = "System.Int16";
                            break;
                        case BusJob.SQLJob.DataType.DBSmallmoney:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBSql_variant:
                            ftype = "System.Object";
                            break;
                        case BusJob.SQLJob.DataType.DBTime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBTimestamp:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetimeoffset:
                            ftype = "System.TimeSpan";
                            break;
                        case BusJob.SQLJob.DataType.DBTinyint:
                            ftype = "System.Int16";
                            break;
                        case BusJob.SQLJob.DataType.DBVarbinary:
                            ftype = "System.Object";
                            break;
                        default:
                            ftype = "System.String";
                            break;
                    }
                    #endregion


                    string fval = (fattr.DataType == SQLJob.DataType.DBChar || fattr.DataType == SQLJob.DataType.DBNchar || fattr.DataType == SQLJob.DataType.DBNtext || fattr.DataType == SQLJob.DataType.DBNvarchar || fattr.DataType == SQLJob.DataType.DBSysname || fattr.DataType == SQLJob.DataType.DBText || fattr.DataType == SQLJob.DataType.DBUniqueidentifier || fattr.DataType == SQLJob.DataType.DBVarchar || fattr.DataType == SQLJob.DataType.DBXml ?
                        "'" + fvalue + "'" : fvalue);
                    griddata.Columns.Add(new DataColumn(fname, Type.GetType(ftype), fvalue));

                    //更新字段列表字符串
                    if (!arflds.Contains(fname))
                    {
                        arflds = arflds.Concat(new string[1] { fname }).ToArray();
                        fldlist = string.Join(",", arflds);
                    }
                }
                else
                {
                    //更新记录数据集中filter参数字段的值（防止可能grid中没有包含此字段的情况，如新增的行）
                    if (griddata.Columns.Contains(fname))
                    {
                        foreach (DataRow dr in griddata.Rows)
                        {
                            dr[fname] = fvalue;
                        }
                        griddata.AcceptChanges();
                    }
                }
            }

            List<NameValueCollection> valueinsertrows;
            List<DataHandler.BusJob.SQLJob.UpdateDefine> updatedefines;
            //NameValueCollection[] valueupdaterows;
            valueinsertrows = new List<NameValueCollection>();// NameValueCollection[griddata.Rows.Count];
            updatedefines = new List<BusJob.SQLJob.UpdateDefine>();

            //获取当前table的行标识字段表
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> dickeyfields = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.PrimaryKeyandAutoIncrementFields);
            string dickeyfieldlist = string.Empty;
            foreach (KeyValuePair<string, BusJob.SQLJob.FieldAttr> fld in dickeyfields)
            {
                dickeyfieldlist += (dickeyfieldlist == "" ? "" : ",") + fld;
            }

            //构建主表外键关联字段的参数表
            NameValueCollection masterKeyParameters = new NameValueCollection();
            DataHandler.BusDataClass.JobDataFilter masterKeyValueFilter = new BusDataClass.JobDataFilter();
            DataRow dr0 = (griddata.Rows.Count > 0 ? griddata.Rows[0] : null);
            if (dr0 != null)
            {
                foreach (string s in arForeignKeys)
                {
                    masterKeyParameters.Add(s, (dr0[s] == DBNull.Value ? "" : dr0[s].ToString()));
                    masterKeyValueFilter.FilterParameters.Add(s, (dr0[s] == DBNull.Value ? "" : dr0[s].ToString()));
                }
            }
            //构建现存数据的主键表
            LinkedList<NameValueCollection> deleteKeys = new LinkedList<NameValueCollection>();  //要删除的行
            Dictionary<NameValueCollection, DataRow> currentKeys = new Dictionary<NameValueCollection, DataRow>();
            DataTable dtcurrent = (dr0 == null ? null : dmlJob.QueryDataTable_Job(DataTableName, MasterData_ForeignKeyFields + "," + dickeyfieldlist, masterKeyValueFilter));
            foreach (DataRow dr in dtcurrent.Rows)
            {
                NameValueCollection keyrow = new NameValueCollection();
                string keyvalues = string.Empty;
                foreach (DataColumn dc in dtcurrent.Columns)
                {
                    keyvalues += (keyvalues == string.Empty ? "" : ",") + (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString());
                    keyrow.Add(dc.ColumnName, (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString()));
                }
                currentKeys.Add(keyrow, dr);
                deleteKeys.AddLast(keyrow);
            }

            

            int r = 0;
            string appScript = string.Empty;
            foreach (DataRow dr in griddata.Rows)
            {
                NameValueCollection valrow = new NameValueCollection();
                BusDataClass.JobDataFilter updatefilter = new BusDataClass.JobDataFilter(updatebasefilter);
                
                for (int c = 0; c < arflds.Length; c++)
                {
                    string key = arflds[c].Replace("[", "").Replace("]", "");
                    string value = string.Empty;
                    value = (dr[key] == null || dr[key].ToString() == "" ? "" : dr[key].ToString());
                    valrow.Add(key, value);
                }
                if (dmltype == DMLType.Update)
                {
                    //判断当前行是否为新增行
                    bool isnew = true;
                    //把非空的主键列和Auto列作为Update条件添加到updatefilter中
                    foreach (KeyValuePair<string, BusJob.SQLJob.FieldAttr> fld in dickeyfields)
                    {
                        string fldname = fld.Key;
                        if (griddata.Columns.Contains(fldname) && dr[fldname] != DBNull.Value && dr[fldname].ToString() != "")
                        {
                            isnew = false;
                            if (!updatefilter.FilterParameters.ContainsKey(fldname))
                            {
                                updatefilter.FilterParameters.Add(fldname, dr[fldname].ToString());
                            }
                            else
                            {
                                updatefilter.FilterParameters[fldname] = dr[fldname].ToString();
                            }
                        }
                    }

                    if (isnew)
                    {
                        valueinsertrows.Add(valrow);
                    }
                    else
                    {
                        DataHandler.BusJob.SQLJob.UpdateDefine updDefine = new BusJob.SQLJob.UpdateDefine();
                        updDefine.FieldValues = valrow;
                        updDefine.Filter = updatefilter;
                        updatedefines.Add(updDefine);

                        //对有源记录生成行标识（统一按dtcurrent的列次序生成）
                        NameValueCollection keyrow = new NameValueCollection();
                        foreach (DataColumn dc in dtcurrent.Columns)
                        {
                            keyrow.Add(dc.ColumnName, (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString()));
                        }
                        if (deleteKeys.Contains(keyrow))
                        {
                            deleteKeys.Remove(keyrow);
                        }
                    }
                }
                else
                {
                    valueinsertrows.Add(valrow);
                }
                r++;
            }

            //保存Application的数据内容
            StringBuilder script = new StringBuilder();
            Dictionary<string, BusJob.SQLJob.FieldAttr> dicFld = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.PrimaryKeyandAutoIncrementFields);
            string sqlstring = string.Empty;
            switch (dmltype)
            {
                case DMLType.Insert:
                    sqlstring = dmlJob.GetScript_Add(DataTableName, fldlist, valueinsertrows.ToArray());
                    break;
                case DMLType.Update:
                    sqlstring = dmlJob.GetScript_Add(DataTableName, fldlist, valueinsertrows.ToArray());
                    sqlstring += char.ConvertFromUtf32(13) + dmlJob.GetScript_BatchUpdate(DataTableName, updatedefines.ToArray());
                    break;
            }
            string delsqlstring = dmlJob.GetScript_BatchDelete(DataTableName, deleteKeys.ToArray());
            sqlstring += char.ConvertFromUtf32(13) + delsqlstring;
            script.Append(sqlstring);
            string transctionScript = PubFunctions.CreateTransactionScript(script.ToString());
            DataTable dt = dmlJob.QueryDataTable_Job(transctionScript, filter);

            return dt;
        }

        //将Qin-datagrid数据保存到数据库(客户端需要将其它json格式的griddata转成QinGridData)
        static public string GetSaveJsonDataScript(string JsnGrid, string DataTableName, string MasterData_ForeignKeyFields, DMLType dmltype, BusDataClass.JobDataFilter filter)
        {
            if (MasterData_ForeignKeyFields == string.Empty)
            {
                throw new Exception("主表的外键关联字段没有定义!");
            }
            string[] arForeignKeys = MasterData_ForeignKeyFields.Split(new char[1] { ',' });
            string missingKeys = string.Empty;
            for (int i = 0; i < arForeignKeys.Length; i++)
            {
                if (!filter.FilterParameters.ContainsKey(MasterData_ForeignKeyFields))
                {
                    missingKeys += (missingKeys == string.Empty ? "" : ",") + arForeignKeys[i];
                }
            }
            if (missingKeys != string.Empty) throw new Exception("主表的外键关联字段（" + missingKeys + "）在参数表中没有提供！");

            DataTable griddata = TransJsontoDatatable(JsnGrid);
            string fldlist = TransJsontoDatatable(JsnGrid, JsonSection.fields).ToString();
            fldlist = fldlist.Replace("[", "").Replace("]", "").Replace("\r\n", "").Replace("\"", "").Replace(" ", "");
            string[] arflds = fldlist.Split(',');

            BusJob.SQLJob dmlJob = new BusJob.SQLJob();

            //将filter中的属于本表的字段扫描添加到griddata和fldlist
            BusDataClass.JobDataFilter updatebasefilter = new BusDataClass.JobDataFilter();
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allfields = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
            foreach (KeyValuePair<string, object> fld in filter.FilterParameters)
            {
                string fname = fld.Key;
                string fvalue = fld.Value == null || fld.Value == string.Empty ? null : "'" + fld.Value.ToString() + "'";
                if (allfields.ContainsKey(fname) && !griddata.Columns.Contains(fname))
                {
                    updatebasefilter.FilterParameters.Add(fname, fvalue);
                    //更新数据集
                    DataHandler.BusJob.SQLJob.FieldAttr fattr = allfields[fname];
                    string ftype = "System.Int32";
                    #region 转换字段的数据类型
                    switch (fattr.DataType)
                    {
                        case BusJob.SQLJob.DataType.DBBigint:
                            ftype = "System.Int64";
                            break;
                        case BusJob.SQLJob.DataType.DBBit:
                            ftype = "System.Boolean";
                            break;
                        case BusJob.SQLJob.DataType.DBDate:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetime2:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDecimal:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBFloat:
                            ftype = "System.Single";
                            break;
                        case BusJob.SQLJob.DataType.DBImage:
                            ftype = "System.Object";
                            break;
                        case BusJob.SQLJob.DataType.DBInt:
                            ftype = "System.Int32";
                            break;
                        case BusJob.SQLJob.DataType.DBMoney:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBNumeric:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBReal:
                            ftype = "System.Double";
                            break;
                        case BusJob.SQLJob.DataType.DBSmalldatetime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBSmallint:
                            ftype = "System.Int16";
                            break;
                        case BusJob.SQLJob.DataType.DBSmallmoney:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBSql_variant:
                            ftype = "System.Object";
                            break;
                        case BusJob.SQLJob.DataType.DBTime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBTimestamp:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetimeoffset:
                            ftype = "System.TimeSpan";
                            break;
                        case BusJob.SQLJob.DataType.DBTinyint:
                            ftype = "System.Int16";
                            break;
                        case BusJob.SQLJob.DataType.DBVarbinary:
                            ftype = "System.Object";
                            break;
                        default:
                            ftype = "System.String";
                            break;
                    }
                    #endregion

                    griddata.Columns.Add(new DataColumn(fname, Type.GetType(ftype), fvalue));

                    //更新字段列表字符串
                    if (!arflds.Contains(fname))
                    {
                        arflds = arflds.Concat(new string[1] { fname }).ToArray();
                        fldlist = string.Join(",", arflds);
                    }
                }
                else
                {
                    //更新记录数据集中filter参数字段的值（防止可能grid中没有包含此字段的情况，如新增的行）
                    if (griddata.Columns.Contains(fname))
                    {
                        foreach (DataRow dr in griddata.Rows)
                        {
                            dr[fname] = fvalue;
                        }
                        griddata.AcceptChanges();
                    }
                }
            }

            List<NameValueCollection> valueinsertrows;
            List<DataHandler.BusJob.SQLJob.UpdateDefine> updatedefines;
            //NameValueCollection[] valueupdaterows;
            valueinsertrows = new List<NameValueCollection>();// NameValueCollection[griddata.Rows.Count];
            updatedefines = new List<BusJob.SQLJob.UpdateDefine>();

            //获取当前table的行标识字段表
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> dickeyfields = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.PrimaryKeyandAutoIncrementFields);
            string dickeyfieldlist = string.Empty;
            foreach (KeyValuePair<string, BusJob.SQLJob.FieldAttr> fld in dickeyfields)
            {
                dickeyfieldlist += (dickeyfieldlist == "" ? "" : ",") + fld;
            }

            //构建主表外键关联字段的参数表
            NameValueCollection masterKeyParameters = new NameValueCollection();
            DataHandler.BusDataClass.JobDataFilter masterKeyValueFilter = new BusDataClass.JobDataFilter();
            DataRow dr0 = (griddata.Rows.Count > 0 ? griddata.Rows[0] : null);
            if (dr0 != null)
            {
                foreach (string s in arForeignKeys)
                {
                    masterKeyParameters.Add(s, (dr0[s] == DBNull.Value ? "" : dr0[s].ToString()));
                    masterKeyValueFilter.FilterParameters.Add(s, (dr0[s] == DBNull.Value ? "" : dr0[s].ToString()));
                }
            }
            //构建现存数据的主键表
            LinkedList<NameValueCollection> deleteKeys = new LinkedList<NameValueCollection>();  //要删除的行
            Dictionary<NameValueCollection, DataRow> currentKeys = new Dictionary<NameValueCollection, DataRow>();
            DataTable dtcurrent = (dr0 == null ? null : dmlJob.QueryDataTable_Job(DataTableName, MasterData_ForeignKeyFields + "," + dickeyfieldlist, masterKeyValueFilter));
            foreach (DataRow dr in dtcurrent.Rows)
            {
                NameValueCollection keyrow = new NameValueCollection();
                string keyvalues = string.Empty;
                foreach (DataColumn dc in dtcurrent.Columns)
                {
                    keyvalues += (keyvalues == string.Empty ? "" : ",") + (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString());
                    keyrow.Add(dc.ColumnName, (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString()));
                }
                currentKeys.Add(keyrow, dr);
                deleteKeys.AddLast(keyrow);
            }



            int r = 0;
            string appScript = string.Empty;
            foreach (DataRow dr in griddata.Rows)
            {
                NameValueCollection valrow = new NameValueCollection();
                BusDataClass.JobDataFilter updatefilter = new BusDataClass.JobDataFilter(updatebasefilter);

                for (int c = 0; c < arflds.Length; c++)
                {
                    string key = arflds[c].Replace("[", "").Replace("]", "");
                    string value = string.Empty;
                    value = (dr[key] == null || dr[key].ToString() == "" ? "" : dr[key].ToString());
                    valrow.Add(key, value);
                }
                if (dmltype == DMLType.Update)
                {
                    //判断当前行是否为新增行
                    bool isnew = true;
                    //把非空的主键列和Auto列作为Update条件添加到updatefilter中
                    foreach (KeyValuePair<string, BusJob.SQLJob.FieldAttr> fld in dickeyfields)
                    {
                        string fldname = fld.Key;
                        if (griddata.Columns.Contains(fldname) && dr[fldname] != DBNull.Value && dr[fldname].ToString() != "")
                        {
                            isnew = false;
                            if (!updatefilter.FilterParameters.ContainsKey(fldname))
                            {
                                updatefilter.FilterParameters.Add(fldname, dr[fldname].ToString());
                            }
                            else
                            {
                                updatefilter.FilterParameters[fldname] = dr[fldname].ToString();
                            }
                        }
                    }

                    if (isnew)
                    {
                        valueinsertrows.Add(valrow);
                    }
                    else
                    {
                        DataHandler.BusJob.SQLJob.UpdateDefine updDefine = new BusJob.SQLJob.UpdateDefine();
                        updDefine.FieldValues = valrow;
                        updDefine.Filter = updatefilter;
                        updatedefines.Add(updDefine);

                        //对有源记录生成行标识（统一按dtcurrent的列次序生成）
                        NameValueCollection keyrow = new NameValueCollection();
                        foreach (DataColumn dc in dtcurrent.Columns)
                        {
                            keyrow.Add(dc.ColumnName, (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString()));
                        }
                        if (deleteKeys.Contains(keyrow))
                        {
                            deleteKeys.Remove(keyrow);
                        }
                    }
                }
                else
                {
                    valueinsertrows.Add(valrow);
                }
                r++;
            }

            //保存Application的数据内容
            StringBuilder script = new StringBuilder();
            Dictionary<string, BusJob.SQLJob.FieldAttr> dicFld = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.PrimaryKeyandAutoIncrementFields);
            string sqlstring = string.Empty;
            switch (dmltype)
            {
                case DMLType.Insert:
                    sqlstring = dmlJob.GetScript_Add(DataTableName, fldlist, valueinsertrows.ToArray());
                    break;
                case DMLType.Update:
                    sqlstring = dmlJob.GetScript_Add(DataTableName, fldlist, valueinsertrows.ToArray());
                    sqlstring += char.ConvertFromUtf32(13) + dmlJob.GetScript_BatchUpdate(DataTableName, updatedefines.ToArray());
                    break;
            }
            string delsqlstring = dmlJob.GetScript_BatchDelete(DataTableName, deleteKeys.ToArray());
            sqlstring += char.ConvertFromUtf32(13) + delsqlstring;
            script.Append(sqlstring);

            return script.ToString();
        }
        //public Dictionary<string, TableSchema> DicDataSchema;
        static public Dictionary<string, SQLJob.TableSchema> GetSaveJsonDataScript(Dictionary<string, SQLJob.TableSchema> DicDataSchema, string JsnGrid, string DataTableName, string NoSavedFields, bool includedeletion, int SelectedRowIndex, string MasterForeignTable, DMLType dmltype, BusDataClass.JobDataFilter filter)
        {
            //IsInnerMasterForeignValue: 主表关联外键值是否事务内部赋值？
            SQLJob.TableSchema rst;
            rst.TableName = DataTableName;
            rst.CurrentRowIndex = SelectedRowIndex;

            DataRow MasterForeignRow = null;
            SQLJob.TableSchema MasterTblSchema = new SQLJob.TableSchema();
            bool masterforeigntableisexist = DicDataSchema.ContainsKey(MasterForeignTable);
            if (masterforeigntableisexist)
            {
                MasterTblSchema = DicDataSchema[MasterForeignTable];
                DataTable masterTbl = MasterTblSchema.GridData;
                if (MasterTblSchema.CurrentRowIndex > 0) MasterForeignRow = masterTbl.Rows[MasterTblSchema.CurrentRowIndex - 1];
            }
            if (DataTableName == string.Empty) throw new Exception("没有定义要保存的表名称，请在GetSaveJsonDataScript方法中定义非空的DataTableName参数！");

            DataTable griddata = TransJsontoDatatable(JsnGrid);
            string fldlist = TransJsontoDatatable(JsnGrid, JsonSection.fields).ToString();
            fldlist = fldlist.Replace("[", "").Replace("]", "").Replace("\r\n", "").Replace("\"", "").Replace(" ", "");
            string[] arflds = fldlist.Split(',');
            griddata.Columns.Add("RowIndex", Type.GetType("System.Int32"));
            griddata.Columns.Add("DMLType", Type.GetType("System.String"));

            //
            if (griddata.Rows.Count > 1 && (MasterForeignTable == null || MasterForeignTable == string.Empty))
            {
                throw new Exception("多行数据集必须定义关联外键主表，请在GetSaveJsonDataScript方法中定义非空的MasterForeignTable参数。");
            }

            //确定本表自增键、本表主键、主表外键
            string TableAutoIncreamKey = string.Empty;
            string TablePMKey = string.Empty;
            string TablePMKeyFields = string.Empty;  //TableAutoIncreamKey + TablePMKey
            string MasterData_ForeignPMKeyFields = string.Empty;
            string Table_ForeignKeyFields = string.Empty;
            BusJob.SQLJob dmlJob = new BusJob.SQLJob();
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> allfields = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.AllFields);
            foreach (KeyValuePair<string, BusJob.SQLJob.FieldAttr> fldAtt in allfields)
            {
                BusJob.SQLJob.FieldAttr fldA = fldAtt.Value;
                if (fldA.IsAutoIncrement) { TableAutoIncreamKey = fldAtt.Key; TablePMKeyFields += (TablePMKeyFields == string.Empty ? "" : ",") + fldAtt.Key; }
                if (fldA.IsPrimaryKey) { TablePMKey += (TablePMKey == string.Empty ? "" : ",") + fldAtt.Key; if (("," + TablePMKeyFields + ",").IndexOf("," + fldAtt.Key + ",") < 0) TablePMKeyFields += (TablePMKeyFields == string.Empty ? "" : ",") + fldAtt.Key; }
                string mkey = fldA.MasterForeignKeys;
                if (mkey != null && mkey != string.Empty && ("," + mkey).IndexOf("," + MasterForeignTable + ".") > -1)
                {
                    MasterData_ForeignPMKeyFields += (MasterData_ForeignPMKeyFields == string.Empty ? "" : ",") + mkey;
                    Table_ForeignKeyFields += (Table_ForeignKeyFields == string.Empty ? "" : ",") + fldAtt.Key;
                }
            }
            rst.ForeignKeyFields = TablePMKeyFields;
            string[] arForeignKeys = (Table_ForeignKeyFields == string.Empty ? new string[0] : Table_ForeignKeyFields.Split(new char[1] { ',' }));
            string[] arForeignPMKeys = (MasterData_ForeignPMKeyFields == string.Empty ? new string[0] : MasterData_ForeignPMKeyFields.Split(new char[1] { ',' }));
            for (int i = 0; i < arForeignPMKeys.Length; i++) { arForeignPMKeys[i] = arForeignPMKeys[i].Substring(arForeignPMKeys[i].IndexOf('.') + 1); }
            MasterData_ForeignPMKeyFields = string.Join(",", arForeignPMKeys);


            //优先看是否有外键值由外部输入【在中，一般最外层的外键值是由外部传入的】, 则判断是否传入参数
            string missingKeys = string.Empty;
            if (MasterForeignTable != null && MasterForeignRow == null)  //Link主表数据行为空表示外键值由外部参数传入
            {
                missingKeys = string.Empty;
                for (int i = 0; i < arForeignKeys.Length; i++)
                {
                    if (!filter.FilterParameters.ContainsKey(arForeignKeys[i]) && arForeignKeys[i] != string.Empty)
                    {
                        missingKeys += (missingKeys == string.Empty ? "" : ",") + arForeignKeys[i];
                    }
                }
                if (missingKeys != string.Empty) throw new Exception("关联主表【" + MasterForeignTable + "】的在本表【" + DataTableName + "】的外键字段（" + missingKeys + "）的值在参数表中没有提供！");
            }

            //补全：将filter中的属于本表的字段(包括可能存在的主键字段)扫描添加到griddata和fldlist
            BusDataClass.JobDataFilter updatebasefilter = new BusDataClass.JobDataFilter();
            foreach (KeyValuePair<string, object> fld in filter.FilterParameters)
            {
                string fname = fld.Key;
                string fvalue = fld.Value == null || fld.Value == string.Empty ? null : "'" + fld.Value.ToString() + "'";
                if (allfields.ContainsKey(fname) && !griddata.Columns.Contains(fname))
                {
                    updatebasefilter.FilterParameters.Add(fname, fvalue);
                    //更新数据集
                    DataHandler.BusJob.SQLJob.FieldAttr fattr = allfields[fname];
                    string ftype = "System.Int32";
                    #region 转换字段的数据类型
                    switch (fattr.DataType)
                    {
                        case BusJob.SQLJob.DataType.DBBigint:
                            ftype = "System.Int64";
                            break;
                        case BusJob.SQLJob.DataType.DBBit:
                            ftype = "System.Boolean";
                            break;
                        case BusJob.SQLJob.DataType.DBDate:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetime2:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDecimal:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBFloat:
                            ftype = "System.Single";
                            break;
                        case BusJob.SQLJob.DataType.DBImage:
                            ftype = "System.Object";
                            break;
                        case BusJob.SQLJob.DataType.DBInt:
                            ftype = "System.Int32";
                            break;
                        case BusJob.SQLJob.DataType.DBMoney:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBNumeric:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBReal:
                            ftype = "System.Double";
                            break;
                        case BusJob.SQLJob.DataType.DBSmalldatetime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBSmallint:
                            ftype = "System.Int16";
                            break;
                        case BusJob.SQLJob.DataType.DBSmallmoney:
                            ftype = "System.Decimal";
                            break;
                        case BusJob.SQLJob.DataType.DBSql_variant:
                            ftype = "System.Object";
                            break;
                        case BusJob.SQLJob.DataType.DBTime:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBTimestamp:
                            ftype = "System.DateTime";
                            break;
                        case BusJob.SQLJob.DataType.DBDatetimeoffset:
                            ftype = "System.TimeSpan";
                            break;
                        case BusJob.SQLJob.DataType.DBTinyint:
                            ftype = "System.Int16";
                            break;
                        case BusJob.SQLJob.DataType.DBVarbinary:
                            ftype = "System.Object";
                            break;
                        default:
                            ftype = "System.String";
                            break;
                    }
                    #endregion

                    griddata.Columns.Add(new DataColumn(fname, Type.GetType(ftype), fvalue));

                    //更新字段列表字符串
                    if (!arflds.Contains(fname))
                    {
                        arflds = arflds.Concat(new string[1] { fname }).ToArray();
                        fldlist = string.Join(",", arflds);
                    }
                }
                else
                {
                    //更新记录数据集中filter参数字段的非空值（防止可能grid中没有包含此字段的情况，如新增的行）
                    if (griddata.Columns.Contains(fname))
                    {
                        foreach (DataRow dr in griddata.Rows)
                        {
                            if (fvalue != string.Empty) dr[fname] = fvalue;
                        }
                        griddata.AcceptChanges();
                    }
                }
            }

            //判断主键是否在data中有定义
            missingKeys = string.Empty;
            bool PMKeyContainsAutoIncreamKey = false;
            string[] arTblKeys = TablePMKey.Split(new char[1] { ',' });
            for (int i = 0; i < arTblKeys.Length; i++)
            {
                PMKeyContainsAutoIncreamKey = (TableAutoIncreamKey.IndexOf(arTblKeys[i]) > -1 ? true : false);
                if (!griddata.Columns.Contains(arTblKeys[i]) && arTblKeys[i] != string.Empty) missingKeys += (missingKeys == string.Empty ? "" : ",") + arTblKeys[i];
            }
            if (missingKeys != string.Empty && dmltype != DMLType.Query) throw new Exception("本表【" + DataTableName + "】的" + (PMKeyContainsAutoIncreamKey ? "" : "非AutoIncream类型的") + "主键字段（" + missingKeys + "）在Grid的data对象中没有定义，这将导致数据无法保存。" + char.ConvertFromUtf32(13) + "可以在页面上与本表【" + DataTableName + "】对应的Table元素域中定义该主键的隐藏域来传送该主键值，或者通过Filter参数表来传送该主键值。");

            if (TablePMKey == string.Empty && TableAutoIncreamKey == string.Empty && dmltype != DMLType.Query) throw new Exception("本表【" + DataTableName + "】既没有定义AutoIncream类型的索引字段也没有定义任何其它主键字段，这将可能导致数据无法正确保存或定位。" + char.ConvertFromUtf32(13) + "请检查本表的设计是否完善？");

            //判断DML类型，同时创建当前行
            string SelectRowKeyValues = string.Empty;
            int ridx = 0;
            foreach (DataRow dr in griddata.Rows)
            {
                ridx++;
                dr["RowIndex"] = ridx;
                string dml = "Update";
                bool iscurrent = (ridx == SelectedRowIndex ? true : false);
                for (int i = 0; i < arTblKeys.Length; i++)
                {
                    if (arTblKeys[i] != string.Empty)
                    {
                        if (dml.ToLower() == "update" && (dr[arTblKeys[i]] == DBNull.Value || dr[arTblKeys[i]].ToString() == ""))
                        {
                            dml = "Insert";
                        }
                        if (iscurrent) SelectRowKeyValues += (SelectRowKeyValues == "" ? "" : ",") + arTblKeys[i] + ":" + dr[arTblKeys[i]].ToString();
                    }
                }
                dr["DMLType"] = dml;
                dr.AcceptChanges();
            }
            rst.GridData = griddata;
            rst.CurrentRowKeyValues = SelectRowKeyValues;  //若当前选择行是新建行，那么这个表达式包含的主键值是空的
            //至此已完成数据加工修饰

            //================================================================//
            List<NameValueCollection> valueinsertrows;
            List<DataHandler.BusJob.SQLJob.UpdateDefine> updatedefines;
            valueinsertrows = new List<NameValueCollection>();
            updatedefines = new List<BusJob.SQLJob.UpdateDefine>();

            //获取当前table的行标识字段表
            Dictionary<string, DataHandler.BusJob.SQLJob.FieldAttr> dickeyfields = dmlJob.GetTableFields(DataTableName, string.Empty, SQLJob.SQLDataObjectType.Table, DataHandler.BusJob.SQLJob.FieldCatagory.PrimaryKeyandAutoIncrementFields);
            string dickeyfieldlist = TablePMKeyFields;

            //构建主表外键关联字段的参数表（外部传入或内部生成）
            NameValueCollection masterKeyParameters = new NameValueCollection();
            DataHandler.BusDataClass.JobDataFilter masterKeyValueFilter = new BusDataClass.JobDataFilter();
            foreach (string s in arForeignKeys)
            {
                if (s != string.Empty)
                {
                    string mfk = allfields[s].MasterForeignKeys;
                    if (("," + mfk).IndexOf("," + MasterForeignTable + ".") > -1)
                    {
                        mfk = ("," + mfk).Substring(("," + mfk).IndexOf("," + MasterForeignTable + ".") + ("," + MasterForeignTable + ".").Length);
                        mfk = (mfk.IndexOf(',') > 0 ? mfk.Substring(0, mfk.IndexOf(',')) : mfk);
                        string cvalue = (MasterForeignRow[mfk] != DBNull.Value && MasterForeignRow[mfk].ToString() != "" ? (MasterTblSchema.CurrentRowIndex > 0 && MasterTblSchema.DataSchema[MasterTblSchema.CurrentRowIndex - 1].RowDMLType.ToLower() == "new" ? string.Empty : MasterForeignRow[mfk].ToString()) : string.Empty);   //优先取schema内部生成的主键值
                        if (filter.FilterParameters.ContainsKey(s) && cvalue == string.Empty) cvalue = (filter.FilterParameters[s] != null && filter.FilterParameters[s].ToString() != string.Empty ? filter.FilterParameters[s].ToString() : cvalue);
                        if (cvalue != string.Empty)
                        {
                            masterKeyParameters.Add(s, cvalue);
                            masterKeyValueFilter.FilterParameters.Add(s, cvalue);
                        }
                    }
                }
            }
            
            //构建现存数据的主键表
            List<NameValueCollection> deleteKeys = new List<NameValueCollection>();  //要删除的行
            Dictionary<string, NameValueCollection> currentKeys = new Dictionary<string, NameValueCollection>();
            DataTable dtcurrent = (masterKeyParameters.Keys.Count == 0 ? null : dmlJob.QueryDataTable_Job(DataTableName, Table_ForeignKeyFields + (Table_ForeignKeyFields == string.Empty ? "" : ",") + dickeyfieldlist, masterKeyValueFilter));
            if (dtcurrent != null)
            {
                foreach (DataRow dr in dtcurrent.Rows)
                {
                    NameValueCollection keyrow = new NameValueCollection();
                    string keyvalues = string.Empty;
                    foreach (DataColumn dc in dtcurrent.Columns)
                    {
                        keyvalues += (keyvalues == string.Empty ? "" : ",") + dc.ColumnName + ":" + (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString());
                        keyrow.Add(dc.ColumnName, (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString()));
                    }
                    currentKeys.Add(keyvalues, keyrow);
                }
            }



            int r = 0;
            int iIns = 0, iUpd = 0;
            string appScript = string.Empty;
            List<SQLJob.RowStatus> RowsSchema = new List<SQLJob.RowStatus>();
            foreach (DataRow dr in griddata.Rows)
            {
                NameValueCollection valrow = new NameValueCollection();
                NameValueCollection keyvalues = new NameValueCollection();
                SQLJob.RowStatus rowstate;
                BusDataClass.JobDataFilter updatefilter = new BusDataClass.JobDataFilter(updatebasefilter);

                for (int c = 0; c < arflds.Length; c++)
                {
                    string key = arflds[c].Replace("[", "").Replace("]", "");
                    string value = string.Empty;
                    value = (dr[key] == null || dr[key].ToString() == "" ? "" : dr[key].ToString());
                    valrow.Add(key, value);
                    if (dickeyfieldlist.Contains(key)) keyvalues.Add(key, value);
                }
                rowstate.RowIndex = (r + 1);
                rowstate.RowKeyValue = keyvalues;

                if (dmltype != DMLType.None || dmltype != DMLType.Query)
                {
                    //判断当前行是否为新增行
                    bool isnew = true;
                    //把非空的主键列和Auto列作为Update条件添加到updatefilter中
                    foreach (KeyValuePair<string, BusJob.SQLJob.FieldAttr> fld in dickeyfields)
                    {
                        string fldname = fld.Key;
                        if (griddata.Columns.Contains(fldname) && dr[fldname] != DBNull.Value && dr[fldname].ToString() != "")
                        {
                            isnew = false;
                            if (!updatefilter.FilterParameters.ContainsKey(fldname))
                            {
                                updatefilter.FilterParameters.Add(fldname, dr[fldname].ToString());
                            }
                            else
                            {
                                updatefilter.FilterParameters[fldname] = dr[fldname].ToString();
                            }
                        }
                    }

                    if (isnew)
                    {
                        iIns = iIns + 1;
                        rowstate.RowDMLType = "new";
                        valueinsertrows.Add(valrow);
                    }
                    else
                    {
                        iUpd = iUpd + 1;
                        rowstate.RowDMLType = "update";
                        DataHandler.BusJob.SQLJob.UpdateDefine updDefine = new BusJob.SQLJob.UpdateDefine();
                        updDefine.FieldValues = valrow;
                        updDefine.Filter = updatefilter;
                        updDefine.NoSavedFields = NoSavedFields;
                        updatedefines.Add(updDefine);

                        //对有源记录生成行标识（统一按dtcurrent的列次序生成）
                        if (dtcurrent != null)
                        {
                            NameValueCollection keyrow = new NameValueCollection();
                            string valuekey = string.Empty;
                            foreach (DataColumn dc in dtcurrent.Columns)
                            {
                                valuekey += (valuekey == string.Empty ? "" : ",") + dc.ColumnName + ":" + (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString());
                                keyrow.Add(dc.ColumnName, (dr[dc.ColumnName] == DBNull.Value ? "" : dr[dc.ColumnName].ToString()));
                            }
                            if (currentKeys.ContainsKey(valuekey))
                            {
                                currentKeys.Remove(valuekey);
                            }
                        }
                    }
                    rowstate.InsertIndex = (isnew ? iIns : -1);
                    rowstate.UpdateIndex = (isnew ? -1 : iUpd);
                }
                else
                {
                    iIns = -1;
                    iUpd = -1;
                    rowstate.InsertIndex = iIns;
                    rowstate.UpdateIndex = iUpd;
                    rowstate.RowDMLType = "query";
                    valueinsertrows.Add(valrow);
                }
                RowsSchema.Add(rowstate);
                r++;
            }
            rst.DataSchema = RowsSchema;

            //整理删除行
            foreach (KeyValuePair<string, NameValueCollection> item in currentKeys)
            {
                deleteKeys.Add(item.Value);
            }

            //保存Application的数据内容
            StringBuilder script = new StringBuilder();
            string sqlstring = string.Empty;
            switch (dmltype)
            {
                case DMLType.None:
                    break;
                case DMLType.Query:
                    sqlstring = "DECLARE @" + DataTableName + "_Table_PrimaryCurrentValue NVARCHAR(MAX) = '" + rst.CurrentRowKeyValues + "'";
                    break;
                case DMLType.Update:
                    sqlstring = dmlJob.GetScript_DML_ReturnKeys(DataTableName, fldlist, NoSavedFields, valueinsertrows.ToArray(), updatedefines.ToArray(), SelectedRowIndex, true, MasterForeignTable, string.Empty, MasterTblSchema.ForeignKeyFields, RowsSchema, MasterTblSchema.DataSchema, MasterTblSchema.CurrentRowIndex, MasterTblSchema.CurrentRowKeyValues);
                    break;
                case DMLType.Insert:
                    sqlstring = dmlJob.GetScript_DML_ReturnKeys(DataTableName, fldlist, NoSavedFields, valueinsertrows.ToArray(), updatedefines.ToArray(), SelectedRowIndex, true, MasterForeignTable, string.Empty, MasterTblSchema.ForeignKeyFields, RowsSchema, MasterTblSchema.DataSchema, MasterTblSchema.CurrentRowIndex, MasterTblSchema.CurrentRowKeyValues);
                    break;
                case DMLType.MixedDML:
                    sqlstring = dmlJob.GetScript_DML_ReturnKeys(DataTableName, fldlist, NoSavedFields, valueinsertrows.ToArray(), updatedefines.ToArray(), SelectedRowIndex, true, MasterForeignTable, string.Empty, MasterTblSchema.ForeignKeyFields, RowsSchema, MasterTblSchema.DataSchema, MasterTblSchema.CurrentRowIndex, MasterTblSchema.CurrentRowKeyValues);
                    break;
                default:
                    sqlstring = dmlJob.GetScript_DML_ReturnKeys(DataTableName, fldlist, NoSavedFields, valueinsertrows.ToArray(), updatedefines.ToArray(), SelectedRowIndex, true, MasterForeignTable, string.Empty, MasterTblSchema.ForeignKeyFields, RowsSchema, MasterTblSchema.DataSchema, MasterTblSchema.CurrentRowIndex, MasterTblSchema.CurrentRowKeyValues);
                    break;
            }
            string delsqlstring = "";
            if (dmltype != DMLType.None && dmltype != DMLType.Query && includedeletion && deleteKeys.Count > 0) delsqlstring = dmlJob.GetScript_BatchDelete(DataTableName, deleteKeys.ToArray());  //加入deleteKeys.Count>0减少删除全部数据的发生概率
            sqlstring += char.ConvertFromUtf32(13) + delsqlstring;
            script.Append(sqlstring);
            rst.GridSaveScript = script.ToString();

            DicDataSchema.Add(DataTableName, rst);
            return DicDataSchema;
        }

        #endregion

    }
}
