using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Business;
using Business.BusDataClass;
using Business.BusJob;

namespace DynamicPage
{
    public class FieldsManagement
    {
        public string CreatePage(string PageName)
        {
            string strhtml = string.Empty;

            string sqlstr = @"SELECT [PageName]
      ,[ViewName]
      ,[FieldName]
      ,[Caption]
      ,[Visible]
      ,[Enabled]
      ,[Required]
      ,[GridColumn]
      ,[Coordinate]
      ,[ControlType]
      ,[WidthType]
      ,[Datasource]
  FROM [dbo].[PageFieldsLayout]
  WHERE [PageName] = @PageName
  ORDER BY PageName,GridColumn,ViewName,Coordinate";
            JobDataFilter filter = new JobDataFilter();
            filter.FilterParameters.Add("PageName", PageName);
            SQLJob job = new SQLJob();
            DataTable dt = null;
            dt = job.QueryDataTable_Job(sqlstr, filter);
            strhtml = GetPageHtml(dt);

            return strhtml;
        }

        public string GetPageHtml(DataTable dt)
        {
            //dt: 是按PageName查询的页面字段布局表

            string chtml = "";
            Dictionary<string, ArrayList> layout = new Dictionary<string,ArrayList>();  //Page页的HTML布局表
            Dictionary<string, ControlDefineandEditor> ctldef = EasyUIControlAttributes();

            try
            {
                for (int r = 0; r < dt.Rows.Count; r++)
                {
                    layout = SetFieldHtml(dt.Rows[r], layout, ctldef);
                }

                //生成页面的HTML的Body
                for (int level = 1; level <= layout.Count; level++)
                {
                    ArrayList viewlayout = layout.ElementAt(level -1).Value;
                    string sectionHtml = "";
                    switch (level)
                    {
                        case 1:   //非Grid
                            sectionHtml = "";
                            foreach (KeyValuePair<int, ArrayList> rkvp in viewlayout)
                            {
                                //rowlayout
                                ArrayList rowlayout = rkvp.Value;

                                string rowhtml = "";
                                string cellhtml = "";
                                string colwidth = "";
                                for (int c = 0; c < rowlayout.Count; c++) { colwidth += " 1fr"; }
                                foreach (KeyValuePair<int, htmlControl> ckvp in rowlayout)
                                {
                                    htmlControl colval = ckvp.Value;
                                    cellhtml = "<div class='gridcolumn' field='" + colval.FieldName + "' view ='" + colval.View + "' page ='" + colval.Page + "'>" + colval.ControlHtml + "</div>" + char.ConvertFromUtf32(13);
                                    rowhtml += cellhtml;
                                }
                                rowhtml = "<div class='gridrow' style='grid-template-columns:" + colwidth + "'>" + rowhtml + "</div>" + char.ConvertFromUtf32(13);
                                sectionHtml += rowhtml;
                            }
                            chtml += sectionHtml;
                            chtml = "<div class='grid'>" + chtml + "</div>" + char.ConvertFromUtf32(13);
                            break;
                        default:   //Grid
                            sectionHtml = "";
                            string headrowhtml = "";
                            string bodyrowhtml = "";
                            foreach (KeyValuePair<int, ArrayList> rkvp in viewlayout)
                            {
                                //rowlayout
                                ArrayList rowlayout = rkvp.Value;

                                string cellhtml = "";
                                string headcellshtml = "";
                                string bodycellshtml = "";
                                //string colwidth = "";
                                foreach (KeyValuePair<int, htmlControl> ckvp in rowlayout)
                                {
                                    htmlControl colval = ckvp.Value;
                                    cellhtml = "<th class='gridcell' field='" + colval.FieldName + "' view ='" + colval.View + "' page ='" + colval.Page + "' " + colval.ControlHtml + ">"
                                        + colval.FieldCaption
                                        + "</th>" + char.ConvertFromUtf32(13);
                                    headcellshtml += cellhtml;
                                    bodycellshtml += "<td></td>" + char.ConvertFromUtf32(13);
                                }
                                headcellshtml = "<tr>" + headcellshtml + "</tr>" + char.ConvertFromUtf32(13);
                                bodycellshtml = "<tr>" + bodycellshtml + "</tr>" + char.ConvertFromUtf32(13);
                                headrowhtml += headcellshtml;
                                bodyrowhtml += bodycellshtml;
                            }
                            string gridclass = (level == 2 ? "master" : "slave");
                            string toolname = (level == 2 ? "mastertoolbar" : "slavetoolbar");
                            sectionHtml = "<thead>" + headrowhtml + "</thead>" + char.ConvertFromUtf32(13)
                                + "<tbody>" + bodyrowhtml + "</tbody>" + char.ConvertFromUtf32(13);
                            sectionHtml = "<div class='" + toolname + "'>"
                                +"<div style=\"margin-bottom:5px\">"
			                    +"<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-add\" plain=\"true\">"
                                + (level == 2 ? "参照":"增加")
                                +"</a>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-save\" plain=\"true\">保存</a>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-edit\" plain=\"true\">修改</a>"
			                    +"<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-print\" plain=\"true\">打印</a>"
			                    +"<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-remove\" plain=\"true\">删除</a>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-add\" plain=\"true\">增行</a>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-remove\" plain=\"true\">删行</a>"
                                + "</div>"
                                + "<div>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-first\" plain=\"true\">首单</a>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-previous\" plain=\"true\">上一单</a>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-next\" plain=\"true\">下一单</a>"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-last\" plain=\"true\">末单</a>"
                                + "单号: <input class=\"easyui-searchbox\" style=\"width:80px\">"
                                + "<a href=\"#\" class=\"easyui-linkbutton\" iconCls=\"icon-search\">查询</a>"
                                +"    </div>"
                                +"</div>" + char.ConvertFromUtf32(13) +
                                "<table class='easyui-datagrid " + gridclass + "' "
                                + " data-options='"
                                + "onSelect:function(index,row){selectrow(\"" + gridclass + "\",index,row);},"
                                + "toolbar: \"." + toolname + "\"'>" + sectionHtml + "</table>" + char.ConvertFromUtf32(13) 
                                + "<div class='" + (level == 2 ? " masterfooter" : " slavefooter") + "'></div>" + char.ConvertFromUtf32(13);
                            chtml += sectionHtml;
                            break;
                    }
                }
            }
            catch (Exception exp)
            {
                string errstr = "错误信息：" + exp.Message + "; 堆栈：" + exp.StackTrace;
            }
            return chtml;
        }


        //根据PageName过滤出页面的字段布局表layout
        //layout结构：Dictionary<ViewName, Arraylist(
        //                                          KeyValuePair<rowindex, ArrayList(
        //                                                                          KeyValuePair<colindex,fieldhtmlstring>
        //                                                                           )
        //                                                      >
        //                                          )
        //                      >
        public Dictionary<string, ArrayList> SetFieldHtml(DataRow dr, Dictionary<string, ArrayList> layout, Dictionary<string, ControlDefineandEditor> ctldef)
        {

            string ViewName = dr["ViewName"].ToString();
            string PageName = dr["PageName"].ToString();
            //int ControlType = (dr["ControlType"] == DBNull.Value ? 0 : Convert.ToInt16(dr["ControlType"]));
            //string FieldName = (dr["FieldName"] == DBNull.Value ? "" : dr["FieldName"].ToString());
            //string Caption = (dr["Caption"] == DBNull.Value ? "" : dr["Caption"].ToString());
            bool DisplayonPage = (dr["Visible"] == DBNull.Value ? false : Convert.ToBoolean(dr["Visible"]));
            //bool Required = (dr["Required"] == DBNull.Value ? false : Convert.ToBoolean(dr["Required"]));
            bool GridColumn = (dr["GridColumn"] == DBNull.Value ? false : Convert.ToBoolean(dr["GridColumn"]));
            string Coordinate = (dr["Coordinate"] == DBNull.Value ? "" : dr["Coordinate"].ToString());
            string WidthType = (dr["Coordinate"] == DBNull.Value ? "" : dr["WidthType"].ToString());
            //string Datasource = (dr["Datasource"] == DBNull.Value ? "" : dr["Datasource"].ToString());

            int r = (Coordinate.IndexOf(',') > 0 ? Convert.ToInt16(Coordinate.Substring(1, Coordinate.IndexOf(',') - 1)) : -1);
            int c = (Coordinate.IndexOf(',') > 0 ? Convert.ToInt16(Coordinate.Substring(Coordinate.IndexOf(',') + 1, Coordinate.IndexOf(']') - Coordinate.IndexOf(',') - 1)) : -1);
            int icol = (Coordinate.IndexOf(',') > 0 ? -1 : Convert.ToInt16(Coordinate));

            //生成字段元素的HTML


            //第一步：根据ViewName获取数据所在层
            ArrayList viewlayout;
            if(layout.ContainsKey(ViewName)){
                viewlayout = layout[ViewName];
                
            }else{
                viewlayout = new ArrayList();
                layout.Add(ViewName, viewlayout);
            }
            int laylevel = 0;
            foreach (KeyValuePair<string, ArrayList> kvp in layout)
            {
                laylevel++;
                if (kvp.Key == ViewName) break;
            }


            if (laylevel > 1)   //Grid元素
            {
                //文本输入框(纯),文本输入框(带参照),数字输入框,日期控件,下拉单选框,下拉多选框,Checkbox,Tree
                htmlControl htmControl = GetEasyUIFieldHtml(dr, ctldef);

                //定位或创建布局行
                KeyValuePair<int, ArrayList> layrow = (viewlayout.Count == 0 ? new KeyValuePair<int, ArrayList>(1, new ArrayList()) : (KeyValuePair<int, ArrayList>)viewlayout[0]);
                if (viewlayout.Count > 0)
                {
                    layrow = (KeyValuePair<int, ArrayList>)viewlayout[0];
                }
                else
                {
                    viewlayout.Add(layrow);
                }
                //定位或创建布局列
                ArrayList celllist = layrow.Value;
                KeyValuePair<int, htmlControl> laycell;
                int curCount = celllist.Count;
                if (icol > curCount)
                {
                    for (int df = 0; df < icol - curCount; df++)
                    {
                        laycell = new KeyValuePair<int, htmlControl>(celllist.Count + 1, (df + 1 == icol - curCount ? htmControl : new htmlControl()));
                        celllist.Add(laycell);
                    }
                }
                else
                {
                    laycell = new KeyValuePair<int, htmlControl>(icol, htmControl);
                    celllist[icol - 1] = laycell;
                }
            }
            else   //非Grid元素
            {
                //文本输入框(纯),文本输入框(带参照),数字输入框,日期控件,下拉单选框,下拉多选框,Checkbox,Tree
                htmlControl htmControl = GetEasyUIFieldHtml(dr, ctldef);

                //定位或创建布局行
                KeyValuePair<int, ArrayList> layrow = new KeyValuePair<int,ArrayList>();
                if (r > viewlayout.Count)
                {
                    for (int df = 0; df < r - viewlayout.Count; df++)
                    {
                        layrow = new KeyValuePair<int, ArrayList>(viewlayout.Count + 1, new ArrayList());
                        viewlayout.Add(layrow);
                    }
                }
                else
                {
                    layrow = (KeyValuePair<int, ArrayList>)viewlayout[r - 1];

                }

                //定位或创建布局列
                ArrayList celllist = layrow.Value;
                KeyValuePair<int, htmlControl> laycell;
                int curCount = celllist.Count;
                if (c > curCount)
                {
                    for (int df = 0; df < c - curCount; df++)
                    {
                        laycell = new KeyValuePair<int, htmlControl>(celllist.Count + 1, (df + 1 == c - curCount ? htmControl : new htmlControl()));
                        celllist.Add(laycell);
                    }
                }
                else
                {
                    laycell = new KeyValuePair<int, htmlControl>(c, htmControl);
                    celllist[c - 1] = laycell;
                }
            }

            return layout;
        }

        #region 字段html控制参数
        public struct htmlControl
        {
            public string FieldName;
            public string FieldCaption;
            public string View;
            public string Page;
            public string ControlType;
            public string ControlHtml;
            public string ControlDatasource;

            public bool Visible;
            public bool Enabled;
            public bool Required;
            public string FunctionType;
            public string ActionType;
            public bool IsContainer;
            public string ContainerType;
            public string ContainerField;
            public bool IsPopwindow;
            public string ActionField;

        }
        public struct ControlDefineAttributes
        {
            public string Tag;
            public string EditorType;
            public string Options;
            public string EasyUIClass;
            public string HandlerFun;
        }
        public struct ControlDefineandEditor
        {
            public ControlDefineAttributes ControlDefine;
            public string ControlEditorName;
        }
        public enum FunctionType
        {
            //字段的功能类型：
            //0：divlayoutData, 普通数据字段，div分列布局
            //1：gridcolumnData, 表格列字段，grid分列布局
            //2：toolbarElement，工具菜单字段，toolbar元素布局
            //3：divcontainer，层容器字段，div容器
            //4：gridcontainer，grid容器字段，grid同期
            divlayoutData = 0,
            gridcolumnData = 1,
            toolbarElement = 2,
            divcontainer = 3,
            gridcontainer = 4
        }
        public enum ActionType
        {
            //动作类型：
            //空或0：无动作
            //1：link弹出
            //2：pop弹出
            //3：折叠/展开
            //4：功能函数（需要定义function()方法）
            none = 0,
            link = 1,
            pop = 2,
            collapse = 3,
            function = 4
        }
        public enum ControlType
        {
            div = 0,
            span = 1,
            label = 2,
            textbox = 3,
            searchbox = 4,
            numberbox = 5,
            datebox = 6,
            sngcombobox = 7,
            mulcombobox = 8,
            checkbox = 9,
            combotreegrid = 10
        }
        public enum EasyUIEditor
        {
            text = 0,
            numberbox = 1,
            datebox = 2,
            combobox = 3,
            checkbox = 4,
            combotree = 5
        }
        public enum ContainerType
        {
            //容器类型，设置IsContainer=1生效，包括：
            //0：数据字段容器，
            //1：菜单栏字段容器，
            //2：标题字段容器，
            //3：抬头（左对齐）字段容器，
            //4：抬头（右对齐）字段容器，
            //5：落款（左对齐）字段容器，
            //6：落款（右对齐）字段容器
            databar = 0,
            toolbar = 1,
            pagetitlebar = 2,
            gridtitlebar_left = 3,
            gridtitlebar_right = 4,
            gridfooterbar_left = 5,
            gridfooterbar_right = 6
        }
        #endregion

        public Dictionary<string, ControlDefineandEditor> EasyUIControlAttributes()
        {
            Dictionary<string, ControlDefineandEditor> dicdefedt = new Dictionary<string, ControlDefineandEditor>();


            //文本输入框(纯),文本输入框(带参照),数字输入框,日期控件,下拉单选框,下拉多选框,Checkbox,Tree
            //string[] arControlType = { "div|", "span|", "label|", "textbox|text", "searchbox|text", "numberbox|numberbox", "datebox|datebox", "sngcombobox|combobox", "mulcombobox|combobox", "checkbox|checkbox", "combotreegrid|combotree" };
            //string[] easyuieditorType = { "", "", "", "text", "text", "numberbox", "datebox", "combobox", "combobox", "checkbox", "combotree" };

            //将控件类型枚举转成名称和ID集合
            Array arControlTypeID = Enum.GetValues(typeof(ControlType));
            Array arControlTypeName = Enum.GetNames(typeof(ControlType));

            NameValueCollection ControlEasyUIEditor = new NameValueCollection();
            #region  对不同的控件类型匹配当它作为editor的名称
            foreach (ControlType ctt in arControlTypeID)
            {
                switch (ctt)
                {
                    case ControlType.div:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), "");
                        break;
                    case ControlType.span:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), "");
                        break;
                    case ControlType.label:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), "");
                        break;
                    case ControlType.textbox:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.text));
                        break;
                    case ControlType.searchbox:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.text));
                        break;
                    case ControlType.numberbox:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.numberbox));
                        break;
                    case ControlType.datebox:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.datebox));
                        break;
                    case ControlType.sngcombobox:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.combobox));
                        break;
                    case ControlType.mulcombobox:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.combobox));
                        break;
                    case ControlType.checkbox:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.checkbox));
                        break;
                    case ControlType.combotreegrid:
                        ControlEasyUIEditor.Add(Enum.GetName(typeof(ControlType), ctt), Enum.GetName(typeof(EasyUIEditor), EasyUIEditor.combotree));
                        break;
                }
            }
            #endregion

            Dictionary<string, ControlDefineAttributes> ControlDefine = new Dictionary<string, ControlDefineAttributes>();
            #region 将不同控件类型对应的html代码的相关属性的定义缓存起来
            foreach (string s in arControlTypeName)
            {
                //KeyValuePair<string,ControlDefineAttributes> nvc = new KeyValuePair<string,ControlDefineAttributes>();

                ControlDefineAttributes flddef = new ControlDefineAttributes();
                flddef.Tag = "";
                flddef.EditorType = "";
                flddef.Options = "";
                flddef.EasyUIClass = "";
                flddef.HandlerFun = "";
                #region 设置控件的属性
                switch (s)
                {
                    case "div":
                        flddef.Tag = "div";
                        break;
                    case "span":
                        flddef.Tag = "span";
                        break;
                    case "label":
                        flddef.Tag = "label";
                        break;
                    case "textbox":
                        flddef.Tag = "input";
                        flddef.EditorType = "text";
                        flddef.EasyUIClass = "textbox";
                        break;
                    case "searchbox":
                        flddef.Tag = "input";
                        flddef.EditorType = "text";
                        flddef.EasyUIClass = "searchbox";
                        flddef.HandlerFun = "searcher:openSearchwin";
                        break;
                    case "numberbox":
                        flddef.Tag = "input";
                        flddef.EditorType = "numberbox";
                        flddef.EasyUIClass = "numberbox";
                        break;
                    case "datebox":
                        flddef.Tag = "input";
                        flddef.EditorType = "datebox";
                        flddef.EasyUIClass = "datebox";
                        break;
                    case "sngcombobox":
                        flddef.Tag = "input";
                        flddef.EditorType = "combobox";
                        flddef.EasyUIClass = "combobox";
                        break;
                    case "mulcombobox":
                        flddef.Tag = "input";
                        flddef.EditorType = "combobox";
                        flddef.EasyUIClass = "combobox";
                        flddef.Options = "multiple:true";
                        break;
                    case "checkbox":
                        flddef.Tag = "input type='checkbox'";
                        flddef.EditorType = "checkbox";
                        break;
                    case "combotreegrid":
                        flddef.Tag = "input";
                        flddef.EditorType = "combotree";
                        flddef.EasyUIClass = "combotreegrid";
                        break;
                }
                #endregion

                ControlDefine.Add(s, flddef);
            }
            #endregion

            //defandedt.ControlDefine = ControlDefine;
            //defandedt.ControlEditorName = ControlEasyUIEditor;
            foreach (string k in arControlTypeName)
            {
                ControlDefineandEditor defandedt = new ControlDefineandEditor();
                defandedt.ControlDefine = ControlDefine[k];
                defandedt.ControlEditorName = ControlEasyUIEditor[k];
                dicdefedt.Add(k, defandedt);
            }

            return dicdefedt;

        }

        public htmlControl GetEasyUIFieldHtml(DataRow dr, Dictionary<string, ControlDefineandEditor> ControlDefandEditor)
        {
            string chtml = "";

            string fViewName = dr["ViewName"].ToString();
            string fPageName = dr["PageName"].ToString();
            int fControlType = (dr["ControlType"] == DBNull.Value ? 0 : Convert.ToInt16(dr["ControlType"]));
            string fFieldName = (dr["FieldName"] == DBNull.Value ? "" : dr["FieldName"].ToString());
            string fCaption = (dr["Caption"] == DBNull.Value ? "" : dr["Caption"].ToString());
            bool fDisplayonPage = (dr["Visible"] == DBNull.Value ? false : Convert.ToBoolean(dr["Visible"]));
            bool fEnabled = (dr["Enabled"] == DBNull.Value ? true : Convert.ToBoolean(dr["Enabled"].ToString()));
            bool fRequired = (dr["Required"] == DBNull.Value ? false : Convert.ToBoolean(dr["Required"]));
            int fFunctionType = (dr["FunctionType"] == DBNull.Value ? 0 : Convert.ToInt16(dr["FunctionType"]));
            int fActionType = (dr["ActionType"] == DBNull.Value ? 0 : Convert.ToInt16(dr["ActionType"]));
            string fCoordinate = (dr["Coordinate"] == DBNull.Value ? "" : dr["Coordinate"].ToString());
            string fWidthType = (dr["Coordinate"] == DBNull.Value ? "" : dr["WidthType"].ToString());
            string fDatasource = (dr["Datasource"] == DBNull.Value ? "" : dr["Datasource"].ToString());


            bool fIsContainer = (dr["IsContainer"] == DBNull.Value ? false : Convert.ToBoolean(dr["IsContainer"].ToString()));
            int fContainerType = (dr["ContainerType"] == DBNull.Value ? 0 : Convert.ToInt16(dr["ContainerType"].ToString()));
            string fContainerField = (dr["ContainerField"] == DBNull.Value ? "" : dr["ContainerField"].ToString());
            bool fIsPopwindow = (dr["Datasource"] == DBNull.Value ? false : Convert.ToBoolean(dr["Datasource"].ToString()));
            string fActionField = (dr["ActionField"] == DBNull.Value ? "" : dr["ActionField"].ToString());
            

            htmlControl htmControl = new htmlControl();
            htmControl.Page = fPageName;
            htmControl.View = fViewName;
            htmControl.FieldName = fFieldName;
            htmControl.FieldCaption = fCaption;
            htmControl.ControlType = Enum.GetName(typeof(ControlType),fControlType);// arControlType[ControlType - 1];
            htmControl.Visible = fDisplayonPage;
            htmControl.Enabled = fEnabled;
            htmControl.Required = fRequired;
            htmControl.FunctionType = Enum.GetName(typeof(FunctionType),fFunctionType);
            htmControl.ActionType = Enum.GetName(typeof(ActionType),fActionType);
            htmControl.IsContainer = fIsContainer;
            htmControl.ContainerType = Enum.GetName(typeof(ContainerType),fContainerType);
            htmControl.ContainerField = fContainerField;
            htmControl.IsPopwindow = fIsPopwindow;
            htmControl.ActionField = fActionField;

            //editor type：字符串，编辑类型，可能的类型：text、textarea、checkbox、numberbox、validatebox、datebox、combobox、combotree
            //"textbox", "searchbox", "numberbox", "datebox", "sngcombobox", "mulcombobox", "checkbox", "combotreegrid" 
            //string[] editorType = { "", "", "", "text", "text", "numberbox", "datebox", "combobox", "combobox", "checkbox", "combotree" };

            string gridcelleditor = string.Empty;
            string htmlelement = string.Empty;

            gridcelleditor = "";// @"  editor:{type:'" + nvControlType[ControlType - 1] + "',"
                        //+ @"  options:{"
                        //+ (editorType[ControlType - 1] == "combobox" || editorType[ControlType - 1] == "combotree" ?
                        //"	    valueField:'FieldName',textField:'FieldName'," : "")
                        //+ @"	    method:'get'"
                        //+ (handlerfun == "" && !IsEditor ? "" : "," + handlerfun)
                        //+ "	    }}";

            switch ((FunctionType)fFunctionType)
            {
                case FunctionType.divcontainer:
                    break;
                case FunctionType.divlayoutData:
                    break;
                case FunctionType.gridcolumnData:  //
                    break;
                case FunctionType.gridcontainer:
                    break;
                case FunctionType.toolbarElement:
                    break;
            }
            string editor = "";// (ControlType == 0 || !IsEditor ? "" :
                        //@"  editor:{type:'" + nvControlType[ControlType - 1] + "',"
                        //+ @"  options:{"
                        //+ (editorType[ControlType - 1] == "combobox" || editorType[ControlType - 1] == "combotree" ?
                        //"	    valueField:'FieldName',textField:'FieldName'," : "")
                        //+ @"	    method:'get'"
                        //+ (handlerfun == "" && !IsEditor ? "" : "," + handlerfun)
                        //+ "	    }}");
            //switch (ControlType)
            //{
            //    case 0:
            //        htmControl.ControlHtml = (IsEditor ? "" : "<label>" + label + "</label>:")
            //            + (IsEditor ? "" : "<" + tag)
            //            + (IsEditor ? "" : " id='" + PageName + "_" + ViewName + "_" + FieldName + "'")
            //            + (IsEditor ? "" : " class='" + easycls + "'")
            //            + " data-options=\""
            //            + "field:'" + FieldName + "'"
            //            + ",width:150"
            //            + "\" "
            //            + (IsEditor ? "" : "/>");
            //        break;
            //    case 7:
            //        htmControl.ControlHtml = (IsEditor ? "" : "<label>" + label + "</label>:")
            //            + (IsEditor ? "" : "<" + tag)
            //            + (IsEditor ? "" : " id='" + PageName + "_" + ViewName + "_" + FieldName + "'")
            //            + (IsEditor ? "" : " class='" + easycls + "'")
            //            + " data-options=\""
            //            + "field:'" + FieldName + "'"
            //            + ",width:150"
            //            + (editor == "" ? "" : "," + editor)
            //            + "\" "
            //            + (IsEditor ? "" : "/>");
            //        break;
            //    default:
            //        htmControl.ControlHtml = (IsEditor ? "" : "<" + tag)
            //        + (IsEditor ? "" : " id='" + PageName + "_" + ViewName + "_" + FieldName + "'")
            //        + (IsEditor ? "" : " class='" + easycls + "'")
            //        + (IsEditor ? "" : " label='" + label + ":' labelPosition='left' ")
            //        + " data-options=\""
            //        + "field:'" + FieldName + "'"
            //        + ",width:300"
            //        + (editor == "" ? "" : "," + editor)
            //        + (handlerfun == "" || IsEditor ? "" : "," + handlerfun + "")
            //        + "\" "
            //        +(IsEditor ? "" : "/>");
            //        break;
            //}
            return htmControl;
        }
    }
}
