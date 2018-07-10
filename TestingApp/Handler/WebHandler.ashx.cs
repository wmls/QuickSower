using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using DynamicPage;
using Business;
using Business.BusDataClass;
using Business.BusJob;

namespace WebDynamicPage.Handler
{
    /// <summary>
    /// WebHandler1 的摘要说明
    /// </summary>
    public class WebHandler1 : IHttpHandler
    {

        public void ProcessRequest(HttpContext context)
        {
            context.Response.ContentType = "text/plain";
            string rtnJsn = string.Empty;
            try
            {
                JobDataFilter datFilter = PubFunctions.GetPageParameters(context);
                string pageName = datFilter.FilterParameters["PageName"].ToString();
                DynamicPage.FieldsManagement fldmngt = new FieldsManagement();
                rtnJsn = fldmngt.CreatePage(pageName);
            }
            catch (Exception exp)
            {
                rtnJsn = "1|" + "；错误：" + exp.Message + "；堆栈：" + exp.StackTrace;
            }

            context.Response.Write(rtnJsn);
        }

        public bool IsReusable
        {
            get
            {
                return false;
            }
        }
    }
}