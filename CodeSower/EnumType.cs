using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CodeSower
{
    public class EnumType
    {
        public enum DataSourceType
        {
            Local = 0, //本地服务器上的数据库
            LinkServer = 1,//本地网络上的数据库
            ODBCDataSource = 2,//本地网络上的数据库
            WebAPI = 3,//跨域网络上的接口式数据源
            WebService = 4//跨域网络上的接口式数据源
        }
    }
}
