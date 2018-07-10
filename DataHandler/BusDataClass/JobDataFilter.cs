using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataHandler.BusDataClass
{
    public class JobDataFilter
    {
        private Dictionary<string, object> _FilterParameters;
        public Dictionary<string, object> FilterParameters
        {
            get
            {
                return _FilterParameters;
            }
            set
            {
                _FilterParameters = value;
            }
        }
        public JobDataFilter()
        {
            _FilterParameters = new Dictionary<string, object>();
            
        }
        public JobDataFilter(JobDataFilter Filter)
        {
            _FilterParameters = new Dictionary<string, object>();
            foreach (string k in Filter.FilterParameters.Keys)
            {
                this._FilterParameters.Add(k, Filter.FilterParameters[k]);
            }

        }
        public JobDataFilter(Dictionary<string, object> Parameters)
        {
            _FilterParameters = new Dictionary<string, object>();
            foreach (string k in Parameters.Keys)
            {
                this._FilterParameters.Add(k, Parameters[k]);
            }

        }
    }
}
