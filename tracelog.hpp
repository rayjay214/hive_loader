#include "tracelog.h"
#include "tostring.h"
#include <inttypes.h>
#include <stdarg.h>  
#include <set>


enum GMC_TRACE_ENUM
{
    GMC_TRACE_PRINT_UIDS = 0,       // 输出当前跟踪的所有LINEID
    GMC_TRACE_SET_UID = 1,          // 增加跟踪某个LINEID，设置P2
    GMC_TRACE_REMOVE_UID = 2,       // 删除跟踪某个LINEID，设置P2
    GMC_TRACE_CLEAR_ALL_UID = 5     // 清空跟踪的所有LINEID
};


bool CMyTraceLog::check(const std::string & master_lineid)
{
    return (_trace_ids.find(master_lineid) != _trace_ids.end());
}


void CMyTraceLog::setConfig(const Log::cfg& c)
{
    std::set<std::string>::iterator it;
    int op_type = c.p1;
    switch(op_type)
    {
    case (int)GMC_TRACE_PRINT_UIDS:
        if (!_trace_ids.empty())
        {
            for (it = _trace_ids.begin(); it != _trace_ids.end(); ++it)
            {
                this->DLog("Tracing uid:%s", it->c_str());
            }
        }
        else
        {
            this->DLog("Tracing uid list is empty!");
        }
        return;
    case (int)GMC_TRACE_SET_UID:
        _trace_ids.insert(ToString(c.p2));
        this->DLog("add tracing uid(%d) success !",c.p2);
        return;
    case (int)GMC_TRACE_REMOVE_UID:
        it = _trace_ids.find(ToString(c.p2));
        if (it != _trace_ids.end())
        {
            _trace_ids.erase(it);
            this->DLog("delete tracing uid(%d) success !",c.p2);
        }
        else
        {
            this->DLog("don't find uid(%d)!",c.p2);
        }
        break;
    case (int)GMC_TRACE_CLEAR_ALL_UID:
        _trace_ids.clear();
        break;
    default:
        break;
    }
}


