#include <Ice/Ice.h>
#include <IceUtil/IceUtil.h>
#include "common.h"
#include "gsci.h"
#include "gsc.pb.h"
#include "log_parser_define.h"
#include "gsc_define.h"
#include "cLog.h"
#include "StringUtility.h"
#include "LogParserBase.h"
#include "getlocalip.h"


class GSCLogParser : public LogParserBase
{
public:
    GSCLogParser():m_strSpliter(PARTITION_SYMBOL)
    {
        snprintf(m_szModuleName, sizeof(m_szModuleName), "%s", "GSCLogParser");
    }

    virtual ~GSCLogParser()
    {
        LOG_DEBUG("%s exit!", m_szModuleName);
    }

    virtual int Init(Ice::CommunicatorPtr & oIC);

    virtual int AfterAllParsed();

    virtual int ParseLogLine(const char * pszLogLine, int dwLineNumber);
protected:
    virtual int ParseLogLineVer0(const char * pszLogLine, const std::vector<std::string> & oList, int dwLineNumber);

    virtual int ParseLogLineVer1(const char * pszLogLine, const std::vector<std::string> & oList, int dwLineNumber);

private:
    GSCLogInfoMap m_oAppRunningStatus;    // 解析日志获取到的，最近一段时间内的本机服务调用状态信息

    Ice::CommunicatorPtr m_oIC;

    GSC::GSCMgmtPrx m_ServerPrx;    // 与GSC通信句柄

    std::string m_strGSCEndpoints;

    std::string m_strSpliter;       // 日志中各字段分隔符

    time_t m_dwStartMinute;
};

int GSCLogParser::Init(Ice::CommunicatorPtr & oIC)
{
    m_oIC = oIC;

    // 获取配置
    Ice::PropertiesPtr props;
    try
    {
        props = m_oIC->getProperties();
    }
    catch (::Ice::Exception& ex)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Can't get property, err=%s", ex.what());
        return -1;
    }

    m_strGSCEndpoints = props->getPropertyWithDefault(KEY_GSC_ENDPOINT, KEY_GSC_ENDPOINT_DEFAULT);
    if ("" == m_strGSCEndpoints)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Config error!");
        return -1;
    }

    // 连接到GSC
    try
    {
        Ice::ObjectPrx base = m_oIC->stringToProxy(m_strGSCEndpoints);

        m_ServerPrx = GSC::GSCMgmtPrx::checkedCast(base);
        if (!m_ServerPrx)
        {
            throw("Invalid proxy");
        }
    }
    catch (::Ice::Exception& ex)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Connect to GSC failed! endpoint=%s, err=%s", m_strGSCEndpoints.c_str(), ex.what());
        return -1;
    }

    // 得到当前所在分钟的时间（整分钟时的秒数，如00:00:00, 00:01:00时的秒数）
    struct tm oTM;
    time_t dwNow = time(0);
    localtime_r(&dwNow, &oTM);
    oTM.tm_sec = 0;
    m_dwStartMinute = mktime(&oTM);

    return 0;
}


int GSCLogParser::AfterAllParsed()
{
	std::string SelfIpStr;
	(void) GetOneSelfIpAddress (SelfIpStr);
    GSCproto::ServiceCallTable oPB;
    for (GSCLogInfoMap::iterator itLogInfo = m_oAppRunningStatus.begin(); itLogInfo != m_oAppRunningStatus.end(); ++itLogInfo)
    {
        GSCproto::ServiceCallLine * pCallLine =  oPB.add_service_call();
        pCallLine->set_caller_id(itLogInfo->first.strCallerID);
        pCallLine->set_callee_id(itLogInfo->first.strCalleeID);
        pCallLine->set_start_time(itLogInfo->first.dwStartTime);
		pCallLine->set_caller_ip(SelfIpStr);
		pCallLine->set_version(GSC_VERSION_NUM);

        for (MethodIdCallCount::iterator itMCC = itLogInfo->second.oMethodIdCallCount.begin();
            itMCC != itLogInfo->second.oMethodIdCallCount.end();
            ++itMCC)
        {
            GSCproto::CallStatistics * pMethodIdLine = pCallLine->add_call_statistics();
            pMethodIdLine->set_method_id(itMCC->first);
			pMethodIdLine->set_succ_call_count(itMCC->second.dwSuccCall);
			pMethodIdLine->set_total_call_count(itMCC->second.dwTotalCall);
			pMethodIdLine->set_succ_call_cost(itMCC->second.uddwSuccCallCost);
			pMethodIdLine->set_fail_call_cost(itMCC->second.uddwFailCallCost);
			pMethodIdLine->set_total_call_cost(itMCC->second.uddwTotalCallCost);
			pMethodIdLine->set_max_call_cost(itMCC->second.uddwMaxCallCost);
            
        }

        for (ReturnCodeCount::iterator itRCC = itLogInfo->second.oReturnCodeCount.begin();
            itRCC != itLogInfo->second.oReturnCodeCount.end();
            ++itRCC)
        {
            GSCproto::ReturnCodeStatistics * pRetCodeLine = pCallLine->add_return_code_stat();
            pRetCodeLine->set_return_code(itRCC->first);
            pRetCodeLine->set_count(itRCC->second);
        }

        for (CalleeIPCount::iterator itCalleeCount = itLogInfo->second.oCalleeIPCount.begin();
            itCalleeCount != itLogInfo->second.oCalleeIPCount.end();
            ++itCalleeCount)
        {
            GSCproto::CalleeIPStatistics * pCalleeIPStatLine = pCallLine->add_calleeip_stat();
            pCalleeIPStatLine->set_calleeip(itCalleeCount->first);
            pCalleeIPStatLine->set_count(itCalleeCount->second);
        }

        LOG_DEBUG("Caller(%s), Callee(%s), start_time:%d. "   \
            "MethodIdStat size=%d, RetcodeStat size=%d, CalleeIPStat size=%d",
            itLogInfo->first.strCallerID.c_str(), itLogInfo->first.strCalleeID.c_str(), itLogInfo->first.dwStartTime,
            pCallLine->call_statistics_size(), pCallLine->return_code_stat_size(), pCallLine->calleeip_stat_size());
    }

    std::string strLocalStatus;
    if (!oPB.SerializeToString(&strLocalStatus))
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "%s", "SerializeToString fail!");
        return -1;
    }

    // 上报各Caller调用状态信息给GSC
    try
    {
        m_ServerPrx->ReportServiceCall(strLocalStatus);
    }
    catch (::Ice::Exception& ex)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Call ICE fail! %s", ex.what());
        return -1;
    }

    LOG_DEBUG("%s AfterAllParsed!", m_szModuleName);
    
    return 0;
}

int GSCLogParser::ParseLogLine(const char * pszLogLine, int dwLineNumber)
{
    std::string strLogLine(pszLogLine);
    std::vector<std::string> oList;

    
    Goome::SplitString(strLogLine, m_strSpliter, oList);
    if (oList.empty())
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Parse line %d '%s' fail! split size=%d", dwLineNumber, pszLogLine, oList.size());
        return -1;
    }

    if (oList[0] == "0")
    {
        return ParseLogLineVer0(pszLogLine, oList, dwLineNumber);
    }
    else if (oList[0] == "1")
    {
        return ParseLogLineVer1(pszLogLine, oList, dwLineNumber);
    }
    else
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Parse line %d '%s' fail! unknown log version!", dwLineNumber, pszLogLine);
        return -1;
    }
}

int GSCLogParser::ParseLogLineVer0(const char * pszLogLine,const std::vector<std::string> & oList, int dwLineNumber)
{
    GSCCallRelation oCallRelation;
    // 版本0的日志格式如下，各字段以“|”分隔：
    // version | CallerID | CalleeID | MethodID | CalleeIP | CalleePort | ReturnCode| StartTime | EndTime
    if (oList.size() != 9)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Parse line %d '%s' fail! split size=%d", dwLineNumber, pszLogLine, oList.size());
        return -1;
    }

    oCallRelation.strCallerID = oList[1];
    oCallRelation.strCalleeID = oList[2];
    time_t dwStart = (time_t)(atoll(oList[7].c_str()) / 1000000);
    if (0 == dwStart)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Start time(%s) invalid!", oList[7].c_str());
        return -1;
    }
    // 将开始时间修正为整分钟的时间
    dwStart -= (dwStart >= m_dwStartMinute) ? (dwStart - m_dwStartMinute) % 60 : 60 - (m_dwStartMinute - dwStart) % 60;
    oCallRelation.dwStartTime = dwStart;

	int method_id = atoi(oList[3].c_str());
    std::string strCalleeIP = oList[4];
    int dwReturnCode = atoi(oList[6].c_str());
    int dwTimeCost = (int)(atoll(oList[8].c_str()) - atoll(oList[7].c_str()));  // 调用耗时（微秒）
    dwTimeCost = (dwTimeCost < 0) ? 0 : dwTimeCost;

    GSCLogInfoMap::iterator itLogInfo = m_oAppRunningStatus.find(oCallRelation);
    if (itLogInfo == m_oAppRunningStatus.end())
    {
        GSCLogInfo oLogInfo;
		ServiceCallCount oServiceCallCount;
		oServiceCallCount.dwTotalCall = 1;
		oServiceCallCount.dwSuccCall += (dwReturnCode == 0) ? 1 : 0;
		oServiceCallCount.uddwTotalCallCost = dwTimeCost;
		oServiceCallCount.uddwSuccCallCost = (dwReturnCode == 0) ? dwTimeCost : 0;
		oServiceCallCount.uddwFailCallCost = (dwReturnCode == 0) ? 0 : dwTimeCost;
		oServiceCallCount.uddwMaxCallCost = oServiceCallCount.uddwSuccCallCost;
		oLogInfo.oMethodIdCallCount.insert(std::make_pair<int, ServiceCallCount>(method_id, oServiceCallCount));
        oLogInfo.oReturnCodeCount.insert(std::make_pair<int, int>(dwReturnCode, 1));
        oLogInfo.oCalleeIPCount.insert(std::make_pair<std::string, int>(strCalleeIP, 1));
        m_oAppRunningStatus.insert(std::make_pair<GSCCallRelation, GSCLogInfo>(oCallRelation, oLogInfo));
    }
    else
    {
		MethodIdCallCount::iterator itMCC = itLogInfo->second.oMethodIdCallCount.find(method_id);
		if (itMCC == itLogInfo->second.oMethodIdCallCount.end())
		{
			ServiceCallCount oServiceCallCount;
			oServiceCallCount.dwTotalCall = 1;
			oServiceCallCount.dwSuccCall += (dwReturnCode == 0) ? 1 : 0;
			oServiceCallCount.uddwTotalCallCost = dwTimeCost;
			oServiceCallCount.uddwSuccCallCost = (dwReturnCode == 0) ? dwTimeCost : 0;
			oServiceCallCount.uddwFailCallCost = (dwReturnCode == 0) ? 0 : dwTimeCost;
			oServiceCallCount.uddwMaxCallCost = oServiceCallCount.uddwSuccCallCost;
			itLogInfo->second.oMethodIdCallCount.insert(std::make_pair<int, ServiceCallCount>(method_id, oServiceCallCount));
		}
		else
		{
			++itMCC->second.dwTotalCall;
			itMCC->second.dwSuccCall += (dwReturnCode == 0) ? 1 : 0;
			itMCC->second.uddwTotalCallCost += dwTimeCost;
			itMCC->second.uddwSuccCallCost += (dwReturnCode == 0) ? dwTimeCost : 0;
			itMCC->second.uddwFailCallCost += (dwReturnCode == 0) ? 0 : dwTimeCost;
			if (dwReturnCode == 0)
			{
				itMCC->second.uddwMaxCallCost = MAX(itMCC->second.uddwMaxCallCost, dwTimeCost);
			}
		}

        ReturnCodeCount::iterator itRCC = itLogInfo->second.oReturnCodeCount.find(dwReturnCode);
        if (itRCC == itLogInfo->second.oReturnCodeCount.end())
        {
            itLogInfo->second.oReturnCodeCount.insert(std::make_pair<int, int>(dwReturnCode, 1));
        }
        else
        {
            ++itRCC->second;
        }

        CalleeIPCount::iterator itCalleeCount = itLogInfo->second.oCalleeIPCount.find(strCalleeIP);
        if (itCalleeCount == itLogInfo->second.oCalleeIPCount.end())
        {
            itLogInfo->second.oCalleeIPCount.insert(std::make_pair<std::string, int>(strCalleeIP, 1));
        }
        else
        {
            ++itCalleeCount->second;
        }
    }

    return 0;
}

int GSCLogParser::ParseLogLineVer1(const char * pszLogLine,const std::vector<std::string> & oList, int dwLineNumber)
{
    GSCCallRelation oCallRelation;
    // 版本1的日志格式如下，各字段以“|”分隔：
    // version | CallerID | CalleeID | MethodID | CalleeIP | CalleePort | ReturnCode| Count(在左边都相等的情况下，计数值) | StartTime | MinCost | MaxCost | TotalCost
    if (oList.size() != 12)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Parse line %d '%s' fail! split size=%d", dwLineNumber, pszLogLine, oList.size());
        return -1;
    }

    oCallRelation.strCallerID = oList[1];
    oCallRelation.strCalleeID = oList[2];
    time_t dwStart = (time_t)(atoll(oList[8].c_str()) / 1000000);
    if (0 == dwStart)
    {
        snprintf(m_szErrorMsg, sizeof(m_szErrorMsg), "Start time(%s) invalid!", oList[8].c_str());
        return -1;
    }
    // 将开始时间修正为整分钟的时间
    dwStart -= (dwStart >= m_dwStartMinute) ? (dwStart - m_dwStartMinute) % 60 : 60 - (m_dwStartMinute - dwStart) % 60;
    oCallRelation.dwStartTime = dwStart;

	int methodId = atoi(oList[3].c_str());
    std::string strCalleeIP = oList[4];
    int dwReturnCode = atoi(oList[6].c_str());
    int dwCallCount = atoi(oList[7].c_str());
    uint64_t uddwMaxCost = atoll(oList[10].c_str());
    uint64_t uddwTotalCost = atoll(oList[11].c_str());

    GSCLogInfoMap::iterator itLogInfo = m_oAppRunningStatus.find(oCallRelation);
    if (itLogInfo == m_oAppRunningStatus.end())
    {
        GSCLogInfo oLogInfo;
		
		ServiceCallCount oServiceCallCount;
        oServiceCallCount.dwTotalCall = dwCallCount;
        oServiceCallCount.dwSuccCall += (dwReturnCode == 0) ? dwCallCount : 0;
        oServiceCallCount.uddwTotalCallCost = uddwTotalCost;
        oServiceCallCount.uddwSuccCallCost = (dwReturnCode == 0) ? uddwTotalCost : 0;
        oServiceCallCount.uddwFailCallCost = (dwReturnCode == 0) ? 0 : uddwTotalCost;
        oServiceCallCount.uddwMaxCallCost = oServiceCallCount.uddwSuccCallCost;
		
		oLogInfo.oMethodIdCallCount.insert(std::make_pair<int, ServiceCallCount>(methodId, oServiceCallCount));
        oLogInfo.oReturnCodeCount.insert(std::make_pair<int, int>(dwReturnCode, dwCallCount));
        oLogInfo.oCalleeIPCount.insert(std::make_pair<std::string, int>(strCalleeIP, dwCallCount));
        m_oAppRunningStatus.insert(std::make_pair<GSCCallRelation, GSCLogInfo>(oCallRelation, oLogInfo));
    }
    else
    {
		MethodIdCallCount::iterator itMethodIdCallCount = itLogInfo->second.oMethodIdCallCount.find(methodId);
		if (itMethodIdCallCount == itLogInfo->second.oMethodIdCallCount.end())
		{
			ServiceCallCount oServiceCallCount;
			oServiceCallCount.dwTotalCall = dwCallCount;
			oServiceCallCount.dwSuccCall += (dwReturnCode == 0) ? dwCallCount : 0;
			oServiceCallCount.uddwTotalCallCost = uddwTotalCost;
			oServiceCallCount.uddwSuccCallCost = (dwReturnCode == 0) ? uddwTotalCost : 0;
			oServiceCallCount.uddwFailCallCost = (dwReturnCode == 0) ? 0 : uddwTotalCost;
			oServiceCallCount.uddwMaxCallCost = oServiceCallCount.uddwSuccCallCost;

			itLogInfo->second.oMethodIdCallCount.insert(std::make_pair<int, ServiceCallCount>(methodId, oServiceCallCount));
		}
		else
		{
	        itMethodIdCallCount->second.dwTotalCall += dwCallCount;
	        itMethodIdCallCount->second.dwSuccCall += (dwReturnCode == 0) ? dwCallCount : 0;
	        itMethodIdCallCount->second.uddwTotalCallCost += uddwTotalCost;
	        itMethodIdCallCount->second.uddwSuccCallCost += (dwReturnCode == 0) ? uddwTotalCost : 0;
	        itMethodIdCallCount->second.uddwFailCallCost += (dwReturnCode == 0) ? 0 : uddwTotalCost;
			if (dwReturnCode == 0)
			{
				itMethodIdCallCount->second.uddwMaxCallCost = MAX(itMethodIdCallCount->second.uddwMaxCallCost, uddwMaxCost);
			}
		}

        ReturnCodeCount::iterator itRCC = itLogInfo->second.oReturnCodeCount.find(dwReturnCode);
        if (itRCC == itLogInfo->second.oReturnCodeCount.end())
        {
            itLogInfo->second.oReturnCodeCount.insert(std::make_pair<int, int>(dwReturnCode, dwCallCount));
        }
        else
        {
            itRCC->second += dwCallCount;
        }

        CalleeIPCount::iterator itCalleeCount = itLogInfo->second.oCalleeIPCount.find(strCalleeIP);
        if (itCalleeCount == itLogInfo->second.oCalleeIPCount.end())
        {
            itLogInfo->second.oCalleeIPCount.insert(std::make_pair<std::string, int>(strCalleeIP, dwCallCount));
        }
        else
        {
            itCalleeCount->second += dwCallCount;
        }
    }

    return 0;
}

//////////////////////////////////////////////////////////////////////////
// 创建及销毁函数，各派生类统一使用create及destory的名字
extern "C" LogParserBase* create() 
{
    return new GSCLogParser;
}

extern "C" void destroy(LogParserBase* p) 
{
    delete p;
}

