#ifndef _LOG_PARSER_BASE_H_
#define _LOG_PARSER_BASE_H_

// 日志分析模块接口类，只供继承用。
class LogParserBase {
public:
    LogParserBase()
    {
        memset(m_szErrorMsg, 0, sizeof(m_szErrorMsg));
        memset(m_szModuleName, 0, sizeof(m_szModuleName));
    }

    virtual ~LogParserBase() {}

    // 函数功能：   初始化本模块
    // 输入参数：   oIC    ICE对象操作句柄
    // 返回值：     若初始化成功，返回0，否则返回-1
    virtual int Init(Ice::CommunicatorPtr & oIC)
    {
        return 0;
    }

    // 函数功能：   获取模块名称，供外部模块输出
    // 返回值：     模块名称字符串指针
    virtual const char * GetModuleName()
    {
        return m_szModuleName;
    }

    // 函数功能：   获取错误消息，供外部模块输出
    // 返回值：     错误消息字符串指针
    virtual const char * GetLastError()
    {
        return m_szErrorMsg;
    }

    // 函数功能：   打开一个新日志进行解析前的操作
    // 返回值：     若操作成功，返回0，否则返回-1
    virtual int StartingParseSingleLog()
    {
        return 0;
    }

    // 函数功能：   对单个日志文件解析完毕后的操作
    // 返回值：     若操作成功，返回0，否则返回-1
    virtual int ParseSingleLogFinish()
    {
        return 0;
    }

    // 函数功能：   解析所有LOG完毕后调用的操作
    // 返回值：     若操作成功，返回0，否则返回-1
    virtual int AfterAllParsed()
    {
        return 0;
    }

    // 函数功能：   解析单行日志的内容
    // 输入参数：   pszLogLine      单行日志的内容
    // 输入参数：   dwLineNumber    当前行在日志文件中的行号
    // 返回值：     若操作成功，返回0，否则返回-1
    virtual int ParseLogLine(const char * pszLogLine, int dwLineNumber) = 0;

protected:

    char m_szErrorMsg[1024];        // 保存错误消息

    char m_szModuleName[128];       // 模块名称
};


// the types of the class factories
#ifdef __cplusplus
extern "C" {
#endif
    typedef LogParserBase* create_t();
    typedef void destroy_t(LogParserBase*);

#ifdef __cplusplus
}
#endif



#endif  // _LOG_PARSER_BASE_H_


