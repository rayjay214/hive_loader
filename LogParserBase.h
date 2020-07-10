#ifndef _LOG_PARSER_BASE_H_
#define _LOG_PARSER_BASE_H_

// ��־����ģ��ӿ��ֻ࣬���̳��á�
class LogParserBase {
public:
    LogParserBase()
    {
        memset(m_szErrorMsg, 0, sizeof(m_szErrorMsg));
        memset(m_szModuleName, 0, sizeof(m_szModuleName));
    }

    virtual ~LogParserBase() {}

    // �������ܣ�   ��ʼ����ģ��
    // ���������   oIC    ICE����������
    // ����ֵ��     ����ʼ���ɹ�������0�����򷵻�-1
    virtual int Init(Ice::CommunicatorPtr & oIC)
    {
        return 0;
    }

    // �������ܣ�   ��ȡģ�����ƣ����ⲿģ�����
    // ����ֵ��     ģ�������ַ���ָ��
    virtual const char * GetModuleName()
    {
        return m_szModuleName;
    }

    // �������ܣ�   ��ȡ������Ϣ�����ⲿģ�����
    // ����ֵ��     ������Ϣ�ַ���ָ��
    virtual const char * GetLastError()
    {
        return m_szErrorMsg;
    }

    // �������ܣ�   ��һ������־���н���ǰ�Ĳ���
    // ����ֵ��     �������ɹ�������0�����򷵻�-1
    virtual int StartingParseSingleLog()
    {
        return 0;
    }

    // �������ܣ�   �Ե�����־�ļ�������Ϻ�Ĳ���
    // ����ֵ��     �������ɹ�������0�����򷵻�-1
    virtual int ParseSingleLogFinish()
    {
        return 0;
    }

    // �������ܣ�   ��������LOG��Ϻ���õĲ���
    // ����ֵ��     �������ɹ�������0�����򷵻�-1
    virtual int AfterAllParsed()
    {
        return 0;
    }

    // �������ܣ�   ����������־������
    // ���������   pszLogLine      ������־������
    // ���������   dwLineNumber    ��ǰ������־�ļ��е��к�
    // ����ֵ��     �������ɹ�������0�����򷵻�-1
    virtual int ParseLogLine(const char * pszLogLine, int dwLineNumber) = 0;

protected:

    char m_szErrorMsg[1024];        // ���������Ϣ

    char m_szModuleName[128];       // ģ������
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


