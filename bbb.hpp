#include "base_proc.h"
#include <boost/bind/bind.hpp>

using namespace boost::placeholders;

void onRequest(const muduo::net::TcpConnectionPtr& conn, ParamMap& params, muduo::net::Buffer* in);

void onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        typedef boost::shared_ptr<FastCgiCodec> CodecPtr;
        CodecPtr codec(new FastCgiCodec(onRequest));
        conn->setContext(codec);
        conn->setMessageCallback(boost::bind(&FastCgiCodec::onMessage, codec, _1, _2, _3));
        conn->setTcpNoDelay(true);
    }
}

void ParseParam(ParamMap& params, muduo::net::Buffer* in, SSMap& qmap, SSMap& header)
{
    std::string queryString = params["QUERY_STRING"];
    std::string queryType   = params["CONTENT_TYPE"];
    std::string queryCookie = params["HTTP_COOKIE"];
    std::string postData;
    if (in->readableBytes() > 0)
    {
        postData = in->retrieveAllAsString();
    }

    // Parse Url String
    // Parse Post Data With Standard content type = application/x-www-form-urlencoded 
    cgicc::Cgicc cgi(queryString, queryCookie, postData, queryType);
    cgicc::const_form_iterator iterE;
    for(iterE = cgi.getElements().begin(); iterE != cgi.getElements().end(); ++iterE) 
    {   
        std::string key = boost::to_lower_copy(iterE->getName());
        qmap.insert(SSMap::value_type(key, iterE->getValue()));
        //LOG_DEBUG << "Query Element:" << key << " Value:" << iterE->getValue();
    }

    // Parse File Uplod, File upload type = multipart/form-data 
    cgicc::const_file_iterator  iterF;
    for(iterF = cgi.getFiles().begin(); iterF != cgi.getFiles().end(); ++iterF)
    {
        qmap.insert(SSMap::value_type(iterF->getName(), iterF->getData()));
    }

    for (ParamMap::const_iterator it = params.begin(); it != params.end(); ++it)
    {
        header[it->first] = it->second;
        //LOG_DEBUG << "Query Headers " << it->first << " = " << it->second;
    }

    // Parse Cookie List
    cgicc::const_cookie_iterator iterV;
    for(iterV = cgi.getCookieList().begin(); iterV != cgi.getCookieList().end(); ++iterV)
    {
        header[iterV->getName()] = iterV->getValue();
        //LOG_DEBUG << "Cookie Param: Name["<< iterV->getName() << "] Value[" << iterV->getValue() << "].";
    }
}

void GetScriptNameByUrl(SSMap& httpHeader, std::string& strScriptName)
{
	strScriptName = zb_common::TrimRight(httpHeader["DOCUMENT_URI"], "/");
    size_t pos = strScriptName.find_last_of('/');
    if (pos != std::string::npos)
    {
        strScriptName = strScriptName.substr(pos+1);
    }
}

std::string GenHttpResp(HttpStatusEnum http_code, std::string& resp)
{
	std::ostringstream httpresp;
    if (http_code == OK)
    {
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
        httpresp << "Content-Length: " << resp.length() << "\r\n\r\n";
        httpresp << resp;
    }
    else
    {
        httpresp << "Status: " << http_code << "\r\n";
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
	    httpresp << "\r\n";
    }
    return httpresp.str();
}

bool CheckPermission(const SSMap& qmap, const SSMap& header)
{
	return true;
}

#include "base_proc.h"
#include <boost/bind/bind.hpp>

using namespace boost::placeholders;

void onRequest(const muduo::net::TcpConnectionPtr& conn, ParamMap& params, muduo::net::Buffer* in);

void onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        typedef boost::shared_ptr<FastCgiCodec> CodecPtr;
        CodecPtr codec(new FastCgiCodec(onRequest));
        conn->setContext(codec);
        conn->setMessageCallback(boost::bind(&FastCgiCodec::onMessage, codec, _1, _2, _3));
        conn->setTcpNoDelay(true);
    }
}

void ParseParam(ParamMap& params, muduo::net::Buffer* in, SSMap& qmap, SSMap& header)
{
    std::string queryString = params["QUERY_STRING"];
    std::string queryType   = params["CONTENT_TYPE"];
    std::string queryCookie = params["HTTP_COOKIE"];
    std::string postData;
    if (in->readableBytes() > 0)
    {
        postData = in->retrieveAllAsString();
    }

    // Parse Url String
    // Parse Post Data With Standard content type = application/x-www-form-urlencoded 
    cgicc::Cgicc cgi(queryString, queryCookie, postData, queryType);
    cgicc::const_form_iterator iterE;
    for(iterE = cgi.getElements().begin(); iterE != cgi.getElements().end(); ++iterE) 
    {   
        std::string key = boost::to_lower_copy(iterE->getName());
        qmap.insert(SSMap::value_type(key, iterE->getValue()));
        //LOG_DEBUG << "Query Element:" << key << " Value:" << iterE->getValue();
    }

    // Parse File Uplod, File upload type = multipart/form-data 
    cgicc::const_file_iterator  iterF;
    for(iterF = cgi.getFiles().begin(); iterF != cgi.getFiles().end(); ++iterF)
    {
        qmap.insert(SSMap::value_type(iterF->getName(), iterF->getData()));
    }

    for (ParamMap::const_iterator it = params.begin(); it != params.end(); ++it)
    {
        header[it->first] = it->second;
        //LOG_DEBUG << "Query Headers " << it->first << " = " << it->second;
    }

    // Parse Cookie List
    cgicc::const_cookie_iterator iterV;
    for(iterV = cgi.getCookieList().begin(); iterV != cgi.getCookieList().end(); ++iterV)
    {
        header[iterV->getName()] = iterV->getValue();
        //LOG_DEBUG << "Cookie Param: Name["<< iterV->getName() << "] Value[" << iterV->getValue() << "].";
    }
}

void GetScriptNameByUrl(SSMap& httpHeader, std::string& strScriptName)
{
	strScriptName = zb_common::TrimRight(httpHeader["DOCUMENT_URI"], "/");
    size_t pos = strScriptName.find_last_of('/');
    if (pos != std::string::npos)
    {
        strScriptName = strScriptName.substr(pos+1);
    }
}

std::string GenHttpResp(HttpStatusEnum http_code, std::string& resp)
{
	std::ostringstream httpresp;
    if (http_code == OK)
    {
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
        httpresp << "Content-Length: " << resp.length() << "\r\n\r\n";
        httpresp << resp;
    }
    else
    {
        httpresp << "Status: " << http_code << "\r\n";
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
	    httpresp << "\r\n";
    }
    return httpresp.str();
}

bool CheckPermission(const SSMap& qmap, const SSMap& header)
{
	return true;
}

#include "base_proc.h"
#include <boost/bind/bind.hpp>

using namespace boost::placeholders;

void onRequest(const muduo::net::TcpConnectionPtr& conn, ParamMap& params, muduo::net::Buffer* in);

void onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        typedef boost::shared_ptr<FastCgiCodec> CodecPtr;
        CodecPtr codec(new FastCgiCodec(onRequest));
        conn->setContext(codec);
        conn->setMessageCallback(boost::bind(&FastCgiCodec::onMessage, codec, _1, _2, _3));
        conn->setTcpNoDelay(true);
    }
}

void ParseParam(ParamMap& params, muduo::net::Buffer* in, SSMap& qmap, SSMap& header)
{
    std::string queryString = params["QUERY_STRING"];
    std::string queryType   = params["CONTENT_TYPE"];
    std::string queryCookie = params["HTTP_COOKIE"];
    std::string postData;
    if (in->readableBytes() > 0)
    {
        postData = in->retrieveAllAsString();
    }

    // Parse Url String
    // Parse Post Data With Standard content type = application/x-www-form-urlencoded 
    cgicc::Cgicc cgi(queryString, queryCookie, postData, queryType);
    cgicc::const_form_iterator iterE;
    for(iterE = cgi.getElements().begin(); iterE != cgi.getElements().end(); ++iterE) 
    {   
        std::string key = boost::to_lower_copy(iterE->getName());
        qmap.insert(SSMap::value_type(key, iterE->getValue()));
        //LOG_DEBUG << "Query Element:" << key << " Value:" << iterE->getValue();
    }

    // Parse File Uplod, File upload type = multipart/form-data 
    cgicc::const_file_iterator  iterF;
    for(iterF = cgi.getFiles().begin(); iterF != cgi.getFiles().end(); ++iterF)
    {
        qmap.insert(SSMap::value_type(iterF->getName(), iterF->getData()));
    }

    for (ParamMap::const_iterator it = params.begin(); it != params.end(); ++it)
    {
        header[it->first] = it->second;
        //LOG_DEBUG << "Query Headers " << it->first << " = " << it->second;
    }

    // Parse Cookie List
    cgicc::const_cookie_iterator iterV;
    for(iterV = cgi.getCookieList().begin(); iterV != cgi.getCookieList().end(); ++iterV)
    {
        header[iterV->getName()] = iterV->getValue();
        //LOG_DEBUG << "Cookie Param: Name["<< iterV->getName() << "] Value[" << iterV->getValue() << "].";
    }
}

void GetScriptNameByUrl(SSMap& httpHeader, std::string& strScriptName)
{
	strScriptName = zb_common::TrimRight(httpHeader["DOCUMENT_URI"], "/");
    size_t pos = strScriptName.find_last_of('/');
    if (pos != std::string::npos)
    {
        strScriptName = strScriptName.substr(pos+1);
    }
}

std::string GenHttpResp(HttpStatusEnum http_code, std::string& resp)
{
	std::ostringstream httpresp;
    if (http_code == OK)
    {
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
        httpresp << "Content-Length: " << resp.length() << "\r\n\r\n";
        httpresp << resp;
    }
    else
    {
        httpresp << "Status: " << http_code << "\r\n";
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
	    httpresp << "\r\n";
    }
    return httpresp.str();
}

bool CheckPermission(const SSMap& qmap, const SSMap& header)
{
	return true;
}

#include "base_proc.h"
#include <boost/bind/bind.hpp>

using namespace boost::placeholders;

void onRequest(const muduo::net::TcpConnectionPtr& conn, ParamMap& params, muduo::net::Buffer* in);

void onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        typedef boost::shared_ptr<FastCgiCodec> CodecPtr;
        CodecPtr codec(new FastCgiCodec(onRequest));
        conn->setContext(codec);
        conn->setMessageCallback(boost::bind(&FastCgiCodec::onMessage, codec, _1, _2, _3));
        conn->setTcpNoDelay(true);
    }
}

void ParseParam(ParamMap& params, muduo::net::Buffer* in, SSMap& qmap, SSMap& header)
{
    std::string queryString = params["QUERY_STRING"];
    std::string queryType   = params["CONTENT_TYPE"];
    std::string queryCookie = params["HTTP_COOKIE"];
    std::string postData;
    if (in->readableBytes() > 0)
    {
        postData = in->retrieveAllAsString();
    }

    // Parse Url String
    // Parse Post Data With Standard content type = application/x-www-form-urlencoded 
    cgicc::Cgicc cgi(queryString, queryCookie, postData, queryType);
    cgicc::const_form_iterator iterE;
    for(iterE = cgi.getElements().begin(); iterE != cgi.getElements().end(); ++iterE) 
    {   
        std::string key = boost::to_lower_copy(iterE->getName());
        qmap.insert(SSMap::value_type(key, iterE->getValue()));
        //LOG_DEBUG << "Query Element:" << key << " Value:" << iterE->getValue();
    }

    // Parse File Uplod, File upload type = multipart/form-data 
    cgicc::const_file_iterator  iterF;
    for(iterF = cgi.getFiles().begin(); iterF != cgi.getFiles().end(); ++iterF)
    {
        qmap.insert(SSMap::value_type(iterF->getName(), iterF->getData()));
    }

    for (ParamMap::const_iterator it = params.begin(); it != params.end(); ++it)
    {
        header[it->first] = it->second;
        //LOG_DEBUG << "Query Headers " << it->first << " = " << it->second;
    }

    // Parse Cookie List
    cgicc::const_cookie_iterator iterV;
    for(iterV = cgi.getCookieList().begin(); iterV != cgi.getCookieList().end(); ++iterV)
    {
        header[iterV->getName()] = iterV->getValue();
        //LOG_DEBUG << "Cookie Param: Name["<< iterV->getName() << "] Value[" << iterV->getValue() << "].";
    }
}

void GetScriptNameByUrl(SSMap& httpHeader, std::string& strScriptName)
{
	strScriptName = zb_common::TrimRight(httpHeader["DOCUMENT_URI"], "/");
    size_t pos = strScriptName.find_last_of('/');
    if (pos != std::string::npos)
    {
        strScriptName = strScriptName.substr(pos+1);
    }
}

std::string GenHttpResp(HttpStatusEnum http_code, std::string& resp)
{
	std::ostringstream httpresp;
    if (http_code == OK)
    {
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
        httpresp << "Content-Length: " << resp.length() << "\r\n\r\n";
        httpresp << resp;
    }
    else
    {
        httpresp << "Status: " << http_code << "\r\n";
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
	    httpresp << "\r\n";
    }
    return httpresp.str();
}

bool CheckPermission(const SSMap& qmap, const SSMap& header)
{
	return true;
}

#include "base_proc.h"
#include <boost/bind/bind.hpp>

using namespace boost::placeholders;

void onRequest(const muduo::net::TcpConnectionPtr& conn, ParamMap& params, muduo::net::Buffer* in);

void onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        typedef boost::shared_ptr<FastCgiCodec> CodecPtr;
        CodecPtr codec(new FastCgiCodec(onRequest));
        conn->setContext(codec);
        conn->setMessageCallback(boost::bind(&FastCgiCodec::onMessage, codec, _1, _2, _3));
        conn->setTcpNoDelay(true);
    }
}

void ParseParam(ParamMap& params, muduo::net::Buffer* in, SSMap& qmap, SSMap& header)
{
    std::string queryString = params["QUERY_STRING"];
    std::string queryType   = params["CONTENT_TYPE"];
    std::string queryCookie = params["HTTP_COOKIE"];
    std::string postData;
    if (in->readableBytes() > 0)
    {
        postData = in->retrieveAllAsString();
    }

    // Parse Url String
    // Parse Post Data With Standard content type = application/x-www-form-urlencoded 
    cgicc::Cgicc cgi(queryString, queryCookie, postData, queryType);
    cgicc::const_form_iterator iterE;
    for(iterE = cgi.getElements().begin(); iterE != cgi.getElements().end(); ++iterE) 
    {   
        std::string key = boost::to_lower_copy(iterE->getName());
        qmap.insert(SSMap::value_type(key, iterE->getValue()));
        //LOG_DEBUG << "Query Element:" << key << " Value:" << iterE->getValue();
    }

    // Parse File Uplod, File upload type = multipart/form-data 
    cgicc::const_file_iterator  iterF;
    for(iterF = cgi.getFiles().begin(); iterF != cgi.getFiles().end(); ++iterF)
    {
        qmap.insert(SSMap::value_type(iterF->getName(), iterF->getData()));
    }

    for (ParamMap::const_iterator it = params.begin(); it != params.end(); ++it)
    {
        header[it->first] = it->second;
        //LOG_DEBUG << "Query Headers " << it->first << " = " << it->second;
    }

    // Parse Cookie List
    cgicc::const_cookie_iterator iterV;
    for(iterV = cgi.getCookieList().begin(); iterV != cgi.getCookieList().end(); ++iterV)
    {
        header[iterV->getName()] = iterV->getValue();
        //LOG_DEBUG << "Cookie Param: Name["<< iterV->getName() << "] Value[" << iterV->getValue() << "].";
    }
}

void GetScriptNameByUrl(SSMap& httpHeader, std::string& strScriptName)
{
	strScriptName = zb_common::TrimRight(httpHeader["DOCUMENT_URI"], "/");
    size_t pos = strScriptName.find_last_of('/');
    if (pos != std::string::npos)
    {
        strScriptName = strScriptName.substr(pos+1);
    }
}

std::string GenHttpResp(HttpStatusEnum http_code, std::string& resp)
{
	std::ostringstream httpresp;
    if (http_code == OK)
    {
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
        httpresp << "Content-Length: " << resp.length() << "\r\n\r\n";
        httpresp << resp;
    }
    else
    {
        httpresp << "Status: " << http_code << "\r\n";
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
	    httpresp << "\r\n";
    }
    return httpresp.str();
}

bool CheckPermission(const SSMap& qmap, const SSMap& header)
{
	return true;
}

#include "base_proc.h"
#include <boost/bind/bind.hpp>

using namespace boost::placeholders;

void onRequest(const muduo::net::TcpConnectionPtr& conn, ParamMap& params, muduo::net::Buffer* in);

void onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        typedef boost::shared_ptr<FastCgiCodec> CodecPtr;
        CodecPtr codec(new FastCgiCodec(onRequest));
        conn->setContext(codec);
        conn->setMessageCallback(boost::bind(&FastCgiCodec::onMessage, codec, _1, _2, _3));
        conn->setTcpNoDelay(true);
    }
}

void ParseParam(ParamMap& params, muduo::net::Buffer* in, SSMap& qmap, SSMap& header)
{
    std::string queryString = params["QUERY_STRING"];
    std::string queryType   = params["CONTENT_TYPE"];
    std::string queryCookie = params["HTTP_COOKIE"];
    std::string postData;
    if (in->readableBytes() > 0)
    {
        postData = in->retrieveAllAsString();
    }

    // Parse Url String
    // Parse Post Data With Standard content type = application/x-www-form-urlencoded 
    cgicc::Cgicc cgi(queryString, queryCookie, postData, queryType);
    cgicc::const_form_iterator iterE;
    for(iterE = cgi.getElements().begin(); iterE != cgi.getElements().end(); ++iterE) 
    {   
        std::string key = boost::to_lower_copy(iterE->getName());
        qmap.insert(SSMap::value_type(key, iterE->getValue()));
        //LOG_DEBUG << "Query Element:" << key << " Value:" << iterE->getValue();
    }

    // Parse File Uplod, File upload type = multipart/form-data 
    cgicc::const_file_iterator  iterF;
    for(iterF = cgi.getFiles().begin(); iterF != cgi.getFiles().end(); ++iterF)
    {
        qmap.insert(SSMap::value_type(iterF->getName(), iterF->getData()));
    }

    for (ParamMap::const_iterator it = params.begin(); it != params.end(); ++it)
    {
        header[it->first] = it->second;
        //LOG_DEBUG << "Query Headers " << it->first << " = " << it->second;
    }

    // Parse Cookie List
    cgicc::const_cookie_iterator iterV;
    for(iterV = cgi.getCookieList().begin(); iterV != cgi.getCookieList().end(); ++iterV)
    {
        header[iterV->getName()] = iterV->getValue();
        //LOG_DEBUG << "Cookie Param: Name["<< iterV->getName() << "] Value[" << iterV->getValue() << "].";
    }
}

void GetScriptNameByUrl(SSMap& httpHeader, std::string& strScriptName)
{
	strScriptName = zb_common::TrimRight(httpHeader["DOCUMENT_URI"], "/");
    size_t pos = strScriptName.find_last_of('/');
    if (pos != std::string::npos)
    {
        strScriptName = strScriptName.substr(pos+1);
    }
}

std::string GenHttpResp(HttpStatusEnum http_code, std::string& resp)
{
	std::ostringstream httpresp;
    if (http_code == OK)
    {
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
        httpresp << "Content-Length: " << resp.length() << "\r\n\r\n";
        httpresp << resp;
    }
    else
    {
        httpresp << "Status: " << http_code << "\r\n";
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
	    httpresp << "\r\n";
    }
    return httpresp.str();
}

bool CheckPermission(const SSMap& qmap, const SSMap& header)
{
	return true;
}

#include "base_proc.h"
#include <boost/bind/bind.hpp>

using namespace boost::placeholders;

void onRequest(const muduo::net::TcpConnectionPtr& conn, ParamMap& params, muduo::net::Buffer* in);

void onConnection(const muduo::net::TcpConnectionPtr& conn)
{
    if (conn->connected())
    {
        typedef boost::shared_ptr<FastCgiCodec> CodecPtr;
        CodecPtr codec(new FastCgiCodec(onRequest));
        conn->setContext(codec);
        conn->setMessageCallback(boost::bind(&FastCgiCodec::onMessage, codec, _1, _2, _3));
        conn->setTcpNoDelay(true);
    }
}

void ParseParam(ParamMap& params, muduo::net::Buffer* in, SSMap& qmap, SSMap& header)
{
    std::string queryString = params["QUERY_STRING"];
    std::string queryType   = params["CONTENT_TYPE"];
    std::string queryCookie = params["HTTP_COOKIE"];
    std::string postData;
    if (in->readableBytes() > 0)
    {
        postData = in->retrieveAllAsString();
    }

    // Parse Url String
    // Parse Post Data With Standard content type = application/x-www-form-urlencoded 
    cgicc::Cgicc cgi(queryString, queryCookie, postData, queryType);
    cgicc::const_form_iterator iterE;
    for(iterE = cgi.getElements().begin(); iterE != cgi.getElements().end(); ++iterE) 
    {   
        std::string key = boost::to_lower_copy(iterE->getName());
        qmap.insert(SSMap::value_type(key, iterE->getValue()));
        //LOG_DEBUG << "Query Element:" << key << " Value:" << iterE->getValue();
    }

    // Parse File Uplod, File upload type = multipart/form-data 
    cgicc::const_file_iterator  iterF;
    for(iterF = cgi.getFiles().begin(); iterF != cgi.getFiles().end(); ++iterF)
    {
        qmap.insert(SSMap::value_type(iterF->getName(), iterF->getData()));
    }

    for (ParamMap::const_iterator it = params.begin(); it != params.end(); ++it)
    {
        header[it->first] = it->second;
        //LOG_DEBUG << "Query Headers " << it->first << " = " << it->second;
    }

    // Parse Cookie List
    cgicc::const_cookie_iterator iterV;
    for(iterV = cgi.getCookieList().begin(); iterV != cgi.getCookieList().end(); ++iterV)
    {
        header[iterV->getName()] = iterV->getValue();
        //LOG_DEBUG << "Cookie Param: Name["<< iterV->getName() << "] Value[" << iterV->getValue() << "].";
    }
}

void GetScriptNameByUrl(SSMap& httpHeader, std::string& strScriptName)
{
	strScriptName = zb_common::TrimRight(httpHeader["DOCUMENT_URI"], "/");
    size_t pos = strScriptName.find_last_of('/');
    if (pos != std::string::npos)
    {
        strScriptName = strScriptName.substr(pos+1);
    }
}

std::string GenHttpResp(HttpStatusEnum http_code, std::string& resp)
{
	std::ostringstream httpresp;
    if (http_code == OK)
    {
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
        httpresp << "Content-Length: " << resp.length() << "\r\n\r\n";
        httpresp << resp;
    }
    else
    {
        httpresp << "Status: " << http_code << "\r\n";
        httpresp << "Content-Type: text/html; charset=UTF-8\r\n";
	    httpresp << "\r\n";
    }
    return httpresp.str();
}

bool CheckPermission(const SSMap& qmap, const SSMap& header)
{
	return true;
}

