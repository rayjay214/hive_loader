#include <GPSFormat.h>
#include <Ice/Connection.h>
#include <stdlib.h>
#include "global.h"
#include "mycommon.h"
#include "SyncGmcThread.h"
#include "FakeGMCMgmtImpl.h"
#include "tracelog.h"

extern CGmcDataImporterTraceLog * g_trace_client;

GMCMgmtImpl::GMCMgmtImpl()
{

}

GMCMgmtImpl::~GMCMgmtImpl()
{
	// TODO Auto-generated destructor stub
}

bool GMCMgmtImpl::set(const ::GMC::GPSONES& ones, const ::Ice::Current& cur)
{
	MYLOG_INFO(g_logger, "Get [%d] records from proxy, conn:%s",
			ones.size(),
            std::string(cur.con->toString().erase(cur.con->toString().find_first_of('\n'), 1)).c_str());

	SetGpsOneRecords(ones);
	return true; // always true ...
}

void GMCMgmtImpl::get(const ::GMC::UIDSEQ& uids, ::GMC::GPSONES& ones,
		const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::getLatest(::Ice::Long hBTime, ::GMC::GPSONES& ones,
		const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::rectSearch(::Ice::Int x1, ::Ice::Int y1, ::Ice::Int x2,
		::Ice::Int y2, ::Ice::Int dur, ::Ice::Int limit, ::GMC::GPSONES& ones,
		const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::rectSearchWithType(::Ice::Int x1, ::Ice::Int y1, ::Ice::Int x2,
                                     ::Ice::Int y2, ::Ice::Int dur, ::Ice::Int limit,
                                     const ::GMC::StringVector& devtype, ::Ice::Int devflag,
                                     ::GMC::GPSONES& ones, const ::Ice::Current& cur)
{

}

void GMCMgmtImpl::getOnLineNum(::Ice::Int dur, ::Ice::Int& num,
		const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::getWithTimeRange(::Ice::Long begin, ::Ice::Long end,
		::Ice::Int limit, ::GMC::GPSONES& ones, const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::getStatus(::std::string& status, const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::setStatus(const ::std::string& status,
		const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::getProps(const ::GMC::PropName& names, ::GMC::Prop& props,
		const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::setProps(const ::GMC::Prop& props, const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::getRunningStatus(const ::GMC::PropName& status,
		::GMC::Prop& result, const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::setRunningStatus(const ::GMC::Prop& status,
		const ::Ice::Current& cur)
{
}

void GMCMgmtImpl::setCapcity(int64_t capacity)
{
	if (capacity > 0)
	{
		_capacity = capacity;
		return;
	}

	_capacity = 0;

	MYLOG_INFO(g_logger, "GMC capacity is %lld", _capacity);
}

void GMCMgmtImpl::setStartUid(int64_t uid)
{
	_minUid = uid;
	_maxUid = uid + _capacity - 1;

	MYLOG_INFO(g_logger, "GMC uid map range is [%lld - %lld]", _minUid, _maxUid);
}


void GMCMgmtImpl::GetGpsOneRecords(const ::GMC::UIDSEQ& uids,
		::GMC::GPSONES& ones)
{
}

void GMCMgmtImpl::SetGpsOneRecords(const GMQ::Table& content)
{
	GMQ::Table::const_iterator it;
	std::string one;

	for (it = content.begin(); it != content.end(); ++it)
	{
		if (it->size() != sizeof(GPSVO_V1))
		{
			MYLOG_ERROR(g_logger, "%s: data size[%d] != sizeof GPSVO_V1[%d]", __PRETTY_FUNCTION__,
					it->size(), sizeof(GPSVO_V1));
			continue;
		}

		GPSVO_V1 *gps = (GPSVO_V1 *) &(*it)[0];
		SetGpsVo(gps);
	}
}

void GMCMgmtImpl::SetGpsVo(const GPSVO_V1 *gps)
{
	if (!gps)
	{
		MYLOG_ERROR(g_logger, "gps is null");
		return;
	}

	DumpGpsOneRecordV1(gps, __PRETTY_FUNCTION__);

	int64_t uid = ntoh64(gps->vo.uid);
	if (uid < _minUid || uid > _maxUid)
	{
		return;
	}

    if (g_trace_client->check(uid))
    {
        g_trace_client->DLog("fields = %04X, uid = %lld, dateTime = %llu, sysTime = %llu, heartTime = %llu, "
            "longitude = %d, latitude = %d, speed = %d, route = %d, "
            "seq = %d, sid = %llu, endPoint = %s",
            ntoh32(gps->fields),
            uid, ntoh64(gps->vo.dateTime), ntoh64(gps->vo.sysTime), ntoh64(gps->vo.heartTime),
            ntoh32(gps->vo.longitude), ntoh32(gps->vo.latitude), ntoh32(gps->vo.speed), ntoh32(gps->vo.route),
            ntoh32(gps->vo.seq), ntoh64(gps->vo.sid), gps->vo.endPoint);
    }

    g_oGpsDataList.AddData(gps->vo);
}


void GMCMgmtImpl::DumpGpsOneRecordV1(const GPSVO_V1 *gps, std::string prompt)
{
	assert(gps);
}


void GMCMgmtImpl::setSyncReq(SyncGmcThreadPtr syncReqToProxy)
{
	_syncReqToProxy = syncReqToProxy;
}

bool GMCMgmtImpl::syncFullData(const ::Ice::Current& cur)
{
	if (_syncReqToProxy)
		return _syncReqToProxy->syncFullData();

	return false;
}
