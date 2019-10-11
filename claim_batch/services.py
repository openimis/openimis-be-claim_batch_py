import xml.etree.ElementTree as ET

import core
from django.db import connection


@core.comparable
class ProcessBatchSubmit(object):
    def __init__(self, location_id, year, month):
        self.location_id = location_id
        self.year = year
        self.month = month


@core.comparable
class ProcessBatchSubmitError(Exception):
    ERROR_CODES = {
        1: "General fault",
        2: "Already run before",
    }

    def __init__(self, code, msg=None):
        self.code = code
        self.msg = ProcessBatchSubmitError.ERROR_CODES.get(
            self.code, msg or "Unknown exception")

    def __str__(self):
        return "ProcessBatchSubmitError %s: %s" % (self.code, self.msg)


class ProcessBatchService(object):

    def __init__(self, user):
        self.user = user

    def submit(self, submit):
        with connection.cursor() as cur:
            sql = """\
                DECLARE @ret int;
                EXEC @ret = [dbo].[uspBatchProcess] @AuditUser = %s, @LocationId = %s, @Year = %s, @Period = %s;
                SELECT @ret;
            """
            cur.execute(sql, (self.user.i_user.id, submit.location_id,
                              submit.year, submit.month))
            # stored proc outputs several results,
            # we are only interested in the last one
            next = True
            res = None
            while next:
                try:
                    res = cur.fetchone()
                except:
                    pass
                finally:
                    next = cur.nextset()
            if res[0]:  # zero means "all done"
                raise ProcessBatchSubmitError(res[0])


def process_batch_report_data_with_claims(prms):
    with connection.cursor() as cur:
        sql = """\
            EXEC [dbo].[uspSSRSProcessBatchWithClaim]
                @LocationId = %s,
                @ProdID = %s,
                @RunID = %s,
                @HFID = %s,
                @HFLevel = %s,
                @DateFrom = %s,
                @DateTo = %s
        """
        cur.execute(sql, (
            prms.get('locationId', 0),
            prms.get('prodId', 0),
            prms.get('runId', 0),
            prms.get('hfId', 0),
            prms.get('hfLevel', ''),
            prms.get('dateFrom', ''),
            prms.get('dateTo', '')
        ))
        # stored proc outputs several results,
        # we are only interested in the last one
        next = True
        data = None
        while next:
            try:
                data = cur.fetchall()
            except:
                pass
            finally:
                next = cur.nextset()
    return [{
        "ClaimCode": r[0],
        "DateClaimed": r[1].strftime("%Y-%m-%d") if r[1] is not None else None,
        "OtherNamesAdmin": r[2],
        "LastNameAdmin": r[3],
        "DateFrom": r[4].strftime("%Y-%m-%d") if r[4] is not None else None,
        "DateTo": r[5].strftime("%Y-%m-%d") if r[5] is not None else None,
        "CHFID": r[6],
        "OtherNames": r[7],
        "LastName": r[8],
        "HFID": r[9],
        "HFCode": r[10],
        "HFName": r[11],
        "AccCode": r[12],
        "ProdID": r[13],
        "ProductCode": r[14],
        "ProductName": r[15],
        "PriceAsked": r[16],
        "PriceApproved": r[17],
        "PriceAdjusted": r[18],
        "RemuneratedAmount": r[19],
        "DistrictID": r[20],
        "DistrictName": r[21],
        "RegionID": r[22],
        "RegionName": r[23]
    } for r in data]


def process_batch_report_data(prms):
    with connection.cursor() as cur:
        sql = """\
            EXEC [dbo].[uspSSRSProcessBatch]
                @LocationId = %s,
                @ProdID = %s,
                @RunID = %s,
                @HFID = %s,
                @HFLevel = %s,
                @DateFrom = %s,
                @DateTo = %s
        """
        cur.execute(sql, (
            prms.get('locationId', 0),
            prms.get('prodId', 0),
            prms.get('runId', 0),
            prms.get('hfId', 0),
            prms.get('hfLevel', ''),
            prms.get('dateFrom', ''),
            prms.get('dateTo', '')
        ))
        # stored proc outputs several results,
        # we are only interested in the last one
        next = True
        data = None
        while next:
            try:
                data = cur.fetchall()
            except:
                pass
            finally:
                next = cur.nextset()
    return [{
        "RegionName": r[0],
        "DistrictName": r[1],
        "HFCode": r[2],
        "HFName": r[3],
        "ProductCode": r[4],
        "ProductName": r[5],
        "Remunerated": r[6],
        "AccCodeRemuneration": r[7],
        "AccCode": r[8]
    } for r in data]


class ReportDataService(object):
    def __init__(self, user):
        self.user = user

    def fetch(self, prms):
        show_claims = prms.get("showClaims", "false") == "true"
        if (show_claims):
            data = process_batch_report_data_with_claims(prms)
        else:
            data = process_batch_report_data(prms)
        return data
