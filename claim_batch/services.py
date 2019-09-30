import xml.etree.ElementTree as ET

import core
from django.core.exceptions import PermissionDenied
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
            if res[0]: # zero means "all done"
                raise ProcessBatchSubmitError(res[0])
