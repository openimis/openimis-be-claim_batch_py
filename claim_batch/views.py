from django.core.exceptions import PermissionDenied
from django.http import Http404

from core.jwt_authentication import JWTAuthentication
from report.services import ReportService
from .services import ReportDataService
from .reports import pbh, pbp, pbc_H, pbc_P
from .apps import ClaimBatchConfig
from django.utils.translation import gettext as _


def _report(prms):
    show_claims = prms.get("showClaims", "false") == 'true'
    group = prms.get("group", "H")
    if show_claims:
        report = "claim_batch_pbc_" + group
        default = pbc_H.template if group == 'H' else pbc_P.template
    elif group == 'H':
        report = "claim_batch_pbh"
        default = pbh.template
    else:
        report = "claim_batch_pbp"
        default = pbp.template
    return report, default


def report(request):
    user = JWTAuthentication().authenticate(request)[0]
    if not user.has_perms(ClaimBatchConfig.account_preview_perms):
        raise PermissionDenied(_("unauthorized"))
    report_service = ReportService(user)
    report_name, default = _report(request.GET)
    report_data_service = ReportDataService(user)
    try:
        data = report_data_service.fetch(request.GET)
    except ValueError:
        raise Http404(f"No data found")

    result = report_service.process(report_name,
                                    {'data': data,
                                     'DateFrom': request.GET['dateFrom'],
                                     'DateTo': request.GET['dateTo'],
                                     'RegionCode': request.GET['regionCode'],
                                     'RegionName': request.GET['regionName'],
                                     'HFCode': request.GET['hfCode'],
                                     'HFName': request.GET['hfName'],
                                     'HFLevel': request.GET['hfLevel'],
                                     'ProductCode': request.GET['productCode'],
                                     'ProductName': request.GET['productName'],
                                     'RunDate': request.GET['runDate']
                                     },
                                    default,
                                    report_format=request.GET['fileFormat'])
    return result
