from core.signals import bind_service_signal
from core.service_signals import ServiceSignalBindType
from invoice.services.bill import BillService


def bind_service_signals():
    bind_service_signal(
        'trigger_bill_creation_from_calcrule',
        BillService.bill_creation_from_calculation,
        bind_type=ServiceSignalBindType.AFTER
    )
