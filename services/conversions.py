from services.models import GroupPeriod, IncomeRequest, OutcomeResult
from datetime import timedelta


def collect_period_dataset(request: IncomeRequest) -> list[str]:
    match request.group_type:
        case GroupPeriod.month:
            start = request.dt_from.replace(day=1)
            delta = 0
        case GroupPeriod.day:
            start = request.dt_from
            delta = timedelta(days=1)
        case GroupPeriod.hour:
            start = request.dt_from.replace(hour=0)
            delta = timedelta(hours=1)

    labels = []
    while start <= request.dt_upto:
        labels.append(start.strftime('%Y-%m-%dT%H:%M:%S'))
        if request.group_type == GroupPeriod.month:
            start = (start + timedelta(days=32)).replace(day=1)
        else:
            start += delta

    return labels


def convert_aggregation_for_telegram(aggregated_data, request: IncomeRequest) -> str:
    labels = collect_period_dataset(request)
    dataset = [0 for _ in labels]
    for data in aggregated_data:
        check_exists = [
            index for index, label in enumerate(labels) if label.replace('T', '-').startswith(data['_id'])
        ]
        if check_exists:
            dataset[check_exists[0]] = data['sum_value']

    return OutcomeResult(dataset=dataset, labels=labels).model_dump_json()
