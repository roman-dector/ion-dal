from datetime import datetime


class ConvertISOError(Exception):
    pass

class NoDataError(Exception):
    pass

class NoCorrectDataError(Exception):
    pass


def remove_comments(text: str) -> list[str]:
    return [s for s in text.strip().split('\n') if not s.startswith('#')]


def is_data_available(rows: list) -> bool | NoDataError:
    if len(rows) == 1:
        raise NoDataError
    return True


def split_ion_data_in_rows(rows: list[str]) -> list[list[str]]:
    return [r.split() for r in rows]


def remove_partial_filled_rows(
    values: list[list[str]]
) -> list[list[str]]:
    return [v for v in values if not '---' in v]


def remove_empty_rows(values: list[list[str]]) -> list[list[str]]:
    return [v for v in values if not (len(v) == 0)]


def is_data_correct(values: list[list[str]]) -> bool:
    if len(values) == 0:
        raise NoCorrectDataError
    return True

def remove_delemiters(values: list[list[str]]) -> list[list[str]]:
    idxs = [-(k*2 -1) for k in range(1, int((len(values[0]) - 2) / 2) + 1)]
    idxs.reverse()
    for v in values:
        for i in idxs:
            del v[i]

    return values


def convert_iso_to_datetime(iso_timestamp: str) -> datetime:
    return datetime.fromisoformat(iso_timestamp.removesuffix('Z'))


def convert_time(values: list[list[str]]) -> list[list[datetime]]:
    res = []
    for v in values:
        try:
            dt = convert_iso_to_datetime(v[0])
            res.append([dt] + v[1:])
        except ValueError:
            raise ConvertISOError
    return res


def separate_date_time(values: list[list[datetime]]) -> list[list[str]]:
    result = []
    for idx, v  in enumerate(values):
        full_date: datetime = v[0]
        result.insert(idx, [str(full_date.date()), str(full_date.time()), *v[1:]])

    return result


def transform_ion_response(resp: str) -> list[list[str]]:
    values = remove_comments(resp)

    values = split_ion_data_in_rows(values)
    values = remove_partial_filled_rows(values)
    values = remove_empty_rows(values)

    if is_data_correct(values):
        print(values[0])
        values = remove_delemiters(values)
        print(values[0])
        values = convert_time(values)
        values = separate_date_time(values)

    return values

