from requests import get

from dal.parser import transform_ion_response
from dal.config import BASE_URL


def get_ion_values(
    ursi: str,
    from_date: str,
    to_date: str,
    type: list[str] = ['foF2', 'TEC']
) -> str:
    resp = get(
        url=BASE_URL,
        params={
        'ursiCode': ursi,
        'charName': ','.join(type),
        'DMUF': 3000,
        'fromDate': from_date,
        'toDate': to_date,
    })
    return resp.text


def read_ion_values(file_path: str) -> list[list[str]]:
    with open(file_path, 'r') as file:
        return transform_ion_response(file.read())
