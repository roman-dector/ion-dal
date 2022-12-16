from pprint import pprint
from api import get_ion_values
from dal.parser import transform_ion_response
from models import StationData

getted = ('AL945', 'EA036')

stations = [
    'IC437', 'MHJ45', 'PQ052',
    'WI937', 'AT138', 'EA653', 'IF843', 'MO155',
    'RL052', 'WP937', 'AU930', 'EB040', 'JJ433',
    'MO156', 'RO041', 'BC840', 'EG931', 'JR055',
    'NI135', 'SO148', 'DB049', 'FF051', 'LD160',
    'PA836', 'VT139',
]

values = get_ion_values(
    ursi=stations[0],
    from_date='2019-01-01',
    to_date='2019-12-31',
    type=['B0']
)

values = transform_ion_response(values)

for v in values:
    station = StationData.select().where(
        StationData.ursi == stations[0]
    ).where(
        StationData.date == v[0],
    ).where(
        StationData.time == v[1],
    ).get()

    station.b0 = v[3]
    station.save()

