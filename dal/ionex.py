import ionex

from typing import NamedTuple

from dal.config import IONEX_FILES_PATH
from dal.models import SatelliteTEC


class IonexTEC(NamedTuple):
    date: str
    time: str
    lat: float
    tec_long: list[float]


slice_len = 73
lat_start = 87.5
lat_end = -87.5
lat_step = 2.5
long_start = -180
long_end = 180
long_step = 5


def split_to_slices(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

for d in range(2, 11):
    file_date = f'00{d}' if d < 10 else (f'0{d}' if d < 99 else d)

    with open(IONEX_FILES_PATH + f'/esag{file_date}0.19i') as file:
        inx = ionex.reader(file)

        for ionex_map in inx:
            epoch = ionex_map.epoch

            for idx1, slice in enumerate(list(split_to_slices(ionex_map.tec, slice_len))):
                for idx2, v in enumerate([round(i, 1) for i in slice]):
                    record = SatelliteTEC(
                        date=epoch.strftime('%Y-%m-%d'),
                        time=epoch.strftime('%H:%M:%S'),
                        tec=v,
                        lat=lat_start - idx1 * lat_step,
                        long=long_start + idx2 * long_step,
                    )
                    record.save()
                    # ionex_tec = IonexTEC(
                    #     date=epoch.strftime('%Y-%m-%d'),
                    #     time=epoch.strftime('%H:%M:%S'),
                    #     lat=lat_start - idx * lat_step,
                    #     tec_long=[round(i, 1) for i in slice],
                    # )
