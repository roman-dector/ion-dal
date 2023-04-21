from pprint import pprint
from typing import NamedTuple

from peewee import (
    fn,

    SqliteDatabase,
    Model,
    AutoField,
    TextField,
    FloatField,

    ModelSelect,
)

from dal.config import DB_PATH


db = SqliteDatabase(DB_PATH)


class IonData(NamedTuple):
    datetime: str
    f0f2: float
    tec: float
    b0: float


class Station(Model):
    id = AutoField()
    ursi = TextField()
    name = TextField()
    lat = FloatField()
    long = FloatField()
    start_date = TextField()
    end_date = TextField()

    class Meta:
        database = db
        table_name= 'station'


class StationData(Model):
    id = AutoField()
    ursi = TextField()
    date = TextField()
    time = TextField()
    accuracy = FloatField()
    f0f2 = FloatField()
    tec = FloatField()
    b0 = FloatField()

    class Meta:
        database = db
        table_name= 'station_data'


class F0f2KMeanDay(Model):
    id = AutoField()
    ursi = TextField()
    date = TextField()
    sun_k = FloatField()
    sun_k_err = FloatField()
    moon_k = FloatField()
    moon_k_err = FloatField()

    class Meta:
        database = db
        table_name= 'f0f2_k_mean_day'


class SatelliteTEC(Model):
    id = AutoField()
    date = TextField()
    time = TextField()
    tec = FloatField()
    lat = FloatField()
    long = FloatField()

    class Meta:
        database = db
        table_name= 'satellite_tec'


two_hour_time_groups = {
    '00': 0,
    '01': 0,
    '02': 2,
    '03': 2,
    '04': 4,
    '05': 4,
    '06': 6,
    '07': 6,
    '08': 8,
    '09': 8,
    '10': 10,
    '11': 10,
    '12': 12,
    '13': 12,
    '14': 14,
    '15': 14,
    '16': 16,
    '17': 16,
    '18': 18,
    '19': 18,
    '20': 20,
    '21': 20,
    '22': 22,
    '23': 22,
}


def select_coords_by_ursi(ursi: str) -> dict[str, float]:
    station = Station.get(Station.ursi == ursi)

    return {'lat': station.lat, 'long': station.long}


def select_middle_lat_stations() -> list[str]:
    noth = Station.select().where(
        Station.lat >= 30.0
    ).where(Station.lat <= 60.0)

    south = Station.select().where(
        Station.lat <= -30.0
    ).where(Station.lat >= -60.0)

    return tuple([s.ursi for s in [*noth, *south]])


def select_original_for_day(
    ursi: str,
    date: str,
) -> ModelSelect:
    return StationData.select().where(
        StationData.ursi == ursi
    ).where(
        StationData.date == date
    )


def select_hour_avr_for_day(
    ursi: str,
    date: str,
    cs_floor: int=70,
) -> ModelSelect:
    return select_original_for_day(ursi, date).where(
        (StationData.accuracy >= cs_floor) | (StationData.accuracy == -1)
        ).select(
        fn.strftime('%H', StationData.time).alias('datetime'),
        fn.AVG(StationData.f0f2).alias('f0f2'),
        fn.AVG(StationData.tec).alias('tec'),
        fn.AVG(StationData.b0).alias('b0'),
    ).group_by(fn.strftime('%H', StationData.time))


def select_2h_avr_sat_tec(
    ursi: str,
    date: str,
):
    coords = select_coords_by_ursi(ursi)
    return SatelliteTEC.select(
        fn.strftime('%H', SatelliteTEC.time).alias('time'),
        SatelliteTEC.tec.alias('sat_tec'),
        SatelliteTEC.lat.alias('sat_lat'),
        SatelliteTEC.lat.alias('sat_long'),
    ).where(
        SatelliteTEC.date == date
    ).where(
        ((SatelliteTEC.lat - coords['lat']) < 2.5) & ((SatelliteTEC.long - coords['lat']) < 5)
    )


def select_2h_avr_for_day_with_sat_tec(
    ursi: str,
    date: str,
    cs_floor: int=70,
) -> ModelSelect:
    sat_tec = select_2h_avr_sat_tec(ursi, date)

    subselect = select_original_for_day(ursi, date).where(
        (StationData.accuracy >= cs_floor) | (StationData.accuracy == -1)
        ).select(
        fn.strftime('%H', StationData.time).alias('datetime'),
        fn.AVG(StationData.f0f2).alias('f0f2'),
        fn.AVG(StationData.tec).alias('tec'),
        fn.AVG(StationData.b0).alias('b0'),
    ).group_by(fn.strftime('%H', StationData.time))

    subselect = subselect.select(
        (two_hour_time_groups[subselect['datetime']]).alias('time'),
        fn.AVG(subselect['f0f2']),
        fn.AVG(subselect['tec']),
        fn.AVG(subselect['b0']),
    ).group_by(two_hour_time_groups[subselect['datetime']])

    return subselect.join(
        sat_tec, on=(subselect['time'] == sat_tec['time']),
    )


def select_day_avr_for_year(ursi: str, year: int) -> ModelSelect:
    return StationData.select(
        StationData.date.alias('datetime'),
        fn.AVG(StationData.f0f2).alias('f0f2'),
        fn.AVG(StationData.tec).alias('tec'),
        fn.AVG(StationData.b0).alias('b0'),
    ).where(
        StationData.ursi == ursi
    ).where(
        fn.strftime('%Y', StationData.date) == str(year)
    ).group_by(StationData.date)


def transform_data(data: ModelSelect) -> tuple[IonData]:
    return tuple([
        IonData(d.datetime, d.f0f2, d.tec, d.b0) for d in data
    ])


if __name__ == '__main__':
    pprint(len(select_middle_lat_stations()))

