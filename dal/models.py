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
) -> ModelSelect:
    return select_original_for_day(ursi, date).select(
        fn.strftime('%H', StationData.time).alias('datetime'),
        fn.AVG(StationData.f0f2).alias('f0f2'),
        fn.AVG(StationData.tec).alias('tec'),
        fn.AVG(StationData.b0).alias('b0'),
    ).group_by(fn.strftime('%H', StationData.time))


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

