from pprint import pprint
from typing import NamedTuple
from sqlite3 import connect as con

from datetime import datetime
from dateutil.relativedelta import relativedelta

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

connection = con(DB_PATH)
cur = connection.cursor()


class IonData(NamedTuple):
    datetime: str
    f0f2: float
    tec: float
    b0: float

class SatData(NamedTuple):
    datetime: str
    f0f2: float
    tec: float
    sat_tec: float
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


class SolarFlux(Model):
    id = AutoField()
    date = TextField()
    time = TextField()
    flux = FloatField()

    class Meta:
        database = db
        table_name= 'solar_flux'


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


def select_solar_flux_day_mean(date: str):
    flux = SolarFlux.select(
        fn.AVG(SolarFlux.flux).alias('flux')
    ).where(
        SolarFlux.date == date
    ).group_by(SolarFlux.date)

    return flux[0].flux


def select_solar_flux_81_mean(date: str):
    dt = datetime.strptime(date, '%Y-%m-%d')
    low_dt = dt + relativedelta(days=-40)
    high_dt = dt + relativedelta(days=+40)

    flux = SolarFlux.select(
        fn.AVG(SolarFlux.flux).alias('flux')
    ).where(
        (SolarFlux.date <= high_dt.strftime('%Y-%m-%d')) &
        (SolarFlux.date >= low_dt.strftime('%Y-%m-%d'))
    ).group_by(SolarFlux.date)

    return flux[0].flux


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


# def select_2h_avr_sat_tec(
#     ursi: str,
#     date: str,
# ):
#     coords = select_coords_by_ursi(ursi)
#     return SatelliteTEC.select(
#         fn.strftime('%H', SatelliteTEC.time).alias('time'),
#         SatelliteTEC.tec.alias('sat_tec'),
#         SatelliteTEC.lat.alias('sat_lat'),
#         SatelliteTEC.lat.alias('sat_long'),
#     ).where(
#         SatelliteTEC.date == date
#     ).where(
#         ((SatelliteTEC.lat - coords['lat']) < 2.5) & ((SatelliteTEC.long - coords['lat']) < 5)
#     )


def select_2h_avr_for_day_with_sat_tec(
    ursi: str,
    date: str,
    cs_floor: int=70,
) -> ModelSelect:
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''with ion_table as (
             select
                 strftime('%H', time) as datetime,
                 ROUND(AVG(f0f2),1) as f0f2,
                 ROUND(AVG(tec),1) as tec,
                 ROUND(AVG(b0),1) as b0,
                 lat,
                 long
             from station_data
             join station on station.ursi = station_data.ursi
             where station_data.ursi='{ursi}'
                and date like '{date}'
                and (accuracy >= {cs_floor} or accuracy = -1)
             group by datetime
         ),
         sat_table as (
             select
                 strftime('%H', time) as datetime,
                 tec as sat_tec,
                 lat,
                 long
             from satellite_tec
             where date like '{date}'
                and (ABS(lat - {coords['lat']}) < 1.25)
                and (ABS(long - IIF({coords['long']} > 180, {coords['long']} - 360, {coords['long']})) < 2.5)
                and tec != 999.9
         )
         select
             sat_table.datetime as datetime,
             f0f2, tec, sat_tec, b0
         from sat_table
         left join ion_table on ion_table.datetime = sat_table.datetime;'''
    )
    return res.fetchall()


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


def transform_ion_data(data: ModelSelect) -> tuple[IonData]:
    return tuple([
        IonData(d.datetime, d.f0f2, d.tec, d.b0) for d in data
    ])

def transform_sat_data(data: ModelSelect) -> tuple[IonData]:
    return tuple([
        SatData(d[0], d[1], d[2], d[3], d[4]) for d in data
    ])


if __name__ == '__main__':
    pprint(select_solar_flux_day_mean('2019-01-01'))
    print(select_solar_flux_81_mean('2019-01-01'))

