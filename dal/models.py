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


north_summer = [5, 6, 7, 8, 9, 10]
north_winter = [11, 12, 1, 2, 3, 4]

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
    ion_tec: float
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

class AccuracyJmodel(Model):
    id = AutoField()
    ursi = TextField()
    date = TextField()

    gap_f0f2_sun = FloatField()
    gap_f0f2_moon = FloatField()
    gap_pers_f0f2_sun = FloatField()
    gap_pers_f0f2_moon = FloatField()
    gap_k_sun = FloatField()
    gap_k_moon = FloatField()

    class Meta:
        database = db
        table_name= 'accuracy_jmodel'


class IonSatADRMeanDay(Model):
    id = AutoField()
    ursi = TextField()
    date = TextField()

    a = FloatField()
    a_err = FloatField()
    d = FloatField()
    d_err = FloatField()
    r = FloatField()

    class Meta:
        database = db
        table_name= 'ion_sat_adr_mean_day'


class F0f2KMeanDay(Model):
    id = AutoField()
    ursi = TextField()
    date = TextField()

    ion_sun_k = FloatField()
    ion_sun_err = FloatField()
    ion_moon_k = FloatField()
    ion_moon_err = FloatField()

    sat_sun_k = FloatField()
    sat_sun_err = FloatField()
    sat_moon_k = FloatField()
    sat_moon_err = FloatField()

    jmodel_sun_k = FloatField()
    jmodel_moon_k = FloatField()

    class Meta:
        database = db
        table_name= 'f0f2_k_mean_day'


class B0ABMeanDay(Model):
    id = AutoField()
    ursi = TextField()
    date = TextField()

    sun_a = FloatField()
    sun_a_err = FloatField()
    moon_a = FloatField()
    moon_a_err = FloatField()

    sun_b = FloatField()
    sun_b_err = FloatField()
    moon_b = FloatField()
    moon_b_err = FloatField()

    class Meta:
        database = db
        table_name= 'b0_ab_mean_day'


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


def transform_f0f2_k_spread_for_month(data):
    ion_sun_k = []
    ion_moon_k = []
    sat_sun_k = []
    sat_moon_k = []

    for d in data:
        ion_sun_k.append(d[0])
        ion_moon_k.append(d[1])
        sat_sun_k.append(d[2])
        sat_moon_k.append(d[3])
    return (ion_sun_k, ion_moon_k, sat_sun_k, sat_moon_k)


def transform_ion_data(data: ModelSelect) -> tuple[IonData]:
    return tuple([
        IonData(d.datetime, d.f0f2, d.tec, d.b0) for d in data
    ])


def transform_sat_data(data: ModelSelect) -> tuple[IonData]:
    return tuple([
        SatData(d[0], d[1], d[2], d[3], d[4]) for d in data
    ])


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
            f0f2, tec as ion_tec, sat_tec, b0
        from sat_table
        left join ion_table on ion_table.datetime = sat_table.datetime
        where f0f2 not null
        ;'''
    )
    return res.fetchall()


def select_b0_ab_mean_for_month(
    ursi: str,
    month: int,
    year: int,
):
    str_month = f'0{month}' if month < 10 else f'{month}'

    res = cur.execute(f'''
    with len as (
            select count(*) as len
            from f0f2_k_mean_day
            where
                ursi='PA836' and
                date like '2018-01%'
        )
        select
            ROUND(AVG(sat_sun_k), 3) as ssk,
            ROUND(AVG(sat_sun_err)/len.len, 3) as sse,
            ROUND(AVG(sat_moon_k), 3) as smk,
            ROUND(AVG(sat_moon_err)/len.len, 3) as sme,
            strftime('%Y-%m', date) as yearmonth
        from b0_ab_mean_day, len
        where
            ursi='{ursi}' and
            date like '{year}-{str_month}%'
            group by yearmonth;
    ''')

    return res.fetchall()


def select_f0f2_k_mean_for_month(
    ursi: str,
    month: int,
    year: int,
):
    str_month = f'0{month}' if month < 10 else f'{month}'
    
    res = cur.execute(f'''
    with len as (
            select count(*) as len
            from f0f2_k_mean_day
            where
                ursi='PA836' and
                date like '2018-01%'
        )
        select
            ROUND(AVG(ion_sun_k), 3) as isk,
            ROUND(AVG(ion_sun_err)/len.len, 3) as ise,
            ROUND(AVG(ion_moon_k), 3) as imk,
            ROUND(AVG(ion_moon_err)/len.len, 3) as ime,
            ROUND(AVG(sat_sun_k), 3) as ssk,
            ROUND(AVG(sat_sun_err)/len.len, 3) as sse,
            ROUND(AVG(sat_moon_k), 3) as smk,
            ROUND(AVG(sat_moon_err)/len.len, 3) as sme,
            strftime('%Y-%m', date) as yearmonth
        from f0f2_k_mean_day, len
        where
            ursi='{ursi}' and
            date like '{year}-{str_month}%'
            group by yearmonth;
    ''')

    return res.fetchall()


def select_f0f2_k_spread_for_month(
    ursi: str,
    month: int,
    year: int,
):
    str_month = f'0{month}' if month < 10 else f'{month}'
    res = cur.execute(f'''
        select 
            ROUND(ion_sun_k, 1),
            ROUND(ion_moon_k, 1),
            ROUND(sat_sun_k, 1),
            ROUND(sat_moon_k, 1)
        from f0f2_k_mean_day
        where
            ursi='{ursi}' and
            date like '{year}-{str_month}%'
    ''')

    return res.fetchall()


def select_f0f2_k_spread_for_sum(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)
    res = cur.execute(f'''
            select
                ion_sun_k as sum_ion_sun_k,
                ion_moon_k as sum_ion_moon_k,
                sat_sun_k as sum_sat_sun_k,
                sat_moon_k as sum_sat_moon_k
            from f0f2_k_mean_day
            where
                ursi = '{ursi}' and
                (
                    date glob IIF({coords['lat']} > 0, '{year}-0[5, 6, 7, 8, 9]*', '{year}-0[1, 2, 3, 4]*') or
                    date glob IIF({coords['lat']} > 0, '{year}-10*', '{year}-[11, 12]*')
                )
    ''')
    return res.fetchall()


def select_f0f2_k_spread_for_win(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)
    res = cur.execute(f'''
            select
                ion_sun_k as win_ion_sun_k,
                ion_moon_k as win_ion_moon_k,
                sat_sun_k as win_sat_sun_k,
                sat_moon_k as win_sat_moon_k
            from f0f2_k_mean_day
            where
                ursi = '{ursi}' and
                (
                    date glob IIF({coords['lat']} > 0, '{year}-0[1, 2, 3, 4]*', '{year}-0[5, 6, 7, 8, 9]*') or
                    date glob IIF({coords['lat']} > 0, '{year}-[11, 12]*', '{year}-10*')
                )
    ''')

    return res.fetchall()


def select_f0f2_k_spread_for_year(
    ursi: str,
    year: int,
):
    res = cur.execute(f'''
            select
                ion_sun_k,
                ion_moon_k,
                sat_sun_k,
                sat_moon_k
            from f0f2_k_mean_day
            where
                ursi = '{ursi}' and
                date like '{year}%'
    ''')

    return res.fetchall()


def select_f0f2_sat_tec(
    ursi: str,
    date: str,
    cs_floor: int=70,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        with
            ion_table as (
                select
                    strftime('%H', time) as hour,
                    ROUND(AVG(f0f2),1) as f0f2
                from station_data
                where
                    ursi='{ursi}' and
                    date like '{date}' and
                    (accuracy >= {cs_floor} or accuracy = -1)
                group by hour
            ),
            sat_table as (
                select
                    strftime('%H', time) as hour,
                    tec as sat_tec
                from satellite_tec
                where
                    date like '{date}' and
                    (ABS(lat - {coords['lat']}) < 1.25) and
                    (ABS(long - IIF({coords['long']} > 180, {coords['long']} - 360, {coords['long']})) < 2.5) and
                    tec != 999.9
            )

        select
            sat_table.hour as hour,
            f0f2,
            sat_tec
        from sat_table
        left join ion_table on ion_table.hour = sat_table.hour
        where f0f2 not null
        ;'''
    )
    return res.fetchall()




def select_ion_tec_sat_tec(
    ursi: str,
    date: str,
    cs_floor: int=70,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        with
            ion_table as (
                select
                    strftime('%H', time) as hour,
                    ROUND(AVG(tec),1) as ion_tec
                from station_data
                where
                    ursi='{ursi}' and
                    date like '{date}' and
                    (accuracy >= {cs_floor} or accuracy = -1)
                group by hour
            ),
            sat_table as (
                select
                    strftime('%H', time) as hour,
                    tec as sat_tec
                from satellite_tec
                where
                    date like '{date}' and
                    (ABS(lat - {coords['lat']}) < 1.25) and
                    (ABS(long - IIF({coords['long']} > 180, {coords['long']} - 360, {coords['long']})) < 2.5) and
                    tec != 999.9
            )

        select
            sat_table.hour as hour,
            ion_tec,
            sat_tec
        from sat_table
        left join ion_table on ion_table.hour = sat_table.hour
        where ion_tec not null
        ;'''
    )
    return res.fetchall()


def select_adr_spread_for_month(
    ursi: str,
    month: int,
    year: int,
):
    str_month = f'0{month}' if month < 10 else str(month)

    res = cur.execute(f'''
        select a, d, r
        from ion_sat_adr_mean_day
        where ursi = '{ursi}' and date like '{year}-{str_month}%'
        ;'''
    )
    return res.fetchall()


def select_adr_spread_for_sum(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        select a, d, r
        from ion_sat_adr_mean_day
        where ursi = '{ursi}' and
            (
                date glob IIF({coords['lat']} > 0, '{year}-0[5, 6, 7, 8, 9]*', '{year}-0[1, 2, 3, 4]*') or
                date glob IIF({coords['lat']} > 0, '{year}-10*', '{year}-[11, 12]*')
            )
        ;'''
    )
    return res.fetchall()


def select_adr_spread_for_win(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        select a, d, r
        from ion_sat_adr_mean_day
        where ursi = '{ursi}' and
            (
                date glob IIF({coords['lat']} > 0, '{year}-0[1, 2, 3, 4]*', '{year}-0[5, 6, 7, 8, 9]*') or
                date glob IIF({coords['lat']} > 0, '{year}-[11, 12]*', '{year}-10*')
            )
        ;'''
    )
    return res.fetchall()


def select_adr_spread_for_year(
    ursi: str,
    year: int,
):
    res = cur.execute(f'''
        select a, d, r
        from ion_sat_adr_mean_day
        where ursi = '{ursi}' and date like '{year}%'
        ;'''
    )
    return res.fetchall()


def select_ad_mean_for_year(
    ursi: str,
    year: int,
):
    res = cur.execute(f'''
        select AVG(a), AVG(d), strftime('%Y', date) as year
        from ion_sat_adr_mean_day
        where ursi = '{ursi}' and year like '{year}'
        group by year
        ;'''
    )
    return res.fetchall()


def select_gap_spread_for_month(ursi, month, year):
    str_month = f'0{month}' if month < 10 else str(month)

    res = cur.execute(f'''
        select 
            gap_f0f2_sun,
            gap_f0f2_moon,
            gap_k_sun,
            gap_k_moon
        from accuracy_jmodel
        where ursi = '{ursi}' and date like '{year}-{str_month}%'
        ;'''
    )
    return res.fetchall()



def select_gap_pers_spread_for_sum(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        select
            gap_pers_f0f2_sun,
            gap_pers_f0f2_moon
        from accuracy_jmodel
        where ursi = '{ursi}' and
            (
                date glob IIF({coords['lat']} > 0, '{year}-0[5, 6, 7, 8, 9]*', '{year}-0[1, 2, 3, 4]*') or
                date glob IIF({coords['lat']} > 0, '{year}-10*', '{year}-[11, 12]*')
            )
        ;'''
    )
    return res.fetchall()


def select_gap_pers_spread_for_win(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        select
            gap_pers_f0f2_sun,
            gap_pers_f0f2_moon
        from accuracy_jmodel
        where ursi = '{ursi}' and
            (
                date glob IIF({coords['lat']} > 0, '{year}-0[1, 2, 3, 4]*', '{year}-0[5, 6, 7, 8, 9]*') or
                date glob IIF({coords['lat']} > 0, '{year}-[11, 12]*', '{year}-10*')
            )
        ;'''
    )
    return res.fetchall()


def select_gap_spread_for_sum(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        select
            gap_f0f2_sun,
            gap_f0f2_moon,
            gap_k_sun,
            gap_k_moon
        from accuracy_jmodel
        where ursi = '{ursi}' and
            (
                date glob IIF({coords['lat']} > 0, '{year}-0[5, 6, 7, 8, 9]*', '{year}-0[1, 2, 3, 4]*') or
                date glob IIF({coords['lat']} > 0, '{year}-10*', '{year}-[11, 12]*')
            )
        ;'''
    )
    return res.fetchall()


def select_gap_spread_for_win(
    ursi: str,
    year: int,
):
    coords = select_coords_by_ursi(ursi)

    res = cur.execute(f'''
        select
            gap_f0f2_sun,
            gap_f0f2_moon,
            gap_k_sun,
            gap_k_moon
        from accuracy_jmodel
        where ursi = '{ursi}' and
            (
                date glob IIF({coords['lat']} > 0, '{year}-0[1, 2, 3, 4]*', '{year}-0[5, 6, 7, 8, 9]*') or
                date glob IIF({coords['lat']} > 0, '{year}-[11, 12]*', '{year}-10*')
            )
        ;'''
    )
    return res.fetchall()


def select_gap_spread_for_year(
    ursi: str,
    year: int,
):
    res = cur.execute(f'''
        select
            gap_f0f2_sun,
            gap_f0f2_moon,
            gap_k_sun,
            gap_k_moon
        from accuracy_jmodel
        where ursi = '{ursi}' and date like '{year}%'
        ;'''
    )
    return res.fetchall()


if __name__ == '__main__':
    F0f2KMeanDay.update({F0f2KMeanDay.jmodel_sun_k}).where((F0f2KMeanDay.ursi == 'PA836') & (F0f2KMeanDay.date == '2018-01-01'))

