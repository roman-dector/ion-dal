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
        fn.strftime('%H', StationData.time).alias('hour'),
        fn.AVG(StationData.f0f2).alias('f0f2'),
        fn.AVG(StationData.tec).alias('tec'),
        fn.AVG(StationData.b0).alias('b0'),
    ).group_by(fn.strftime('%H', StationData.time))



def select_day_avr_for_year(ursi: str, year: int) -> ModelSelect:
    return StationData.select(
        StationData.date,
        fn.AVG(StationData.f0f2).alias('f0f2'),
        fn.AVG(StationData.tec).alias('tec'),
        fn.AVG(StationData.b0).alias('b0'),
    ).where(
        StationData.ursi == ursi
    ).where(
        fn.strftime('%Y', StationData.date) == str(year)
    ).group_by(StationData.date)


if __name__ == '__main__':
    for q in select_hour_avr_for_day('AL945', '2019-01-01'):
        print(q.tec)

