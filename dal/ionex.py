from datetime import datetime
import ionex
import asyncio
import aiofiles
import aiosqlite
import sqlite3

from dal.config import DB_PATH, IONEX_FILES_PATH
from dal.models import SatelliteTEC

import os

from dal.parser import convert_iso_to_datetime


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

async def async_split_to_slices(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]
        await asyncio.sleep(0)


async def async_range(start, stop=None, step=1):
    if stop:
        range_ = range(start, stop, step)
    else:
        range_ = range(start)
    for i in range_:
        yield i
        await asyncio.sleep(0)

files = []


async def as_main():
    for y in [20, 21, 22]:
        async for d in async_range(1, 366):
            day = f'00{d}' if d < 10 else (f'0{d}' if d < 100 else f'{d}')
            ionex_file = f'esag{day}0.{y}i'

            with open(IONEX_FILES_PATH + f'/{y}' + f'/{ionex_file}') as file:
                try:
                    inx = ionex.reader(file)
                except Exception as ex:
                    async with aiofiles.open('bad_ionex_file.txt', '+a') as f:
                        await f.write(f'{ionex_file}\n')
                    continue

                try:
                    for ionex_map in inx:
                        epoch = ionex_map.epoch
                        day_of_year = epoch.timetuple().tm_yday
                        if day_of_year != d:
                            continue

                        async with aiosqlite.connect(DB_PATH) as db:
                            for idx1, slice in enumerate(list(split_to_slices(ionex_map.tec, slice_len))):
                                for idx2, v in enumerate([round(i, 1) for i in slice]):
                                    await db.execute(f'''
                                        INSERT INTO
                                            satellite_tec (date, time, tec, lat, long)
                                        VALUES (
                                            '{epoch.strftime('%Y-%m-%d')}',
                                            '{epoch.strftime('%H:%M:%S')}',
                                            {v},
                                            {lat_start - idx1 * lat_step},
                                            {long_start + idx2 * long_step}
                                        );''')
                                    await db.commit()

                except Exception as ex:
                    async with aiofiles.open('bad_ionex.txt', '+a') as f:
                        await f.write(f'{ionex_file}\n')
                    continue

                print(f'DONE! {ionex_file}')




# def main():
#     # for y in range(18, 19):
#     #     for d in range(1, 2):
#     for f in files:
#         y= f[9:11]
#         d = int(f[4:7])

#         day = f'00{d}' if d < 10 else (f'0{d}' if d < 100 else f'{d}')
#         ionex_file = f'esag{day}0.{y}i'

#         with open(IONEX_FILES_PATH + f'/{ionex_file}') as file:
#             try:
#                 inx = ionex.reader(file)
#             except:
#                 with open('bad_ionex_file.txt', '+a') as f:
#                     f.write(f'{ionex_file}\n')
#                 continue

#             try:
#                 for ionex_map in inx:
#                     epoch = ionex_map.epoch
#                     day_of_year = datetime.strptime(epoch, '%Y-%m-%d').timetuple().tm_yday
#                     if day_of_year != d:
#                         continue

#                     for idx1, slice in enumerate(list(split_to_slices(ionex_map.tec, slice_len))):
#                         for idx2, v in enumerate([round(i, 1) for i in slice]):
#                             record = SatelliteTEC(
#                                 date=epoch.strftime('%Y-%m-%d'),
#                                 time=epoch.strftime('%H:%M:%S'),
#                                 tec=v,
#                                 lat=lat_start - idx1 * lat_step,
#                                 long=long_start + idx2 * long_step,
#                             )
#                             record.save()
#             except:
#                 with open('bad_ionex.txt', '+a') as f:
#                     f.write(f'{ionex_file}\n')
#                 continue

#             print(f'DONE! {ionex_file}')

if __name__ == '__main__':
    asyncio.run(as_main())
