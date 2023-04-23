import ionex
import asyncio
import aiofiles
import aiosqlite

from dal.config import DB_PATH, IONEX_FILES_PATH
from dal.models import SatelliteTEC

import os


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


async def as_main():
    async for y in async_range(18, 19):
        async for d in async_range(9, 366):
            day = f'00{d}' if d < 10 else (f'0{d}' if d < 100 else f'{d}')
            ionex_file = f'esag{day}0.{y}i'

            with open(IONEX_FILES_PATH + f'/{ionex_file}') as file:
                try:
                    inx = ionex.reader(file)
                except Exception as ex:
                    async with aiofiles.open('bad_ionex_file.txt', '+a') as f:
                        await f.write(f'{ionex_file}\n')
                    continue

                try:
                    for ionex_map in inx:
                        epoch = ionex_map.epoch
                        if int(epoch.strftime('%d')) != d:
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

                                    # record = SatelliteTEC(
                                    #     date=epoch.strftime('%Y-%m-%d'),
                                    #     time=epoch.strftime('%H:%M:%S'),
                                    #     tec=v,
                                    #     lat=lat_start - idx1 * lat_step,
                                    #     long=long_start + idx2 * long_step,
                                    # )
                                    # record.save()
                except Exception as ex:
                    async with aiofiles.open('bad_ionex.txt', '+a') as f:
                        await f.write(f'{ionex_file}\n')
                    continue

                print(f'DONE! {ionex_file}')

# asyncio.run(as_main())

files = [
'esag0090.18i', 'esag0120.18i', 'esag0130.18i', 'esag0150.18i', 
'esag0160.18i', 'esag0170.18i', 'esag0200.18i', 'esag0210.18i', 
'esag0220.18i', 'esag0230.18i', 'esag0250.18i', 'esag0270.18i', 
'esag0280.18i', 'esag0320.18i', 'esag0330.18i', 'esag0360.18i', 
'esag0370.18i', 'esag0380.18i', 'esag0390.18i', 'esag0410.18i', 
'esag0420.18i', 'esag0430.18i', 'esag0440.18i', 'esag0450.18i', 
'esag0460.18i', 'esag0470.18i', 'esag0490.18i', 'esag0500.18i', 
'esag0510.18i', 'esag0530.18i', 'esag0540.18i', 'esag0560.18i', 
'esag0570.18i', 'esag0580.18i', 'esag0590.18i', 'esag0600.18i', 
'esag0610.18i', 'esag0620.18i', 'esag0630.18i', 'esag0640.18i', 
'esag0660.18i', 'esag0670.18i', 'esag0680.18i', 'esag0700.18i', 
'esag0710.18i', 'esag0720.18i', 'esag0730.18i', 'esag0740.18i', 
'esag0750.18i', 'esag0760.18i', 'esag0770.18i', 'esag0780.18i', 
'esag0790.18i', 'esag0800.18i', 'esag0810.18i', 'esag0820.18i', 
'esag0860.18i', 'esag0870.18i', 'esag0890.18i', 'esag0900.18i', 
'esag0920.18i', 'esag0930.18i', 'esag0940.18i', 'esag0950.18i', 
'esag0960.18i', 'esag0990.18i', 'esag1000.18i', 'esag1010.18i', 
'esag1020.18i', 'esag1030.18i', 'esag1040.18i', 'esag1060.18i', 
'esag1070.18i', 'esag1080.18i', 'esag1090.18i', 'esag1150.18i', 
'esag1190.18i', 'esag1220.18i', 'esag1230.18i', 'esag1250.18i', 
'esag1260.18i', 'esag1280.18i', 'esag1290.18i', 'esag1300.18i', 
'esag1310.18i', 'esag1320.18i', 'esag1330.18i', 'esag1340.18i', 
'esag1350.18i', 'esag1360.18i', 'esag1370.18i', 'esag1380.18i', 
'esag1390.18i', 'esag1400.18i', 'esag1410.18i', 'esag1430.18i', 
'esag1440.18i', 'esag1480.18i', 'esag1490.18i', 'esag1510.18i', 
'esag1530.18i', 'esag1570.18i', 'esag1580.18i', 'esag1680.18i', 
'esag1690.18i', 'esag1700.18i', 'esag1710.18i', 'esag1730.18i', 
'esag1760.18i', 'esag1770.18i', 'esag1790.18i', 'esag1800.18i', 
'esag1810.18i', 'esag1820.18i', 'esag1830.18i', 'esag1840.18i', 
'esag1850.18i', 'esag1860.18i', 'esag1870.18i', 'esag1880.18i', 
'esag1920.18i', 'esag1930.18i', 'esag1940.18i', 'esag1950.18i', 
'esag1970.18i', 'esag1980.18i', 'esag1990.18i', 'esag2010.18i', 
'esag2020.18i', 'esag2060.18i', 'esag2070.18i', 'esag2100.18i', 
'esag2120.18i', 'esag2130.18i', 'esag2140.18i', 'esag2160.18i', 
'esag2190.18i', 'esag2200.18i', 'esag2220.18i', 'esag2230.18i', 
'esag2240.18i', 'esag2250.18i', 'esag2260.18i', 'esag2270.18i', 
'esag2290.18i', 'esag2310.18i', 'esag2320.18i', 'esag2360.18i', 
'esag2390.18i', 'esag2400.18i', 'esag2410.18i', 'esag2430.18i', 
'esag2440.18i', 'esag2450.18i', 'esag2460.18i', 'esag2480.18i', 
'esag2490.18i', 'esag2500.18i', 'esag2510.18i', 'esag2520.18i', 
'esag2530.18i', 'esag2540.18i', 'esag2550.18i', 'esag2560.18i', 
'esag2570.18i', 'esag2580.18i', 'esag2590.18i', 'esag2610.18i', 
'esag2640.18i', 'esag2650.18i', 'esag2660.18i', 'esag2700.18i', 
'esag2710.18i', 'esag2720.18i', 'esag2730.18i', 'esag2740.18i', 
'esag2760.18i', 'esag2800.18i', 'esag2810.18i', 'esag2820.18i', 
'esag2830.18i', 'esag2840.18i', 'esag2850.18i', 'esag2860.18i', 
'esag2870.18i', 'esag2880.18i', 'esag2890.18i', 'esag2900.18i', 
'esag2910.18i', 'esag2920.18i', 'esag2930.18i', 'esag2940.18i', 
'esag2950.18i', 'esag2960.18i', 'esag2970.18i', 'esag2980.18i', 
'esag2990.18i', 'esag3000.18i', 'esag3010.18i', 'esag3030.18i', 
'esag3050.18i', 'esag3060.18i', 'esag3070.18i', 'esag3080.18i', 
'esag3090.18i', 'esag3100.18i', 'esag3110.18i', 'esag3120.18i', 
'esag3130.18i', 'esag3140.18i', 'esag3150.18i', 'esag3160.18i', 
'esag3170.18i', 'esag3180.18i', 'esag3190.18i', 'esag3200.18i', 
'esag3210.18i', 'esag3220.18i', 'esag3230.18i', 'esag3240.18i', 
'esag3250.18i', 'esag3260.18i', 'esag3270.18i', 'esag3280.18i', 
'esag3300.18i', 'esag3310.18i', 'esag3320.18i', 'esag3330.18i', 
'esag3340.18i', 'esag3350.18i', 'esag3370.18i', 'esag3380.18i', 
'esag3390.18i', 'esag3400.18i', 'esag3410.18i', 'esag3430.18i', 
'esag3440.18i', 'esag3450.18i', 'esag3460.18i', 'esag3470.18i', 
'esag3480.18i', 'esag3490.18i', 'esag3530.18i', 'esag3540.18i', 
'esag3550.18i', 'esag3560.18i', 'esag3580.18i', 'esag3590.18i', 
'esag3600.18i', 'esag3610.18i', 'esag3620.18i', 'esag3630.18i', 
'esag3640.18i', 'esag3650.18i']


def main():
    # for y in range(18, 19):
    #     for d in range(1, 2):
    for f in files:
        y= f[9:11]
        d = int(f[4:7])

        day = f'00{d}' if d < 10 else (f'0{d}' if d < 100 else f'{d}')
        ionex_file = f'esag{day}0.{y}i'

        with open(IONEX_FILES_PATH + f'/{ionex_file}') as file:
            try:
                inx = ionex.reader(file)
            except:
                with open('bad_ionex_file.txt', '+a') as f:
                    f.write(f'{ionex_file}\n')
                continue

            try:
                for ionex_map in inx:
                    epoch = ionex_map.epoch
                    if int(epoch.strftime('%d')) != d:
                        continue

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
            except:
                with open('bad_ionex.txt', '+a') as f:
                    f.write(f'{ionex_file}\n')
                continue

            print(f'DONE! {ionex_file}')
