from pprint import pprint
from sqlite3 import connect
from datetime import datetime

from errors import ConvertISOError, NoCorrectDataError, NoDataError
from store import save_values_to_db, select_hour_avr_for_day, select_day_avr_for_year
from api import get_ion_values, read_ion_values

from config import DB_PATH


from_date = '2019.01.01'
to_date = '2019.12.31'

# pprint(select_hour_avr_for_day('PA836', '2019-01-01'))
# pprint(select_day_avr_for_year('PA836'))

with connect(DB_PATH) as con:
    cur = con.cursor()

    # cur.execute('''
    # select ursi from stations
    # where
    #     start_date <= '2019-01-01' and
    #     end_date >= '2019-12-31' and
    #     lat <= 60 and
    #     lat >= 30 and
    #     ursi not in (
    #         'AL945', 'BC840', 'EA653', 'FF051', 'AT138', 'DB049',
    #         'EB040', 'MO156', 'AU930', 'EA036', 'EG931', 'RL052',
    #         'IC437', 'IF843', 'JJ433', 'JR055'
    #     );
    # ''')

    cur.execute('''
    select ursi from stations
    where ursi = 'MO155';
    ''')

    for row in cur.fetchall():
        ursi = row[0]

        try:
            values = get_ion_values(
                ursi=ursi, 
                from_date=from_date,
                to_date=to_date,
            )
        except ConvertISOError:
            with open('logs2.txt', 'a') as file:
                file.write(
                    f'{ursi}: ERROR: iso conver error \
{from_date} -- {to_date}' + '\n'
                )
            continue
        except NoDataError:
            with open('logs2.txt', 'a') as file:
                file.write(
                    f'{ursi}: ERROR: No data found for \
requested period {from_date} -- {to_date}' + '\n'
                )
            continue
        except NoCorrectDataError:
            with open('logs2.txt', 'a') as file:
                file.write(
                    f'{ursi}: ERROR: No correct data found for \
requested period {from_date} -- {to_date} | values was got at {datetime.now()}'
+ '\n'
                )
            continue


        print(f'Got values for {ursi}; len: {len(values)}')

        # save_values_to_db(
        #     cur=cur,
        #     table_name=f'{ursi}_2019',
        #     values=get_ion_values(
        #         ursi=ursi, 
        #         from_date='2019.01.01',
        #         to_date='2019.12.31',
        #     )
        # )
        # con.commit()
        pprint(values)

        print(f'values for {ursi} was successfully saved\n')
