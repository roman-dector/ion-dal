from dal.models import (
    select_f0f2_k_mean_for_month,
    transform_f0f2_k_spread_for_month,
    select_f0f2_k_spread_for_month,
    select_f0f2_k_spread_for_sum,
    select_f0f2_k_spread_for_win,

    # transform_b0_ab_spread_for_month,
    # select_b0_ab_spread_for_sum,

    select_adr_spread_for_month,
    select_adr_spread_for_sum,
    select_adr_spread_for_win,
    select_adr_spread_for_year,

    select_gap_spread_for_month,
    select_gap_spread_for_sum,
    select_gap_spread_for_win,
    select_gap_spread_for_year,

    select_gap_pers_spread_for_sum,
    select_gap_pers_spread_for_win,
)


def get_gap_spread_for_month(ursi, month, year):
    spread = select_gap_spread_for_month(ursi, month, year)
    gap_f0f2_sun = [s[0] for s in spread]
    gap_f0f2_moon = [s[1] for s in spread]
    gap_k_sun = [s[2] for s in spread]
    gap_k_moon = [s[3] for s in spread]

    return gap_f0f2_sun, gap_f0f2_moon, gap_k_sun, gap_k_moon


def get_gap_pers_spread_for_sum_win(
    ursi: str,
    year: int,
):
    sum = select_gap_pers_spread_for_sum(ursi, year)
    win = select_gap_pers_spread_for_win(ursi, year)

    sum_gap_f0f2_sun = [s[0] for s in sum]
    sum_gap_f0f2_moon = [s[1] for s in sum]

    win_gap_f0f2_sun = [s[0] for s in win]
    win_gap_f0f2_moon = [s[1] for s in win]

    return {
        'sum': (sum_gap_f0f2_sun, sum_gap_f0f2_moon),
        'win': (win_gap_f0f2_sun, win_gap_f0f2_moon),
    }


def get_gap_spread_for_sum_win(
    ursi: str,
    year: int,
):
    sum = select_gap_spread_for_sum(ursi, year)
    win = select_gap_spread_for_win(ursi, year)

    sum_gap_f0f2_sun = [s[0] for s in sum]
    sum_gap_f0f2_moon = [s[1] for s in sum]
    sum_gap_k_sun = [s[2] for s in sum]
    sum_gap_k_moon = [s[3] for s in sum]

    win_gap_f0f2_sun = [s[0] for s in win]
    win_gap_f0f2_moon = [s[1] for s in win]
    win_gap_k_sun = [s[2] for s in win]
    win_gap_k_moon = [s[3] for s in win]

    return {
        'sum': (sum_gap_f0f2_sun, sum_gap_f0f2_moon, sum_gap_k_sun, sum_gap_k_moon),
        'win': (win_gap_f0f2_sun, win_gap_f0f2_moon, win_gap_k_sun, win_gap_k_moon),
    }


def get_gap_spread_for_year(
    ursi: str,
    year: int,
):
    spread = select_gap_spread_for_year(ursi, year)
    gap_f0f2_sun = [s[0] for s in spread]
    gap_f0f2_moon = [s[1] for s in spread]
    gap_k_sun = [s[2] for s in spread]
    gap_k_moon = [s[3] for s in spread]

    return gap_f0f2_sun, gap_f0f2_moon, gap_k_sun, gap_k_moon


def get_adr_spread_for_month(
    ursi: str,
    month: int,
    year: int,
):
    spread = select_adr_spread_for_month(ursi, month, year)
    a = [s[0] for s in spread]
    d = [s[1] for s in spread]
    r = [s[2] for s in spread]

    return a, d, r


def get_adr_spread_for_sum_win(
    ursi: str,
    year: int,
):
    sum = select_adr_spread_for_sum(ursi, year)
    win = select_adr_spread_for_win(ursi, year)

    a_sun, d_sun, r_sun = [s[0] for s in sum], [s[1] for s in sum], [s[2] for s in sum]
    a_win, d_win, r_win = [s[0] for s in win], [s[1] for s in win], [s[2] for s in win]

    return ((a_sun, d_sun, r_sun), (a_win, d_win, r_win))


def get_adr_spread_for_year(
    ursi: str,
    year: int,
):
    spread = select_adr_spread_for_year(ursi, year)
    a = [s[0] for s in spread]
    d = [s[1] for s in spread]
    r = [s[2] for s in spread]

    return a, d, r


def calc_f0f2_k_mean_for_month(
    ursi: str,
    month: int,
    year: int,
):
    k_mean = select_f0f2_k_mean_for_month(ursi, month, year)[0]
    return {
        'ion': {
            'sun': {'k': k_mean[0], 'err': k_mean[1]},
            'moon': {'k': k_mean[2], 'err': k_mean[3]},
        },
        'sat': {
            'sun': {'k': k_mean[4], 'err': k_mean[5]},
            'moon': {'k': k_mean[6], 'err': k_mean[7]},
        },
    }


def get_f0f2_k_spread_for_month(
    ursi: str,
    month: int,
    year: int,
):
    return transform_f0f2_k_spread_for_month(
        select_f0f2_k_spread_for_month(ursi, month, year)
    )


# def get_b0_k_spread_for_month(
#     ursi: str,
#     month: int,
#     year: int,
# ):
#     return transform_b0_ab_spread_for_month(
#         select_b0_ab_spread_for_month(ursi, month, year)
#     )


def get_f0f2_k_spread_for_summer_winter(
    ursi: str,
    year: int,
):
    sum_ion_sun = []
    sum_ion_moon = []
    sum_sat_sun = []
    sum_sat_moon = []

    for r in select_f0f2_k_spread_for_sum(ursi, year):
        sum_ion_sun.append(r[0])
        sum_ion_moon.append(r[1])
        sum_sat_sun.append(r[2])
        sum_sat_moon.append(r[3])

    win_ion_sun = []
    win_ion_moon = []
    win_sat_sun = []
    win_sat_moon = []

    for r in select_f0f2_k_spread_for_win(ursi, year):
        win_ion_sun.append(r[0])
        win_ion_moon.append(r[1])
        win_sat_sun.append(r[2])
        win_sat_moon.append(r[3])
    
    return {
        'sum': (sum_ion_sun, sum_ion_moon, sum_sat_sun, sum_sat_moon),
        'win': (win_ion_sun, win_ion_moon, win_sat_sun, win_sat_moon),
    }


def get_f0f2_k_spread_for_year(ursi: str, year: int):
    ion_sun = []
    ion_moon = []
    sat_sun = []
    sat_moon = []

    for r in select_f0f2_k_spread_for_win(ursi, year):
        ion_sun.append(r[0])
        ion_moon.append(r[1])
        sat_sun.append(r[2])
        sat_moon.append(r[3])

    return (ion_sun, ion_moon, sat_sun, sat_moon)
