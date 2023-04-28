from dal.models import (
    select_f0f2_k_spread_for_month,
    transform_f0f2_k_spread_for_month,
    select_f0f2_k_spread_for_sum,
    transform_b0_ab_spread_for_month,
    select_b0_ab_spread_for_sum,
    select_f0f2_k_spread_for_win,

    select_f0f2_k_mean_for_month,
)


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


def get_f0f2_k_spread_for_month(
    ursi: str,
    month: int,
    year: int,
):
    return transform_b0_ab_spread_for_month(
        select_b0_ab_spread_for_month(ursi, month, year)
    )


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
