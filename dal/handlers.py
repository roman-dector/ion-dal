from dal.models import (
    select_f0f2_k_spread_for_month,
    transform_f0f2_k_spread_for_month,

    select_f0f2_k_mean_for_month,
)


def get_f0f2_k_spread_for_month(
    ursi: str,
    month: int,
    year: int,
):
    return transform_f0f2_k_spread_for_month(
        select_f0f2_k_spread_for_month(ursi, month, year)
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
