"""
config.py — конфигурация корпусов техникума.
"""

from datetime import date

CORPS = [
    {
        "id":           "corp1",
        "name":         "1 корпус",
        "folder_id":    "1vzKOEmF84_dr8PUXSKc9z3IkWG6-dmQq",
        "structure":    "nested",
        "table_format": "type_b",
        "file_filter":  ["замен", "расписани"],
    },
    {
        "id":           "corp2",
        "name":         "2 корпус",
        "folder_id":    "1EHepY6k2IAYz-SexM6b0cGMM_cGgO9YS",
        "structure":    "flat",
        "table_format": "type_d",
        "file_filter":  None,
        "main_file_kw": "расписание",
        "subs_file_kw": ["замен", "изменени"],
    },
    {
        "id":           "corp3",
        "name":         "3 корпус",
        "folder_id":    "1fxehYVWNrEC5EoHnrzgaxoSyCDXCDTur",
        "structure":    "flat",
        "table_format": "type_a",
        "file_filter":  None,
    },
    {
        "id":           "corp4",
        "name":         "4 корпус",
        "folder_id":    "1hjwU3dGqK5Ssxsez1KGyMp0urpOOSTxO",
        "structure":    "flat",
        "table_format": "type_b",
        "file_filter":  None,
    },
]

CORPS_BY_ID = {c["id"]: c for c in CORPS}


def get_current_semester() -> int:
    month = date.today().month
    if month >= 9:
        return 1
    return 2
