import polars as pl
from enum import Enum
from typing import Optional, TypedDict


class AbstractExtendedEnum(Enum):
    """
    Базовая абстрактная расширенная версия энума
    """

    @classmethod
    def as_dict(cls):
        """
        Преобразование энум в словарь
        """
        return {i.name: i.value for i in cls}

    @classmethod
    def get_keys(cls):
        """
        Преобразование ключей энума в список
        """
        return list(cls.as_dict().keys())

    @classmethod
    def get_values(cls):
        """
        Преобразование значений энума в список
        """
        return list(cls.as_dict().values())


class DictedEnum(AbstractExtendedEnum):
    """
    Расширенный энум, с возможностью преобразования данных в словарь
    """

    @classmethod
    def list(cls, values_to_remove: Optional[list] = None):
        """
        Преобразование ключей в список
        """
        list_data = list(map(lambda x: x.value, cls))
        if values_to_remove:
            for item in values_to_remove:
                list_data.remove(item)
        return list_data


class SourceColumns(DictedEnum):
    """
    Название колонок для файла с данным по печам
    """

    DATE = 0
    STOVE_ID = 1
    PROGRAM_NAME = 2
    PROGRAM_ID = 3
    EVENT_ID = 4
    OVEN = 5
    AMOUNT_OF_STOVES = 6


class SequenceRows(DictedEnum):
    """
    Название строк в последовательности
    """

    # предпоследний элемент - дата старта выполнения
    START_ROW = -2
    # последний элемент - дата окончания выполнения
    FINISH_ROW = -1


class RowData(TypedDict):
    OVEN: str
    PROGRAM_ID: str
    STOVE_ID: str
    AMOUNT_OF_STOVES: int
    DATE: pl.Datetime
    EVENT_ID: int
