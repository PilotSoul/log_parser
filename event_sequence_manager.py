from abc import ABC, abstractmethod
from typing import List

from config import SourceColumns, RowData


class SequenceState(ABC):
    """
    Абстрактный класс для состояний
    """

    @abstractmethod
    def handle(self, context, event: int, row: RowData) -> None:
        """
        Обработка события в текущем состоянии

        :param context: Контекст, который хранит текущие состояния и последовательности
        :param event: Событие, которое необходимо обработать
        :param row: Строка данных, с которой связано событие
        """
        pass


class ProgramChoiceState(SequenceState):
    """
    Начальное событие - Выбор программы
    """

    def handle(self, context, event: int, row: RowData) -> None:
        if event == 15:
            context.set_state(PreheatingCompletionState())
        else:
            context.reset_sequence(row)


class PreheatingCompletionState(SequenceState):
    """
    Событие - Завершение преднагрева
    """

    def handle(self, context, event: int, row: RowData) -> None:
        if event == 1:
            context.set_state(DoorOpenCloseState())
        else:
            context.reset_sequence(row)


class DoorOpenCloseState(SequenceState):
    """
    Событие - Открытие/закрытие дверей
    """

    def handle(self, context, event: int, row: RowData) -> None:
        if event == 20:
            context.set_state(DoorOpenCloseState())
        elif event == 21:
            context.set_state(BakingStartState())
        else:
            context.reset_sequence(row)


class BakingStartState(SequenceState):
    """
    Событие - Начало выпекания
    """

    def handle(self, context, event: int, row: RowData) -> None:
        if event == 16:
            context.set_state(BakingFinishState())
        else:
            context.reset_sequence(row)


class BakingFinishState(SequenceState):
    """
    Завершение выпекания или прерывание
    """

    def handle(self, context, event: int, row: RowData) -> None:
        if event == 17 or event == 18 or event == 8:
            context.save_sequence()
            context.set_state(ProgramChoiceState())
        else:
            context.reset_sequence(row)


class SequenceFinderManager:
    def __init__(self):
        self.state: SequenceState = ProgramChoiceState()
        self.current_sequence: List[RowData] = []
        self.found_sequences: List[List[RowData]] = []

    def set_state(self, state):
        self.state = state

    def process_event(self, event: int, row: RowData):
        """
        Обработка текущего события
        :param event: id события
        :param row: Строка данных
        :return:
        """
        self.current_sequence.append(row)
        self.state.handle(self, event, row)

    def save_sequence(self):
        """
        Сохранение текущей последовательности
        """
        self.found_sequences.append(self.current_sequence)
        self.current_sequence = []

    def reset_sequence(self, row: RowData):
        """
        Сброс текущей последовательности
        """
        if self.is_valid_start_row(row):
            self.current_sequence = [row]
            self.set_state(PreheatingCompletionState())
        else:
            self.current_sequence = []
            self.set_state(ProgramChoiceState())

    def is_valid_start_row(self, row: RowData) -> bool:
        """
        Проверка, может ли данная строка быть началом новой последовательности
        :param row: Строка данных
        :return: True, если строка валидна для начала новой последовательности
        """
        return row[SourceColumns.EVENT_ID.name] == 15
