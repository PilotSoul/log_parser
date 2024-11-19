from typing import List

from config import SourceColumns, DictedEnum, RowData, SequenceRows
from data_loader import FileReader, FileWriter
import polars as pl
from event_sequence_manager import SequenceFinderManager


class MainApp:
    """
    Главный класс приложения для чтения и обработки данных
    """

    def __init__(self, file_path: str, columns: DictedEnum):
        self.file_path = file_path
        self.column_ids = columns.get_values()
        self.column_names = columns.get_keys()
        self.df = self.read_file()

    def read_file(self) -> pl.DataFrame:
        """
        Чтение файла в формате CSV
        :return: DataFrame polars
        """
        return FileReader.read_csv(
            self.file_path, columns=self.column_ids, separator=";", new_columns=self.column_names
        )

    def run(self) -> pl.DataFrame:
        """
        Запуск процесса обработки данных
        """
        # преобразуем столбец для дальнейшего вычисления длительности
        self.df = self.df.with_columns(
            [
                pl.col(SourceColumns.DATE.name)
                .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.3f")
                .alias(SourceColumns.DATE.name),
            ]
        )
        grouped = self.df.group_by(
            [
                SourceColumns.STOVE_ID.name,
                SourceColumns.PROGRAM_ID.name,
                SourceColumns.OVEN.name,
            ]
        )
        context = SequenceFinderManager()
        # пробегаемся по группам
        for group_key, group_df in grouped:
            # сортировка, так как в polars не гарантируемся порядок
            group_df = group_df.sort([SourceColumns.DATE.name])
            for row in group_df.iter_rows(named=True):
                event = row[SourceColumns.EVENT_ID.name]
                context.process_event(event, row)
        return self.aggregate_sequences(context.found_sequences)

    def aggregate_sequences(self, result_sequences: List[List[RowData]]) -> pl.DataFrame:
        """
        Формирование итогового DataFrame из найденных последовательностей
        :param result_sequences:
        :return:
        """
        selected_columns = {
            SourceColumns.OVEN.name: "Печь",
            SourceColumns.PROGRAM_ID.name: "Номер программы выпечки",
            SourceColumns.STOVE_ID.name: "Номер духовки",
            SourceColumns.AMOUNT_OF_STOVES.name: "Количество духовок",
        }
        rows = []
        for seq in result_sequences:
            first_row = seq[SequenceRows.START_ROW.value]
            last_row = seq[SequenceRows.FINISH_ROW.value]

            new_row = {"Время старта": first_row["DATE"], "Время завершения": last_row["DATE"]}

            for key in selected_columns:
                new_row[selected_columns[key]] = first_row[key]

            new_row["Длительность"] = self.calculate_duration(first_row["DATE"], last_row["DATE"])

            rows.append(new_row)

        return pl.DataFrame(rows)

    def calculate_duration(self, first_date: pl.Datetime, last_date: pl.Datetime) -> str:
        """
        :param first_date: Дата старта
        :param last_date: Дата окончания
        :return: Строка с длительностью работы в формате "0 days HH:MM:SS"
        """
        total_seconds = (last_date - first_date).total_seconds()
        days = int(total_seconds // 86400)
        hours = int((total_seconds % 86400) // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        return f"{days} days {hours:02}:{minutes:02}:{seconds:02}"

    def write_file(self, dataframe: pl.DataFrame, file_path: str) -> pl.DataFrame:
        """
        Запись файла в формате CSV
        :param dataframe: датафрейм для записи
        :param file_path: Путь для записи файла
        :return: DataFrame polars
        """
        return FileWriter.write_to_csv(
            df=dataframe,
            file_path=file_path,
            separator=";",
        )


if __name__ == "__main__":
    csv_file = "files/sources_dataset.csv"
    app = MainApp(csv_file, SourceColumns)
    preprocessed_df = app.run()
    path_to_write = "files/output.csv"
    app.write_file(preprocessed_df, path_to_write)
