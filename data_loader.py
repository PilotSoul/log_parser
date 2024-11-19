import polars as pl


class FileReader:
    """
    Класс для чтения файлов
    """

    @staticmethod
    def read_csv(file_path: str, **kwargs) -> pl.DataFrame:
        """
        Читает CSV файл с использованием Polars

        :param file_path: Путь к CSV файлу
        :return: DataFrame polars
        """
        try:
            return pl.read_csv(file_path, **kwargs)
        except Exception as e:
            raise ValueError(f"Ошибка при чтении CSV файла: {e}")


class FileWriter:
    """
    Класс для записи в файлы
    """

    @staticmethod
    def write_to_csv(df: pl.DataFrame, file_path: str, **kwargs):
        """
        Читает CSV файл с использованием Polars

        :param df: Датафрейм для записи
        :param file_path: Путь к CSV файлу
        """
        try:
            df.write_csv(file_path, **kwargs)
        except Exception as e:
            raise ValueError(f"Ошибка при чтении CSV файла: {e}")
