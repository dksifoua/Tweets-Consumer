import sys
import csv

from src.singleton import Singleton


class AbbreviationManager(Singleton):

    def __init__(self):
        super().__init__()
        self.__financial_abbr = self.__class__.load_abbr('Financial')
        self.__common_abbr = self.__class__.load_abbr('Common')
        self.__class__.__instance = self

    @staticmethod
    def load_abbr(name: str):
        try:
            with open(f'./resources/abbr/{name}.csv', 'r') as csv_file:
                abbr = {}
                for row in csv.reader(csv_file):
                    abbr[row[0]] = row[1]
                return abbr
        except Exception as e:
            print(f'Exception => {e}')
            sys.exit(-1)

    @property
    def financial_abbr(self):
        return self.__financial_abbr

    @property
    def common_abbr(self):
        return self.__common_abbr
