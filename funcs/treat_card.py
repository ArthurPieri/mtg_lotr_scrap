import json
import sqlite3
from logging import getLogger, shutdown

import pandas as pd


class Card:
    '''
    lalala
    '''

    def __init__(self, db_name: str):
        self.__start_log()

        self.__get_connection(db_name)

        self.set_lists: dict = {}
        self.set_dicts: dict = {}

    def create_db(self):
        ...

    def find_missing_card(self):
        ...

    def get_card_price(self):
        ...

    def _read_json_file(self, file_path: str):
        with open(file=file_path, mode='r', encoding='UTF-8') as f:
            data = json.load(f)

        self.set = data['data']['cards']  # pylint: disable=attribute-defined-outside-init

    def _split_lists_and_dicts(self, drop: list, cards_set: list):
        '''
        lalala
        '''
        for card in cards_set:
            card = self.__drop_columns(drop=drop, card=card)

            for key in card:
                if not card[key]:
                    card[key] = None
                if isinstance(card[key], list):
                    if key not in self.set_lists.keys():
                        self.set_lists[key] = []
                    self.set_lists = self.__split_data(card=card, key=key, splited_set=self.set_lists)  # type: ignore  # noqa: E501
                elif isinstance(card[key], dict):
                    if key not in self.set_dicts.keys():
                        self.set_dicts[key] = []
                    self.set_dicts = self.__split_data(card=card, key=key, splited_set=self.set_dicts)  # type: ignore  # noqa: E501

    def __split_data(self, card: dict, key: str, splited_set: dict) -> dict:
        new_card: dict = {}
        new_card[key] = card[key]
        new_card['name'] = card['name']
        new_card['number'] = card['number']
        new_card['uuid'] = card['uuid']
        splited_set[key].append(new_card)
        return splited_set

    def __del__(self):
        self.conn.close()
        self.log.info('Connection to db Closed')
        shutdown()

    def __drop_columns(self, drop: list, card: list):
        for i in drop:
            card.pop(i)
        return card

    def __get_connection(self, db_name: str):
        self.conn = sqlite3.connect(db_name)

    def __start_log(self):
        self.log = getLogger(__name__)  # pylint: disable=attribute-defined-outside-init
        self.log.info('Starting class Card')
