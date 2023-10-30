import bson
import logging
from pymongo import MongoClient, errors
from services.models import IncomeRequest, GroupPeriod

logger = logging.getLogger('test-mongodb-bot')


class MongoDB(object):
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 27017,
        db_name: str = None,
        collection: str = None
    ):
        self._client = MongoClient(f'mongodb://{host}:{port}')
        self._db_name = self._client[db_name]
        self._collection = self._client[db_name][collection]

    def upload_test_salary_collection(self, collection_name: str, filename: str) -> None:
        '''
        Загрузка тестовых документов в базу данных
        '''
        if collection_name not in self._db_name.list_collection_names():
            logger.info('Нет тестовой БД. Восстанавливаем..')
            try:
                with open(filename, 'rb') as f:
                    self.__collection.insert_many(bson.decode_all(f.read()))
                    logger.info('Тестовая база загружена.')

            except FileNotFoundError:
                logger.error(f'Запрашиваемый файл {filename} не найден')

            except bson.errors.BSONError as err:
                logger.error(f'Ошибка BSON: {err}')

            except errors.BulkWriteError as mongo_err:
                logger.error(f'Ошибка записи в БД {mongo_err}')

    def fetch_group_salary(self, request: IncomeRequest) -> dict:
        '''
        Получение выборки документов по входному фильтру
        '''
        match request.group_type:
            case GroupPeriod.month:
                format = '%Y-%m'
            case GroupPeriod.day:
                format = '%Y-%m-%d'
            case GroupPeriod.hour:
                format = '%Y-%m-%d-%H'

        return self._collection.aggregate(
            [
                {
                    '$match': {
                        'dt': {
                            '$gte': request.dt_from,
                            '$lte': request.dt_upto,
                        }
                    }
                },
                {
                    '$project': {
                        'date_filter': {
                            '$dateToString': {'date': '$dt', 'format': format},
                        },
                        'value': '$value',
                    }
                },            {
                    '$group': {
                        '_id': '$date_filter',
                        'sum_value': {'$sum': '$value'},
                    },
                },
                {
                    '$sort': {'_id': 1}
                },
            ]
        )
