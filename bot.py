import asyncio
import logging
from textwrap import dedent
import services.config as config
from services.models import IncomeRequest
from services.mongo_api import MongoDB
from services.conversions import convert_aggregation_for_telegram
from pydantic import ValidationError
from telebot.async_telebot import AsyncTeleBot


bot = AsyncTeleBot(
    token=config.BOT_TOKEN,
    parse_mode='HTML',
)

mongo_db = MongoDB(db_name='test_database', collection='test_salary_collection')
mongo_db.upload_test_salary_collection(
    collection_name='test_salary_collection',
    filename='dump/sampleDB/sample_collection.bson'
)


@bot.message_handler(commands=['help', 'start'])
async def send_welcome(message):

    await bot.send_message(
        chat_id=message.chat.id,
        text="Бот для проверки тестового задания (оправьте запрос)",
    )


@bot.message_handler(func=lambda message: True)
async def request_message(message):
    try:
        income_request = IncomeRequest.model_validate_json(message.text)
        groupby_salary = mongo_db.fetch_group_salary(income_request)
        text = convert_aggregation_for_telegram(groupby_salary, income_request)
        await bot.send_message(message.chat.id, text)

    except ValidationError:
        await bot.send_message(
            chat_id=message.chat.id,
            text=dedent(
                '''
                Неверные входные данные или ошибка валидации

                Пример запроса:
                <code>
                {
                "dt_from":"2022-09-01T00:00:00",
                "dt_upto":"2022-12-31T23:59:00",
                "group_type":"month"
                }
                </code>
                '''
            )
        )


logger = logging.getLogger('test-mongodb-bot')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger.info('Запущен чат-бот "Тестовое задание RLT"')

asyncio.run(bot.polling())
