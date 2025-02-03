import asyncio
import logging
import os
import openai
import asyncpg
from dotenv import load_dotenv
from pyrogram import Client
import pandas as pd
from asyncpg import exceptions
import argparse
from datetime import datetime, timedelta
import re
from collections import defaultdict
import json
import random

# Инициализация контекста и словаря сообщений для каждого пользователя
context = defaultdict(list)
user_messages = defaultdict(list)
timers = {}

# Загрузка переменных окружения из .env файла
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Настройка логгера
log_filename = os.path.join(os.path.dirname(__file__), 'script_logs.txt')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Загрузка аргументов командной строки
parser = argparse.ArgumentParser
parser.add_argument('--index_name', type=str, required=True)
args = parser.parse_args()
index_name = args.index_name

# Путь к Excel файлу с юзернеймами
EXCEL_FILE = '/example/usernames.xlsx'
COLUMN_NAME = 'example'
COLUMN_NAME = 'examplename'
BATCH_SIZE = 4

# Путь к директории логов
LOGS_DIR = '/example/Logs'
os.makedirs(LOGS_DIR, exist_ok=True)

# Асинхронное подключение к базе данных PostgreSQL
async def create_db_connection():
    return await asyncpg.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

# Функция для получения ответа от fine-tuned модели
async def get_finetuned_answer(messages, max_retries=3, temperature=0.7, top_p=0.6):
    retries = 0
    model_name = "ftjob-3sFdJ2ytxMVfNwOw4dNuvEpY"
    
    while retries < max_retries:
        try:
            response = await openai.ChatCompletion.acreate(
                model=model_name,
                messages=messages,
                temperature=temperature,
                top_p=top_p
            )
            content = response['choices'][0]['message']['content'].strip()
            return content
        except openai.error.RateLimitError as e:
            retries += 1
        except openai.error.APIConnectionError as e:
            retries += 1
        except openai.error.InvalidRequestError as e:
            break
        except Exception as e:
            retries += 1
    
    return "Извините, не могу обработать ваш запрос."

# Функция для получения текущего индекса из базы данных
async def get_current_index(conn, index_name):
    row = await conn.fetchrow("""
        SELECT current_index FROM processing_index WHERE index_name=$1;
    """, index_name)
    if row:
        return row['current_index']
    else:
        await conn.execute("""
            INSERT INTO processing_index (index_name, current_index) VALUES ($1, 0)
            ON CONFLICT (index_name) DO NOTHING;
        """, index_name)
        return 0

# Функция для обновления индекса в базе данных
async def update_current_index(conn, new_index, index_name):
    await conn.execute("""
        UPDATE processing_index SET current_index=$1 WHERE index_name=$2;
    """, new_index, index_name)

# Функция обработки сообщений
async def handle_response(client, conn):
    @client.on_message()
    async def on_message(client, message):
        username = message.chat.username if message.chat.username else "unknown_user"
        if username not in context:
            context[username] = []
        context[username].append({"role": "user", "content": message.text})
        ai_response = await get_finetuned_answer(context[username])
        if ai_response:
            await client.send_message(username, ai_response)
            context[username].append({"role": "assistant", "content": ai_response})

# Основная функция выполнения программы
async def main(index_name):
    clients = []
    conn = await create_db_connection()
    for account in [{'session_name': 'example'}]:
        client = Client(account['session_name'])
        await client.start()
        clients.append(client)
        asyncio.create_task(handle_response(client, conn))
    while True:
        await asyncio.sleep(3600)
    finally:
        await conn.close()
        for client in clients:
            await client.stop()

if __name__ == "__main__":
    asyncio.run(main(index_name))
