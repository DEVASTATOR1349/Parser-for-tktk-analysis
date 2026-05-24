"""Генератор файла service-account.json из переменной окружения."""

import json
import os

import dotenv

dotenv.load_dotenv()


def generate_service_account():
    """Создаёт файл service-account.json из переменной GOOGLE_SERVICE_ACCOUNT_JSON."""
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not json_str:
        print("GOOGLE_SERVICE_ACCOUNT_JSON не задан")
        return

    try:
        data = json.loads(json_str)
        with open("service-account.json", "w") as f:
            json.dump(data, f, indent=2)
        print("service-account.json создан")
    except json.JSONDecodeError:
        print("Ошибка: GOOGLE_SERVICE_ACCOUNT_JSON не является валидным JSON")


if __name__ == "__main__":
    generate_service_account()
