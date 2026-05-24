#!/usr/bin/env python3
"""
build.py — Скрипт для Claude Code.
Собирает всё воедино, устанавливает зависимости, подготавливает к запуску.

Запуск в VS Code терминале:
    python scripts/build.py
"""

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_DIR)


def step(msg: str):
    print(f"\n{'='*50}")
    print(f"  {msg}")
    print(f"{'='*50}")


def run(cmd: str, **kwargs):
    print(f"> {cmd}")
    subprocess.run(cmd, shell=True, check=True, **kwargs)


def main():
    step("1. Создаём виртуальное окружение")
    if not (PROJECT_DIR / "venv").exists():
        run(f"{sys.executable} -m venv venv")

    # Определяем путь к pip
    pip = str(PROJECT_DIR / "venv/bin/pip")

    step("2. Устанавливаем зависимости")
    run(f"{pip} install --upgrade pip setuptools wheel")
    run(f"{pip} install -r requirements.txt")

    step("3. Создаём папку для логов")
    (PROJECT_DIR / "logs").mkdir(exist_ok=True)

    step("4. Проверяем .env")
    env_file = PROJECT_DIR / ".env"
    env_example = PROJECT_DIR / ".env.example"
    if not env_file.exists():
        run(f"cp {env_example} {env_file}")
        print("⚠️  Создан .env из .env.example — отредактируй его!")
    else:
        print("✅ .env уже существует")

    step("5. Проверяем структуру")
    print(f"""
📁 Структура проекта:

{PROJECT_DIR.name}/
├── .env                    ← Сюда токены и ключи
├── .env.example            ← Шаблон
├── requirements.txt        ← Зависимости
├── crontab.example         ← Настройка крона
├── README.md               ← Инструкция
├── logs/                   ← Логи (авто)
├── scripts/
│   ├── build.py            ← Этот скрипт
│   └── generate_service_account.py
└── src/
    ├── main.py             ← Точка входа
    ├── config.py           ← Конфигурация
    ├── sheets.py           ← Google Sheets
    └── parser.py           ← Apify парсер
""")

    step("✅ Всё готово!")
    print("""
Следующие шаги:
1. Отредактируй .env  — вставь APIFY_API_TOKEN и GOOGLE ключи
2. Запусти тест:       python src/main.py
3. Настрой cron:       crontab crontab.example
""")


if __name__ == "__main__":
    main()
