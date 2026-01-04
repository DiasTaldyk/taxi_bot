import aiohttp  # для получения адреса по координатам
import logging
import os
import asyncpg
import asyncio  # <-- ЭТО ВАЖНО! Добавить эту строку
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from datetime import datetime, timedelta

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# ================= CONFIG =================
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7641635083:AAEv_ij2aNZhJpLTsmZKCe1vjYdgWQsFfVE")
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123@localhost:5432/taxi_bot")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
db_pool = None

# ================= TEXTS =================

TEXT = {
    "kz": {
        "cancel": "❌ Бас тарту",
        "lang_selected": "🇰🇿 Тіл сәтті таңдалды!\n\nСәлем! 👋\nСізге қандай қызмет көрсете аламын?",
         "main_menu": "Негізгі мәзір",
        "become_driver": "🚕 Жүргізуші болу",
        "enter_car_model": "🚗 Көлік маркасын жазыңыз (мысалы: Toyota Camry):",
        "enter_car_number": "🚘 Көлік нөмірін жазыңыз (мысалы: 01KZ123ABC):",
        "enter_phone": "📱 Телефон нөміріңізді жазыңыз:",
        
        
        
        "choose_from": "📍 Шығатын мекен-жайыңызды жазыңыз немесе локация жіберіңіз",
        "choose_to": "📍 Баратын жерді жазыңыз",
        "send_phone": "☎️ Байланыс нөміріңізді жіберіңіз",
        "order_done": "✅ Тапсырыс қабылданды!\n\n📍 Қайдан: {from_}\n📍 Қайда: {to}\n💰 Бағасы: {price} ₸\n☎️ Телефон: {phone}\n\n🚕 Жүргізушілерге жіберілді",
        "select_lang": "Тілді таңдаңыз",
        "no_lang": "Алдымен тілді таңдаңыз",
        "action_canceled": "Әрекет бас тартылды",
        "db_error": "❌ Дерекқор қатесі",
        "need_lang": "Тілді таңдау үшін /start басыңыз",
        "blocked": "🚫 Сіздің аккаунтыңыз блокталған! Жалған тапсырыс бергеніңіз үшін.",
        "active_order": "⏳ Сізде әлі де белсенді тапсырыс бар! Жаңа тапсырыс беру үшін 5 минут күтіңіз немесе алдыңғы тапсырысыңызды аяқтаңыз.",
        "too_many_orders": "❌ Сізде тым көп белсенді тапсырыс бар! Бір уақытта тек бір тапсырыс бере аласыз.",
        
        "choose_price": "💰 Бағаны таңдаңыз немесе өз бағаңызды жазыңыз (тек сан):",
        "price_options": ["300 ₸", "400 ₸", "500 ₸", "600 ₸", "📝 Өз бағам"],
        "invalid_price": "❌ Қате баға! Тек сан жазыңыз (мысалы: 750)",
        "price_selected": "✅ Баға таңдалды: {price} ₸",
        
        
        # Для водителей
        "driver_welcome": "👋 Сәлем, жүргізуші!\n\nЖүйеге кірдіңіз.",
        "driver_menu": "🚕 Жүргізуші панелі:\n\n📋 /active_orders - Белсенді тапсырыстар\n✅ /my_orders - Менің тапсырыстарым\n📊 /stats - Статистика\n⚙️ /settings - Баптаулар\n🚫 /logout - Жүйеден шығу",
        "active_orders": "📋 Белсенді тапсырыстар:",
        "no_active_orders": "📭 Белсенді тапсырыстар жоқ",
        "order_details": "📋 Тапсырыс #{id}\n\n📍 Қайдан: {from_}\n📍 Қайда: {to}\n💰 Бағасы: {price} ₸\n📞 Телефон: {phone}\n⏰ Уақыт: {time}",
        "accept_order": "✅ Тапсырысты қабылдау",
        "order_accepted": "✅ Сіз тапсырысты қабылдадыңыз! Клиентке хабарласыңыз: +{phone}",
        "order_completed": "✅ Тапсырыс аяқталды!",
        "my_stats": "📊 Сіздің статистикаңыз:\n\n📈 Барлық тапсырыс: {total}\n⭐ Рейтинг: {rating}\n💰 Табыс: {income} ₸",
        "driver_settings": "⚙️ Жүргізуші баптаулары:\n\n🚗 Көлік: {car}\n📱 Телефон: {phone}\n🌐 Статус: {status}",
        "driver_offline": "🚫 Жүйеден шықтыңыз",
        "driver_online": "✅ Жүйеге кірдіңіз",
        
        # Уведомления для клиентов
        "driver_accepted": "🚕 Жүргізуші тапсырысыңызды қабылдады!\n\n👤 Жүргізуші: {driver_name}\n🚗 Көлік: {car_model}\n🚘 Нөмірі: {car_number}\n📱 Телефоны: +{driver_phone}\n\n📞 Жүргізуші сізбен байланысады немесе сіз оған қоңырау шалыңыз.",
        "waiting_for_driver": "⏳ Тапсырысыңыз жүргізушілерге жіберілді. Жүргізуші қабылдағанша күтіңіз...",
        "order_accepted_title": "✅ Тапсырыс қабылданды!",
         # Завершение заказа
        "order_completed_client": "✅ Тапсырыс #{order_id} аяқталды!\n\n📍 Қайдан: {from_}\n📍 Қайда: {to}\n💰 Төленді: {price} ₸\n\n🎉 Рахмет! Тағы кездескенше!"
        
        
        
    },
    "ru": {
        "cancel": "❌ Отмена",
        "lang_selected": "🇷🇺 Язык успешно выбран!\n\nЗдравствуйте! 👋\nЧем могу помочь?",
         "main_menu": "Главное меню",
        "become_driver": "🚕 Стать водителем",
        "enter_car_model": "🚗 Введите марку автомобиля (например: Toyota Camry):",
        "enter_car_number": "🚘 Введите номер автомобиля (например: 01KZ123ABC):",
        "enter_phone": "📱 Введите ваш номер телефона:",
        
        
        "choose_from": "📍 Откуда вас забрать? Напишите адрес или отправьте локацию",
        "choose_to": "📍 Куда поедем?",
        "send_phone": "☎️ Отправьте номер телефона",
        "order_done": "✅ Заказ принят!\n\n📍 Откуда: {from_}\n📍 Куда: {to}\n💰 Цена: {price} ₸\n☎️ Телефон: {phone}\n\n🚕 Передано водителям",
        "select_lang": "Выберите язык",
        "no_lang": "Сначала выберите язык",
        "action_canceled": "Действие отменено",
        "db_error": "❌ Ошибка базы данных",
        "need_lang": "Нажмите /start для выбора языка",
        "blocked": "🚫 Ваш аккаунт заблокирован! За размещение ложных заказов.",
        "active_order": "⏳ У вас еще есть активный заказ! Подождите 5 минут для создания нового заказа или завершите предыдущий.",
        "too_many_orders": "❌ У вас слишком много активных заказов! Одновременно можно иметь только один заказ.",
        
        "choose_price": "💰 Выберите цену или напишите свою цену (только цифры):",
        "price_options": ["300 ₸", "400 ₸", "500 ₸", "600 ₸", "📝 Моя цена"],
        "invalid_price": "❌ Неверная цена! Введите только цифры (например: 750)",
        "price_selected": "✅ Цена выбрана: {price} ₸",
        
        
        
        
        # Для водителей
        "driver_welcome": "👋 Привет, водитель!\n\nВы вошли в систему.",
        "driver_menu": "🚕 Панель водителя:\n\n📋 /active_orders - Активные заказы\n✅ /my_orders - Мои заказы\n📊 /stats - Статистика\n⚙️ /settings - Настройки\n🚫 /logout - Выйти из системы",
        "active_orders": "📋 Активные заказы:",
        "no_active_orders": "📭 Активных заказов нет",
        "order_details": "📋 Заказ #{id}\n\n📍 Откуда: {from_}\n📍 Куда: {to}\n💰 Цена: {price} ₸\n📞 Телефон: {phone}\n⏰ Время: {time}",
        "accept_order": "✅ Принять заказ",
        
        "order_accepted": "✅ Вы приняли заказ! Свяжитесь с клиентом: +{phone}",
        "order_completed": "✅ Заказ завершен!",
        "my_stats": "📊 Ваша статистика:\n\n📈 Всего заказов: {total}\n⭐ Рейтинг: {rating}\n💰 Доход: {income} ₸",
        "driver_settings": "⚙️ Настройки водителя:\n\n🚗 Автомобиль: {car}\n📱 Телефон: +{phone}\n🌐 Статус: {status}",
        
        "driver_offline": "🚫 Вы вышли из системы",
        "driver_online": "✅ Вы вошли в систему",
        
         # Уведомления для клиентов
        "driver_accepted": "🚕 Водитель принял ваш заказ!\n\n👤 Водитель: @{driver_name}\n🚗 Автомобиль: {car_model}\n🚘 Номер: {car_number}\n📱 Телефон: +{driver_phone}\n\n📞 Водитель свяжется с вами или вы можете позвонить ему.",
        "waiting_for_driver": "⏳ Ваш заказ отправлен водителям. Ожидайте, пока водитель примет заказ...",
        "order_accepted_title": "✅ Заказ принят!",
        # Завершение заказа
        "order_completed_client": "✅ Заказ #{order_id} завершен!\n\n📍 Откуда: {from_}\n📍 Куда: {to}\n💰 Оплачено: {price} ₸\n\n🎉 Спасибо! До новых поездок!"

    }
}

# ================= STATES =================

class OrderState(StatesGroup):
    language = State()
    from_place = State()
    to_place = State()
    price = State()  # <-- Жаңа state қосамыз
    contact = State()

class DriverState(StatesGroup):
    language = State()
    car_model = State()
    car_number = State()
    phone = State()
# ================= DATABASE =================

async def create_tables():
    """Создание таблиц в базе данных"""
    async with db_pool.acquire() as conn:
        # Таблица деактиваций водителей
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS driver_deactivations (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                deactivated_by BIGINT,
                reason TEXT,
                deactivated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица заявок на водителей
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS driver_applications (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE,
                username VARCHAR(255),
                phone VARCHAR(50),
                car_model VARCHAR(100),
                car_number VARCHAR(20),
                status VARCHAR(20) DEFAULT 'pending', -- pending, approved, rejected
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_by BIGINT,
                approved_at TIMESTAMP,
                rejected_by BIGINT,
                rejected_at TIMESTAMP,
                reject_reason TEXT,
                expires_at TIMESTAMP
            )
        """)
        
        # Таблица водителей (жаңартамыз)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE,
                username VARCHAR(255),
                phone VARCHAR(50),
                car_model VARCHAR(100),
                car_number VARCHAR(20),
                status VARCHAR(20) DEFAULT 'offline',
                rating FLOAT DEFAULT 5.0,
                total_orders INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP, -- Срок действия водительских прав
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # Таблица рейтингов и отзывов водителей
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS driver_ratings (
                id SERIAL PRIMARY KEY,
                driver_id BIGINT,
                user_id BIGINT,
                order_id INTEGER,
                rating INTEGER CHECK (rating >= 1 AND rating <= 5),
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        
        
        # Таблица заказов (полная структура)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                username VARCHAR(255),
                phone VARCHAR(50),
                from_text TEXT,
                from_lat FLOAT,
                from_lon FLOAT,
                to_text TEXT,
                price INTEGER,
                language VARCHAR(10),
                status VARCHAR(20) DEFAULT 'new',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                driver_id BIGINT,
                driver_username VARCHAR(255),
                accepted_at TIMESTAMP
            )
        """)
        
        # Таблица заблокированных пользователей (ложные заказы)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS blocked_users (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                phone VARCHAR(50),
                reason VARCHAR(255) DEFAULT 'false_order',
                blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                blocked_by BIGINT,
                UNIQUE(user_id, phone)
            )
        """)
        
        logger.info("✅ Базовые таблицы созданы/проверены")

async def check_and_add_columns():
    """Проверяем и добавляем недостающие колонки в таблицу orders"""
    try:
        async with db_pool.acquire() as conn:
            # Проверяем наличие колонки is_active
            try:
                result = await conn.fetchval("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'orders' AND column_name = 'is_active'
                """)
                
                if not result:
                    # Добавляем колонку is_active
                    await conn.execute("""
                        ALTER TABLE orders ADD COLUMN is_active BOOLEAN DEFAULT TRUE
                    """)
                    logger.info("✅ Колонка is_active добавлена в таблицу orders")
            except Exception as e:
                logger.warning(f"Ошибка при проверке колонки is_active: {e}")
            
            # Проверяем наличие колонки driver_id
            try:
                result = await conn.fetchval("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'orders' AND column_name = 'driver_id'
                """)
                
                if not result:
                    # Добавляем колонку driver_id
                    await conn.execute("""
                        ALTER TABLE orders ADD COLUMN driver_id BIGINT
                    """)
                    logger.info("✅ Колонка driver_id добавлена в таблицу orders")
            except Exception as e:
                logger.warning(f"Ошибка при проверке колонки driver_id: {e}")
            
            # Проверяем наличие колонки driver_username
            try:
                result = await conn.fetchval("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'orders' AND column_name = 'driver_username'
                """)
                
                if not result:
                    # Добавляем колонку driver_username
                    await conn.execute("""
                        ALTER TABLE orders ADD COLUMN driver_username VARCHAR(255)
                    """)
                    logger.info("✅ Колонка driver_username добавлена в таблицу orders")
            except Exception as e:
                logger.warning(f"Ошибка при проверке колонки driver_username: {e}")
            
            # Проверяем наличие колонки accepted_at
            try:
                result = await conn.fetchval("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'orders' AND column_name = 'accepted_at'
                """)
                
                if not result:
                    # Добавляем колонку accepted_at
                    await conn.execute("""
                        ALTER TABLE orders ADD COLUMN accepted_at TIMESTAMP
                    """)
                    logger.info("✅ Колонка accepted_at добавлена в таблицу orders")
            except Exception as e:
                logger.warning(f"Ошибка при проверке колонки accepted_at: {e}")
                
            # Создаем индекс для быстрого поиска активных заказов
            try:
                # Проверяем существует ли индекс
                index_exists = await conn.fetchval("""
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'orders' AND indexname = 'idx_active_orders'
                """)
                
                if not index_exists:
                    await conn.execute("""
                        CREATE INDEX idx_active_orders 
                        ON orders(user_id, is_active, created_at) 
                        WHERE is_active = TRUE
                    """)
            except Exception as e:
                logger.warning(f"Ошибка при создании индекса: {e}")
            
            logger.info("✅ Структура таблицы orders проверена и обновлена")
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении структуры таблиц: {e}")      
        
        
        
# ================= STARTUP =================

async def on_startup(dp):
  
    """Функция запуска при старте бота"""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DB_URL)
        await create_tables()
        
        # Проверяем просроченных водителей при запуске
        await check_expired_drivers()
        
         # Очищаем старые заказы при запуске
        await cleanup_old_orders()
        
        await check_and_add_columns()
        logger.info("✅ PostgreSQL подключен и таблицы обновлены")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")

# ================= USER DATA STORAGE =================

# Простое хранилище данных пользователя
user_data = {}

# ================= CHECK FUNCTIONS =================

async def is_user_blocked(user_id: int, phone: str = None) -> bool:
    """Проверка, заблокирован ли пользователь"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            if phone:
                # Проверяем по user_id ИЛИ phone (если номер указан)
                blocked = await conn.fetchval("""
                    SELECT 1 FROM blocked_users 
                    WHERE user_id = $1 OR phone = $2 
                    LIMIT 1
                """, user_id, phone)
            else:
                # Проверяем только по user_id
                blocked = await conn.fetchval("""
                    SELECT 1 FROM blocked_users 
                    WHERE user_id = $1 
                    LIMIT 1
                """, user_id)
            
            return bool(blocked)
    except Exception as e:
        logger.error(f"Ошибка при проверке блокировки пользователя: {e}")
        return False

async def has_active_order(user_id: int) -> bool:
    """Проверяет, есть ли у пользователя активный заказ (не старше 5 минут)"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли колонка is_active
            column_exists = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'is_active'
            """)
            
            if column_exists:
                # Проверяем активные заказы через is_active
                active_order = await conn.fetchrow("""
                    SELECT id, created_at, status
                    FROM orders 
                    WHERE user_id = $1 
                    AND is_active = TRUE 
                    AND status IN ('new', 'accepted', 'in_progress')
                    AND created_at > NOW() - INTERVAL '5 minutes'
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, user_id)
            else:
                # Проверяем активные заказы через статус
                active_order = await conn.fetchrow("""
                    SELECT id, created_at, status
                    FROM orders 
                    WHERE user_id = $1 
                    AND status IN ('new', 'accepted', 'in_progress')
                    AND created_at > NOW() - INTERVAL '5 minutes'
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, user_id)
            
            if active_order:
                logger.info(f"✅ Найден активный заказ для пользователя {user_id}: #{active_order['id']}, статус: {active_order['status']}, время: {active_order['created_at']}")
                return True
            else:
                logger.info(f"❌ Активных заказов для пользователя {user_id} не найдено")
                return False
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке активных заказов: {e}")
        return False


async def get_active_order_count(user_id: int) -> int:
    """Возвращает количество активных заказов пользователя"""
    if not db_pool:
        return 0
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли колонка is_active
            column_exists = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'is_active'
            """)
            
            if column_exists:
                # Пробуем проверить через is_active
                count = await conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM orders 
                    WHERE user_id = $1 
                    AND is_active = TRUE 
                    AND status IN ('new', 'accepted', 'in_progress')
                    AND created_at > NOW() - INTERVAL '5 minutes'
                """, user_id)
                
                return count or 0
            else:
                # Альтернативная проверка по статусу и времени
                count = await conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM orders 
                    WHERE user_id = $1 
                    AND status IN ('new', 'accepted', 'in_progress')
                    AND created_at > NOW() - INTERVAL '5 minutes'
                """, user_id)
                
                return count or 0
    except Exception as e:
        logger.error(f"Ошибка при подсчете активных заказов: {e}")
        return 0


async def deactivate_old_orders(user_id: int):
    """Деактивирует ВСЕ старые заказы пользователя (независимо от времени)"""
    if not db_pool:
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли колонка is_active
            column_exists = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'is_active'
            """)
            
            if column_exists:
                # Деактивируем ВСЕ активные заказы пользователя через is_active
                await conn.execute("""
                    UPDATE orders 
                    SET is_active = FALSE 
                    WHERE user_id = $1 AND is_active = TRUE
                """, user_id)
                logger.info(f"✅ Все активные заказы пользователя {user_id} деактивированы (через is_active)")
            else:
                # Если колонки is_active нет, обновляем статус
                await conn.execute("""
                    UPDATE orders 
                    SET status = 'cancelled' 
                    WHERE user_id = $1 AND status IN ('new', 'accepted', 'in_progress')
                """, user_id)
                logger.info(f"✅ Все активные заказы пользователя {user_id} отменены (через статус)")
            
            # Также удаляем заказы из списка активных для водителей
            # Обновляем статус заказов, которые могут быть у водителей
            await conn.execute("""
                UPDATE orders 
                SET status = 'cancelled',
                    driver_id = NULL,
                    driver_username = NULL
                WHERE user_id = $1 AND status = 'new'
            """, user_id)
            
    except Exception as e:
        logger.error(f"Ошибка при деактивации старых заказов пользователя {user_id}: {e}")

# команду для проверки активных заказов пользователя
@dp.message_handler(commands=["my_active_orders"])
async def my_active_orders_command(message: types.Message):
    """Проверка активных заказов пользователя"""
    user_id = message.from_user.id
    
    # Проверяем, есть ли активный заказ
    has_active = await has_active_order(user_id)
    
    # Получаем количество активных заказов
    active_count = await get_active_order_count(user_id)
    
    response = f"👤 Ваш ID: {user_id}\n"
    response += f"📊 Активных заказов: {active_count}\n"
    response += f"✅ Есть активный заказ: {'Да' if has_active else 'Нет'}\n\n"
    
    # Показываем детали активных заказов
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                orders = await conn.fetch("""
                    SELECT id, status, created_at, price, from_text, to_text
                    FROM orders 
                    WHERE user_id = $1 
                    AND status IN ('new', 'accepted', 'in_progress')
                    AND created_at > NOW() - INTERVAL '30 minutes'
                    ORDER BY created_at DESC
                """, user_id)
                
                if orders:
                    response += "📋 Ваши активные заказы:\n\n"
                    for order in orders:
                        time_diff = datetime.now() - order["created_at"]
                        minutes_ago = int(time_diff.total_seconds() / 60)
                        response += f"#{order['id']} - {order['status']} - {minutes_ago} мин. назад\n"
                        response += f"📍 {order['from_text'][:20]}... → {order['to_text'][:20]}...\n"
                        response += f"💰 {order['price']} ₸\n\n"
                else:
                    response += "📭 Активных заказов нет"
                    
        except Exception as e:
            response += f"❌ Ошибка: {e}"
    
    await message.answer(response)



# Создадим функцию для деактивации конкретных старых заказов
async def deactivate_user_old_orders(user_id: int, current_order_id: int = None):
    """Деактивирует ВСЕ предыдущие заказы пользователя (кроме текущего, если указан)"""
    if not db_pool:
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли колонка is_active
            column_exists = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'is_active'
            """)
            
            if current_order_id:
                # Деактивируем все заказы пользователя, кроме текущего
                if column_exists:
                    result = await conn.execute("""
                        UPDATE orders 
                        SET is_active = FALSE 
                        WHERE user_id = $1 
                        AND id != $2
                        AND (is_active = TRUE OR status IN ('new', 'accepted', 'in_progress'))
                    """, user_id, current_order_id)
                else:
                    result = await conn.execute("""
                        UPDATE orders 
                        SET status = 'cancelled' 
                        WHERE user_id = $1 
                        AND id != $2
                        AND status IN ('new', 'accepted', 'in_progress')
                    """, user_id, current_order_id)
                    
                logger.info(f"✅ Все предыдущие заказы пользователя {user_id} деактивированы (кроме #{current_order_id})")
            else:
                # Деактивируем все заказы пользователя
                if column_exists:
                    result = await conn.execute("""
                        UPDATE orders 
                        SET is_active = FALSE 
                        WHERE user_id = $1 
                        AND (is_active = TRUE OR status IN ('new', 'accepted', 'in_progress'))
                    """, user_id)
                else:
                    result = await conn.execute("""
                        UPDATE orders 
                        SET status = 'cancelled' 
                        WHERE user_id = $1 
                        AND status IN ('new', 'accepted', 'in_progress')
                    """, user_id)
                    
                logger.info(f"✅ Все активные заказы пользователя {user_id} деактивированы")
            
            # Также обновляем заказы, которые могут быть у водителей
            if current_order_id:
                await conn.execute("""
                    UPDATE orders 
                    SET status = 'cancelled',
                        driver_id = NULL,
                        driver_username = NULL
                    WHERE user_id = $1 
                    AND id != $2
                    AND status = 'new'
                """, user_id, current_order_id)
            else:
                await conn.execute("""
                    UPDATE orders 
                    SET status = 'cancelled',
                        driver_id = NULL,
                        driver_username = NULL
                    WHERE user_id = $1 
                    AND status = 'new'
                """, user_id)
                
    except Exception as e:
        logger.error(f"Ошибка при деактивации заказов пользователя {user_id}: {e}")


# Добавим функцию для автоматической очистки старых заказов при запуске
async def cleanup_old_orders():
    """Очистка старых заказов при запуске бота"""
    if not db_pool:
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли колонка is_active
            column_exists = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'is_active'
            """)
            
            if column_exists:
                # Деактивируем все заказы старше 5 минут
                result = await conn.execute("""
                    UPDATE orders 
                    SET is_active = FALSE 
                    WHERE (is_active = TRUE OR status IN ('new', 'accepted', 'in_progress'))
                    AND created_at <= NOW() - INTERVAL '5 minutes'
                """)
                logger.info(f"✅ Автоматически деактивированы старые заказы (старше 5 минут)")
            else:
                # Обновляем статус старых заказов
                result = await conn.execute("""
                    UPDATE orders 
                    SET status = 'cancelled' 
                    WHERE status IN ('new', 'accepted', 'in_progress')
                    AND created_at <= NOW() - INTERVAL '5 minutes'
                """)
                logger.info(f"✅ Автоматически отменены старые заказы (старше 5 минут)")
                
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке старых заказов: {e}")
        
        

# ================= CANCEL HANDLER =================

async def cancel_handler(message: types.Message, state: FSMContext):
    """Обработчик отмены действий"""
    current_state = await state.get_state()
    if current_state is None:
        return
    
    # Получаем язык из данных пользователя
    user_id = message.from_user.id
    lang = user_data.get(user_id, {}).get("language", "kz")
    
    await state.finish()
    await message.answer(
        TEXT[lang]["action_canceled"],
        reply_markup=types.ReplyKeyboardRemove()
    )

# ================= COMMANDS =================

@dp.message_handler(commands=["start"], state="*")
async def start_command(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
    # Завершаем текущее состояние если оно есть
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    
    # Проверяем блокировку пользователя
    user_id = message.from_user.id
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        return
    
    # Создаем клавиатуру для выбора языка
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("🇰🇿 Қазақша", "🇷🇺 Русский")
    
    await message.answer("🌐 Тілді таңдаңыз / Выберите язык", reply_markup=kb)
    await OrderState.language.set()


# Добавим обработчик для возврата в главное меню:
@dp.message_handler(commands=["menu", "меню"], state="*")
async def menu_command(message: types.Message, state: FSMContext):
    """Возврат в главное меню"""
    # Завершаем текущее состояние если оно есть
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    
    user_id = message.from_user.id
    lang = user_data.get(user_id, {}).get("language", "ru")
    
    # Проверяем, является ли пользователь водителем
    driver = await get_driver(user_id)
    
    if driver:
        # Если водитель - показываем меню водителя
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add("/active_orders", "/my_orders")
        kb.add("/stats", "/settings", "/logout")
        
        await message.answer(TEXT[lang]["driver_menu"], reply_markup=kb)
    else:
        # Если не водитель - показываем обычное меню
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        kb.add(
            "/taxi - 🚕 Такси шақыру" if lang == "kz" else "/taxi - 🚕 Вызвать такси",
            TEXT[lang]["become_driver"]
        )
        
        await message.answer(TEXT[lang]["main_menu"], reply_markup=kb)

# функцию logout_command для возврата в главное меню 
@dp.message_handler(commands=["logout"])
async def logout_command(message: types.Message):
    """Выход из системы водителя"""
    driver = await get_driver(message.from_user.id)
    if not driver:
        await message.answer("❌ Вы не зарегистрированы как водитель")
        return
    
    await update_driver_status(message.from_user.id, "offline")
    
    lang = "ru"
    await message.answer(TEXT[lang]["driver_offline"], reply_markup=types.ReplyKeyboardRemove())
    
    # Возвращаем в главное меню
    user_id = message.from_user.id
    lang = user_data.get(user_id, {}).get("language", "ru")
    
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        "/taxi - 🚕 Такси шақыру" if lang == "kz" else "/taxi - 🚕 Вызвать такси",
        TEXT[lang]["become_driver"]
    )
    
    await message.answer(TEXT[lang]["main_menu"], reply_markup=kb)

#  Добавим команду для обновления структуры таблиц:
@dp.message_handler(commands=["update_tables"])
async def update_tables_command(message: types.Message):
    """Обновление структуры таблиц"""
    if not db_pool:
        await message.answer("❌ Нет подключения к БД")
        return
    
    try:
        await check_and_add_columns()
        await message.answer("✅ Структура таблиц обновлена")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
#  Добавим команду для обновления структуры таблиц




# get_driver функциясы 
# ================= DRIVER FUNCTIONS =================


async def get_driver(user_id: int):
    """Получение информации о водителе"""
    if not db_pool:
        return None
    
    try:
        async with db_pool.acquire() as conn:
            driver = await conn.fetchrow("""
                SELECT * FROM drivers WHERE user_id = $1
            """, user_id)
            return driver
    except Exception as e:
        logger.error(f"Ошибка при получении водителя: {e}")
        return None








# Бұл функцияларды қосыңыз:
async def create_driver_application(user_id: int, username: str, phone: str, car_model: str, car_number: str) -> bool:
    """Создание заявки на водителя"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли уже заявка
            existing_application = await conn.fetchval("""
                SELECT id FROM driver_applications WHERE user_id = $1
            """, user_id)
            
            if existing_application:
                # Обновляем существующую заявку
                await conn.execute("""
                    UPDATE driver_applications 
                    SET phone = $1, car_model = $2, car_number = $3, 
                        status = 'pending', created_at = CURRENT_TIMESTAMP
                    WHERE user_id = $4
                """, phone, car_model, car_number, user_id)
            else:
                # Создаем новую заявку
                await conn.execute("""
                    INSERT INTO driver_applications 
                    (user_id, username, phone, car_model, car_number, status)
                    VALUES ($1, $2, $3, $4, $5, 'pending')
                """, user_id, username, phone, car_model, car_number)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при создании заявки водителя: {e}")
        return False



# функция register_driver
async def register_driver(user_id: int, username: str, car_model: str, car_number: str, phone: str) -> bool:
    """Регистрация водителя"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли уже водитель
            existing_driver = await conn.fetchval("""
                SELECT id FROM drivers WHERE user_id = $1
            """, user_id)
            
            if existing_driver:
                # Обновляем существующего водителя
                await conn.execute("""
                    UPDATE drivers 
                    SET username = $1, car_model = $2, car_number = $3,
                        phone = $4, last_active = CURRENT_TIMESTAMP
                    WHERE user_id = $5
                """, username, car_model, car_number, phone, user_id)
            else:
                # Создаем нового водителя
                await conn.execute("""
                    INSERT INTO drivers 
                    (user_id, username, car_model, car_number, phone, status)
                    VALUES ($1, $2, $3, $4, $5, 'online')
                """, user_id, username, car_model, car_number, phone)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при регистрации водителя: {e}")
        return False


# Альтернативная простая функция без ON CONFLICT
async def simple_create_driver_application(user_id: int, username: str, phone: str, car_model: str, car_number: str) -> bool:
    """Простая функция создания заявки без ON CONFLICT"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Просто создаем или перезаписываем заявку
            # Сначала удаляем старую заявку если есть
            await conn.execute("""
                DELETE FROM driver_applications WHERE user_id = $1
            """, user_id)
            
            # Создаем новую заявку
            await conn.execute("""
                INSERT INTO driver_applications 
                (user_id, username, phone, car_model, car_number, status)
                VALUES ($1, $2, $3, $4, $5, 'pending')
            """, user_id, username, phone, car_model, car_number)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при создании заявки водителя: {e}")
        return False


    
async def get_driver_application(user_id: int):
    """Получение заявки водителя"""
    if not db_pool:
        return None
    
    try:
        async with db_pool.acquire() as conn:
            application = await conn.fetchrow("""
                SELECT * FROM driver_applications WHERE user_id = $1
            """, user_id)
            return application
    except Exception as e:
        logger.error(f"Ошибка при получении заявки водителя: {e}")
        return None

# ================= ADMIN FUNCTIONS =================

async def get_pending_applications():
    """Получение ожидающих заявок"""
    if not db_pool:
        return []
    
    try:
        async with db_pool.acquire() as conn:
            applications = await conn.fetch("""
                SELECT * FROM driver_applications 
                WHERE status = 'pending'
                ORDER BY created_at DESC
            """)
            return applications
    except Exception as e:
        logger.error(f"Ошибка при получении заявок: {e}")
        return []

async def approve_driver_application(user_id: int, approved_by: int, duration_days: int = 30):
    """Одобрение заявки водителя"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Получаем данные заявки
            application = await conn.fetchrow("""
                SELECT * FROM driver_applications 
                WHERE user_id = $1 AND status = 'pending'
            """, user_id)
            
            if not application:
                return False
            
            # Вычисляем дату окончания срока
            expires_date = datetime.now() + timedelta(days=duration_days)
            
            # Проверяем, существует ли уже водитель
            existing_driver = await conn.fetchval("""
                SELECT id FROM drivers WHERE user_id = $1
            """, user_id)
            
            if existing_driver:
                # Обновляем существующего водителя
                await conn.execute("""
                    UPDATE drivers 
                    SET username = $1, phone = $2, car_model = $3, 
                        car_number = $4, status = 'online', expires_at = $5,
                        last_active = CURRENT_TIMESTAMP, is_active = TRUE
                    WHERE user_id = $6
                """, 
                application["username"],
                application["phone"],
                application["car_model"],
                application["car_number"],
                expires_date,
                user_id
                )
            else:
                # Создаем нового водителя
                await conn.execute("""
                    INSERT INTO drivers 
                    (user_id, username, phone, car_model, car_number, status, expires_at)
                    VALUES ($1, $2, $3, $4, $5, 'online', $6)
                """, 
                user_id,
                application["username"],
                application["phone"],
                application["car_model"],
                application["car_number"],
                expires_date
                )
            
            # Обновляем статус заявки
            await conn.execute("""
                UPDATE driver_applications 
                SET status = 'approved',
                    approved_by = $1,
                    approved_at = CURRENT_TIMESTAMP,
                    expires_at = $2
                WHERE user_id = $3
            """, approved_by, expires_date, user_id)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при одобрении заявки водителя: {e}")
        return False

async def reject_driver_application(user_id: int, rejected_by: int, reason: str = ""):
    """Отклонение заявки водителя"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE driver_applications 
                SET status = 'rejected',
                    rejected_by = $1,
                    rejected_at = CURRENT_TIMESTAMP,
                    reject_reason = $2
                WHERE user_id = $3 AND status = 'pending'
            """, rejected_by, reason, user_id)
            return True
    except Exception as e:
        logger.error(f"Ошибка при отклонении заявки водителя: {e}")
        return False

async def get_all_drivers():
    """Получение всех водителей"""
    if not db_pool:
        return []
    
    try:
        async with db_pool.acquire() as conn:
            drivers = await conn.fetch("""
                SELECT * FROM drivers 
                WHERE is_active = TRUE
                ORDER BY created_at DESC
            """)
            return drivers
    except Exception as e:
        logger.error(f"Ошибка при получении водителей: {e}")
        return []

async def deactivate_driver(user_id: int, deactivated_by: int, reason: str = ""):
    """Деактивация водителя"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE drivers 
                SET is_active = FALSE, status = 'deactivated'
                WHERE user_id = $1
            """, user_id)
            
            # Также можно добавить запись в историю деактиваций
            await conn.execute("""
                INSERT INTO driver_deactivations (user_id, deactivated_by, reason)
                VALUES ($1, $2, $3)
            """, user_id, deactivated_by, reason)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при деактивации водителя: {e}")
        return False


# Добавим команду для ручной очистки заказов:

@dp.message_handler(commands=["cleanup_orders"])
async def cleanup_orders_command(message: types.Message):
    """Очистка старых заказов (для админов)"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав администратора")
        return
    
    try:
        await cleanup_old_orders()
        await message.answer("✅ Старые заказы очищены")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")






    

# Универсальная функция для обновления/вставки данных
async def upsert_driver_application(user_id: int, username: str, phone: str, car_model: str, car_number: str) -> bool:
    """Обновление или вставка заявки водителя (PostgreSQL 9.5+)"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Для PostgreSQL 9.5+ можно использовать ON CONFLICT
            # Проверяем версию PostgreSQL
            version = await conn.fetchval("SELECT version()")
            
            if "PostgreSQL 9.5" in version or "PostgreSQL 10" in version or "PostgreSQL 11" in version or "PostgreSQL 12" in version or "PostgreSQL 13" in version or "PostgreSQL 14" in version or "PostgreSQL 15" in version or "PostgreSQL 16" in version:
                # Используем ON CONFLICT для новых версий
                await conn.execute("""
                    INSERT INTO driver_applications 
                    (user_id, username, phone, car_model, car_number, status)
                    VALUES ($1, $2, $3, $4, $5, 'pending')
                    ON CONFLICT (user_id) DO UPDATE SET
                    phone = EXCLUDED.phone,
                    car_model = EXCLUDED.car_model,
                    car_number = EXCLUDED.car_number,
                    status = EXCLUDED.status,
                    created_at = CURRENT_TIMESTAMP
                """, user_id, username, phone, car_model, car_number)
            else:
                # Для старых версий используем проверку существования
                existing = await conn.fetchval("SELECT 1 FROM driver_applications WHERE user_id = $1", user_id)
                
                if existing:
                    await conn.execute("""
                        UPDATE driver_applications 
                        SET phone = $1, car_model = $2, car_number = $3, 
                            status = 'pending', created_at = CURRENT_TIMESTAMP
                        WHERE user_id = $4
                    """, phone, car_model, car_number, user_id)
                else:
                    await conn.execute("""
                        INSERT INTO driver_applications 
                        (user_id, username, phone, car_model, car_number, status)
                        VALUES ($1, $2, $3, $4, $5, 'pending')
                    """, user_id, username, phone, car_model, car_number)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при создании заявки водителя: {e}")
        return False





# нужно обновить функцию taxi_command для возврата в главное меню:
@dp.message_handler(commands=["taxi"], state="*")
async def taxi_command(message: types.Message, state: FSMContext):
    """Обработчик команды /taxi"""
    user_id = message.from_user.id
    
    # 1. Проверяем блокировку пользователя
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        return
    
    # 2. Проверяем, есть ли активный заказ (в пределах 5 минут) - ОЧЕНЬ ВАЖНО!
    if await has_active_order(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["active_order"])
        
        # Показываем информацию о текущем активном заказе
        try:
            if db_pool:
                async with db_pool.acquire() as conn:
                    active_order = await conn.fetchrow("""
                        SELECT id, created_at, status, from_text, to_text, price
                        FROM orders 
                        WHERE user_id = $1 
                        AND status IN ('new', 'accepted', 'in_progress')
                        AND created_at > NOW() - INTERVAL '5 minutes'
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """, user_id)
                    
                    if active_order:
                        order_time = active_order["created_at"].strftime("%H:%M")
                        time_diff = datetime.now() - active_order["created_at"]
                        minutes_left = max(0, 5 - int(time_diff.total_seconds() / 60))
                        
                        if lang == "kz":
                            info = f"📋 Сіздің белсенді тапсырысыңыз:\n\n"
                            info += f"🆔 #{active_order['id']}\n"
                            info += f"📍 Қайдан: {active_order['from_text'][:30]}...\n"
                            info += f"📍 Қайда: {active_order['to_text'][:30]}...\n"
                            info += f"💰 Бағасы: {active_order['price']} ₸\n"
                            info += f"⏰ Уақыт: {order_time}\n"
                            info += f"⏳ Күту уақыты: {minutes_left} минут\n\n"
                            info += f"📞 Жүргізуші {active_order['phone'] if 'phone' in active_order else ''} сізбен хабарласады."
                        else:
                            info = f"📋 Ваш активный заказ:\n\n"
                            info += f"🆔 #{active_order['id']}\n"
                            info += f"📍 Откуда: {active_order['from_text'][:30]}...\n"
                            info += f"📍 Куда: {active_order['to_text'][:30]}...\n"
                            info += f"💰 Цена: {active_order['price']} ₸\n"
                            info += f"⏰ Время: {order_time}\n"
                            info += f"⏳ Ожидание: {minutes_left} минут\n\n"
                            info += f"📞 Водитель {active_order['phone'] if 'phone' in active_order else ''} свяжется с вами."
                        
                        await message.answer(info)
        except Exception as e:
            logger.error(f"Ошибка при получении информации о заказе: {e}")
        
        return
    
    # 3. Проверяем количество активных заказов
    active_count = await get_active_order_count(user_id)
    if active_count >= 1:
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["too_many_orders"])
        return
    
    # 4. Проверяем язык
    lang = user_data.get(user_id, {}).get("language")
    
    # Если язык не выбран, просим выбрать
    if not lang:
        await message.answer("🌐 Сначала выберите язык / Алдымен тілді таңдаңыз")
        
        # Просто отправляем сообщение без перехода в состояние
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        kb.add("🇰🇿 Қазақша", "🇷🇺 Русский")
        
        await message.answer("🌐 Тілді таңдаңыз / Выберите язык", reply_markup=kb)
        return
    
    # 5. Деактивируем старые заказы (старше 5 минут) - чистим базу
    await deactivate_old_orders(user_id)
    
    # 6. Начинаем создание нового заказа
    # Создаем клавиатуру с кнопкой локации
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("📍 Локация", request_location=True))
    kb.add(TEXT[lang]["cancel"])
    
    await message.answer(TEXT[lang]["choose_from"], reply_markup=kb)
    await OrderState.from_place.set()
    
    # Сохраняем язык в состоянии
    await state.update_data(language=lang)  
    
# ================= LANGUAGE SELECTION =================

# Обновим функцию process_language для главного меню
@dp.message_handler(state=OrderState.language)
async def process_language(message: types.Message, state: FSMContext):
    """Обработка выбора языка"""
    # Проверяем блокировку пользователя
    user_id = message.from_user.id
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        await state.finish()
        return
    
    # Определяем язык
    if message.text.startswith("🇰🇿"):
        lang = "kz"
    elif message.text.startswith("🇷🇺"):
        lang = "ru"
    else:
        await message.answer("Пожалуйста, выберите язык / Тілді таңдаңыз")
        return
    
    # Сохраняем язык в данных пользователя
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["language"] = lang
    
    # Создаем основную клавиатуру с двумя кнопками
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    # Добавляем обе кнопки
    kb.add(
        "/taxi - 🚕 Такси шақыру" if lang == "kz" else "/taxi - 🚕 Вызвать такси",
        TEXT[lang]["become_driver"] if lang in TEXT else "🚕 Стать водителем"
    )
    
    await message.answer(TEXT[lang]["lang_selected"], reply_markup=kb)
    await state.finish()

    
# Добавим обработчик для кнопки "Стать водителем":    
@dp.message_handler(lambda message: message.text in [
    TEXT["kz"]["become_driver"], 
    TEXT["ru"]["become_driver"]
], state="*")
async def become_driver_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки 'Стать водителем'"""
    # Завершаем текущее состояние если оно есть
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    
    user_id = message.from_user.id
    
    # Определяем язык пользователя
    lang = user_data.get(user_id, {}).get("language", "ru")
    
    # Проверяем, есть ли уже водитель
    driver = await get_driver(user_id)
    
    if driver:
        # Водитель уже зарегистрирован
        await update_driver_status(user_id, "online")
        
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add("/active_orders", "/my_orders")
        kb.add("/stats", "/settings", "/logout")
        
        await message.answer(TEXT[lang]["driver_welcome"], reply_markup=kb)
        await message.answer(TEXT[lang]["driver_menu"])
        return
    
    # Проверяем, есть ли ожидающая заявка
    application = await get_driver_application(user_id)
    
    if application and application["status"] == "pending":
        # Заявка уже отправлена
        await message.answer(
            "⏳ Ваша заявка на рассмотрении. Ожидайте одобрения администратора." if lang == "ru" else
            "⏳ Сіздің өтінішіңіз қарастыруда. Администратордың мақұлдауын күтіңіз."
        )
        return
    
    # Начинаем процесс подачи заявки
    if lang == "kz":
        welcome_text = "👋 Жүргізуші ретінде тіркелу үшін тілді таңдаңыз:"
        lang_text = "🌐 Тілді таңдаңыз"
    else:
        welcome_text = "👋 Для подачи заявки на водителя выберите язык:"
        lang_text = "🌐 Выберите язык"
    
    await message.answer(welcome_text)
    
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("🇰🇿 Қазақша", "🇷🇺 Русский")
    
    await message.answer(lang_text, reply_markup=kb)
    await DriverState.language.set()


# Обработчики для регистрации водителя:

@dp.message_handler(state=DriverState.language)
async def driver_language_choose(message: types.Message, state: FSMContext):
    """Выбор языка для водителя"""
    if message.text.startswith("🇰🇿"):
        lang = "kz"
    elif message.text.startswith("🇷🇺"):
        lang = "ru"
    else:
        await message.answer("Пожалуйста, выберите язык / Тілді таңдаңыз")
        return
    
    await state.update_data(language=lang)
    await message.answer(
        TEXT[lang]["enter_car_model"] if lang in TEXT else "🚗 Введите марку автомобиля (например: Toyota Camry):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await DriverState.car_model.set()

@dp.message_handler(state=DriverState.car_model)
async def driver_car_model(message: types.Message, state: FSMContext):
    """Ввод марки автомобиля"""
    if len(message.text) < 2:
        data = await state.get_data()
        lang = data.get("language", "ru")
        await message.answer(TEXT[lang]["enter_car_model"] if lang in TEXT else "🚗 Введите марку автомобиля:")
        return
    
    await state.update_data(car_model=message.text)
    
    data = await state.get_data()
    lang = data.get("language", "ru")
    
    await message.answer(
        TEXT[lang]["enter_car_number"] if lang in TEXT else "🚘 Введите номер автомобиля (например: 01KZ123ABC):"
    )
    await DriverState.car_number.set()

@dp.message_handler(state=DriverState.car_number)
async def driver_car_number(message: types.Message, state: FSMContext):
    """Ввод номера автомобиля"""
    if len(message.text) < 4:
        data = await state.get_data()
        lang = data.get("language", "ru")
        await message.answer(TEXT[lang]["enter_car_number"] if lang in TEXT else "🚘 Введите номер автомобиля:")
        return
    
    await state.update_data(car_number=message.text)
    
    data = await state.get_data()
    lang = data.get("language", "ru")
    
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("📞 Телефон", request_contact=True))
    
    await message.answer(
        TEXT[lang]["enter_phone"] if lang in TEXT else "📱 Введите ваш номер телефона:",
        reply_markup=kb
    )
    await DriverState.phone.set()

@dp.message_handler(state=DriverState.phone, content_types=["contact", "text"])
async def driver_phone(message: types.Message, state: FSMContext):
    """Ввод телефона водителя"""
    phone = ""
    
    if message.contact:
        phone = message.contact.phone_number
    elif message.text and message.text.replace('+', '').isdigit():
        phone = message.text
    else:
        data = await state.get_data()
        lang = data.get("language", "ru")
        await message.answer(TEXT[lang]["enter_phone"] if lang in TEXT else "📱 Введите номер телефона:")
        return
    
    data = await state.get_data()
    lang = data.get("language", "ru")
    car_model = data.get("car_model", "")
    car_number = data.get("car_number", "")
    
    # Регистрируем заявку водителя (используем безопасную функцию)
    success = await create_driver_application(
        message.from_user.id,
        message.from_user.username or "",
        phone,
        car_model,
        car_number
    )
    
    if success:
        # Отправляем подтверждение
        registration_text = ""
        if lang == "kz":
            registration_text = f"✅ Өтініш жіберілді!\n\n🚗 Көлік: {car_model}\n🚘 Нөмір: {car_number}\n📱 Телефон: {phone}\n\nӨтінішіңіз қарастыру үшін жіберілді. Администратор сізбен хабарласады."
        else:
            registration_text = f"✅ Заявка отправлена!\n\n🚗 Автомобиль: {car_model}\n🚘 Номер: {car_number}\n📱 Телефон: {phone}\n\nВаша заявка отправлена на рассмотрение. Администратор свяжется с вами."
        
        await message.answer(
            registration_text,
            reply_markup=types.ReplyKeyboardRemove()
        )
        
        # Возвращаем в главное меню
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        kb.add(
            "/taxi - 🚕 Такси шақыру" if lang == "kz" else "/taxi - 🚕 Вызвать такси",
            TEXT[lang]["become_driver"] if lang in TEXT else "🚕 Стать водителем"
        )
        
        
        await message.answer(TEXT[lang]["main_menu"] if lang in TEXT else "Главное меню", reply_markup=kb)
    else:
        await message.answer("❌ Ошибка при отправке заявки. Попробуйте позже.")
    
    await state.finish()

# Добавим команду для проверки версии PostgreSQL:
@dp.message_handler(commands=["db_version"])
async def db_version_command(message: types.Message):
    """Проверка версии PostgreSQL"""
    if not db_pool:
        await message.answer("❌ Нет подключения к БД")
        return
    
    try:
        async with db_pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            await message.answer(f"📊 Версия PostgreSQL:\n\n{version}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
   
   
   
 # ================= DRIVER FUNCTIONS (ДОБАВИТЬ) =================


# Добавим недостающие функции  
async def update_driver_status(user_id: int, status: str):
    """Обновление статуса водителя"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE drivers 
                SET status = $1, last_active = CURRENT_TIMESTAMP
                WHERE user_id = $2
            """, status, user_id)
            return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении статуса водителя: {e}")
        return False


async def get_active_orders():
    """Получение активных заказов для водителей"""
    if not db_pool:
        return []
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли колонка is_active
            column_exists = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'is_active'
            """)
            
            if column_exists:
                # Получаем заказы без водителя И активные
                orders = await conn.fetch("""
                    SELECT * FROM orders 
                    WHERE status = 'new' 
                    AND driver_id IS NULL
                    AND is_active = TRUE
                    AND created_at > NOW() - INTERVAL '30 minutes'
                    ORDER BY created_at DESC
                """)
            else:
                # Получаем заказы без водителя (старая версия)
                orders = await conn.fetch("""
                    SELECT * FROM orders 
                    WHERE status = 'new' 
                    AND driver_id IS NULL
                    AND created_at > NOW() - INTERVAL '30 minutes'
                    ORDER BY created_at DESC
                """)
            
            return orders
    except Exception as e:
        logger.error(f"Ошибка при получении активных заказов: {e}")
        return []
    

async def accept_order(order_id: int, driver_id: int, driver_username: str):
    """Принятие заказа водителем"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Получаем информацию о заказе
            order = await conn.fetchrow("""
                SELECT * FROM orders 
                WHERE id = $1 AND status = 'new' AND driver_id IS NULL
            """, order_id)
            
            if not order:
                logger.error(f"Заказ {order_id} не найден или уже принят")
                return False
            
            # Получаем информацию о водителе
            driver = await conn.fetchrow("""
                SELECT * FROM drivers WHERE user_id = $1
            """, driver_id)
            
            if not driver:
                logger.error(f"Водитель {driver_id} не найден")
                return False
            
            # Обновляем заказ
            await conn.execute("""
                UPDATE orders 
                SET status = 'accepted',
                    driver_id = $1,
                    driver_username = $2,
                    accepted_at = CURRENT_TIMESTAMP
                WHERE id = $3
            """, driver_id, driver_username, order_id)
            
            # Обновляем статус водителя
            await conn.execute("""
                UPDATE drivers 
                SET status = 'busy',
                    total_orders = total_orders + 1,
                    last_active = CURRENT_TIMESTAMP
                WHERE user_id = $1
            """, driver_id)
            
            logger.info(f"✅ Заказ {order_id} принят водителем {driver_id}")
            
            # Отправляем уведомление клиенту
            await notify_client_about_driver(order, driver)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при принятии заказа: {e}")
        return False
    

async def complete_order(order_id: int, driver_id: int):
    """Завершение заказа"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, что заказ принадлежит этому водителю
            order = await conn.fetchrow("""
                SELECT * FROM orders 
                WHERE id = $1 AND driver_id = $2 AND status = 'accepted'
            """, order_id, driver_id)
            
            if not order:
                logger.error(f"Заказ {order_id} не найден или не принадлежит водителю {driver_id}")
                return False
            
            # Обновляем заказ
            await conn.execute("""
                UPDATE orders 
                SET status = 'completed',
                    is_active = FALSE
                WHERE id = $1
            """, order_id)
            
            # Обновляем статус водителя
            await conn.execute("""
                UPDATE drivers 
                SET status = 'online',
                    last_active = CURRENT_TIMESTAMP
                WHERE user_id = $1
            """, driver_id)
            
            logger.info(f"✅ Заказ {order_id} завершен водителем {driver_id}")
            
            # Отправляем уведомление клиенту о завершении заказа
            await notify_client_order_completed(order)
            
            return True
    except Exception as e:
        logger.error(f"Ошибка при завершении заказа: {e}")
        return False


# Ааа, понял! Ты про функции для карт! 
# ДОБАВЬ ЭТИ 3 ФУНКЦИИ после функции complete_order
async def create_map_urls(order: dict):
    """Создание URL для карт с координатами"""
    urls = {}
    
    # Если есть координаты отправки
    if order.get("from_lat") and order.get("from_lon"):
        from_lat = order["from_lat"]
        from_lon = order["from_lon"]
        
        # Яндекс.Карты с координатами
        yandex_url = f"https://yandex.ru/maps/?text={order['from_text']}"
        
        # Google Maps с координатами
        google_url = f"https://www.google.com/maps/search/?api=1&query={order['from_text']}"
        
        # Waze
        waze_url = f"https://waze.com/ul?ll={from_lat},{from_lon}&navigate=yes"
        
        urls = {
            "yandex": yandex_url,
            "google": google_url,
            "waze": waze_url
        }
    else:
        # Если координат нет - используем текст адреса
        from_text = order['from_text'].replace(' ', '%20')
        to_text = order['to_text'].replace(' ', '%20') if order.get('to_text') else ''
        
        yandex_url = f"https://yandex.ru/maps/?text={from_text}"
        google_url = f"https://www.google.com/maps/search/?api=1&query={from_text}"
        
        urls = {
            "yandex": yandex_url,
            "google": google_url
        }
    
    return urls

async def send_driver_order_with_maps(driver_id: int, order: dict):
    """Отправка заказа водителю с кнопками карт"""
    try:
        # Текст заказа
        order_time = order["created_at"].strftime("%H:%M")
        order_text = f"✅ **ЗАКАЗ ПРИНЯТ!**\n\n"
        order_text += f"📋 Заказ #{order['id']}\n"
        order_text += f"📍 Откуда: {order['from_text']}\n"
        
        if order.get('to_text'):
            order_text += f"📍 Куда: {order['to_text']}\n"
            
        order_text += f"💰 Цена: {order['price']} ₸\n"
        order_text += f"📞 Клиент: +{order['phone']}\n"
        order_text += f"⏰ Время: {order_time}\n\n"
        order_text += "🗺️ **Выберите карту для навигации:**"
        
        # Получаем ссылки на карты
        map_urls = await create_map_urls(order)
        
        # Создаем кнопки
        kb = types.InlineKeyboardMarkup(row_width=2)
        
        # Основные кнопки
        buttons = []
        
        if "yandex" in map_urls:
            buttons.append(types.InlineKeyboardButton("🗺️ Яндекс", url=map_urls["yandex"]))
        
        if "google" in map_urls:
            buttons.append(types.InlineKeyboardButton("🗺️ Google", url=map_urls["google"]))
        
        if "waze" in map_urls:
            buttons.append(types.InlineKeyboardButton("🗺️ Waze", url=map_urls["waze"]))
        
        # Добавляем 2GIS для Казахстана
        if order.get('from_text'):
            gis_url = f"https://2gis.ru/search/{order['from_text'].replace(' ', '%20')}"
            buttons.append(types.InlineKeyboardButton("🗺️ 2GIS", url=gis_url))
        
        # Распределяем кнопки по 2 в ряд
        for i in range(0, len(buttons), 2):
            if i + 1 < len(buttons):
                kb.row(buttons[i], buttons[i + 1])
            else:
                kb.row(buttons[i])
        
        # Кнопка завершения
        kb.row(
            types.InlineKeyboardButton(
                "✅ Завершить заказ", 
                callback_data=f"driver_complete_{order['id']}"
            )
        )
        
        # Отправляем водителю
        await bot.send_message(
            driver_id,
            order_text,
            reply_markup=kb,
            parse_mode="Markdown"
        )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке карт водителю: {e}")
        return False

async def update_active_orders_with_maps():
    """Обновление команды active_orders для показа карт"""
    # Эту функцию НЕ нужно добавлять отдельно
    # Мы просто обновим существующую функцию active_orders_command
    pass








   
async def get_driver_orders(driver_id: int, limit: int = 10):
    """Получение заказов водителя"""
    if not db_pool:
        return []
    
    try:
        async with db_pool.acquire() as conn:
            orders = await conn.fetch("""
                SELECT * FROM orders 
                WHERE driver_id = $1
                ORDER BY created_at DESC
                LIMIT $2
            """, driver_id, limit)
            return orders
    except Exception as e:
        logger.error(f"Ошибка при получении заказов водителя: {e}")
        return []


    """Получение статистики водителя"""
    if not db_pool:
        return None
    
    try:
        async with db_pool.acquire() as conn:
            # Общая статистика
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_orders,
                    COALESCE(SUM(price), 0) as total_income,
                    COALESCE(AVG(rating), 5.0) as avg_rating
                FROM orders o
                LEFT JOIN driver_ratings dr ON o.id = dr.order_id
                WHERE o.driver_id = $1 AND o.status = 'completed'
            """, driver_id)
            
            # Сегодняшние заказы
            today_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as today_orders,
                    COALESCE(SUM(price), 0) as today_income
                FROM orders 
                WHERE driver_id = $1 
                AND status = 'completed'
                AND DATE(created_at) = CURRENT_DATE
            """, driver_id)
            
            return {
                "total_orders": stats["total_orders"] or 0,
                "total_income": stats["total_income"] or 0,
                "avg_rating": round(stats["avg_rating"] or 5.0, 1),
                "today_orders": today_stats["today_orders"] or 0,
                "today_income": today_stats["today_income"] or 0
            }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики водителя: {e}")
        return None

async def get_driver_stats(driver_id: int):
    """Получение статистики водителя"""
    if not db_pool:
        return None
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем существование таблицы driver_ratings
            try:
                # Общая статистика с рейтингом
                stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(DISTINCT o.id) as total_orders,
                        COALESCE(SUM(o.price), 0) as total_income,
                        COALESCE(AVG(dr.rating), 5.0) as avg_rating
                    FROM orders o
                    LEFT JOIN driver_ratings dr ON o.id = dr.order_id
                    WHERE o.driver_id = $1 AND o.status = 'completed'
                """, driver_id)
                
                if stats is None:
                    # Если нет заказов
                    return {
                        "total_orders": 0,
                        "total_income": 0,
                        "avg_rating": 5.0,
                        "today_orders": 0,
                        "today_income": 0
                    }
                
                # Сегодняшние заказы
                today_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as today_orders,
                        COALESCE(SUM(price), 0) as today_income
                    FROM orders 
                    WHERE driver_id = $1 
                    AND status = 'completed'
                    AND DATE(created_at) = CURRENT_DATE
                """, driver_id)
                
                return {
                    "total_orders": stats["total_orders"] or 0,
                    "total_income": stats["total_income"] or 0,
                    "avg_rating": round(float(stats["avg_rating"] or 5.0), 1),
                    "today_orders": today_stats["today_orders"] or 0,
                    "today_income": today_stats["today_income"] or 0
                }
                
            except Exception as e:
                logger.error(f"Ошибка при получении статистики с рейтингом: {e}")
                # Простая статистика без рейтинга
                simple_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_orders,
                        COALESCE(SUM(price), 0) as total_income
                    FROM orders 
                    WHERE driver_id = $1 AND status = 'completed'
                """, driver_id)
                
                # Сегодняшние заказы
                today_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as today_orders,
                        COALESCE(SUM(price), 0) as today_income
                    FROM orders 
                    WHERE driver_id = $1 
                    AND status = 'completed'
                    AND DATE(created_at) = CURRENT_DATE
                """, driver_id)
                
                return {
                    "total_orders": simple_stats["total_orders"] or 0,
                    "total_income": simple_stats["total_income"] or 0,
                    "avg_rating": 5.0,  # По умолчанию
                    "today_orders": today_stats["today_orders"] or 0,
                    "today_income": today_stats["today_income"] or 0
                }
                
    except Exception as e:
        logger.error(f"Ошибка при получении статистики водителя: {e}")
        return None
    
# Добавим недостающие функции  


# Добавим функцию для отправки уведомления клиенту
async def notify_client_about_driver(order, driver):
    """Отправка уведомления клиенту о водителе"""
    try:
        user_id = order["user_id"]
        lang = order["language"] or "ru"
        
        # Формируем имя водителя
        driver_name = driver["username"] or f"Водитель {driver['user_id']}"
        if driver["username"] and driver["username"].startswith("@"):
            driver_name = driver["username"]
        
        # Формируем сообщение для клиента
        notification = f"🎉 {TEXT[lang]['order_accepted_title']}\n\n"
        notification += TEXT[lang]["driver_accepted"].format(
            driver_name=driver_name,
            car_model=driver["car_model"],
            car_number=driver["car_number"],
            driver_phone=driver["phone"]
        )
        
        # Добавляем информацию о заказе
        order_time = order["created_at"].strftime("%H:%M")
        notification += f"\n\n📋 Заказ #{order['id']}:"
        notification += f"\n📍 Откуда: {order['from_text']}"
        notification += f"\n📍 Куда: {order['to_text']}"
        notification += f"\n💰 Цена: {order['price']} ₸"
        notification += f"\n⏰ Время заказа: {order_time}"
        
        # Отправляем сообщение клиенту
        await bot.send_message(user_id, notification)
        
        logger.info(f"✅ Уведомление отправлено клиенту {user_id} о водителе {driver['user_id']}")
        
        # Ждем и возвращаем в главное меню
        await asyncio.sleep(3)
        await send_main_menu(user_id, lang)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке уведомления клиенту: {e}")

# Добавим функцию для уведомления клиента о завершении заказа
async def notify_client_order_completed(order):
    """Уведомление клиента о завершении заказа"""
    try:
        user_id = order["user_id"]
        lang = order["language"] or "ru"
        
        # Формируем сообщение
        message = ""
        if lang == "kz":
            message = f"✅ Тапсырыс #{order['id']} аяқталды!\n\n"
            message += f"📍 Қайдан: {order['from_text']}\n"
            message += f"📍 Қайда: {order['to_text']}\n"
            message += f"💰 Төленді: {order['price']} ₸\n\n"
            message += f"🎉 Рахмет! Тағы кездескенше!"
        else:
            message = f"✅ Заказ #{order['id']} завершен!\n\n"
            message += f"📍 Откуда: {order['from_text']}\n"
            message += f"📍 Куда: {order['to_text']}\n"
            message += f"💰 Оплачено: {order['price']} ₸\n\n"
            message += f"🎉 Спасибо! До новых поездок!"
        
        # Отправляем сообщение клиенту
        await bot.send_message(user_id, message)
        
        logger.info(f"✅ Уведомление о завершении отправлено клиенту {user_id}")
        
        # Ждем и возвращаем в главное меню
        await asyncio.sleep(2)
        await send_main_menu(user_id, lang)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке уведомления о завершении: {e}")
        
        
# Создадим универсальную функцию для возврата в главное меню
async def send_main_menu(user_id: int, lang: str = None):
    """Отправка главного меню пользователю"""
    try:
        if not lang:
            lang = user_data.get(user_id, {}).get("language", "ru")
        
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        kb.add(
            "/taxi - 🚕 Такси шақыру" if lang == "kz" else "/taxi - 🚕 Вызвать такси",
            TEXT[lang]["become_driver"]
        )
        
        await bot.send_message(user_id, TEXT[lang]["main_menu"], reply_markup=kb)
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке главного меню пользователю {user_id}: {e}")
        return False


# Добавим команды для водителей  
# ================= DRIVER COMMANDS (ДОБАВИТЬ) =================

@dp.message_handler(commands=["driver"], state="*")
async def driver_command(message: types.Message, state: FSMContext):
    """Команда для водителей"""
    # Завершаем текущее состояние если оно есть
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    
    user_id = message.from_user.id
    
    # Проверяем, есть ли водитель в системе
    driver = await get_driver(user_id)
    
    if driver:
        # Водитель уже зарегистрирован
        await update_driver_status(user_id, "online")
        
        # Определяем язык водителя
        lang = user_data.get(user_id, {}).get("language", "ru")
        
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add("/active_orders", "/my_orders")
        kb.add("/stats", "/settings", "/logout")
        
        await message.answer(TEXT[lang]["driver_welcome"], reply_markup=kb)
        await message.answer(TEXT[lang]["driver_menu"])
    else:
        # Водитель не найден - предлагаем подать заявку
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(
            "❌ Вы не зарегистрированы как водитель. Подайте заявку, нажав '🚕 Стать водителем'"
        )

@dp.message_handler(commands=["active_orders"])
async def active_orders_command(message: types.Message):
    """Показать активные заказы"""
    driver = await get_driver(message.from_user.id)
    if not driver:
        await message.answer("❌ Вы не зарегистрированы как водитель. Используйте /driver")
        return
    
    # Проверяем, что водитель онлайн
    if driver["status"] == "offline":
        await message.answer("❌ Вы в офлайн режиме. Перейдите в онлайн через /settings")
        return
    
    orders = await get_active_orders()
    
    if not orders:
        await message.answer("📭 Активных заказов нет")
        return
    
    lang = user_data.get(message.from_user.id, {}).get("language", "ru")
    
    response = f"📋 Доступные заказы ({len(orders)}):\n\n"
    
    for order in orders:
        order_time = order["created_at"].strftime("%H:%M")
        
        order_text = TEXT[lang]["order_details"].format(
            id=order["id"],
            from_=order["from_text"],
            to=order["to_text"],
            price=order["price"],
            phone=order["phone"],
            time=order_time
        )
        
                # Создаем ссылки для карт
        from_text_encoded = order["from_text"].replace(' ', '%20')
        yandex_url = f"https://yandex.ru/maps/?text={from_text_encoded}"
        google_url = f"https://www.google.com/maps/search/?api=1&query={from_text_encoded}"
        
        kb = types.InlineKeyboardMarkup(row_width=2)
        
        # Кнопки карт
        kb.row(
            types.InlineKeyboardButton("🗺️ Яндекс", url=yandex_url),
            types.InlineKeyboardButton("🗺️ Google", url=google_url)
        )
        
        # Кнопка принятия заказа
        kb.row(
            types.InlineKeyboardButton(
                TEXT[lang]["accept_order"],
                callback_data=f"driver_accept_{order['id']}"
            )
        )
        
        await message.answer(order_text, reply_markup=kb)
        
        
@dp.message_handler(commands=["my_orders"])
async def my_orders_command(message: types.Message):
    """Мои заказы"""
    driver = await get_driver(message.from_user.id)
    if not driver:
        await message.answer("❌ Вы не зарегистрированы как водитель")
        return
    
    orders = await get_driver_orders(message.from_user.id, 10)
    
    if not orders:
        await message.answer("📭 У вас еще нет выполненных заказов")
        return
    
    lang = user_data.get(message.from_user.id, {}).get("language", "ru")
    
    response = "📋 Ваши последние заказы:\n\n"
    
    for order in orders:
        order_time = order["created_at"].strftime("%d.%m %H:%M")
        status_emoji = "🟢" if order["status"] == "completed" else "🟡" if order["status"] == "accepted" else "🔴"
        
        response += f"{status_emoji} Заказ #{order['id']}\n"
        response += f"📍 {order['from_text']} → {order['to_text']}\n"
        response += f"💰 {order['price']} ₸ | ⏰ {order_time} | 📞 {order['phone']}\n"
        response += f"Статус: {order['status']}\n\n"
    
    await message.answer(response)

@dp.message_handler(commands=["stats"])
async def stats_command(message: types.Message):
    """Статистика водителя"""
    driver = await get_driver(message.from_user.id)
    if not driver:
        await message.answer("❌ Вы не зарегистрированы как водитель")
        return
    
    stats = await get_driver_stats(message.from_user.id)
    
    if stats:
        lang = user_data.get(message.from_user.id, {}).get("language", "ru")
        
        response = TEXT[lang]["my_stats"].format(
            total=stats["total_orders"],
            rating=stats["avg_rating"],
            income=stats["total_income"]
        )
        
        response += f"\n\n📅 Сегодня:\n"
        response += f"📦 Заказов: {stats['today_orders']}\n"
        response += f"💰 Заработано: {stats['today_income']} ₸"
        
        await message.answer(response)
    else:
        await message.answer("❌ Не удалось получить статистику")

@dp.message_handler(commands=["settings"])
async def settings_command(message: types.Message):
    """Настройки водителя"""
    driver = await get_driver(message.from_user.id)
    if not driver:
        await message.answer("❌ Вы не зарегистрированы как водитель")
        return
    
    lang = user_data.get(message.from_user.id, {}).get("language", "ru")
    
    response = TEXT[lang]["driver_settings"].format(
        car=f"{driver['car_model']} ({driver['car_number']})",
        phone=driver["phone"],
        status="🟢 Онлайн" if driver["status"] == "online" else "🟡 Занят" if driver["status"] == "busy" else "🔴 Офлайн"
    )
    
    kb = types.InlineKeyboardMarkup(row_width=2)
    if driver["status"] == "online":
        kb.add(types.InlineKeyboardButton("🚫 Перейти в офлайн", callback_data="driver_status_offline"))
    else:
        kb.add(types.InlineKeyboardButton("✅ Перейти в онлайн", callback_data="driver_status_online"))
    
    await message.answer(response, reply_markup=kb)




# Добавим команды для водителей  




# Добавим обработчики для водительских действий

# ================= DRIVER CALLBACK HANDLERS (ДОБАВИТЬ) =================

@dp.callback_query_handler(lambda c: c.data.startswith('driver_accept_'))
async def driver_accept_order_callback(callback_query: types.CallbackQuery):
    """Обработка принятия заказа водителем с картами"""
    try:
        order_id = int(callback_query.data.split('_')[2])
        driver_id = callback_query.from_user.id
        
        driver = await get_driver(driver_id)
        if not driver:
            await callback_query.answer("❌ Вы не зарегистрированы как водитель")
            return
        
        if driver["status"] == "busy":
            await callback_query.answer("❌ У вас уже есть активный заказ")
            return
        
        if driver["status"] == "offline":
            await callback_query.answer("❌ Вы в офлайн режиме")
            return
        
        # Принимаем заказ
        success = await accept_order(
            order_id,
            driver_id,
            callback_query.from_user.username or ""
        )
        
        if success:
            # Получаем информацию о заказе
            async with db_pool.acquire() as conn:
                order = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
            
            if order:
                # Отправляем водителю заказ с картами
                await send_driver_order_with_maps(driver_id, order)
                
                # Удаляем старое сообщение с кнопкой "Принять"
                await callback_query.message.delete()
            else:
                await callback_query.message.edit_text("❌ Заказ не найден")
        else:
            await callback_query.message.edit_text("❌ Не удалось принять заказ. Возможно, заказ уже принят другим водителем.")
    
    except Exception as e:
        logger.error(f"Ошибка в обработчике принятия заказа: {e}")
        await callback_query.message.edit_text("❌ Произошла ошибка при принятии заказа")
    
    await callback_query.answer()



#Добавим функцию для получения заказа по ID
async def get_order_by_id(order_id: int):
    """Получение заказа по ID"""
    if not db_pool:
        return None
    
    try:
        async with db_pool.acquire() as conn:
            order = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
            return order
    except Exception as e:
        logger.error(f"Ошибка при получении заказа: {e}")
        return None





       
    
@dp.callback_query_handler(lambda c: c.data.startswith('driver_complete_'))
async def driver_complete_order_callback(callback_query: types.CallbackQuery):
    """Обработка завершения заказа водителем"""
    order_id = int(callback_query.data.split('_')[2])
    driver_id = callback_query.from_user.id
    
    success = await complete_order(order_id, driver_id)
    
    if success:
        await callback_query.message.edit_text("✅ Заказ завершен!")
    else:
        await callback_query.message.edit_text("❌ Ошибка при завершении заказа")
    
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data in ["driver_status_online", "driver_status_offline"])
async def driver_status_callback(callback_query: types.CallbackQuery):
    """Обработка изменения статуса водителя"""
    driver_id = callback_query.from_user.id
    action = callback_query.data
    
    if action == "driver_status_online":
        await update_driver_status(driver_id, "online")
        await callback_query.message.edit_text("✅ Вы перешли в онлайн режим")
    
    elif action == "driver_status_offline":
        await update_driver_status(driver_id, "offline")
        await callback_query.message.edit_text("🚫 Вы перешли в офлайн режим")
    
    await callback_query.answer()  
 
# Добавим обработчики для водительских действий




    
   
        
        

# Администраторлық командалар
# ================= ADMIN COMMANDS =================

@dp.message_handler(commands=["admin"])
async def admin_command(message: types.Message):
    """Администраторская панель"""
    # Здесь можно добавить проверку на админа
    # Например: if message.from_user.id not in ADMIN_IDS: return
    
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add("📋 Заявки", "🚕 Водители")
    kb.add("📊 Статистика", "🚫 Блокировки")
    kb.add("📈 Отчеты", "⚙️ Настройки")
    
    await message.answer("👑 Администраторская панель:", reply_markup=kb)

@dp.message_handler(lambda message: message.text == "📋 Заявки")
@dp.message_handler(commands=["applications"])
async def applications_command(message: types.Message):
    """Просмотр заявок на водителей"""
    applications = await get_pending_applications()
    
    if not applications:
        await message.answer("📭 Нет ожидающих заявок")
        return
    
    for app in applications:
        app_time = app["created_at"].strftime("%d.%m.%Y %H:%M")
        
        app_text = f"📋 Заявка #{app['id']}\n\n"
        app_text += f"👤 Пользователь: @{app['username'] or 'нет'}\n"
        app_text += f"🆔 ID: {app['user_id']}\n"
        app_text += f"📞 Телефон: {app['phone']}\n"
        app_text += f"🚗 Авто: {app['car_model']}\n"
        app_text += f"🚘 Номер: {app['car_number']}\n"
        app_text += f"⏰ Дата: {app_time}\n"
        
        # Создаем клавиатуру с кнопками для одобрения/отклонения
        kb = types.InlineKeyboardMarkup(row_width=3)
        kb.add(
            types.InlineKeyboardButton("✅ 1 нед", callback_data=f"approve_{app['user_id']}_7"),
            types.InlineKeyboardButton("✅ 2 нед", callback_data=f"approve_{app['user_id']}_14"),
            types.InlineKeyboardButton("✅ 1 мес", callback_data=f"approve_{app['user_id']}_30")
        )
        kb.add(
            types.InlineKeyboardButton("✅ 3 мес", callback_data=f"approve_{app['user_id']}_90"),
            types.InlineKeyboardButton("✅ 6 мес", callback_data=f"approve_{app['user_id']}_180"),
            types.InlineKeyboardButton("✅ 1 год", callback_data=f"approve_{app['user_id']}_365")
        )
        kb.add(
            types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{app['user_id']}")
        )
        
        await message.answer(app_text, reply_markup=kb)

@dp.message_handler(lambda message: message.text == "🚕 Водители")
@dp.message_handler(commands=["drivers"])
async def drivers_command(message: types.Message):
    """Список всех активных водителей"""
    drivers = await get_all_drivers()
    
    if not drivers:
        await message.answer("📭 Нет активных водителей")
        return
    
    response = "🚕 Список активных водителей:\n\n"
    
    for driver in drivers:
        status_emoji = "🟢" if driver["status"] == "online" else "🟡" if driver["status"] == "busy" else "🔴"
        
        # Проверяем срок действия
        expires_info = ""
        if driver["expires_at"]:
            days_left = (driver["expires_at"] - datetime.now()).days
            if days_left > 0:
                expires_info = f" | ⏳ {days_left} дн."
            else:
                expires_info = " | ⚠️ Истек"
                status_emoji = "⚫"
        
        response += f"{status_emoji} @{driver['username'] or 'нет'}\n"
        response += f"   🆔 {driver['user_id']} | 🚗 {driver['car_model']} ({driver['car_number']})\n"
        response += f"   📞 {driver['phone']} | 📊 {driver['total_orders']} зак.{expires_info}\n"
        
        # Добавляем кнопки управления
        kb = types.InlineKeyboardMarkup()
        kb.add(
            types.InlineKeyboardButton("🚫 Деактивировать", callback_data=f"deactivate_{driver['user_id']}")
        )
        
        await message.answer(response, reply_markup=kb)
        response = ""  # Сбрасываем для следующего сообщения

@dp.message_handler(commands=["stats_admin"])
async def stats_admin_command(message: types.Message):
    """Статистика для администратора"""
    if not db_pool:
        await message.answer("❌ Нет подключения к БД")
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Общая статистика
            total_orders = await conn.fetchval("SELECT COUNT(*) FROM orders")
            total_users = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM orders")
            total_drivers = await conn.fetchval("SELECT COUNT(*) FROM drivers WHERE is_active = TRUE")
            
            # Сегодняшние заказы
            today_orders = await conn.fetchval("""
                SELECT COUNT(*) FROM orders 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            
            # Ожидающие заявки
            pending_apps = await conn.fetchval("""
                SELECT COUNT(*) FROM driver_applications 
                WHERE status = 'pending'
            """)
            
            # Доход за сегодня
            today_income = await conn.fetchval("""
                SELECT COALESCE(SUM(price), 0) FROM orders 
                WHERE DATE(created_at) = CURRENT_DATE AND status = 'completed'
            """)
            
            response = "📊 Статистика системы:\n\n"
            response += f"📦 Всего заказов: {total_orders}\n"
            response += f"👥 Уникальных клиентов: {total_users}\n"
            response += f"🚕 Активных водителей: {total_drivers}\n"
            response += f"📋 Ожидающих заявок: {pending_apps}\n\n"
            response += f"📅 Сегодня:\n"
            response += f"   📦 Заказов: {today_orders}\n"
            response += f"   💰 Доход: {today_income} ₸\n"
            
            await message.answer(response)
            
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")




# ================= ADMIN CALLBACK HANDLERS =================

@dp.callback_query_handler(lambda c: c.data.startswith('approve_'))
async def approve_application_callback(callback_query: types.CallbackQuery):
    """Одобрение заявки"""
    parts = callback_query.data.split('_')
    user_id = int(parts[1])
    duration_days = int(parts[2]) if len(parts) > 2 else 30
    
    # Одобряем заявку
    success = await approve_driver_application(user_id, callback_query.from_user.id, duration_days)
    
    if success:
        # Уведомляем администратора
        duration_text = get_duration_text(duration_days)
        await callback_query.message.edit_text(
            f"✅ Заявка пользователя {user_id} одобрена на {duration_text}"
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f"🎉 Поздравляем! Ваша заявка на водителя одобрена!\n\n"
                f"✅ Вы теперь водитель такси.\n"
                f"⏳ Срок действия: {duration_text}\n\n"
                f"Используйте кнопку '🚕 Стать водителем' или команду /driver для начала работы."
            )
        except Exception as e:
            logger.error(f"Ошибка при уведомлении пользователя: {e}")
    
    else:
        await callback_query.message.edit_text("❌ Ошибка при одобрении заявки")
    
    await callback_query.answer()

def get_duration_text(days: int) -> str:
    """Преобразование дней в читаемый текст"""
    if days == 7:
        return "1 неделю"
    elif days == 14:
        return "2 недели"
    elif days == 30:
        return "1 месяц"
    elif days == 90:
        return "3 месяца"
    elif days == 180:
        return "6 месяцев"
    elif days == 365:
        return "1 год"
    else:
        return f"{days} дней"

@dp.callback_query_handler(lambda c: c.data.startswith('reject_'))
async def reject_application_callback(callback_query: types.CallbackQuery):
    """Отклонение заявки"""
    user_id = int(callback_query.data.split('_')[1])
    
    # Запрашиваем причину отклонения
    await callback_query.message.answer(
        f"Укажите причину отклонения заявки пользователя {user_id}:"
    )
    
    # Сохраняем данные для следующего шага
    admin_id = callback_query.from_user.id
    if admin_id not in user_data:
        user_data[admin_id] = {}
    
    user_data[admin_id]["rejecting_user_id"] = user_id
    user_data[admin_id]["reject_message_id"] = callback_query.message.message_id
    
    await callback_query.answer()

@dp.message_handler(lambda message: user_data.get(message.from_user.id, {}).get("rejecting_user_id"))
async def process_reject_reason(message: types.Message):
    """Обработка причины отклонения"""
    admin_id = message.from_user.id
    data = user_data.get(admin_id, {})
    user_id = data.get("rejecting_user_id")
    
    if not user_id:
        return
    
    reason = message.text
    
    # Отклоняем заявку
    success = await reject_driver_application(user_id, admin_id, reason)
    
    if success:
        # Удаляем временные данные
        if admin_id in user_data:
            if "rejecting_user_id" in user_data[admin_id]:
                del user_data[admin_id]["rejecting_user_id"]
            if "reject_message_id" in user_data[admin_id]:
                del user_data[admin_id]["reject_message_id"]
        
        # Редактируем оригинальное сообщение
        try:
            message_id = data.get("reject_message_id")
            if message_id:
                await bot.edit_message_text(
                    f"❌ Заявка пользователя {user_id} отклонена.\nПричина: {reason}",
                    chat_id=message.chat.id,
                    message_id=message_id
                )
        except:
            pass
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f"❌ Ваша заявка на водителя отклонена.\n\n"
                f"Причина: {reason}\n\n"
                f"Вы можете подать новую заявку через некоторое время."
            )
        except Exception as e:
            logger.error(f"Ошибка при уведомлении пользователя: {e}")
        
        await message.answer("✅ Заявка отклонена")
    else:
        await message.answer("❌ Ошибка при отклонении заявки")

@dp.callback_query_handler(lambda c: c.data.startswith('deactivate_'))
async def deactivate_driver_callback(callback_query: types.CallbackQuery):
    """Деактивация водителя"""
    user_id = int(callback_query.data.split('_')[1])
    
    # Запрашиваем причину деактивации
    await callback_query.message.answer(
        f"Укажите причину деактивации водителя {user_id}:"
    )
    
    # Сохраняем данные для следующего шага
    admin_id = callback_query.from_user.id
    if admin_id not in user_data:
        user_data[admin_id] = {}
    
    user_data[admin_id]["deactivating_user_id"] = user_id
    user_data[admin_id]["deactivate_message_id"] = callback_query.message.message_id
    
    await callback_query.answer()

@dp.message_handler(lambda message: user_data.get(message.from_user.id, {}).get("deactivating_user_id"))
async def process_deactivate_reason(message: types.Message):
    """Обработка причины деактивации"""
    admin_id = message.from_user.id
    data = user_data.get(admin_id, {})
    user_id = data.get("deactivating_user_id")
    
    if not user_id:
        return
    
    reason = message.text
    
    # Деактивируем водителя
    success = await deactivate_driver(user_id, admin_id, reason)
    
    if success:
        # Удаляем временные данные
        if admin_id in user_data:
            if "deactivating_user_id" in user_data[admin_id]:
                del user_data[admin_id]["deactivating_user_id"]
            if "deactivate_message_id" in user_data[admin_id]:
                del user_data[admin_id]["deactivate_message_id"]
        
        # Редактируем оригинальное сообщение
        try:
            message_id = data.get("deactivate_message_id")
            if message_id:
                await bot.edit_message_text(
                    f"🚫 Водитель {user_id} деактивирован.\nПричина: {reason}",
                    chat_id=message.chat.id,
                    message_id=message_id
                )
        except:
            pass
        
        # Уведомляем водителя
        try:
            await bot.send_message(
                user_id,
                f"🚫 Ваш аккаунт водителя деактивирован.\n\n"
                f"Причина: {reason}\n\n"
                f"По вопросам обращайтесь к администрации."
            )
        except Exception as e:
            logger.error(f"Ошибка при уведомлении водителя: {e}")
        
        await message.answer("✅ Водитель деактивирован")
    else:
        await message.answer("❌ Ошибка при деактивации водителя")


# Администраторлық командалар




# Добавим функцию для проверки админов

# ================= CONFIG =================
ADMIN_IDS = [886699157, 1769921919]  # Замените на ваши ID

def is_admin(user_id: int) -> bool:
    """Проверка, является ли пользователь администратором"""
    return user_id in ADMIN_IDS

# Обновим админские команды с проверкой
@dp.message_handler(commands=["admin", "applications", "drivers", "stats_admin"])
async def admin_check_command(message: types.Message):
    """Проверка прав администратора"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав администратора")
        return
    
    # Перенаправляем на соответствующий обработчик
    if message.text == "/admin":
        await admin_command(message)
    elif message.text == "/applications":
        await applications_command(message)
    elif message.text == "/drivers":
        await drivers_command(message)
    elif message.text == "/stats_admin":
        await stats_admin_command(message)



# Функция для удаления просроченных водителей
async def delete_expired_driver(user_id: int):
    """Удаление водителя с истекшим сроком"""
    if not db_pool:
        return False
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE drivers 
                SET is_active = FALSE, status = 'expired'
                WHERE user_id = $1
            """, user_id)
            return True
    except Exception as e:
        logger.error(f"Ошибка при удалении просроченного водителя: {e}")
        return False

async def check_expired_drivers():
    """Проверка и отключение просроченных водителей"""
    if not db_pool:
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Находим просроченных водителей
            expired_drivers = await conn.fetch("""
                SELECT user_id, username 
                FROM drivers 
                WHERE is_active = TRUE 
                AND expires_at < NOW()
            """)
            
            for driver in expired_drivers:
                # Отключаем водителя
                await conn.execute("""
                    UPDATE drivers 
                    SET is_active = FALSE, status = 'expired'
                    WHERE user_id = $1
                """, driver["user_id"])
                
                # Уведомляем водителя
                try:
                    await bot.send_message(
                        driver["user_id"],
                        "🚫 Срок ваших водительских прав истек.\n\n"
                        "Для продолжения работы подайте новую заявку на водителя."
                    )
                except Exception as e:
                    logger.error(f"Ошибка при уведомлении водителя {driver['user_id']}: {e}")
            
            if expired_drivers:
                logger.info(f"Отключено {len(expired_drivers)} просроченных водителей")
                
    except Exception as e:
        logger.error(f"Ошибка при проверке просроченных водителей: {e}")
# Функция для удаления просроченных водителей








  
    
# ================= FROM LOCATION/TEXT =================
async def get_address_from_coords(lat: float, lon: float) -> str:
    """Получение адреса по координатам (обратное геокодирование)"""
    try:
        # Используем Яндекс.Геокодер
        yandex_url = f"https://geocode-maps.yandex.ru/1.x/"
        params = {
            "format": "json",
            "geocode": f"{lon},{lat}",
            "apikey": "",  # если есть API ключ Яндекс
            "lang": "ru_RU"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(yandex_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Пытаемся извлечь адрес
                    try:
                        # Яндекс возвращает адрес так
                        address = data["response"]["GeoObjectCollection"][
                            "featureMember"][0]["GeoObject"]["metaDataProperty"][
                            "GeocoderMetaData"]["text"]
                        return address
                    except:
                        pass
    except:
        pass
    
    # Если не получилось через Яндекс, используем Nominatim (OpenStreetMap)
    try:
        nominatim_url = f"https://nominatim.openstreetmap.org/reverse"
        params = {
            "format": "json",
            "lat": lat,
            "lon": lon,
            "zoom": 18,
            "addressdetails": 1
        }
        
        headers = {
            'User-Agent': 'TaxiBot/1.0 (contact@example.com)'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(nominatim_url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Формируем адрес из компонентов
                    address_parts = []
                    if "road" in data.get("address", {}):
                        address_parts.append(data["address"]["road"])
                    if "house_number" in data.get("address", {}):
                        address_parts.append(data["address"]["house_number"])
                    if "city" in data.get("address", {}):
                        address_parts.append(data["address"]["city"])
                    
                    if address_parts:
                        return ", ".join(address_parts)
    except:
        pass
    
    # Если ничего не получилось - возвращаем координаты
    return f"{lat:.6f}, {lon:.6f}"



@dp.message_handler(content_types=["location"], state=OrderState.from_place)
async def process_from_location(message: types.Message, state: FSMContext):
    """Обработка отправки локации"""
    user_id = message.from_user.id
    
    # Проверяем блокировку
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        await state.finish()
        return
    
    lang = user_data.get(user_id, {}).get("language", "kz")
    
    if message.text and message.text == TEXT[lang]["cancel"]:
        await cancel_handler(message, state)
        return
    
    # ПОЛУЧАЕМ АДРЕС ПО КООРДИНАТАМ (обратное геокодирование)
    address = await get_address_from_coords(
        message.location.latitude, 
        message.location.longitude
    )
    
    # Сохраняем данные локации
    await state.update_data(
        from_lat=message.location.latitude,
        from_lon=message.location.longitude,
        from_text=address or "📍 Локация",  # <--- ТЕПЕРЬ РЕАЛЬНЫЙ АДРЕС
        language=lang
    )
    
    # Клавиатура для следующего шага
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(TEXT[lang]["cancel"])
    
    await message.answer(TEXT[lang]["choose_to"], reply_markup=kb)
    await OrderState.to_place.set()
    

@dp.message_handler(state=OrderState.from_place)
async def process_from_text(message: types.Message, state: FSMContext):
    """Обработка текстового адреса"""
    user_id = message.from_user.id
    
    # Проверяем блокировку
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        await state.finish()
        return
    
    lang = user_data.get(user_id, {}).get("language", "kz")
    
    if message.text == TEXT[lang]["cancel"]:
        await cancel_handler(message, state)
        return
    
    # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убедимся, что за это время не создали другой заказ
    if await has_active_order(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["active_order"])
        await state.finish()
        return
    
    # Сохраняем текстовый адрес
    await state.update_data(
        from_text=message.text,
        from_lat=None,
        from_lon=None,
        language=lang
    )
    
    # Клавиатура для следующего шага
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(TEXT[lang]["cancel"])
    
    await message.answer(TEXT[lang]["choose_to"], reply_markup=kb)
    await OrderState.to_place.set()

# ================= TO DESTINATION =================

@dp.message_handler(state=OrderState.to_place)
async def process_to_destination(message: types.Message, state: FSMContext):
    """Обработка пункта назначения"""
    user_id = message.from_user.id
    
    # Проверяем блокировку
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        await state.finish()
        return
    
    lang = user_data.get(user_id, {}).get("language", "kz")
    
    if message.text == TEXT[lang]["cancel"]:
        await cancel_handler(message, state)
        return
    
    # Сохраняем пункт назначения
    await state.update_data(
        to_text=message.text,
        language=lang
    )
    
    # Создаем клавиатуру для выбора цены
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    # Добавляем кнопки с ценами
    price_options = TEXT[lang]["price_options"]
    buttons = []
    for option in price_options:
        buttons.append(types.KeyboardButton(option))
    
    # Распределяем кнопки по 2 в строку
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            kb.add(buttons[i], buttons[i + 1])
        else:
            kb.add(buttons[i])
    
    kb.add(TEXT[lang]["cancel"])
    
    await message.answer(TEXT[lang]["choose_price"], reply_markup=kb)
    await OrderState.price.set()



# ================= PRICE SELECTION =================

@dp.message_handler(state=OrderState.price)
async def process_price(message: types.Message, state: FSMContext):
    """Обработка выбора цены"""
    user_id = message.from_user.id
    lang = user_data.get(user_id, {}).get("language", "kz")
    
    # Проверяем отмену
    if message.text == TEXT[lang]["cancel"]:
        await cancel_handler(message, state)
        return
    
    price = None
    
    # Проверяем, выбрал ли пользователь одну из стандартных цен
    if message.text in TEXT[lang]["price_options"]:
        # Если это "Моя цена" или эквивалент
        if "📝" in message.text or "Өз бағам" in message.text or "Моя цена" in message.text:
            await message.answer(
                f"✏️ {TEXT[lang]['choose_price'].split('(')[0]}",
                reply_markup=types.ReplyKeyboardRemove()
            )
            # Оставляем пользователя в том же состоянии для ввода своей цены
            return
        
        # Извлекаем цифры из текста (например: "300 ₸" -> 300)
        try:
            price = int(''.join(filter(str.isdigit, message.text)))
        except:
            price = None
    
    # Если не стандартная цена, проверяем ввод пользователя
    if price is None:
        # Проверяем, что введено только число
        try:
            # Убираем пробелы и проверяем, что это число
            price_text = message.text.strip().replace(' ', '')
            if not price_text.isdigit():
                raise ValueError
                
            price = int(price_text)
            
            # Проверяем минимальную цену (например, не меньше 200 тенге)
            if price < 200:
                await message.answer(f"❌ {TEXT[lang]['invalid_price']}. Минимальная цена: 200 ₸")
                return
                
            # Проверяем максимальную цену (например, не больше 5000 тенге)
            if price > 5000:
                await message.answer(f"❌ {TEXT[lang]['invalid_price']}. Максимальная цена: 5000 ₸")
                return
                
        except (ValueError, AttributeError):
            await message.answer(f"❌ {TEXT[lang]['invalid_price']}")
            return
    
    # Сохраняем цену
    await state.update_data(price=price)
    
    # Подтверждаем выбор цены
    await message.answer(
        TEXT[lang]["price_selected"].format(price=price),
        reply_markup=types.ReplyKeyboardRemove()
    )
    
    # Создаем клавиатуру для номера телефона
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("📞 Телефон", request_contact=True))
    kb.add(TEXT[lang]["cancel"])
    
    await message.answer(TEXT[lang]["send_phone"], reply_markup=kb)
    await OrderState.contact.set()


# ================= CONTACT =================

@dp.message_handler(content_types=["contact"], state=OrderState.contact)
async def process_contact(message: types.Message, state: FSMContext):
    """Обработка контакта и сохранение заказа"""
    user_id = message.from_user.id
    phone_number = message.contact.phone_number
    
    # 1. Проверяем блокировку
    if await is_user_blocked(user_id, phone_number):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        await state.finish()
        return
    
    # 2. Проверяем, есть ли активный заказ (в пределах 5 минут) ПЕРЕД созданием нового
    if await has_active_order(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["active_order"])
        await state.finish()
        
        # Возвращаем в главное меню
        await send_main_menu(user_id, lang)
        return
    
    # 3. Проверяем количество активных заказов
    active_count = await get_active_order_count(user_id)
    if active_count >= 1:
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["too_many_orders"])
        await state.finish()
        
        # Возвращаем в главное меню
        await send_main_menu(user_id, lang)
        return
    
    # Получаем данные
    data = await state.get_data()
    lang = data.get("language", "kz")
    price = data.get("price", 1000)
    
    # Проверяем подключение к БД
    if not db_pool:
        await message.answer(TEXT[lang]["db_error"])
        await state.finish()
        return
    
    try:
        # 4. Деактивируем старые заказы пользователя (старше 5 минут)
        # Это нужно для очистки базы, но не должно мешать проверке выше
        await deactivate_old_orders(user_id)
        
        # 5. Создаем новый заказ
        async with db_pool.acquire() as conn:
            result = await conn.fetchrow("""
                INSERT INTO orders (
                    user_id, username, phone,
                    from_text, from_lat, from_lon,
                    to_text, price, language, status, is_active
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
            """,
            user_id,
            message.from_user.username or "",
            phone_number,
            data.get("from_text", ""),
            data.get("from_lat"),
            data.get("from_lon"),
            data.get("to_text", ""),
            price,
            lang,
            "new",
            True
            )
        
        order_id = result["id"] if result else None
        
        # Отправляем подтверждение пользователю
        await message.answer(
            TEXT[lang]["order_done"].format(
                from_=data.get("from_text", ""),
                to=data.get("to_text", ""),
                price=price,
                phone=phone_number
            ),
            reply_markup=types.ReplyKeyboardRemove()
        )
        
        # Отправляем уведомление о ожидании водителя
        await message.answer(TEXT[lang]["waiting_for_driver"])
        
        logger.info(f"✅ Новый заказ #{order_id} от пользователя {user_id} на сумму {price} ₸")
        
        # Ждем 2 секунды перед показом меню
        await asyncio.sleep(2)
        
        # Возвращаем в главное меню
        await send_main_menu(user_id, lang)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении заказа: {e}")
        await message.answer(
            "❌ Произошла ошибка при сохранении заказа. Попробуйте еще раз.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        
        # Даже при ошибке возвращаем в главное меню
        await asyncio.sleep(1)
        await send_main_menu(user_id, lang)
    
    # Завершаем состояние
    await state.finish()
         
# process_contact_text функциясына түзету енгіземіз:    

@dp.message_handler(state=OrderState.contact)
async def process_contact_text(message: types.Message, state: FSMContext):
    """Обработка текста вместо контакта"""
    user_id = message.from_user.id
    
    # Проверяем блокировку
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        await state.finish()
        return
    
    lang = user_data.get(user_id, {}).get("language", "kz")
    
    if message.text == TEXT[lang]["cancel"]:
        await cancel_handler(message, state)
        return
    
    await message.answer(
        f"❌ Пожалуйста, отправьте контакт, нажав кнопку '📞 Телефон'\n\n{TEXT[lang]['send_phone']}"
    )




# ================= ADMIN COMMANDS =================

@dp.message_handler(commands=["check_tables"])
async def check_tables_command(message: types.Message):
    """Проверка структуры таблиц"""
    if not db_pool:
        await message.answer("❌ Нет подключения к базе данных")
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Проверяем таблицу orders
            orders_columns = await conn.fetch("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'orders' 
                ORDER BY ordinal_position
            """)
            
            response = "📊 Структура таблицы orders:\n"
            for col in orders_columns:
                response += f"{col['column_name']} ({col['data_type']})\n"
            
            # Проверяем таблицу blocked_users
            blocked_columns = await conn.fetch("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'blocked_users' 
                ORDER BY ordinal_position
            """)
            
            response += "\n📊 Структура таблицы blocked_users:\n"
            for col in blocked_columns:
                response += f"{col['column_name']} ({col['data_type']})\n"
            
            await message.answer(response)
            
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message_handler(commands=["add_is_active"])
async def add_is_active_command(message: types.Message):
    """Команда для добавления колонки is_active"""
    try:
        await check_and_add_columns()
        await message.answer("✅ Колонка is_active добавлена/проверена")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

# ================= FALLBACK MESSAGE HANDLER =================

@dp.message_handler(lambda message: message.text and "/taxi" in message.text, state=None)
async def taxi_fallback(message: types.Message):
    """Обработчик для команды /taxi без состояния"""
    user_id = message.from_user.id
    
    # Проверяем блокировку
    if await is_user_blocked(user_id):
        lang = user_data.get(user_id, {}).get("language", "ru")
        await message.answer(TEXT[lang]["blocked"])
        return
    
    lang = user_data.get(user_id, {}).get("language")
    
    if not lang:
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        kb.add("🇰🇿 Қазақша", "🇷🇺 Русский")
        
        await message.answer(
            "🌐 Сначала выберите язык / Алдымен тілді таңдаңыз\n\n🌐 Тілді таңдаңыз / Выберите язык",
            reply_markup=kb
        )
    else:
        await message.answer(TEXT[lang]["need_lang"])

# ================= ERROR HANDLER =================

@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    """Обработчик ошибок"""
    logger.error(f"Ошибка при обработке обновления {update}: {exception}")
    return True

# ================= MAIN =================

if __name__ == "__main__":
    print("🤖 Бот запускается...")
    print("✅ Функции:")
    print("   - Проверка блокировки пользователей (ложные заказы)")
    print("   - Один активный заказ на пользователя")
    print("   - Ограничение 5 минут между заказами")
    print("   - Автоматическое обновление структуры таблиц")
    print("\n📋 Команды для проверки:")
    print("   /check_tables - показать структуру таблиц")
    print("   /add_is_active - добавить недостающие колонки")
    
    executor.start_polling(
        dp,
        on_startup=on_startup,
        skip_updates=True,
        timeout=30
    )