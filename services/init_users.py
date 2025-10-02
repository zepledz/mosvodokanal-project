import sys
import os

sys.path.append(os.path.dirname(__file__))

from models import User, get_db, init_db


def create_initial_users():
    db = next(get_db())
    try:
        # Проверяем, есть ли уже пользователи
        if db.query(User).count() > 0:
            print("✅ Пользователи уже созданы")
            return

        # Создаем администратора
        admin = User(
            username='admin',
            email='admin@mosvodokanal.ru',
            role='admin'
        )
        admin.set_password('admin123')

        # Создаем диспетчера
        dispatcher = User(
            username='dispatcher',
            email='dispatcher@mosvodokanal.ru',
            role='dispatcher'
        )
        dispatcher.set_password('dispatcher123')

        db.add(admin)
        db.add(dispatcher)
        db.commit()

        print("✅ Пользователи созданы:")
        print("   admin/admin123 (Администратор)")
        print("   dispatcher/dispatcher123 (Диспетчер)")

    except Exception as e:
        db.rollback()
        print(f"❌ Ошибка создания пользователей: {e}")
    finally:
        db.close()


if __name__ == '__main__':
    print("🔄 Инициализация пользователей...")
    init_db()
    create_initial_users()