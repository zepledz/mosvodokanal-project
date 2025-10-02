from flask import Blueprint, request, jsonify
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from models import User, get_db
import logging

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__)
login_manager = LoginManager()


@login_manager.user_loader
def load_user(user_id):
    db = next(get_db())
    try:
        return db.query(User).filter(User.id == int(user_id)).first()
    except Exception as e:
        logger.error(f"Ошибка загрузки пользователя: {e}")
        return None
    finally:
        db.close()


@auth_bp.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': 'Необходимы имя пользователя и пароль'}), 400

    db = next(get_db())
    try:
        user = db.query(User).filter(User.username == username).first()

        if user and user.check_password(password) and user.is_active:
            login_user(user)
            from datetime import datetime
            user.last_login = datetime.utcnow()
            db.commit()

            logger.info(f"✅ Пользователь {username} вошел в систему")
            return jsonify({
                'message': 'Успешный вход',
                'user': {
                    'id': user.id,
                    'username': user.username,
                    'email': user.email,
                    'role': user.role
                }
            })
        else:
            return jsonify({'error': 'Неверные учетные данные'}), 401

    except Exception as e:
        logger.error(f"❌ Ошибка входа: {e}")
        return jsonify({'error': 'Ошибка сервера'}), 500
    finally:
        db.close()


@auth_bp.route('/api/auth/logout', methods=['POST'])
@login_required
def logout():
    logout_user()
    return jsonify({'message': 'Успешный выход'})


@auth_bp.route('/api/auth/me', methods=['GET'])
@login_required
def get_current_user():
    return jsonify({
        'user': {
            'id': current_user.id,
            'username': current_user.username,
            'email': current_user.email,
            'role': current_user.role
        }
    })