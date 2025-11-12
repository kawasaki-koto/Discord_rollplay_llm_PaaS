import os
import pymongo
from utils.console_display import log_system, log_error, log_success
import utils.config_manager as config

# グローバル変数
_db_client = None
_db = None
_data_cache = {} # 従来通り、メモリキャッシュも使用する

# データキーとMongoDBのコレクション名をマッピング
COLLECTION_MAP = {
    'emotion': 'emotion',
    'setting': 'setting',
    'memory': 'memory',
    'schedule': 'schedule',
    'history': 'history',
    'unread': 'unread'
}

def init_db():
    """
    起動時にデータベースに接続し、コレクションを初期化する
    """
    global _db_client, _db
    try:
        uri = os.getenv("MONGODB_URI")
        db_name = os.getenv("DB_NAME", config.CHARACTER_NAME) # DB名がなければキャラ名を使う

        if not uri:
            log_error("DB_MANAGER", "環境変数 'MONGODB_URI' が設定されていません。")
            return False
        
        _db_client = pymongo.MongoClient(uri)
        _db = _db_client[db_name]
        
        log_system(f"データベース '{db_name}' への接続に成功しました。")
        return True
    except Exception as e:
        log_error("DB_MANAGER", f"データベース接続中にエラー: {e}")
        return False

def load_all_data():
    """
    起動時に全てのデータをDBから読み込み、メモリにキャッシュする
    """
    if not _db:
        log_error("DB_MANAGER", "DBが初期化されていません。load_all_dataをスキップします。")
        return

    global _data_cache
    
    # 各コレクションからデータを1件取得（または初期化）
    for key, collection_name in COLLECTION_MAP.items():
        collection = _db[collection_name]
        data = collection.find_one()

        if data:
            _data_cache[key] = data.get('data', {}) # 'data' フィールドに実データを入れる想定
        else:
            # DBにデータがない場合、デフォルト値で初期化
            log_system(f"DBに '{collection_name}' のデータがないため、初期化します。")
            default_data = {}
            if key in ['memory']: # memory.jsonはリストだった
                default_data = []

            # find_one_and_updateで、なければ挿入(upsert=True)
            collection.find_one_and_update(
                {},
                {"$setOnInsert": {"data": default_data}},
                upsert=True
            )
            _data_cache[key] = default_data

    log_system("全てのデータをDBからメモリにロードしました。")

def save_data(key: str, data: dict | list):
    """
    指定されたキーのデータ（メモリキャッシュ）をDBに保存する
    """
    if not _db:
        return False

    try:
        collection_name = COLLECTION_MAP.get(key)
        if collection_name:
            # 常に単一のドキュメントを更新（なければ挿入）
            _db[collection_name].update_one(
                {}, # 空のフィルター（ドキュメントが1つしかない前提）
                {"$set": {"data": data}},
                upsert=True # もしドキュメントが消えていても作成する
            )
            # log_success("DB_MANAGER", f"データ '{key}' をDBに保存しました。")
            return True
    except Exception as e:
        log_error("DB_MANAGER", f"データ '{key}' のDB保存中にエラー: {e}")
    
    return False

def get_data(key: str):
    """
    メモリ上のデータキャッシュへの参照を取得する
    """
    return _data_cache.get(key)

# --- 従来(ai_request_handler)から呼ばれる用の互換関数 ---
# ※ ai_request_handler 側も修正したほうが望ましいが、一旦互換性を持たせる

def initialize_histories():
    """
    ai_request_handler.pyから呼ばれる
    """
    if 'history' not in _data_cache:
        log_error("DB_MANAGER", "履歴キャッシュがまだロードされていません。")
        _data_cache['history'] = {} # 緊急初期化

def reset_histories():
    """
    commands.pyから呼ばれる
    """
    _data_cache['history'] = {}
    save_data('history', {})
    log_system("履歴をリセットし、DBに保存しました。")

def get_history_for_channel(channel_id: int):
    """
    commands.pyから呼ばれる
    """
    return _data_cache.get('history', {}).get(str(channel_id))

def apply_persona_to_channel(channel_id: int):
    """
    commands.pyから呼ばれる (注: load_personaはai_request_handler側にある)
    """
    from utils.ai_request_handler import _load_persona # 循環参照に注意しつつインポート
    persona = _load_persona()
    if persona:
        str_channel_id = str(channel_id)
        _data_cache.setdefault('history', {})[str_channel_id] = [{"role": "user", "parts": [persona]}]
        save_data('history', _data_cache['history'])
        log_system(f"CH[{channel_id}] の履歴にペルソナを適用し、DBに保存しました。")

def load_persona():
    """
    commands.pyから呼ばれる (ai_request_handlerの関数をそのまま呼ぶ)
    """
    from utils.ai_request_handler import _load_persona
    return _load_persona()

def save_all_data():
    """
    メモリ上の全てのデータをDBに保存する（シャットダウン時や!saveコマンド用）
    """
    if not _db:
        log_error("DB_MANAGER", "DB未接続のため、全データの保存をスキップします。")
        return

    log_system("全キャッシュデータをデータベースに保存しています...")
    saved_keys = []
    failed_keys = []

    for key, data in _data_cache.items():
        if save_data(key, data): # 既存のsave_data関数を流用
            saved_keys.append(key)
        else:
            failed_keys.append(key)
    
    if not failed_keys:
        log_success("DB_MANAGER", f"全データ ({', '.join(saved_keys)}) のDB保存に成功しました。")
    else:
        log_error("DB_MANAGER", f"一部データ ({', '.join(failed_keys)}) のDB保存に失敗しました。")