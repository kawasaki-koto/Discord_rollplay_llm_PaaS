import os
import pymongo
import threading  # ★ 追加
import copy       # ★ 追加
from utils.console_display import log_system, log_error, log_success
import utils.config_manager as config

# グローバル変数
_db_client = None
_db = None
_data_cache = {}

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
    global _db_client, _db
    try:
        uri = os.getenv("MONGODB_URI")
        db_name = os.getenv("DB_NAME", config.CHARACTER_NAME)

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
    if _db is None:
        log_error("DB_MANAGER", "DBが初期化されていません。load_all_dataをスキップします。")
        return

    global _data_cache
    
    for key, collection_name in COLLECTION_MAP.items():
        try:
            collection = _db[collection_name]
            data = collection.find_one()

            if data:
                _data_cache[key] = data.get('data', {})
            else:
                log_system(f"DBに '{collection_name}' のデータがないため、初期化します。")
                default_data = {}
                if key in ['memory']:
                    default_data = []

                collection.find_one_and_update(
                    {},
                    {"$setOnInsert": {"data": default_data}},
                    upsert=True
                )
                _data_cache[key] = default_data
        except Exception as e:
             log_error("DB_MANAGER", f"データのロード中にエラー({key}): {e}")
             _data_cache[key] = {} if key != 'memory' else []

    log_system("全てのデータをDBからメモリにロードしました。")

# ★★★ 内部用: 実際にDBに書き込む関数（別スレッドで動く） ★★★
def _save_worker(collection_name, data_copy):
    try:
        _db[collection_name].update_one(
            {},
            {"$set": {"data": data_copy}},
            upsert=True
        )
    except Exception as e:
        print(f"![DB_BG_SAVE_ERROR] {collection_name} の保存失敗: {e}")

def save_data(key: str, data: dict | list):
    """
    指定されたキーのデータを、バックグラウンドでDBに保存する（待機しない）
    """
    if _db is None:
        return False

    collection_name = COLLECTION_MAP.get(key)
    if collection_name:
        # データのコピーを作成してスレッドに渡す（スレッド実行中に元のデータが変更されるのを防ぐため）
        # データ量が巨大な場合は deepcopy はコストになるが、テキストベースなら許容範囲
        try:
            # メモリキャッシュと同期をとるために念のためコピー
            # ※ data自体が _data_cache[key] への参照であることが多いため
            data_copy = copy.deepcopy(data)
            
            # スレッドを作成してスタート（処理を待たずに即returnする）
            t = threading.Thread(target=_save_worker, args=(collection_name, data_copy))
            t.start()
            return True
        except Exception as e:
            log_error("DB_MANAGER", f"保存スレッド作成エラー: {e}")
            return False
    
    return False

def get_data(key: str):
    return _data_cache.get(key)

# --- 互換関数 ---

def initialize_histories():
    if 'history' not in _data_cache:
        log_error("DB_MANAGER", "履歴キャッシュがまだロードされていません。")
        _data_cache['history'] = {}

def reset_histories():
    _data_cache['history'] = {}
    save_data('history', {})
    log_system("履歴をリセットし、DBに保存しました。")

def get_history_for_channel(channel_id: int):
    return _data_cache.get('history', {}).get(str(channel_id))

def apply_persona_to_channel(channel_id: int):
    from utils.ai_request_handler import _load_persona
    persona = _load_persona()
    if persona:
        str_channel_id = str(channel_id)
        _data_cache.setdefault('history', {})[str_channel_id] = [{"role": "user", "parts": [persona]}]
        save_data('history', _data_cache['history'])
        log_system(f"CH[{channel_id}] の履歴にペルソナを適用し、DBに保存しました。")

def load_persona():
    from utils.ai_request_handler import _load_persona
    return _load_persona()

def save_all_data():
    # シャットダウン時はスレッドではなく同期的に保存したほうが安全だが、
    # 簡易的にsave_dataを呼ぶ（プロセス終了まで少し待つ必要があるかも）
    if _db is None: return

    log_system("全キャッシュデータをデータベースに保存しています...")
    for key, data in _data_cache.items():
        # ここではコピーせずメインスレッドで保存してもよいが、
        # 実装を統一するため save_data を使う
        save_data(key, data)
    
    log_success("DB_MANAGER", "全データの保存リクエストを発行しました。")