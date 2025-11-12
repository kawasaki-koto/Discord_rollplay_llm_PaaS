import os
import json
from utils.console_display import log_error, log_system
from utils import data_manager

# --- グローバル設定変数 ---
# これらの変数は init() が呼ばれるまで空です
CHARACTER_NAME = ""
BASE_DIR = ""
DATA_DIR = ""
TOKEN_ENV_VAR = ""
PERSONA_FILE = ""
EMOTION_ANALYZER_PERSONA_FILE = ""
SETTING_FILE = ""
HISTORY_FILE = ""
UNREAD_MESSAGES_FILE = ""
EMOTION_FILE = ""
SCHEDULE_FILE = ""
MEMORY_FILE = ""

GEMINI_API_KEY_1 = os.getenv("GEMINI_API_KEY_1")
GEMINI_API_KEY_2 = os.getenv("GEMINI_API_KEY_2")
GEMINI_API_KEY_3 = os.getenv("GEMINI_API_KEY_3")

# --- 静的設定 ---
MODEL_PRO = 'gemini-2.5-pro'
MODEL_PRO_2 = 'gemini-2.5-flash'
MODEL_PRO_3 = 'gemini-2.5-flash-lite'
MODEL_FLASH = 'gemini-2.0-flash'

# APIリクエストのタイムアウト時間 (秒)
API_TIMEOUT = 120 # 例: 120秒

# 履歴の最大長 (会話ターン数ではなく、user/modelメッセージの合計数)
# 例: 50件 = 25往復分程度
MAX_HISTORY_LENGTH = 200

def get_api_timeout():
    """APIリクエストのタイムアウト時間を取得"""
    return API_TIMEOUT

def get_max_history_length():
    """履歴の最大長を取得"""
    return MAX_HISTORY_LENGTH

# discord.Bot インスタンスを保持 (ai_request_handler.py からアクセスするため)
bot = None

def set_bot_instance(bot_instance):
    """Botインスタンスを設定"""
    global bot
    bot = bot_instance

def get_default_channel_id() -> int | None:
    """
    メモリ上のsettingデータからデフォルトチャンネルIDを取得する
    """
    settings = data_manager.get_data('setting')
    if settings:
        return settings.get('config', {}).get('default_channel')
    return None

def init(character_name: str):
    """
    起動時に指定されたキャラクター名に基づいて、全てのパスと設定を動的に初期化する
    """
    global CHARACTER_NAME, BASE_DIR, DATA_DIR, TOKEN_ENV_VAR, PERSONA_FILE
    global EMOTION_ANALYZER_PERSONA_FILE, SETTING_FILE, HISTORY_FILE
    global UNREAD_MESSAGES_FILE, EMOTION_FILE, SCHEDULE_FILE, MEMORY_FILE
    
    CHARACTER_NAME = character_name
    log_system(f"キャラクター '{CHARACTER_NAME}' の設定を初期化します。")
    
    # --- 動的パス設定 ---
    # instances/{キャラクター名} のディレクトリ
    BASE_DIR = os.path.join("instances", CHARACTER_NAME)
    if not os.path.isdir(BASE_DIR):
        log_error("CONFIG", f"キャラクターディレクトリ '{BASE_DIR}' が見つかりません。")
        return False
        
    DATA_DIR = os.path.join(BASE_DIR, "data")
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR) # dataフォルダがなければ作成
        log_system(f"データディレクトリ '{DATA_DIR}' を作成しました。")

    # --- ファイルパス ---
    PERSONA_FILE = os.path.join(BASE_DIR, "persona.txt")
    EMOTION_ANALYZER_PERSONA_FILE = os.path.join(BASE_DIR, "emotion.txt")
    
    SETTING_FILE = os.path.join(DATA_DIR, "setting.json")
    HISTORY_FILE = os.path.join(DATA_DIR, "history.json")
    UNREAD_MESSAGES_FILE = os.path.join(DATA_DIR, "unread_messages.json")
    EMOTION_FILE = os.path.join(DATA_DIR, "emotion.json")
    SCHEDULE_FILE = os.path.join(DATA_DIR, "schedule.json")
    MEMORY_FILE = os.path.join(DATA_DIR, "memory.json")

# --- 環境変数 (ここのロジックは簡略化) ---
    # setting.jsonを読み込んでトークン名を探すロジックは、
    # main.pyで直接DISCORD_TOKENを読むようにしたため、不要になりました。
    # ただし、他の処理でSETTING_FILEのパスが必要なため、パス設定は残します。
    
    # ★ 以下の動的生成ロジックは削除（またはコメントアウト）
    # token_name = json.load(open(SETTING_FILE)).get("config").get("character_name")
    # TOKEN_ENV_VAR = f"DISCORD_TOKEN_{token_name.UPPER()}"
    
    # ★ TOKEN_ENV_VAR を固定値にする（ai_request_handler等での参照エラーを防ぐため）
    #    ※ main.py で直接 os.getenv("DISCORD_TOKEN") を使うので、この変数は実質不要
    TOKEN_ENV_VAR = "DISCORD_TOKEN" 

    return True