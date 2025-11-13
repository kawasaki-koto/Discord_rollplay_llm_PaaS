import discord
from discord.ext import commands
import os
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from utils import config_manager
from utils.console_display import display_startup_banner, log_system, log_info, log_success, log_error
from utils import db_manager as data_manager

# --- Keep-Alive用 Webサーバーの定義 ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"OK")
    
    # ログ出力を抑制してコンソールを汚さないようにする
    def log_message(self, format, *args):
        pass

def run_http_server():
    port = int(os.getenv("PORT", 8080)) # Renderから渡されるPORTを使用
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    log_system(f"Keep-Alive用Webサーバーをポート {port} で起動しました。")
    server.serve_forever()

def start_keep_alive():
    t = threading.Thread(target=run_http_server)
    t.daemon = True
    t.start()
# ---------------------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.presences = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

config_manager.set_bot_instance(bot)

@bot.event
async def on_ready():
    log_success("SYSTEM", f"キャラクター '{config_manager.CHARACTER_NAME}' が {bot.user} としてログインしました")
    log_system("ユーザーからの接続を待機しています...")

async def load_cogs():
    for filename in os.listdir('./cogs'):
        if filename.endswith('.py') and filename != 'voice.py':
            try:
                await bot.load_extension(f'cogs.{filename[:-3]}')
                log_info("SYSTEM", f"モジュール '{filename}' のロード完了")
            except Exception as e:
                log_error("SYSTEM", f"モジュール '{filename}' のロード中にエラー: {e}")

async def main():
    # ★ Webサーバーをバックグラウンドで起動
    start_keep_alive()

    character_name = os.getenv("CHARACTER_NAME")
    if not character_name:
        log_error("SYSTEM", "環境変数 'CHARACTER_NAME' が設定されていません。")
        return

    if not config_manager.init(character_name):
        return

    if not data_manager.init_db():
        log_error("SYSTEM", "データベースの初期化に失敗しました。起動を中止します。")
        return

    data_manager.load_all_data()

    DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
    if not DISCORD_TOKEN:
        log_error("SYSTEM", "環境変数 'DISCORD_TOKEN' が設定されていません。")
        return

    display_startup_banner()
    log_system(f"[{config_manager.CHARACTER_NAME}] 初期化シークエンスを開始します...")
    
    from utils import ai_request_handler
    ai_request_handler.initialize_histories()

    await load_cogs()
    log_success("SYSTEM", "全モジュールのロード完了")
    
    try:
        await bot.start(DISCORD_TOKEN)
    finally:
        log_system("シャットダウン処理を実行します...")
        data_manager.save_all_data()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_system("プログラムが割り込みにより終了しました。")