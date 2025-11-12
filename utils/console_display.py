import colorama
from datetime import datetime

# coloramaを初期化します
colorama.init(autoreset=True)
Fore = colorama.Fore
Style = colorama.Style

BANNER = f"""
{Fore.LIGHTCYAN_EX}
    +----------------------+
    |     Project East     |
    |   - System Boot -    |
    +----------------------+
{Style.RESET_ALL}
"""

def display_startup_banner():
    """起動時に表示するバナーです。"""
    print(BANNER)

def log_system(message):
    """システム全体の重要なメッセージを表示します。"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"{Fore.LIGHTYELLOW_EX}✙ [SYSTEM @ {timestamp}] {Style.RESET_ALL}{message}")

def log_info(cog_name, message):
    """各部品（Cog）からの通常のお知らせです。"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"{Fore.CYAN}  > [{cog_name} @ {timestamp}] {Style.RESET_ALL}{message}")

def log_success(cog_name, message):
    """成功メッセージです。"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"{Fore.GREEN}  ✓ [{cog_name} @ {timestamp}] {Style.RESET_ALL}{message}")

def log_error(cog_name, message):
    """エラーメッセージです。"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"{Fore.RED}  ! [{cog_name} @ {timestamp}] {Style.RESET_ALL}{message}")

def log_warning(cog_name, message):
    """警告メッセージです。"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"{Fore.YELLOW}  ⚠️ [{cog_name} @ {timestamp}] {Style.RESET_ALL}{message}")