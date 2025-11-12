from utils import data_manager
from datetime import datetime, timezone, timedelta

def get_current_time_str():
    """JSTの現在時刻をフォーマットした文字列で返します。"""
    JST = timezone(timedelta(hours=+9), 'JST')
    now = datetime.now(JST)
    weekday_jp_list = ["月", "火", "水", "木", "金", "土", "日"]
    weekday_jp = weekday_jp_list[now.weekday()]
    return now.strftime(f"%Y年%m月%d日({weekday_jp}) %H時%M分")

def get_bot_status_text(bot) -> str:
    """Botの現在の感情と記憶から、状況説明テキストを生成します。"""
    emotion_cog = bot.get_cog('EmotionCog')
    memory_cog = bot.get_cog('MemoryCog')
    chat_cog = bot.get_cog('ChatManagerCog')

    if not emotion_cog or not memory_cog or not chat_cog:
        return "# 内部状態\n（Cogがロードされていません）"

    # 感情データを取得
    current_emotions = emotion_cog.current_emotions
    emotion_map = emotion_cog.emotion_map
    
    emotion_lines = [f"* {ja_name}: {current_emotions.get(name, 0)}" for name, (_, ja_name) in emotion_map.items()]
    emotions_text = "\n".join(emotion_lines)

    # 記憶データを取得
    memories = memory_cog.memories
    memories_text = ""
    if memories:
        memories_list = "\n".join(f"* {m}" for m in memories)
        memories_text = f"\n\n# 重要な記憶\n{memories_list}"

    # ここ
    # return None
    return f"""
# 現在のあなたの感情
# 0-500の数値で表されます
{emotions_text}
{memories_text}
* 現在時刻:
{get_current_time_str()}
"""

def build_response_prompt(messages: list, bot_status: str) -> str:
    """
    AIに応答を生成させるためのプロンプトを組み立てます。
    未読メッセージの有無で内容を切り替えます。
    """
    if messages:
        # 1. 未読メッセージがある場合
        # ★ アクティビティ情報を含めるようにフォーマットを変更
        conversation_log = "\n".join(
            # f"[{m['author']} @ {m['timestamp']}] (現在の行動: {m.get('activity', '不明')}): {m['content']}"
            f"[{m['author']} @ {m['timestamp']}] : {m['content']}"
            for m in messages
        )
        instruction = "あなたはDiscordを確認したところ、以下の未読メッセージが溜まっていました。\n相手の「現在の行動」も参考にしながら、これら全ての会話の流れを踏まえて、あなたの次のメッセージを生成してください。"
        return f"{instruction}\n\n{conversation_log}\n\n{bot_status}"
    else:
        # 2. 自発的メッセージを生成させたい場合
        instruction = "あなたはDiscordを確認したところ、未読メッセージはありませんでした。\n相手の「現在の行動」も参考にしながら、これら全ての会話の流れを踏まえて、あなたの次のメッセージを生成してください。"
        return f"{instruction}\n\n{conversation_log}\n\n{bot_status}"

def build_emotion_analysis_prompt(emotion_map: dict, persona: str, user_input: str, bot_response: str) -> str:
    """
    対話から感情の変化を分析させるためのプロンプトを組み立てます。
    """
    emotion_list_str = ", ".join([f"'{name}({ja_name})'" for name, (_, ja_name) in emotion_map.items()])
    
    return (
        f"{persona}\n\n"
        f"分析可能な感情リスト:\n{emotion_list_str}\n\n"
        f'分析対象の対話:\n'
        f'[ユーザー]: "{user_input}"\n'
        f'[AIの応答]: "{bot_response}"'
    )