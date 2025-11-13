import discord
from discord.ext import commands, tasks
from datetime import datetime
import random
import asyncio

import utils.config_manager as config
from utils.console_display import log_info, log_system, log_success, log_error, log_warning
from utils import db_manager as data_manager
from utils import ai_request_handler, prompt_builder

async def send_splittable_message(channel: discord.TextChannel, text: str, file: discord.File = None):
    """
    Discordの文字数制限(2000字)を超えた場合、メッセージを分割して送信する。
    """
    if not text:
        return
    if len(text) <= 2000:
        await channel.send(text, file=file)
        return
    log_info("MESSAGE", f"長文メッセージ({len(text)}文字)を分割して送信します。")
    remaining_text = text
    while len(remaining_text) > 2000:
        split_point = remaining_text.rfind('\n', 0, 2000)
        if split_point == -1:
            split_point = 2000
        await channel.send(remaining_text[:split_point])
        remaining_text = remaining_text[split_point:].lstrip()
        await asyncio.sleep(0.5)
    if remaining_text:
        await channel.send(remaining_text, file=file)

class ChatManagerCog(commands.Cog, name="ChatManagerCog"):
    def __init__(self, bot):
        self.bot = bot
        self.processing_channels = set() # 処理中チャンネルを管理するセット

        self.unread_data = data_manager.get_data('unread')
        schedule_data = data_manager.get_data('schedule')
        self.weekday_schedule = schedule_data.get("weekday", {})
        self.weekend_schedule = schedule_data.get("weekend", {})
        self.activity_params = schedule_data.get("activity_params", {})

        settings_data = data_manager.get_data('setting')
        self.channel_settings = settings_data.get('channel_settings', {})

        self.current_action = "待機中"
        self.current_activity_level = 'normal'

        log_system("チャット管理モジュールを初期化し、活動サイクルを開始します。")
        self.activity_loop.start()

    def reset_unread_messages(self):
        """メモリ上の全ての未読メッセージをクリアし、DBに保存します。"""
        self.unread_data.clear()
        log_success("UNREAD", "メモリ上の全未読メッセージがリセットされました。")
        data_manager.save_data('unread', self.unread_data)

    def pop_unread_message(self, channel_id: int) -> dict | None:
        """指定されたチャンネルの最も古い未読メッセージを1件削除し、DBに保存します。"""
        str_channel_id = str(channel_id)
        if self.unread_data.get(str_channel_id):
            popped_message = self.unread_data[str_channel_id].pop(0)
            log_info("UNREAD", f"CH[{channel_id}] の未読メッセージを1件popしました。")
            data_manager.save_data('unread', self.unread_data)
            return popped_message
        return None

    @commands.Cog.listener()
    async def on_message(self, message):
        """メッセージを受信したら未読リストに追加する"""
        # 自分自身のメッセージやコマンドは無視
        if message.author == self.bot.user or message.content.startswith(self.bot.command_prefix):
            return

        channel_id_str = str(message.channel.id)
        channel_setting = self.channel_settings.get(channel_id_str, {})
        
        # チャットモードがONでない場合は無視
        if not channel_setting.get('chat_mode', False):
            return

        if channel_id_str not in self.unread_data:
            self.unread_data[channel_id_str] = []

        # 送信者のアクティビティを取得
        activity_str = self._get_user_activity_str(message.author)

        self.unread_data[channel_id_str].append({
            'author': message.author.display_name, 
            'content': message.content,
            'timestamp': prompt_builder.get_current_time_str(),
            'activity': activity_str
        })
        log_info("UNREAD", f"[{message.channel.name}] に未読メッセージを1件追加。(Activity: {activity_str})")
        data_manager.save_data('unread', self.unread_data)

    @tasks.loop(seconds=1.0)
    async def activity_loop(self):
        """一定時間待機し、ランダムなチャンネルのメッセージ処理または自発的発言を行うループ"""
        now = datetime.now()
        current_hour = str(now.hour)
        weekday = now.weekday()
        active_schedule = self.weekend_schedule if weekday >= 5 else self.weekday_schedule
        current_schedule = active_schedule.get(current_hour, {"level": "normal", "action": "🕒 不明"})
        self.current_activity_level = current_schedule['level']
        self.current_action = current_schedule['action']
        params = self.activity_params.get(self.current_activity_level, {'seconds': 3600, 'sigma': 900})
        wait_duration = max(60.0, random.normalvariate(params['seconds'], params['sigma']))

        log_info("ACTIVITY", f"現在の行動: {self.current_action} | 次の活動まで {wait_duration/60:.2f} 分待機します。")
        await asyncio.sleep(wait_duration)

        # 処理対象チャンネルの選択
        default_channel_id = config.get_default_channel_id()
        channels_with_unread = [int(ch_id) for ch_id, msgs in self.unread_data.items() if msgs]
        candidate_channel_ids = channels_with_unread or ([default_channel_id] if default_channel_id else [])

        if not candidate_channel_ids:
            log_info("ACTIVITY", "処理対象のチャンネルが見つかりませんでした。")
            return

        target_channel_id = random.choice(candidate_channel_ids)
        await self.process_channel_activity(target_channel_id)

    async def process_channel_activity(self, channel_id: int):
        """チャンネルの活動（未読処理 or 自発発言）を行う共通関数"""
        str_channel_id = str(channel_id)
        
        if str_channel_id in self.processing_channels:
            log_warning("PROCESS_SKIP", f"CH[{channel_id}] は既に処理中のためスキップします。")
            return

        target_channel = self.bot.get_channel(channel_id)
        if not target_channel:
            log_error("PROCESS", f"CH[{channel_id}] が見つかりません。")
            return

        self.processing_channels.add(str_channel_id)
        log_info("PROCESS_START", f"CH[{channel_id}] の処理を開始します。")

        try:
            messages_to_process = self.unread_data.get(str_channel_id, [])

            # プロンプト組み立て
            bot_status = prompt_builder.get_bot_status_text(self.bot)
            prompt_instruction = prompt_builder.build_response_prompt(messages_to_process, bot_status)

            # AIに応答を要求
            async with target_channel.typing():
                response_text = await ai_request_handler.send_request(
                    config.MODEL_PRO,
                    prompt_instruction,
                    channel_id=channel_id
                )

            if response_text is None:
                return

            # 応答送信
            await send_splittable_message(target_channel, response_text, file=None)
            log_success("PROCESS", f"CH[{target_channel.name}] に応答しました。")
            
            # 感情更新
            emotion_cog = self.bot.get_cog('EmotionCog')
            if emotion_cog:
                user_input = "\n".join(f"[{m['author']}]: {m['content']}" for m in messages_to_process) if messages_to_process else ""
                try:
                    await emotion_cog.update_emotions(response_text, user_input)
                except Exception as e:
                    log_error("EMOTION", f"感情更新中にエラーが発生しました: {e}")

            # 処理済み未読メッセージをクリア
            if messages_to_process:
                self.unread_data[str_channel_id] = []
                log_info("UNREAD", f"CH[{channel_id}] の未読メッセージをクリアしました。")
                data_manager.save_data('unread', self.unread_data)

        except Exception as e:
             log_error("PROCESS_ERROR", f"CH[{channel_id}] の処理中に予期せぬエラーが発生しました: {type(e).__name__} - {e}")

        finally:
            if str_channel_id in self.processing_channels:
                self.processing_channels.remove(str_channel_id)
                log_info("PROCESS_END", f"CH[{channel_id}] の処理を終了します。")
            else:
                 log_warning("PROCESS_END", f"CH[{channel_id}] が処理中セットにありませんでした（終了処理）。")

    async def force_check_channel(self, channel_id: int):
        """
        ループの待機を無視して、指定されたチャンネルの活動を即座に処理する
        """
        str_channel_id = str(channel_id)
        if str_channel_id in self.processing_channels:
            log_warning("FORCE_CHECK_SKIP", f"コマンドによる CH[{channel_id}] の強制チェックは、既に処理中のためスキップします。")
            return

        log_system(f"コマンドにより CH[{channel_id}] の強制チェックを実行します。")
        await self.process_channel_activity(channel_id)

    def _get_user_activity_str(self, member: discord.Member) -> str:
        # 型チェックを追加（Memberでない場合はアクティビティを取得しない）
        if not isinstance(member, discord.Member):
            return "特になし"

        # デバッグ用ログ出力
        # print(f"[DEBUG] User: {member.display_name}, Activities: {member.activities}")

        if not member.activities:
            return "特になし"

        activity_texts = []
        for activity in member.activities:
            if isinstance(activity, discord.Spotify):
                activity_texts.append(f"Spotifyで音楽を聴いている (曲: {activity.title}, アーティスト: {activity.artist})")
            elif isinstance(activity, discord.Game):
                activity_texts.append(f"ゲームをプレイ中 (タイトル: {activity.name})")
            elif isinstance(activity, discord.Streaming):
                activity_texts.append(f"配信中 (タイトル: {activity.name}, ゲーム: {activity.game})")
            elif isinstance(activity, discord.CustomActivity):
                 if activity.name:
                    activity_texts.append(f"カスタムステータス: {activity.name}")
            else:
                activity_texts.append(f"アクティビティ中: {activity.name}")

        return "、".join(activity_texts) if activity_texts else "特になし"

    @activity_loop.before_loop
    async def before_activity_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(ChatManagerCog(bot))