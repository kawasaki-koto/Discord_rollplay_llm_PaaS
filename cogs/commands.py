import discord
from discord.ext import commands
import json
import io
from datetime import datetime

from utils import ai_request_handler
import utils.db_manager as data_manager # ★ db_manager に変更
from utils.console_display import log_info, log_success, log_error
import utils.config_manager as config

class CommandCog(commands.Cog, name="CommandCog"):
    def __init__(self, bot):
        self.bot = bot
        self.settings = data_manager.get_data('setting') # db_managerから取得
        self.channel_settings = self.settings.get('channel_settings', {})
        log_info("COMMAND", "コマンド管理モジュールを初期化します。")

    # ■■■ System Commands ■■■
    @commands.command(name="help", aliases=["h"])
    async def help_command(self, ctx):
        """コマンド一覧を表示します。"""
        embed = discord.Embed(title="EAST ヘルプメニュー", color=0xffa500)
        p = self.bot.command_prefix
        embed.description = f"コマンドのエイリアス(省略形)も利用可能です。\n例: `{p}st`, `{p}mem ls`"
        
        embed.add_field(name=f"**{p}help (h)**", value="このヘルプを表示", inline=False)
        embed.add_field(name=f"**{p}status (st)**", value="Botの現在の感情などを表示", inline=False)
        embed.add_field(name=f"**{p}save (s)**", value="現在の全データをファイルに保存", inline=False)
        embed.add_field(name=f"**{p}history (hist)**", value=f"`{p}hist <reload|reset|export>`\n会話履歴を操作", inline=False)
        embed.add_field(name=f"**{p}persona (ps)**", value=f"`{p}ps <reload|apply>`\nキャラクター設定を操作", inline=False)
        embed.add_field(name=f"**{p}emotion (emo)**", value=f"`{p}emo <set|reset|random|reload>`\n感情値を操作", inline=False)
        embed.add_field(name=f"**{p}memory (mem)**", value=f"`{p}mem <add|list|del|reset>`\n記憶を操作", inline=False)
        embed.add_field(name=f"**{p}unread (ur)**", value=f"`{p}ur <pop|reset|reload>`\n未読メッセージを操作", inline=False)
        embed.add_field(name=f"**{p}chat <on|off>**", value="常時会話モードのON/OFF", inline=False)
        embed.add_field(name=f"**{p}key <1|2|3>**", value="使用するAPIキーを変更", inline=False)
        embed.add_field(name=f"**{p}check (c) <キャラ名>**", value="指定キャラの応答処理を即時実行", inline=False)
        
        await ctx.send(embed=embed)

    @commands.command(name="status", aliases=["st"])
    async def status_command(self, ctx):
        """Botの現在の感情などを表示します。"""
        emotion_cog = self.bot.get_cog('EmotionCog')
        if not emotion_cog: return await ctx.send("> SYSTEM: EmotionCogが読み込まれていません。")

        embed = discord.Embed(title="総合状態モニター", description="現在の私の『心』の中です。", color=0xffa500)
        # Cogへの参照を取得
        chat_cog = self.bot.get_cog('ChatManagerCog')

        # 1. APIキーと活動レベルを表示
        embed.add_field(name="🧠 使用中APIキー", value=f"#{ai_request_handler.get_active_key_number()}", inline=True)
        if chat_cog:
            embed.add_field(name="🕒 現在の行動", value=f"{chat_cog.current_action}", inline=True)

        embed.add_field(name="--- 感情パラメータ ---", value="", inline=False)
        for name, (emoji, ja_name) in emotion_cog.emotion_map.items():
            value = emotion_cog.current_emotions.get(name, 0)
            embed.add_field(name=f"{emoji} {ja_name}", value=f"**{value}** / 500", inline=True)
        
        await ctx.send(embed=embed)

    @commands.command(name="save", aliases=["s"])
    async def save_data(self, ctx):
        """現在の全てのデータをDBに保存します。"""
        try:
            data_manager.save_all_data() # db_manager の save_all_data を呼ぶ
            log_success("COMMAND", "全データのDB保存に成功しました。")
            await ctx.send("> SYSTEM: 全てのデータをデータベースに保存しました。")
        except Exception as e:
            log_error("COMMAND", f"データ保存中にエラーが発生: {e}")
            await ctx.send(f"> SYSTEM: データ保存中にエラーが発生しました。\n`{e}`")

    # ■■■ History Commands ■■■
    @commands.group(name="history", aliases=["hist"], invoke_without_command=True)
    async def history_group(self, ctx):
        # ★ 修正: usageメッセージを更新
        await ctx.send(f"> USAGE: `{self.bot.command_prefix}history <reload|reset|export>`")

    @history_group.command(name="reload", aliases=["rl"])
    async def history_reload(self, ctx):
        if data_manager.reload_data('history'):
            await ctx.send("> SYSTEM: 履歴ファイルを再読み込みしました。")
        else:
            await ctx.send("> SYSTEM: 履歴の再読み込みに失敗しました。")

    @history_group.command(name="reset", aliases=["rs"])
    async def history_reset(self, ctx):
        """全ての会話履歴をリセットします。"""
        try:
            ai_request_handler.reset_histories()
            await ctx.send("> SYSTEM: 全ての会話履歴をリセットしました。")
        except Exception as e:
            await ctx.send(f"> SYSTEM: 履歴のリセット中にエラーが発生しました。\n`{e}`")
    
    @history_group.command(name="export", aliases=["ex"])
    async def history_export(self, ctx):
        history = ai_request_handler.get_history_for_channel(ctx.channel.id)
        if not history: return await ctx.send("> SYSTEM: このチャンネルには会話履歴がありません。")
        
        json_string = json.dumps(history, indent=2, ensure_ascii=False)
        json_buffer = io.StringIO(json_string)
        filename = f"history_{ctx.channel.name}_{datetime.now().strftime('%Y%m%d')}.json"
        await ctx.send(file=discord.File(json_buffer, filename=filename))

    # ■■■ Persona Commands ■■■
    @commands.group(name="persona", aliases=["ps"], invoke_without_command=True)
    async def persona_group(self, ctx):
        await ctx.send(f"> USAGE: `{self.bot.command_prefix}persona <reload|apply>`")

    @persona_group.command(name="reload", aliases=["rl"])

    async def persona_reload(self, ctx):
        if ai_request_handler.load_persona():
            await ctx.send("> SYSTEM: キャラクターシートを再読み込みしました。")
        else:
            await ctx.send("> SYSTEM: エラー: キャラクターシートの読み込みに失敗しました。")

    @persona_group.command(name="apply", aliases=["ap"])
    async def persona_apply(self, ctx):
        ai_request_handler.apply_persona_to_channel(ctx.channel.id)
        await ctx.send(f"> SYSTEM: チャンネル `{ctx.channel.name}` の履歴にペルソナを適用しました。")

    # ■■■ Emotion Commands ■■■
    @commands.group(name="emotion", aliases=["emo"], invoke_without_command=True)
    async def emotion_group(self, ctx):
        await ctx.send(f"> USAGE: `{self.bot.command_prefix}emotion <set|reset|random|reload>`")

    @emotion_group.command(name="reload", aliases=["rl"])
    async def emotion_reload(self, ctx):
        """emotion.jsonを再読み込みし、Botの感情定義を更新します。"""
        emo_cog = self.bot.get_cog("EmotionCog")
        if emo_cog and emo_cog.reload_data():
            await ctx.send("> SYSTEM: 感情ファイルを再読み込みし、設定を更新しました。")
        else:
            await ctx.send("> SYSTEM: 感情ファイルのリロードに失敗しました。")

    @emotion_group.command(name="set", aliases=["s"])
    async def emotion_set(self, ctx, emotion_name: str, value: int):
        emo_cog = self.bot.get_cog("EmotionCog")
        if not emo_cog: return await ctx.send("> SYSTEM: EmotionCogが見つかりません。")
        if emotion_name.lower() not in emo_cog.emotion_map:
            return await ctx.send(f"> SYSTEM: `{emotion_name}`という感情はありません。")
        if not 0 <= value <= 500:
            return await ctx.send("> SYSTEM: 数値は0から500の間で指定してください。")

        emo_cog.set_emotion_value(emotion_name.lower(), value)
        await ctx.send(f"> SYSTEM: 感情`{emotion_name}`の値を **{value}** に設定しました。")

    @emotion_group.command(name="reset", aliases=["rs"])
    async def emotion_reset(self, ctx):
        emo_cog = self.bot.get_cog("EmotionCog")
        if emo_cog:
            emo_cog.reset_emotions()
            await ctx.send("> SYSTEM: 感情データをリセットしました。")

    @emotion_group.command(name="random", aliases=["rn"])
    async def emotion_random(self, ctx):
        emo_cog = self.bot.get_cog("EmotionCog")
        if emo_cog:
            emo_cog.randomize_emotions()
            await ctx.send("> SYSTEM: 全ての感情をランダムな値に設定しました。")

    # ■■■ Memory Commands ■■■
    @commands.group(name="memory", aliases=["mem"], invoke_without_command=True)
    async def memory_group(self, ctx):
        await ctx.send(f"> USAGE: `{self.bot.command_prefix}memory <add|list|del|reset>`")

    @memory_group.command(name="add", aliases=["a"])
    async def memory_add(self, ctx, *, memory_text: str):
        mem_cog = self.bot.get_cog("MemoryCog")
        if mem_cog:
            mem_cog.add_memory(memory_text)
            await ctx.send(f"> SYSTEM: 新しい記憶を追加しました。\n`{memory_text}`")

    @memory_group.command(name="list", aliases=["ls"])
    async def memory_list(self, ctx):
        mem_cog = self.bot.get_cog("MemoryCog")
        if not mem_cog or not mem_cog.memories:
            return await ctx.send("> SYSTEM: 記憶はまだありません。")
        
        embed = discord.Embed(title="記憶リスト", color=0xffa500)
        description = "\n".join(f"**{i+1}.** {mem}\n" for i, mem in enumerate(mem_cog.memories))
        embed.description = description
        await ctx.send(embed=embed)

    @memory_group.command(name="delete", aliases=["del"])
    async def memory_delete(self, ctx, index: int):
        mem_cog = self.bot.get_cog("MemoryCog")
        if not mem_cog: return await ctx.send("> SYSTEM: MemoryCogが見つかりません。")
        
        deleted = mem_cog.delete_memory(index - 1)
        if deleted:
            await ctx.send(f"> SYSTEM: 記憶 `{index}`番 を削除しました。\n`{deleted}`")
        else:
            await ctx.send(f"> SYSTEM: その番号の記憶は見つかりませんでした。")

    @memory_group.command(name="reset", aliases=["rs"])
    async def memory_reset(self, ctx):
        mem_cog = self.bot.get_cog("MemoryCog")
        if mem_cog:
            mem_cog.reset_memories()
            await ctx.send("> SYSTEM: 全ての記憶をリセットしました。")

    # ■■■ Unread Commands ■■■
    @commands.group(name="unread", aliases=["ur"], invoke_without_command=True)
    async def unread_group(self, ctx):
        await ctx.send(f"> USAGE: `{self.bot.command_prefix}unread <pop|reset|reload>`")

    @unread_group.command(name="pop")
    async def unread_pop(self, ctx):
        """現在のチャンネルの未読メッセージを一件(最も古いもの)削除します。"""
        chat_cog = self.bot.get_cog("ChatManagerCog")
        if not chat_cog:
            return await ctx.send("> SYSTEM: ChatManagerCogが見つかりません。")
        
        popped = chat_cog.pop_unread_message(ctx.channel.id)
        if popped:
            await ctx.send(f"> SYSTEM: 以下の未読メッセージを削除しました:\n`{popped['author']}: {popped['content']}`")
        else:
            await ctx.send("> SYSTEM: このチャンネルに未読メッセージはありません。")

    @unread_group.command(name="reset", aliases=["rs"])
    async def unread_reset(self, ctx):
        """全ての未読メッセージをリセット(全削除)します。"""
        chat_cog = self.bot.get_cog("ChatManagerCog")
        if chat_cog:
            chat_cog.reset_unread_messages()
            await ctx.send("> SYSTEM: 全ての未読メッセージをリセットしました。")

    @unread_group.command(name="reload", aliases=["rl"])
    async def unread_reload(self, ctx):
        """unread_messages.jsonを再読み込みします。"""
        if data_manager.reload_data('unread'):
            # ChatCog内部のデータ参照を、再読み込みされた新しいデータに更新する
            chat_cog = self.bot.get_cog("ChatManagerCog")
            if chat_cog:
                chat_cog.unread_data = data_manager.get_data('unread')
            await ctx.send("> SYSTEM: 未読メッセージファイルを再読み込みしました。")
        else:
            await ctx.send("> SYSTEM: 未読メッセージファイルのリロードに失敗しました。")

    # ■■■ Chat & Key Commands ■■■
    @commands.group(name="chat", invoke_without_command=True)
    async def chat_group(self, ctx):
        mode = "ON" if self.channel_settings.get(str(ctx.channel.id), {}).get('chat_mode', False) else "OFF"
        await ctx.send(f"> SYSTEM: 現在のチャットモードは **{mode}** です。")

    @chat_group.command(name="on")
    async def chat_on(self, ctx):
        self.channel_settings.setdefault(str(ctx.channel.id), {})['chat_mode'] = True
        await ctx.send("> SYSTEM: チャットモードを **ON** にしました。")
        data_manager.save_data('setting', self.settings) # ★ DB保存

    @chat_group.command(name="off")
    async def chat_off(self, ctx):
        self.channel_settings.setdefault(str(ctx.channel.id), {})['chat_mode'] = False
        await ctx.send("> SYSTEM: チャットモードを **OFF** にしました。")
        data_manager.save_data('setting', self.settings) # ★ DB保存

    @commands.command(name="key", aliases=["k"])
    async def set_key(self, ctx, key_number: int):
        num_keys = len(ai_request_handler.API_KEYS)
        if 1 <= key_number <= num_keys:
            ai_request_handler.set_active_key_number(key_number)
            await ctx.send(f"> SYSTEM: APIキーを **{key_number}番** に切り替えました。")
        else:
            await ctx.send(f"> SYSTEM: キー番号は1から{num_keys}の間で指定してください。")

    @commands.command(name="check", aliases=["c"])
    async def check_messages(self, ctx, character_name: str):
        """
        指定されたキャラクター名のBotに、このチャンネルの未読チェックを即座に実行させます。
        """
        # コマンドで指定された名前が、このBotインスタンスの名前と一致しない場合は無視
        if character_name.lower() != config.CHARACTER_NAME.lower():
            return

        # チャンネルIDのチェックは不要（どのチャンネルからでも呼び出せるように）
        chat_cog = self.bot.get_cog('ChatManagerCog')
        if chat_cog and hasattr(chat_cog, 'activity_loop'):
            # await ctx.send(f"> SYSTEM: `{config.CHARACTER_NAME}`がチャンネル `{ctx.channel.name}` の未読メッセージをチェックします...")
            await chat_cog.force_check_channel(ctx.channel.id)
            log_success("COMMAND", f"CH[{ctx.channel.name}] の強制チェックを完了しました。")
        else:
            await ctx.send("> SYSTEM: エラー: チャット管理モジュールが見つかりませんでした。")

async def setup(bot):
    await bot.add_cog(CommandCog(bot))