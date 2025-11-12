import json
import random
from discord.ext import commands

import utils.config_manager as config
from utils import ai_request_handler, prompt_builder
from utils import db_manager as data_manager
from utils.console_display import log_error, log_info, log_success

class EmotionCog(commands.Cog, name="EmotionCog"):
    def __init__(self, bot):
        self.bot = bot
        log_info("EMOTION", "感情コアの初期化を開始します...")
        
        # ★ 修正: data_managerから直接データを取得
        emotion_data = data_manager.get_data('emotion')
        self.emotion_map = emotion_data.get('emotion_map', {})
        self.default_emotions = emotion_data.get('default_emotions', {})
        self.current_emotions = emotion_data.get('current_emotions', self.default_emotions.copy())
        
        log_success("EMOTION", "感情コアの準備が完了しました。")

    def reload_data(self):
        """data_managerによってリロードされた最新の感情データをCogに反映させる"""
        if data_manager.reload_data('emotion'):
            emotion_data = data_manager.get_data('emotion')
            self.emotion_map = emotion_data.get('emotion_map', {})
            self.default_emotions = emotion_data.get('default_emotions', {})
            self.current_emotions = emotion_data.get('current_emotions', self.default_emotions.copy())
            current_keys = set(self.current_emotions.keys())
            new_valid_keys = set(self.emotion_map.keys())
            for key in current_keys - new_valid_keys:
                del self.current_emotions[key]
            log_success("EMOTION", "Cog内の感情データを正常にリロードしました。")
            return True
        return False

    def get_current_emotions(self) -> dict:
        # ★ 修正: コピーではなく、メモリ上のデータへの参照を直接返す
        return self.current_emotions
    
    def get_emotion_map(self) -> dict:
        return self.emotion_map

    async def update_emotions(self, bot_response: str, user_input: str = ""):
        log_info("EMOTION", "対話の感情分析を開始...")
        
        try:
            with open(config.EMOTION_ANALYZER_PERSONA_FILE, 'r', encoding='utf-8') as f:
                emotion_persona = f.read()
        except FileNotFoundError:
            log_error("EMOTION", f"感情分析ペルソナ '{config.EMOTION_ANALYZER_PERSONA_FILE}' が見つかりません。")
            emotion_persona = "あなたは、ユーザーとAIの対話を分析する心理学者です。"

        # ★ 修正: prompt_builderを使用してプロンプトを生成
        prompt = prompt_builder.build_emotion_analysis_prompt(
            self.emotion_map, emotion_persona, user_input, bot_response
        )
        
        # 会話履歴に影響しないよう channel_id=None でリクエスト
        response_text = await ai_request_handler.send_request(config.MODEL_FLASH, prompt, channel_id=None)
        # response_text = None

        if not response_text:
            log_error("EMOTION", "AIからの応答がありませんでした。")
            return

        try:
            json_text = response_text.strip().replace('```json', '').replace('```', '')
            emotion_deltas = json.loads(json_text)
            
            for emotion, delta in emotion_deltas.items():
                if emotion in self.current_emotions and isinstance(delta, (int, float)):
                    current_value = self.current_emotions[emotion]
                    new_value = current_value + int(delta)
                    self.current_emotions[emotion] = max(0, min(500, new_value))
            
            # ★ DB保存
            # メモリキャッシュ(_data_cache['emotion'])を直接更新した前提
            data_manager.save_data('emotion', data_manager.get_data('emotion'))
            
            log_success("EMOTION", f"メモリ上の感情データを更新し、DBに保存しました: {emotion_deltas}")

        except Exception as e:
            log_error("EMOTION", f"感情更新中に予期せぬエラーが発生: {e}")

    def reset_emotions(self):
        """メモリ上の感情データをデフォルト値にリセットし、DBに保存します。"""
        self.current_emotions.clear()
        self.current_emotions.update(self.default_emotions.copy())
        log_success("EMOTION", "メモリ上の感情データがリセットされました。")
        data_manager.save_data('emotion', data_manager.get_data('emotion')) # ★ DB保存

    def set_emotion_value(self, name: str, value: int):
        """指定された感情の値をメモリ上で設定し、DBに保存します。"""
        if name in self.current_emotions:
            self.current_emotions[name] = value
            log_info("EMOTION", f"メモリ上の感情 '{name}' が {value} に設定されました。")
            data_manager.save_data('emotion', data_manager.get_data('emotion')) # ★ DB保存

    def randomize_emotions(self):
        """メモリ上の全ての感情をランダムな値に設定し、DBに保存します。"""
        for emotion_name in self.current_emotions.keys():
            self.current_emotions[emotion_name] = random.randint(0, 500)
        log_success("EMOTION", "メモリ上の全ての感情がランダムな値に更新されました。")
        data_manager.save_data('emotion', data_manager.get_data('emotion')) # ★ DB保存

async def setup(bot):
    await bot.add_cog(EmotionCog(bot))