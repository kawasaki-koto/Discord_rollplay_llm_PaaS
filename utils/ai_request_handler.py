# ai_request_handler.py

import google.api_core.exceptions
import google.generativeai as genai
import utils.config_manager as config
import utils.db_manager as data_manager
from utils.console_display import log_system, log_error, log_info, log_warning, log_success
from datetime import datetime
import json
import logging
import traceback
import os
import asyncio
import re

# APIキーの環境変数名のリスト
API_KEY_ENV_VARS = [
    "GEMINI_API_KEY",
    "GEMINI_API_KEY_1",
    "GEMINI_API_KEY_2",
    "GEMINI_API_KEY_3",
]

# 現在使用中のAPIキーのインデックス
current_api_key_index = 0

def initialize_histories():
    """
    履歴キャッシュの初期化。db_managerの互換関数を呼び出す。
    """
    # ★ db_manager の関数を呼び出す
    data_manager.initialize_histories()

def _load_persona() -> str | None:
    """ペルソナファイルを読み込む"""
    try:
        if not hasattr(config, 'PERSONA_FILE'):
             log_error("CONFIG_ERROR", "config_managerにPERSONA_FILEが定義されていません。")
             return None
        persona_path = config.PERSONA_FILE
        if os.path.exists(persona_path):
            with open(persona_path, 'r', encoding='utf-8') as f:
                log_info("PERSONA_LOAD", f"{persona_path} からペルソナを読み込みます。")
                return f.read()
        else:
            log_error("PERSONA_LOAD", f"ペルソナファイルが見つかりません: {persona_path}")
            return None
    except Exception as e:
        log_error("PERSONA_LOAD", f"ペルソナファイルの読み込み中にエラー: {e}")
        return None

def get_channel_history(channel_id: int) -> list | None:
    """
    指定されたチャンネルIDの履歴を db_manager のキャッシュから取得または初期化。
    初期化した場合はDBにも保存する。
    """
    history_cache = data_manager.get_data('history') # ★ db_manager のキャッシュを取得
    if history_cache is None:
        log_error("HISTORY", "db_managerの履歴キャッシュ(_data_cache['history'])が見つかりません。")
        return None

    str_channel_id = str(channel_id)

    # チャンネル履歴が存在しない、または空の場合に初期化
    if str_channel_id not in history_cache or not history_cache[str_channel_id]:
        log_action = "初期化" if str_channel_id not in history_cache else "再初期化"
        log_info("HISTORY", f"CH[{channel_id}] の履歴が見つからないか空のため、ペルソナファイルから{log_action}します。")
        persona_content = _load_persona()
        if persona_content:
            initial_history = [{"role": "user", "parts": [persona_content]}]
            history_cache[str_channel_id] = initial_history
            log_success("HISTORY", f"CH[{channel_id}] の履歴をペルソナで正常に{log_action}しました。")
            
            # ★★★ DBに保存 ★★★
            data_manager.save_data('history', history_cache)
            log_info("HISTORY", f"CH[{channel_id}] の初期化履歴をDBに保存しました。")
            
        else:
            log_error("HISTORY", f"CH[{channel_id}] の履歴{log_action}に失敗しました。ペルソナが読み込めません。")
            history_cache[str_channel_id] = [] # 空のリストで初期化
            
            # ★★★ 空リストでもDBに保存 ★★★
            data_manager.save_data('history', history_cache)

    return history_cache.get(str_channel_id)

def add_message_to_history(channel_id: int, role: str, message: str):
    """履歴にメッセージを追加 (db_manager のキャッシュを更新し、DBに保存)"""
    history = get_channel_history(channel_id)
    if history is None:
         log_error("HISTORY_ADD", f"CH[{channel_id}] の履歴リスト取得に失敗したため、メッセージを追加できません。")
         return

    # 履歴制限チェック (変更なし)
    try:
        max_history_length = config.get_max_history_length()
        # (中略：古い履歴ペアを削除するロジック)
    except Exception as e:
        log_error("HISTORY", f"履歴削除中にエラー: {e}")

    # メモリキャッシュ（_data_cache['history'][str_channel_id]）に append
    history.append({"role": role, "parts": [message]})
    log_info("HISTORY", f"CH[{channel_id}] の履歴に {role} のメッセージを追加しました。 (現在の履歴数: {len(history)})")

    # ★★★ DBに保存 ★★★
    # history 変数はキャッシュ内のリストへの参照なので、キャッシュ全体を保存する
    data_manager.save_data('history', data_manager.get_data('history'))


async def send_request(model_name: str, prompt: str, channel_id: int = None):
    """AIモデルにリクエストを送信し、応答を取得 (APIキー再試行・レート制限対応付き)"""
    global current_api_key_index
    log_info("AI_REQUEST", f"モデル '{model_name}' へのリクエスト処理を開始します...")
    log_info("AI_REQUEST_DEBUG", f"使用モデル名: {model_name}")

    # --- ユーザーメッセージの履歴追加準備 ---
    user_message_content = None
    if channel_id is not None:
        try:
            if config.bot is None:
                log_error("AI_REQUEST_CONFIG", "config.botがNoneです。Cogにアクセスできません。")
            else:
                chat_cog = config.bot.get_cog('ChatManagerCog')
                if chat_cog:
                    str_channel_id = str(channel_id)
                    unread_messages = chat_cog.unread_data.get(str_channel_id, [])
                    if unread_messages:
                         user_messages_for_history = [
                             f"[{m.get('author','Unknown')} @ {m.get('timestamp','')}]: {m.get('content','')}"
                             for m in unread_messages
                         ]
                         user_message_content = "\n".join(user_messages_for_history)
                else:
                    log_warning("AI_REQUEST_COG", "ChatManagerCogが見つかりません。")
        except Exception as e:
            log_error("AI_REQUEST_HISTORY_PREP", f"履歴準備中にエラー: {e}")
            user_message_content = None
    # ------------------------------------

    # --- 履歴取得（ここで初期化も行われる） ---
    # ★ get_channel_history は data_manager._data_cache['history'] 内のリストへの参照を返す
    history_list_ref = get_channel_history(channel_id) if channel_id is not None else []
    if history_list_ref is None and channel_id is not None:
         log_error("AI_REQUEST", f"CH[{channel_id}] の履歴取得/初期化に失敗したため、リクエストを中止します。")
         return None # 履歴がなければリクエストできない
    # ------------------------------------

    # --- APIキーリスト作成 ---
    api_keys_to_try = []
    for env_var in API_KEY_ENV_VARS:
        key = os.getenv(env_var)
        if key:
            api_keys_to_try.append(key)
    log_info("AI_REQUEST_DEBUG", f"読み込んだAPIキーの数: {len(api_keys_to_try)}")
    if not api_keys_to_try:
        log_error("AI_REQUEST_ERROR", "利用可能なGemini APIキーが環境変数に見つかりません。")
        return None
    # ------------------------------------

    # --- 再試行ループ ---
    last_exception = None
    successful_key = None
    response = None
    max_retries_per_key = 1

    start_index = current_api_key_index if 0 <= current_api_key_index < len(api_keys_to_try) else 0
    ordered_keys = api_keys_to_try[start_index:] + api_keys_to_try[:start_index]

    key_index_to_try = 0
    while key_index_to_try < len(ordered_keys):
        api_key = ordered_keys[key_index_to_try]
        current_index_in_original_list = -1
        try:
            current_index_in_original_list = api_keys_to_try.index(api_key)
        except ValueError:
             log_error("AI_REQUEST_INTERNAL", f"キーインデックスの取得に失敗: {api_key[:5]}...")
             key_index_to_try += 1
             continue

        log_info("AI_REQUEST", f"APIキー {current_index_in_original_list + 1}/{len(api_keys_to_try)} (Index: {current_index_in_original_list}) を使用して試行します...")

        retries_with_current_key = 0
        should_wait_before_next_key = False
        wait_duration = 0

        while retries_with_current_key <= max_retries_per_key:
            try:
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(model_name)

                # ★★★ start_chat に渡す履歴リストの参照を使用 ★★★
                if not history_list_ref or history_list_ref[0].get("role") != "user":
                     log_warning("AI_REQUEST_HISTORY_WARN", f"CH[{channel_id}] の履歴が空か、最初の要素が'user'ではありません。API呼び出しに失敗する可能性があります。History: {history_list_ref}")
                     # 空リストで試行
                     chat = model.start_chat(history=[])
                else:
                     chat = model.start_chat(history=history_list_ref) # ★ ここで参照を渡す

                log_info("AI_REQUEST", f"モデル '{model_name}' にリクエストを送信します...")
                try:
                    api_timeout = config.get_api_timeout()
                except AttributeError:
                    log_warning("AI_REQUEST_CONFIG", "configにget_api_timeoutが見つかりません。デフォルトの120秒を使用します。")
                    api_timeout = 120

                response = await asyncio.wait_for(
                    chat.send_message_async(prompt), # 安全性設定なし
                    timeout=api_timeout
                )
                log_info("AI_REQUEST_DEBUG", "chat.send_message_async の呼び出しが完了しました。")

                if not hasattr(response, 'text'):
                     # (応答オブジェクトのチェック処理)
                     feedback = getattr(response, 'prompt_feedback', None)
                     candidates = getattr(response, 'candidates', [])
                     log_error("AI_RESPONSE", "モデルからの応答に text 属性が含まれていません。")
                     if feedback: log_error("AI_RESPONSE_DEBUG", f"Prompt Feedback: {feedback}")
                     if candidates: log_error("AI_RESPONSE_DEBUG", f"Candidates: {candidates}")
                     else: log_error("AI_RESPONSE_DEBUG", f"受信したresponseオブジェクト: {response}")
                     last_exception = Exception(f"Invalid response object received. Feedback: {feedback}, Candidates: {candidates}")
                     retries_with_current_key = max_retries_per_key + 1
                     continue

                # 成功！
                successful_key = api_key
                current_api_key_index = current_index_in_original_list
                log_success("AI_RESPONSE", f"APIキー {current_index_in_original_list + 1} で応答を受信しました。")
                break # 内側ループ脱出

            except google.api_core.exceptions.ResourceExhausted as e:
                # (レート制限エラーの処理)
                log_warning("AI_REQUEST_RATE_LIMIT", f"レート制限エラー発生 (APIキー {current_index_in_original_list + 1}): {e}")
                last_exception = e
                retries_with_current_key += 1
                retry_delay_seconds = 60
                try:
                    match = re.search(r"Please retry in (\d+\.?\d*)s", str(e))
                    if match: retry_delay_seconds = float(match.group(1)) + 1.5
                except Exception as parse_error:
                    log_warning("AI_REQUEST_RATE_LIMIT", f"待機時間の抽出に失敗: {parse_error}。デフォルトの{retry_delay_seconds}秒を使用します。")
                wait_duration = retry_delay_seconds
                should_wait_before_next_key = True
                if retries_with_current_key <= max_retries_per_key:
                    log_info("AI_REQUEST_RATE_LIMIT", f"{retry_delay_seconds:.1f}秒待機してから同じAPIキーで再試行します (試行 {retries_with_current_key}/{max_retries_per_key})...")
                    await asyncio.sleep(retry_delay_seconds)
                    continue
                else:
                    log_warning("AI_REQUEST_RATE_LIMIT", f"APIキー {current_index_in_original_list + 1} での再試行上限に達しました。")
                    break

            except genai.types.StopCandidateException as e:
                 # (安全性ブロックエラーの処理 - 次のキーへ)
                 log_error("AI_REQUEST_SAFETY", f"コンテンツが安全性によりブロックされました (APIキー {current_index_in_original_list + 1}) - 安全設定削除後も発生?: {e}")
                 try:
                     if response and hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                          log_error("AI_REQUEST_SAFETY", f"Prompt Feedback: {response.prompt_feedback}")
                 except Exception as feedback_error:
                      log_error("AI_REQUEST_SAFETY", f"Feedback取得中にエラー: {feedback_error}")
                 last_exception = e
                 retries_with_current_key = max_retries_per_key + 1
                 break

            except asyncio.TimeoutError:
                # (タイムアウトエラーの処理 - 次のキーへ)
                log_error("AI_REQUEST_ERROR", f"APIリクエストがタイムアウトしました (APIキー {current_index_in_original_list + 1})。")
                last_exception = asyncio.TimeoutError("API request timed out.")
                retries_with_current_key = max_retries_per_key + 1
                break
            except Exception as e:
                # (その他のエラーの処理)
                if "history must begin with a user message" in str(e) or "must alternate between" in str(e):
                    log_error("AI_REQUEST_HISTORY_INVALID", f"履歴形式エラー (APIキー {current_index_in_original_list + 1}): {e}")
                    log_error("AI_REQUEST_HISTORY_INVALID", f"問題の履歴 (先頭5件): {history_list_ref[:5]}")
                    last_exception = e
                    successful_key = None
                    key_index_to_try = len(ordered_keys)
                    break
                else:
                    log_error("AI_REQUEST_ERROR", f"予期せぬエラー (APIキー {current_index_in_original_list + 1}): {type(e).__name__} - {e}")
                    log_error("AI_REQUEST_ERROR", traceback.format_exc())
                    last_exception = e
                    retries_with_current_key = max_retries_per_key + 1
                    break
        # --- 内側ループ終了 ---

        if successful_key:
            break

        if should_wait_before_next_key and wait_duration > 0:
            log_info("AI_REQUEST_RATE_LIMIT", f"{wait_duration:.1f}秒待機してから次のAPIキーを試します...")
            await asyncio.sleep(wait_duration)

        key_index_to_try += 1
    # --- 外側ループ終了 ---

    # --- 最終的な失敗処理 ---
    if successful_key is None:
        log_error("AI_REQUEST_FATAL", "すべてのAPIキーと再試行でリクエストに失敗しました。")
        if last_exception:
             log_error("AI_REQUEST_FATAL", f"最後の試行でのエラー: {type(last_exception).__name__} - {last_exception}")
        return None
    # -----------------------

    # --- 成功時の処理 ---
    response_text = response.text

    # --- リクエスト成功後に履歴を追加 (add_message_to_historyがDB対応済みに) ---
    if channel_id is not None:
        try:
            if user_message_content:
                add_message_to_history(channel_id, "user", user_message_content)
            
            if response_text:
                 add_message_to_history(channel_id, "model", response_text)
        except Exception as history_error:
            log_error("AI_REQUEST_HISTORY_ADD", f"履歴追加中に予期せぬエラーが発生しました: {type(history_error).__name__} - {history_error}")
            log_error("AI_REQUEST_HISTORY_ADD", traceback.format_exc())
    else:
        log_warning("AI_REQUEST_HISTORY_ADD", "channel_idがNoneのため、履歴は追加されません。")
    # -----------------------------

    # --- トークン数をログに出力 ---
    try:
        if hasattr(response, 'usage_metadata') and response.usage_metadata:
            prompt_token_count = response.usage_metadata.prompt_token_count
            candidates_token_count = response.usage_metadata.candidates_token_count
            total_token_count = response.usage_metadata.total_token_count
            log_info("TOKEN_COUNT", f"Prompt: {prompt_token_count}, Candidates: {candidates_token_count}, Total: {total_token_count}")
        else:
            log_info("TOKEN_COUNT", "Usage metadata not available.")
    except Exception as token_error:
        log_error("AI_REQUEST_TOKEN_LOG", f"トークン数ログ出力中にエラー: {token_error}")
    # -----------------------------

    return response_text

# --- cogs/commands.py から呼び出される関数群 (DB対応) ---

def reset_histories():
    """全ての会話履歴をリセットし、DBに保存します。"""
    log_system("全チャンネルの会話履歴をリセットします...")
    # ★ db_manager の互換関数（または直接操作）を呼び出す
    data_manager.reset_histories() 

def get_history_for_channel(channel_id: int):
    """指定チャンネルの履歴をキャッシュから取得します（!history export用）。"""
    # ★ db_manager の互換関数を呼び出す
    return data_manager.get_history_for_channel(channel_id)

def load_persona() -> bool:
    """ペルソナファイルを読み込み確認（!persona reload用）。"""
    # ★ db_manager の互換関数を呼び出す
    return data_manager.load_persona() is not None

def apply_persona_to_channel(channel_id: int):
    """指定チャンネルの履歴をペルソナで上書きし、DBに保存します。"""
    # ★ db_manager の互換関数を呼び出す
    data_manager.apply_persona_to_channel(channel_id)